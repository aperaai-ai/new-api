package model

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/QuantumNous/new-api/common"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
)

const (
	AsyncQueued      = "queued"
	AsyncSubmitting  = "submitting"
	AsyncSubmitted   = "submitted"
	AsyncRunning     = "running"
	AsyncReconciling = "reconciling"
	AsyncSucceeded   = "succeeded"
	AsyncFailed      = "failed"
)

var ErrAsyncConflict = errors.New("idempotency key already used for different parameters")
var ErrAsyncFunds = errors.New("insufficient wallet or token quota")
var ErrAsyncCapacity = errors.New("task queue capacity reached")

// AsyncJob is separate from legacy video tasks: query failures must never trigger a refund.
// Monetary values are integral platform quota units, snapshotted at acceptance.
type AsyncJob struct {
	ID              string `gorm:"type:varchar(64);primaryKey" json:"id"`
	IdempotencyHash string `gorm:"type:varchar(64);uniqueIndex" json:"-"`
	RequestHash     string `gorm:"type:varchar(64)" json:"-"`
	UserID          int    `gorm:"index:idx_async_owner" json:"-"`
	TokenID         int    `json:"-"`
	Account         string `gorm:"type:varchar(64);index:idx_async_due,priority:1" json:"-"`
	Provider        string `gorm:"type:varchar(32)" json:"-"`
	Model           string `gorm:"type:varchar(128)" json:"model"`
	Kind            string `gorm:"type:varchar(32)" json:"type"`
	RequestBody     string `gorm:"type:text" json:"-"`
	Credential      string `gorm:"type:text" json:"-"`
	CredentialHash  string `gorm:"type:varchar(64)" json:"-"`
	UpstreamID      string `gorm:"type:varchar(191);index" json:"-"`
	Status          string `gorm:"type:varchar(32);index:idx_async_due,priority:2;index:idx_async_state,priority:1" json:"status"`
	Billing         string `gorm:"type:varchar(16);index:idx_async_state,priority:2" json:"billing_status"`
	Quota           int    `json:"quota"`
	TokenCharged    bool   `json:"-"`
	Progress        int    `json:"progress"`
	Output          string `gorm:"type:text" json:"-"`
	Credits         string `gorm:"type:varchar(64)" json:"-"`
	ErrorCode       string `gorm:"type:varchar(64)" json:"error_code,omitempty"`
	NextAttempt     int64  `gorm:"index:idx_async_due,priority:3" json:"-"`
	SubmittedAt     int64  `json:"-"`
	CreatedAt       int64  `gorm:"index:idx_async_owner" json:"created_at"`
	UpdatedAt       int64  `json:"updated_at"`
	Version         int64  `json:"-"`
	CacheDirty      bool   `gorm:"index" json:"-"`
	Attempts        int    `json:"-"`
}
type AsyncLedger struct {
	ID        uint   `gorm:"primaryKey"`
	JobID     string `gorm:"type:varchar(64);uniqueIndex:idx_async_ledger"`
	Event     string `gorm:"type:varchar(16);uniqueIndex:idx_async_ledger"`
	UserID    int    `gorm:"index"`
	Quota     int
	CreatedAt int64
}
type AsyncAccount struct {
	ID         string `gorm:"type:varchar(64);primaryKey"`
	LeaseOwner string `gorm:"type:varchar(64)"`
	LeaseUntil int64
	Revision   int64
}

func MigrateAsyncJobs() error {
	return AsyncDB(context.Background()).AutoMigrate(&AsyncJob{}, &AsyncLedger{}, &AsyncAccount{}, &AsyncAudit{})
}
func AsyncFindIdempotency(hash, requestHash string) (*AsyncJob, error) {
	var j AsyncJob
	e := AsyncDB(context.Background()).Where("idempotency_hash = ?", hash).First(&j).Error
	if e == nil && j.RequestHash != requestHash {
		return nil, ErrAsyncConflict
	}
	return &j, e
}

// Every admission serializes through the account row, bounding aggregate backlog
// across API replicas. Balance changes and the durable ledger commit together.
func CreateAsyncJob(j *AsyncJob, maxQueued int) (bool, error) {
	if j.Quota < 1 || j.UserID < 1 || j.TokenID < 1 || j.ID == "" || j.IdempotencyHash == "" || j.Account == "" || maxQueued < 1 {
		return false, fmt.Errorf("invalid task admission")
	}
	if existing, e := AsyncFindIdempotency(j.IdempotencyHash, j.RequestHash); e == nil {
		*j = *existing
		return false, nil
	} else if !errors.Is(e, gorm.ErrRecordNotFound) {
		return false, e
	}
	now := time.Now().Unix()
	j.CreatedAt = now
	j.UpdatedAt = now
	j.NextAttempt = now
	j.Status = AsyncQueued
	j.Billing = "held"
	j.Version = 1
	j.CacheDirty = true
	e := AsyncDB(context.Background()).Transaction(func(tx *gorm.DB) error {
		if e := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&AsyncAccount{ID: j.Account}).Error; e != nil {
			return e
		}
		if e := tx.Model(&AsyncAccount{}).Where("id = ?", j.Account).UpdateColumn("revision", gorm.Expr("revision + 1")).Error; e != nil {
			return e
		}
		// Lock the user before the first consistent count read. This also
		// serializes the per-user backlog limit across different accounts.
		var owner User
		if e := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ?", j.UserID).First(&owner).Error; e != nil {
			return e
		}
		var count int64
		if e := tx.Model(&AsyncJob{}).Where("account = ? AND billing = ?", j.Account, "held").Count(&count).Error; e != nil {
			return e
		}
		if count >= int64(maxQueued) {
			return ErrAsyncCapacity
		}
		if e := tx.Model(&AsyncJob{}).Where("user_id = ? AND billing = ?", j.UserID, "held").Count(&count).Error; e != nil {
			return e
		}
		if count >= 20 {
			return ErrAsyncCapacity
		}
		r := tx.Model(&User{}).Where("id = ? AND status = ? AND quota >= ?", j.UserID, common.UserStatusEnabled, j.Quota).UpdateColumn("quota", gorm.Expr("quota - ?", j.Quota))
		if r.Error != nil {
			return r.Error
		}
		if r.RowsAffected != 1 {
			return ErrAsyncFunds
		}
		var token Token
		if e := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ? AND user_id = ?", j.TokenID, j.UserID).First(&token).Error; e != nil {
			return e
		}
		if token.Status != common.TokenStatusEnabled || token.ExpiredTime != -1 && token.ExpiredTime < now {
			return ErrAsyncFunds
		}
		j.TokenCharged = !token.UnlimitedQuota
		q := tx.Model(&Token{}).Where("id = ? AND status = ?", token.Id, common.TokenStatusEnabled)
		if j.TokenCharged {
			q = q.Where("remain_quota >= ?", j.Quota)
			r = q.UpdateColumn("remain_quota", gorm.Expr("remain_quota - ?", j.Quota))
		} else {
			r = &gorm.DB{RowsAffected: 1}
		}
		if r.Error != nil {
			return r.Error
		}
		if r.RowsAffected != 1 {
			return ErrAsyncFunds
		}
		if e := tx.Create(j).Error; e != nil {
			return e
		}
		return tx.Create(&AsyncLedger{JobID: j.ID, UserID: j.UserID, Event: "hold", Quota: j.Quota, CreatedAt: now}).Error
	})
	if e != nil {
		if old, lookup := AsyncFindIdempotency(j.IdempotencyHash, j.RequestHash); lookup == nil {
			*j = *old
			return false, nil
		} else if errors.Is(lookup, ErrAsyncConflict) {
			return false, lookup
		}
		return false, e
	}
	_ = FlushAsyncCache(*j)
	return true, nil
}
func FlushAsyncCache(j AsyncJob) error {
	if common.RedisEnabled {
		if e := invalidateUserCache(j.UserID); e != nil {
			return e
		}
		var token Token
		if e := AsyncDB(context.Background()).Unscoped().Where("id = ?", j.TokenID).First(&token).Error; e != nil {
			return e
		}
		if e := cacheDeleteToken(token.Key); e != nil {
			return e
		}
	}
	return AsyncDB(context.Background()).Model(&AsyncJob{}).Where("id = ? AND version = ?", j.ID, j.Version).UpdateColumn("cache_dirty", false).Error
}
func FlushAsyncCaches() error {
	var jobs []AsyncJob
	if e := AsyncDB(context.Background()).Where("cache_dirty = ?", true).Limit(100).Find(&jobs).Error; e != nil {
		return e
	}
	for _, j := range jobs {
		if e := FlushAsyncCache(j); e != nil {
			return e
		}
	}
	return nil
}
func ClaimAsyncAccount(id, owner string, now int64) (bool, error) {
	if e := AsyncDB(context.Background()).Clauses(clause.OnConflict{DoNothing: true}).Create(&AsyncAccount{ID: id}).Error; e != nil {
		return false, e
	}
	r := AsyncDB(context.Background()).Model(&AsyncAccount{}).Where("id = ? AND lease_until < ?", id, now).Updates(map[string]any{"lease_owner": owner, "lease_until": now + 120})
	return r.RowsAffected == 1, r.Error
}
func ReleaseAsyncAccount(id, owner string) error {
	return AsyncDB(context.Background()).Model(&AsyncAccount{}).Where("id = ? AND lease_owner = ?", id, owner).Updates(map[string]any{"lease_until": 0, "lease_owner": ""}).Error
}
func TransitionAsync(j *AsyncJob, values map[string]any) error {
	values["version"] = gorm.Expr("version + 1")
	values["updated_at"] = time.Now().Unix()
	r := AsyncDB(context.Background()).Model(&AsyncJob{}).Where("id = ? AND version = ? AND billing = ?", j.ID, j.Version, "held").Updates(values)
	if r.Error != nil {
		return r.Error
	}
	if r.RowsAffected != 1 {
		return fmt.Errorf("task transition lost ownership")
	}
	j.Version++
	return nil
}

// FinishAsync commits the terminal state and money exactly once. A stale worker
// loses the CAS before any financial mutation; SQL errors roll everything back.
func FinishAsync(j *AsyncJob, success bool, code, output, credits string, audit ...AsyncAudit) error {
	e := AsyncDB(context.Background()).Transaction(func(tx *gorm.DB) error {
		status, billing, event := AsyncFailed, "released", "release"
		if success {
			status, billing, event = AsyncSucceeded, "settled", "settle"
		}
		now := time.Now().Unix()
		r := tx.Model(&AsyncJob{}).Where("id = ? AND version = ? AND billing = ?", j.ID, j.Version, "held").Updates(map[string]any{"status": status, "billing": billing, "error_code": code, "output": output, "credits": credits, "progress": 100, "updated_at": now, "version": gorm.Expr("version + 1"), "cache_dirty": true})
		if r.Error != nil {
			return r.Error
		}
		if r.RowsAffected != 1 {
			return fmt.Errorf("task already transitioned")
		}
		if success {
			if e := tx.Model(&User{}).Where("id = ?", j.UserID).Updates(map[string]any{"used_quota": gorm.Expr("used_quota + ?", j.Quota), "request_count": gorm.Expr("request_count + 1")}).Error; e != nil {
				return e
			}
			if e := tx.Unscoped().Model(&Token{}).Where("id = ?", j.TokenID).UpdateColumn("used_quota", gorm.Expr("used_quota + ?", j.Quota)).Error; e != nil {
				return e
			}
			if e := tx.Create(&Log{UserId: j.UserID, TokenId: j.TokenID, CreatedAt: now, Type: LogTypeConsume, ModelName: j.Model, Quota: j.Quota, RequestId: j.ID, UpstreamRequestId: j.UpstreamID, Content: "asynchronous generation completed"}).Error; e != nil {
				return e
			}
		} else {
			if e := tx.Unscoped().Model(&User{}).Where("id = ?", j.UserID).UpdateColumn("quota", gorm.Expr("quota + ?", j.Quota)).Error; e != nil {
				return e
			}
			if j.TokenCharged {
				if e := tx.Unscoped().Model(&Token{}).Where("id = ?", j.TokenID).UpdateColumn("remain_quota", gorm.Expr("remain_quota + ?", j.Quota)).Error; e != nil {
					return e
				}
			}
		}
		if !success {
			if e := tx.Create(&Log{UserId: j.UserID, TokenId: j.TokenID, CreatedAt: now, Type: LogTypeRefund, ModelName: j.Model, Quota: j.Quota, RequestId: j.ID, Content: "asynchronous generation hold released: " + code}).Error; e != nil {
				return e
			}
		}
		if len(audit) > 0 {
			if e := tx.Create(&audit[0]).Error; e != nil {
				return e
			}
		}
		return tx.Create(&AsyncLedger{JobID: j.ID, UserID: j.UserID, Event: event, Quota: j.Quota, CreatedAt: now}).Error
	})
	if e == nil {
		j.Version++
		_ = FlushAsyncCache(*j)
	}
	return e
}

// RunAperaMigrations is only called by the dedicated migration executable.
func RunAperaMigrations() error { return migrateDB() }

// AsyncAudit records privileged reconciliation without provider secrets.
type AsyncAudit struct {
	ID        uint   `gorm:"primaryKey"`
	JobID     string `gorm:"type:varchar(64);index"`
	ActorID   int
	Action    string `gorm:"type:varchar(32)"`
	Reason    string `gorm:"type:text"`
	CreatedAt int64
}

func AttachAsyncUpstream(j AsyncJob, upstreamID string, audit AsyncAudit) error {
	return AsyncDB(context.Background()).Transaction(func(tx *gorm.DB) error {
		if e := tx.Model(&AsyncAccount{}).Where("id = ?", j.Account).UpdateColumn("revision", gorm.Expr("revision + 1")).Error; e != nil {
			return e
		}
		var count int64
		if e := tx.Model(&AsyncJob{}).Where("provider = ? AND upstream_id = ?", j.Provider, upstreamID).Count(&count).Error; e != nil {
			return e
		}
		if count != 0 {
			return fmt.Errorf("upstream task already linked")
		}
		r := tx.Model(&AsyncJob{}).Where("id = ? AND version = ? AND status = ? AND upstream_id = ? AND billing = ?", j.ID, j.Version, AsyncReconciling, "", "held").Updates(map[string]any{"upstream_id": upstreamID, "status": AsyncSubmitted, "error_code": "", "next_attempt": time.Now().Unix(), "updated_at": time.Now().Unix(), "version": gorm.Expr("version + 1")})
		if r.Error != nil {
			return r.Error
		}
		if r.RowsAffected != 1 {
			return fmt.Errorf("job no longer eligible for reconciliation")
		}
		return tx.Create(&audit).Error
	})
}

// AsyncDB deliberately suppresses SQL statement logging, which can otherwise
// include API keys, prompts and credential snapshots on database errors.
func AsyncDB(ctx context.Context) *gorm.DB {
	return DB.WithContext(ctx).Session(&gorm.Session{Logger: logger.Default.LogMode(logger.Silent)})
}
