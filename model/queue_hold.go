package model

import (
	"context"
	"errors"
	"time"

	"github.com/QuantumNous/new-api/common"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// QueueHold makes relay pre-consumption survive an API crash. Only API processes
// settle/release these records. Unknown supplier outcomes remain held for review.
type QueueHold struct {
	Version      int64
	ID           string `gorm:"type:varchar(64);primaryKey"`
	UserID       int    `gorm:"index"`
	RequestID    string `gorm:"type:varchar(128);index"`
	TokenID      int
	Reserved     int
	Actual       int
	TokenCharged bool
	State        string `gorm:"type:varchar(24);index"`
	CreatedAt    int64
	UpdatedAt    int64
	CacheDirty   bool `gorm:"index"`
}

func CreateQueueHold(ctx context.Context, h *QueueHold) error {
	if h.ID == "" || h.Reserved < 0 {
		return errors.New("invalid model hold")
	}
	h.State = "held"
	h.Version = 1
	h.CreatedAt = time.Now().Unix()
	h.UpdatedAt = h.CreatedAt
	h.CacheDirty = true
	e := AsyncDB(ctx).Transaction(func(tx *gorm.DB) error {
		var user User
		if e := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&user, "id = ?", h.UserID).Error; e != nil {
			return e
		}
		var count int64
		if e := tx.Model(&QueueHold{}).Where("user_id = ? AND state = ?", h.UserID, "held").Count(&count).Error; e != nil {
			return e
		}
		if count >= 100 {
			return ErrAsyncCapacity
		}
		if user.Status != common.UserStatusEnabled || user.Quota < h.Reserved {
			return ErrAsyncFunds
		}
		var token Token
		if e := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&token, "id = ? AND user_id = ?", h.TokenID, h.UserID).Error; e != nil {
			return e
		}
		if token.Status != common.TokenStatusEnabled || token.ExpiredTime != -1 && token.ExpiredTime < h.CreatedAt {
			return ErrAsyncFunds
		}
		h.TokenCharged = !token.UnlimitedQuota
		if h.TokenCharged && token.RemainQuota < h.Reserved {
			return ErrAsyncFunds
		}
		if e := tx.Model(&User{}).Where("id = ?", h.UserID).UpdateColumn("quota", gorm.Expr("quota - ?", h.Reserved)).Error; e != nil {
			return e
		}
		if h.TokenCharged {
			if e := tx.Model(&Token{}).Where("id = ?", h.TokenID).UpdateColumn("remain_quota", gorm.Expr("remain_quota - ?", h.Reserved)).Error; e != nil {
				return e
			}
		}
		return tx.Create(h).Error
	})
	if e == nil {
		_ = FlushQueueHoldCache(*h)
	}
	return e
}
func AdjustQueueHold(ctx context.Context, id string, target int, finish string, audit ...QueueHoldAudit) error {
	if target < 0 || (finish != "" && finish != "settled" && finish != "released") {
		return errors.New("invalid hold adjustment")
	}
	var h QueueHold
	e := AsyncDB(ctx).Transaction(func(tx *gorm.DB) error {
		if e := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&h, "id = ?", id).Error; e != nil {
			return e
		}
		if h.State != "held" {
			if h.State == finish && h.Actual == target {
				return nil
			}
			return errors.New("hold already finalized")
		}
		if finish == "" && target <= h.Reserved {
			return nil
		}
		// A refund is valid only if every accepted execution definitively never ran
		// or returned a known HTTP error. Unknown/crashed executions require review.
		if len(audit) > 0 {
			if audit[0].ActorID <= 0 || len(audit[0].Reason) < 16 {
				return errors.New("invalid reconciliation audit")
			}
			var active int64
			if e := tx.Model(&QueueExecution{}).Where("hold_id = ? AND state IN ?", id, []string{QueueQueued, QueueRunning}).Count(&active).Error; e != nil {
				return e
			}
			if active > 0 {
				return errors.New("execution is still active")
			}
		}
		if finish == "released" && len(audit) == 0 {
			var n int64
			if e := tx.Model(&QueueExecution{}).Where("hold_id = ? AND (state NOT IN ? OR (state = ? AND (http_status < ? OR http_status >= ?)))", id, []string{QueueExpired, QueueFailed, QueueSucceeded}, QueueSucceeded, 400, 500).Count(&n).Error; e != nil {
				return e
			}
			if n > 0 {
				return errors.New("execution outcome requires reconciliation")
			}
		}
		delta := target - h.Reserved
		q := tx.Unscoped().Model(&User{}).Where("id = ?", h.UserID)
		if delta > 0 {
			q = q.Where("quota >= ?", delta)
		}
		r := q.UpdateColumn("quota", gorm.Expr("quota - ?", delta))
		if r.Error != nil {
			return r.Error
		}
		if r.RowsAffected != 1 && delta != 0 {
			return ErrAsyncFunds
		}
		if h.TokenCharged {
			q = tx.Unscoped().Model(&Token{}).Where("id = ?", h.TokenID)
			if delta > 0 {
				q = q.Where("remain_quota >= ?", delta)
			}
			r = q.UpdateColumn("remain_quota", gorm.Expr("remain_quota - ?", delta))
			if r.Error != nil {
				return r.Error
			}
			if r.RowsAffected != 1 && delta != 0 {
				return ErrAsyncFunds
			}
		}
		updates := map[string]any{"updated_at": time.Now().Unix(), "cache_dirty": true, "version": gorm.Expr("version + 1")}
		if finish == "" {
			updates["reserved"] = target
		} else {
			updates["state"] = finish
			updates["actual"] = target
			if finish == "settled" {
				if e := tx.Unscoped().Model(&Token{}).Where("id = ?", h.TokenID).UpdateColumn("used_quota", gorm.Expr("used_quota + ?", target)).Error; e != nil {
					return e
				}
			}
		}
		r = tx.Model(&QueueHold{}).Where("id = ? AND state = ? AND reserved = ?", id, "held", h.Reserved).Updates(updates)
		if r.Error != nil {
			return r.Error
		}
		if r.RowsAffected != 1 {
			return errors.New("hold changed concurrently")
		}
		if len(audit) > 0 {
			a := audit[0]
			a.HoldID = id
			a.Action = finish
			a.Actual = target
			a.CreatedAt = time.Now().Unix()
			if e := tx.Create(&a).Error; e != nil {
				return e
			}
		}
		return nil
	})
	if e == nil {
		_ = FlushQueueHoldCache(h)
	}
	return e
}
func FlushQueueHoldCache(h QueueHold) error {
	if common.RedisEnabled {
		if e := invalidateUserCache(h.UserID); e != nil {
			return e
		}
		var token Token
		if e := AsyncDB(context.Background()).Unscoped().First(&token, "id = ?", h.TokenID).Error; e != nil {
			return e
		}
		if e := cacheDeleteToken(token.Key); e != nil {
			return e
		}
	}
	return AsyncDB(context.Background()).Model(&QueueHold{}).Where("id = ? AND version = ?", h.ID, h.Version).UpdateColumn("cache_dirty", false).Error
}
func RecoverQueueHolds(ctx context.Context) error {
	var dirty []QueueHold
	if e := AsyncDB(ctx).Where("cache_dirty = ?", true).Limit(100).Find(&dirty).Error; e != nil {
		return e
	}
	for _, h := range dirty {
		if e := FlushQueueHoldCache(h); e != nil {
			return e
		}
	}
	var old []QueueHold
	if e := AsyncDB(ctx).Where("state = ? AND created_at < ?", "held", time.Now().Unix()-900).Limit(100).Find(&old).Error; e != nil {
		return e
	}
	for _, h := range old {
		var n int64
		if e := AsyncDB(ctx).Model(&QueueExecution{}).Where("hold_id = ? AND state <> ?", h.ID, QueueExpired).Count(&n).Error; e != nil {
			return e
		}
		// Any accepted/running/completed request is kept for reconciliation. Only
		// abandoned holds with no execution, or exclusively expired work, are released.
		if n == 0 {
			_ = AdjustQueueHold(ctx, h.ID, 0, "released")
		}
	}
	return nil
}

// QueueHoldAudit records privileged provider-verified billing reconciliation.
type QueueHoldAudit struct {
	ID        uint   `gorm:"primaryKey"`
	HoldID    string `gorm:"type:varchar(64);index"`
	ActorID   int
	Action    string `gorm:"type:varchar(24)"`
	Reason    string `gorm:"type:text"`
	Actual    int
	CreatedAt int64
}
