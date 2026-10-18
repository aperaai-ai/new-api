package model

import (
	"context"
	"errors"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	QueueQueued    = "queued"
	QueueRunning   = "running"
	QueueSucceeded = "succeeded"
	QueueFailed    = "failed"
	QueueUnknown   = "unknown"
	QueueExpired   = "expired"
)

// QueueExecution stores routing and execution state, not prompts or credentials.
// InputKey and OutputKey refer to encrypted objects in private object storage.
// An expired running lease is ambiguous: it must never be automatically requeued.
type QueueExecution struct {
	HTTPStatus    int
	HoldID        string `gorm:"type:varchar(64);index"`
	ID            string `gorm:"type:varchar(64);primaryKey"`
	Kind          string `gorm:"type:varchar(32);index"`
	Reference     string `gorm:"type:varchar(128)"`
	State         string `gorm:"type:varchar(24);index"`
	InputKey      string `gorm:"type:varchar(256)"`
	OutputKey     string `gorm:"type:varchar(256)"`
	StartDeadline int64  `gorm:"index"`
	Deadline      int64  `gorm:"index"`
	Owner         string `gorm:"type:varchar(64)"`
	LeaseUntil    int64  `gorm:"index"`
	CreatedAt     int64
	UpdatedAt     int64
}

// QueueOutbox is inserted atomically with the execution. SQS publication may
// repeat after a crash; ClaimQueueExecution is the execution deduplication gate.
type QueueOutbox struct {
	ID          string `gorm:"type:varchar(64);primaryKey"`
	Published   bool   `gorm:"index"`
	NextAttempt int64  `gorm:"index"`
	Attempts    int
	CreatedAt   int64
}

func CreateQueueExecution(ctx context.Context, j *QueueExecution) error {
	now := time.Now().Unix()
	if j.ID == "" || j.Kind == "" || j.InputKey == "" || j.Deadline <= now {
		return errors.New("invalid queue execution")
	}
	j.StartDeadline = min(j.Deadline, now+60)
	j.CreatedAt, j.UpdatedAt, j.State = now, now, QueueQueued
	j.Owner, j.LeaseUntil, j.OutputKey = "", 0, ""
	return AsyncDB(ctx).Transaction(func(tx *gorm.DB) error {
		if j.HoldID != "" {
			var hold QueueHold
			if e := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&hold, "id = ? AND state = ?", j.HoldID, "held").Error; e != nil {
				return e
			}
		}
		if e := tx.Create(j).Error; e != nil {
			return e
		}
		return tx.Create(&QueueOutbox{ID: j.ID, CreatedAt: now, NextAttempt: now}).Error
	})
}

func ClaimQueueExecution(ctx context.Context, id, owner string) (bool, error) {
	if owner == "" {
		return false, errors.New("missing execution owner")
	}
	now := time.Now().Unix()
	r := AsyncDB(ctx).Model(&QueueExecution{}).
		Where("id = ? AND state = ? AND deadline > ? AND start_deadline > ?", id, QueueQueued, now, now).
		Updates(map[string]any{"state": QueueRunning, "owner": owner, "lease_until": now + 120, "updated_at": now})
	return r.RowsAffected == 1, r.Error
}

func HeartbeatQueueExecution(ctx context.Context, id, owner string) (bool, error) {
	now := time.Now().Unix()
	r := AsyncDB(ctx).Model(&QueueExecution{}).
		Where("id = ? AND state = ? AND owner = ? AND lease_until > ?", id, QueueRunning, owner, now).
		Updates(map[string]any{"lease_until": now + 120, "updated_at": now})
	return r.RowsAffected == 1, r.Error
}

// CompleteQueueExecution requires a durable output object before acknowledging
// a known provider outcome. Unknown outcomes retain their hold for reconciliation.
func CompleteQueueExecution(ctx context.Context, id, owner, state, outputKey string, httpStatus ...int) (bool, error) {
	if state != QueueSucceeded && state != QueueFailed && state != QueueUnknown {
		return false, errors.New("invalid terminal execution state")
	}
	if state != QueueUnknown && outputKey == "" {
		return false, errors.New("missing durable execution result")
	}
	now := time.Now().Unix()
	status := 0
	if len(httpStatus) > 0 {
		status = httpStatus[0]
	}
	r := AsyncDB(ctx).Model(&QueueExecution{}).
		Where("id = ? AND state = ? AND owner = ? AND lease_until > ?", id, QueueRunning, owner, now).
		Updates(map[string]any{"http_status": status, "state": state, "output_key": outputKey, "updated_at": now, "lease_until": 0})
	return r.RowsAffected == 1, r.Error
}

// ReapQueueExecutions distinguishes work that never started from an uncertain
// provider call. SQS redelivery cannot turn an unknown call into a second charge.
func ReapQueueExecutions(ctx context.Context) error {
	now := time.Now().Unix()
	return AsyncDB(ctx).Transaction(func(tx *gorm.DB) error {
		if e := tx.Model(&QueueExecution{}).Where("state = ? AND (deadline <= ? OR start_deadline <= ?)", QueueQueued, now, now).
			Updates(map[string]any{"state": QueueExpired, "updated_at": now}).Error; e != nil {
			return e
		}
		return tx.Model(&QueueExecution{}).Where("state = ? AND lease_until <= ?", QueueRunning, now).
			Updates(map[string]any{"state": QueueUnknown, "updated_at": now}).Error
	})
}
