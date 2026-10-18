package model

import (
	"context"
	"errors"
	"time"

	"gorm.io/gorm"
)

// AsyncResultEvent is the durable handoff from execution to API-owned billing.
// The worker has no permission to modify wallet, token or ledger tables.
type AsyncResultEvent struct {
	JobID     string `gorm:"type:varchar(64);primaryKey"`
	Success   bool
	Code      string `gorm:"type:varchar(64)"`
	Output    string `gorm:"type:text"`
	Credits   string `gorm:"type:varchar(64)"`
	Processed bool   `gorm:"index"`
	CreatedAt int64
}

func RecordAsyncResult(j *AsyncJob, success bool, code, output, credits string) error {
	return AsyncDB(context.Background()).Transaction(func(tx *gorm.DB) error {
		state := AsyncFailed
		if success {
			state = AsyncSucceeded
		}
		r := tx.Model(&AsyncJob{}).Where("id = ? AND version = ? AND billing = ? AND execution_done = ?", j.ID, j.Version, "held", false).Updates(map[string]any{"execution_done": true, "status": state, "output": output, "error_code": code, "credits": credits, "progress": 100, "version": gorm.Expr("version + 1"), "updated_at": time.Now().Unix()})
		if r.Error != nil {
			return r.Error
		}
		if r.RowsAffected != 1 {
			return errors.New("task result already recorded or task changed")
		}
		return tx.Create(&AsyncResultEvent{JobID: j.ID, Success: success, Code: code, Output: output, Credits: credits, CreatedAt: time.Now().Unix()}).Error
	})
}
func SettleAsyncResults(ctx context.Context) error {
	var events []AsyncResultEvent
	if e := AsyncDB(ctx).Where("processed = ?", false).Order("created_at ASC").Limit(100).Find(&events).Error; e != nil {
		return e
	}
	for _, event := range events {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		var j AsyncJob
		if e := AsyncDB(ctx).First(&j, "id = ?", event.JobID).Error; e != nil {
			return e
		}
		if j.Billing != "held" {
			if e := AsyncDB(ctx).Model(&AsyncResultEvent{}).Where("job_id = ?", j.ID).UpdateColumn("processed", true).Error; e != nil {
				return e
			}
			continue
		}
		if e := FinishAsync(&j, event.Success, event.Code, event.Output, event.Credits); e != nil {
			var latest AsyncJob
			if read := AsyncDB(ctx).First(&latest, "id = ?", j.ID).Error; read != nil || latest.Billing == "held" {
				return e
			}
		}
	}
	return nil
}
