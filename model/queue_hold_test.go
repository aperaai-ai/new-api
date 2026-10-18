package model

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func holdFixture(t *testing.T, id string) (User, Token, *QueueHold) {
	t.Helper()
	u, k := asyncFixture(t)
	require.NoError(t, DB.AutoMigrate(&QueueHoldAudit{}, &QueueHold{}, &QueueExecution{}, &QueueOutbox{}))
	h := &QueueHold{ID: id, UserID: u.Id, TokenID: k.Id, Reserved: 100}
	require.NoError(t, CreateQueueHold(context.Background(), h))
	t.Cleanup(func() {
		DB.Delete(&QueueOutbox{}, "id LIKE ?", id+"%")
		DB.Delete(&QueueExecution{}, "hold_id = ?", id)
		DB.Delete(&QueueHoldAudit{}, "hold_id = ?", id)
		DB.Delete(&QueueHold{}, "id = ?", id)
	})
	return u, k, h
}
func TestQueueHoldSettlementIsAtomicAndIdempotent(t *testing.T) {
	u, k, h := holdFixture(t, "hold-settle")
	ctx := context.Background()
	require.NoError(t, AdjustQueueHold(ctx, h.ID, 150, ""))
	require.NoError(t, AdjustQueueHold(ctx, h.ID, 125, "settled"))
	require.NoError(t, AdjustQueueHold(ctx, h.ID, 125, "settled"))
	require.Error(t, AdjustQueueHold(ctx, h.ID, 0, "released"))
	require.NoError(t, DB.First(&u, u.Id).Error)
	require.NoError(t, DB.First(&k, k.Id).Error)
	require.Equal(t, 875, u.Quota)
	require.Equal(t, 875, k.RemainQuota)
	require.Equal(t, 125, k.UsedQuota)
}
func TestQueueHoldUnknownExecutionCannotRefund(t *testing.T) {
	u, _, h := holdFixture(t, "hold-unknown")
	ctx := context.Background()
	j := &QueueExecution{ID: h.ID + "-exec", HoldID: h.ID, Kind: "http", InputKey: "encrypted", Deadline: time.Now().Unix() + 300}
	require.NoError(t, CreateQueueExecution(ctx, j))
	won, e := ClaimQueueExecution(ctx, j.ID, "worker")
	require.NoError(t, e)
	require.True(t, won)
	won, e = CompleteQueueExecution(ctx, j.ID, "worker", QueueUnknown, "")
	require.NoError(t, e)
	require.True(t, won)
	require.Error(t, AdjustQueueHold(ctx, h.ID, 0, "released"))
	require.NoError(t, DB.First(&u, u.Id).Error)
	require.Equal(t, 900, u.Quota)
}
func TestQueueHoldFailedEnqueueCanRecoverAndCannotLaterDispatch(t *testing.T) {
	u, _, h := holdFixture(t, "hold-orphan")
	ctx := context.Background()
	require.NoError(t, DB.Model(h).UpdateColumn("created_at", time.Now().Unix()-1000).Error)
	require.NoError(t, RecoverQueueHolds(ctx))
	require.NoError(t, DB.First(&u, u.Id).Error)
	require.Equal(t, 1000, u.Quota)
	j := &QueueExecution{ID: h.ID + "-exec", HoldID: h.ID, Kind: "http", InputKey: "encrypted", Deadline: time.Now().Unix() + 300}
	require.Error(t, CreateQueueExecution(ctx, j))
	var n int64
	require.NoError(t, DB.Model(&QueueOutbox{}).Where("id = ?", j.ID).Count(&n).Error)
	require.Zero(t, n)
}
func TestQueueHoldInsufficientExtraFundsRollsBack(t *testing.T) {
	u, k, h := holdFixture(t, "hold-insufficient")
	ctx := context.Background()
	require.NoError(t, DB.Model(&k).UpdateColumn("remain_quota", 0).Error)
	require.Error(t, AdjustQueueHold(ctx, h.ID, 200, ""))
	require.NoError(t, DB.First(&u, u.Id).Error)
	require.Equal(t, 900, u.Quota)
	require.NoError(t, DB.First(h, "id = ?", h.ID).Error)
	require.Equal(t, 100, h.Reserved)
}

func TestQueueHoldSuccessfulSupplierCallCannotBeBlindlyRefunded(t *testing.T) {
	u, _, h := holdFixture(t, "hold-success-review")
	ctx := context.Background()
	j := &QueueExecution{ID: h.ID + "-exec", HoldID: h.ID, Kind: "http", InputKey: "encrypted", Deadline: time.Now().Unix() + 300}
	require.NoError(t, CreateQueueExecution(ctx, j))
	won, e := ClaimQueueExecution(ctx, j.ID, "worker")
	require.NoError(t, e)
	require.True(t, won)
	won, e = CompleteQueueExecution(ctx, j.ID, "worker", QueueSucceeded, "encrypted-output", 200)
	require.NoError(t, e)
	require.True(t, won)
	require.Error(t, AdjustQueueHold(ctx, h.ID, 0, "released"))
	require.NoError(t, DB.First(&u, u.Id).Error)
	require.Equal(t, 900, u.Quota)
}

func TestQueueHoldProviderVerifiedReconciliationIsAuditedOnce(t *testing.T) {
	u, _, h := holdFixture(t, "hold-reviewed")
	ctx := context.Background()
	j := &QueueExecution{ID: h.ID + "-exec", HoldID: h.ID, Kind: "http", InputKey: "encrypted", Deadline: time.Now().Unix() + 300}
	require.NoError(t, CreateQueueExecution(ctx, j))
	won, e := ClaimQueueExecution(ctx, j.ID, "worker")
	require.NoError(t, e)
	require.True(t, won)
	audit := QueueHoldAudit{ActorID: 99, Reason: "Provider billing verified no request was charged"}
	require.Error(t, AdjustQueueHold(ctx, h.ID, 0, "released", audit)) // A running supplier call cannot be overridden.
	won, e = CompleteQueueExecution(ctx, j.ID, "worker", QueueUnknown, "")
	require.NoError(t, e)
	require.True(t, won)
	require.NoError(t, AdjustQueueHold(ctx, h.ID, 0, "released", audit))
	require.NoError(t, AdjustQueueHold(ctx, h.ID, 0, "released", audit))
	require.NoError(t, DB.First(&u, u.Id).Error)
	require.Equal(t, 1000, u.Quota)
	var count int64
	require.NoError(t, DB.Model(&QueueHoldAudit{}).Where("hold_id = ?", h.ID).Count(&count).Error)
	require.EqualValues(t, 1, count)
}
