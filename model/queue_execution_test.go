package model

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func queueFixture(t *testing.T, id string) *QueueExecution {
	t.Helper()
	require.NoError(t, DB.AutoMigrate(&QueueExecution{}, &QueueOutbox{}))
	j := &QueueExecution{ID: id, Kind: "http", InputKey: "encrypted-input/" + id, Deadline: time.Now().Unix() + 300}
	require.NoError(t, CreateQueueExecution(context.Background(), j))
	t.Cleanup(func() { DB.Delete(&QueueOutbox{}, "id = ?", id); DB.Delete(&QueueExecution{}, "id = ?", id) })
	return j
}
func TestQueueDuplicateDeliveryAndOwnerFence(t *testing.T) {
	ctx := context.Background()
	j := queueFixture(t, "queue-duplicate")
	var outbox QueueOutbox
	require.NoError(t, DB.First(&outbox, "id = ?", j.ID).Error)
	require.False(t, outbox.Published)
	won, e := ClaimQueueExecution(ctx, j.ID, "worker-a")
	require.NoError(t, e)
	require.True(t, won)
	won, e = ClaimQueueExecution(ctx, j.ID, "worker-b")
	require.NoError(t, e)
	require.False(t, won)
	won, e = CompleteQueueExecution(ctx, j.ID, "worker-b", QueueSucceeded, "output")
	require.NoError(t, e)
	require.False(t, won)
	won, e = CompleteQueueExecution(ctx, j.ID, "worker-a", QueueSucceeded, "")
	require.Error(t, e)
	require.False(t, won)
	won, e = CompleteQueueExecution(ctx, j.ID, "worker-a", QueueSucceeded, "encrypted-output")
	require.NoError(t, e)
	require.True(t, won)
	won, e = ClaimQueueExecution(ctx, j.ID, "worker-b")
	require.NoError(t, e)
	require.False(t, won)
}
func TestQueueLostWorkerNeverReexecutes(t *testing.T) {
	ctx := context.Background()
	j := queueFixture(t, "queue-lost")
	won, e := ClaimQueueExecution(ctx, j.ID, "worker-a")
	require.NoError(t, e)
	require.True(t, won)
	require.NoError(t, DB.Model(j).UpdateColumn("lease_until", time.Now().Unix()-1).Error)
	won, e = HeartbeatQueueExecution(ctx, j.ID, "worker-a")
	require.NoError(t, e)
	require.False(t, won)
	require.NoError(t, ReapQueueExecutions(ctx))
	require.NoError(t, DB.First(j, "id = ?", j.ID).Error)
	require.Equal(t, QueueUnknown, j.State)
	won, e = ClaimQueueExecution(ctx, j.ID, "worker-b")
	require.NoError(t, e)
	require.False(t, won)
	won, e = CompleteQueueExecution(ctx, j.ID, "worker-a", QueueSucceeded, "output")
	require.NoError(t, e)
	require.False(t, won)
}
func TestQueueUnstartedDeadline(t *testing.T) {
	ctx := context.Background()
	j := queueFixture(t, "queue-expired")
	require.NoError(t, DB.Model(j).UpdateColumn("deadline", time.Now().Unix()-1).Error)
	won, e := ClaimQueueExecution(ctx, j.ID, "worker-a")
	require.NoError(t, e)
	require.False(t, won)
	require.NoError(t, ReapQueueExecutions(ctx))
	require.NoError(t, DB.First(j, "id = ?", j.ID).Error)
	require.Equal(t, QueueExpired, j.State)
}
