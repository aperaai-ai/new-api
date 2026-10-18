package service

import (
	"context"
	"testing"
	"time"

	"github.com/QuantumNous/new-api/model"
	"github.com/QuantumNous/new-api/pkg/asyncprovider"
	"github.com/stretchr/testify/require"
)

func asyncSeed(t *testing.T) *model.AsyncJob {
	t.Helper()
	truncate(t)
	require.NoError(t, model.MigrateAsyncJobs())
	for _, table := range []string{"async_jobs", "async_accounts", "async_ledgers", "async_audits"} {
		require.NoError(t, model.DB.Exec("DELETE FROM "+table).Error)
	}
	seedUser(t, 501, 1000)
	seedToken(t, 601, 501, "async-key", 1000)
	job := &model.AsyncJob{ID: "async-service-test", UserID: 501, TokenID: 601, Account: "test-account", Provider: "tripo", Model: "m", Kind: "text_to_model", Quota: 100, IdempotencyHash: "service-key", RequestHash: "body"}
	_, e := model.CreateAsyncJob(job, 100)
	require.NoError(t, e)
	return job
}
func TestAsyncPollingUnknownStateNeverRefunds(t *testing.T) {
	job := asyncSeed(t)
	result := asyncprovider.Result{ID: "task_1", Status: "expired"}
	require.NoError(t, ApplyAsyncResult(job, result, time.Now().Unix()))
	var stored model.AsyncJob
	require.NoError(t, model.DB.First(&stored, "id = ?", job.ID).Error)
	require.Equal(t, model.AsyncReconciling, stored.Status)
	require.Equal(t, "held", stored.Billing)
	var user model.User
	require.NoError(t, model.DB.First(&user, 501).Error)
	require.Equal(t, 900, user.Quota)
	result.Status = "success"
	result.Output.ModelURL = "https://cdn.example.com/model.glb"
	require.NoError(t, ApplyAsyncResult(&stored, result, time.Now().Unix()))
	require.NoError(t, model.DB.First(&user, 501).Error)
	require.Equal(t, 900, user.Quota)
	require.Equal(t, 100, user.UsedQuota)
}
func TestAsyncCrashInSubmitDoesNotRecreateTask(t *testing.T) {
	job := asyncSeed(t)
	require.NoError(t, model.TransitionAsync(job, map[string]any{"status": model.AsyncSubmitting}))
	require.NoError(t, model.DB.Model(&model.AsyncJob{}).Where("id = ?", job.ID).UpdateColumn("updated_at", time.Now().Unix()-121).Error)
	won, e := model.ClaimAsyncAccount(job.Account, "new-worker", time.Now().Unix())
	require.NoError(t, e)
	require.True(t, won)
	// MaxRunning=1 and the unknown task conservatively occupies the slot. No
	// credential/rate limiter/network path is reached, even after worker restart.
	require.NoError(t, runAsyncAccount(context.Background(), job.Account, "new-worker", AsyncAccountConfig{MaxRunning: 1}))
	var stored model.AsyncJob
	require.NoError(t, model.DB.First(&stored, "id = ?", job.ID).Error)
	require.Equal(t, model.AsyncReconciling, stored.Status)
	require.Equal(t, "submission_unknown", stored.ErrorCode)
	require.Empty(t, stored.UpstreamID)
	require.Equal(t, 0, stored.Attempts)
}
func TestAsyncLocalQueueTimeoutRefundsEvenWithFullAccount(t *testing.T) {
	job := asyncSeed(t)
	require.NoError(t, model.DB.Model(&model.AsyncJob{}).Where("id = ?", job.ID).UpdateColumn("created_at", time.Now().Unix()-1801).Error)
	won, e := model.ClaimAsyncAccount(job.Account, "worker", time.Now().Unix())
	require.NoError(t, e)
	require.True(t, won)
	require.NoError(t, runAsyncAccount(context.Background(), job.Account, "worker", AsyncAccountConfig{MaxRunning: 1}))
	var user model.User
	require.NoError(t, model.DB.First(&user, 501).Error)
	require.Equal(t, 1000, user.Quota)
}
