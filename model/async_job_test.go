package model

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/stretchr/testify/require"
)

func asyncFixture(t *testing.T) (User, Token) {
	t.Helper()
	require.NoError(t, MigrateAsyncJobs())
	truncateTables(t)
	for _, table := range []string{"async_jobs", "async_ledgers", "async_accounts", "async_audits"} {
		require.NoError(t, DB.Exec("DELETE FROM "+table).Error)
	}
	user := User{Username: "async-test", Quota: 1000, Status: common.UserStatusEnabled}
	require.NoError(t, DB.Create(&user).Error)
	token := Token{UserId: user.Id, Key: "async-test-key", RemainQuota: 1000, Status: common.TokenStatusEnabled, ExpiredTime: -1}
	require.NoError(t, DB.Create(&token).Error)
	return user, token
}
func testAsyncJob(u User, k Token, id, idem string) *AsyncJob {
	return &AsyncJob{ID: id, IdempotencyHash: idem, RequestHash: "request", UserID: u.Id, TokenID: k.Id, Account: "account", Provider: "tripo", Model: "tripo-standard", Quota: 100}
}
func TestAsyncConcurrentIdempotency(t *testing.T) {
	u, k := asyncFixture(t)
	var wg sync.WaitGroup
	var mu sync.Mutex
	created := 0
	errs := []error{}
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			j := testAsyncJob(u, k, fmt.Sprintf("job-%d", i), "same-key")
			ok, e := CreateAsyncJob(j, 100)
			mu.Lock()
			defer mu.Unlock()
			if ok {
				created++
			}
			if e != nil {
				errs = append(errs, e)
			}
		}(i)
	}
	wg.Wait()
	require.Empty(t, errs)
	require.Equal(t, 1, created)
	require.NoError(t, DB.First(&u, u.Id).Error)
	require.Equal(t, 900, u.Quota)
	require.NoError(t, DB.First(&k, k.Id).Error)
	require.Equal(t, 900, k.RemainQuota)
	j := testAsyncJob(u, k, "different", "same-key")
	j.RequestHash = "changed"
	_, e := CreateAsyncJob(j, 100)
	require.ErrorIs(t, e, ErrAsyncConflict)
}
func TestAsyncCompetingFinalizersCannotDoubleChargeOrRefund(t *testing.T) {
	u, k := asyncFixture(t)
	j := testAsyncJob(u, k, "job-1", "key-1")
	_, e := CreateAsyncJob(j, 100)
	require.NoError(t, e)
	stale := *j
	require.NoError(t, FinishAsync(j, true, "", `{"model_url":"https://example.com/a.glb"}`, "100"))
	require.Error(t, FinishAsync(&stale, false, "timeout", "", ""))
	require.NoError(t, DB.First(&u, u.Id).Error)
	require.Equal(t, 900, u.Quota)
	require.Equal(t, 100, u.UsedQuota)
	var ledgerCount int64
	require.NoError(t, DB.Model(&AsyncLedger{}).Where("job_id = ?", j.ID).Count(&ledgerCount).Error)
	require.EqualValues(t, 2, ledgerCount)
	j2 := testAsyncJob(u, k, "job-2", "key-2")
	_, e = CreateAsyncJob(j2, 100)
	require.NoError(t, e)
	stale = *j2
	require.NoError(t, FinishAsync(j2, false, "rejected", "", ""))
	require.Error(t, FinishAsync(&stale, false, "rejected", "", ""))
	require.NoError(t, DB.First(&u, u.Id).Error)
	require.Equal(t, 900, u.Quota)
}
func TestAsyncAdmissionLimitsAndLease(t *testing.T) {
	u, k := asyncFixture(t)
	j := testAsyncJob(u, k, "job-1", "key-1")
	_, e := CreateAsyncJob(j, 1)
	require.NoError(t, e)
	_, e = CreateAsyncJob(testAsyncJob(u, k, "job-2", "key-2"), 1)
	require.ErrorIs(t, e, ErrAsyncCapacity)
	now := time.Now().Unix()
	won, e := ClaimAsyncAccount("account", "worker-a", now)
	require.NoError(t, e)
	require.True(t, won)
	won, e = ClaimAsyncAccount("account", "worker-b", now)
	require.NoError(t, e)
	require.False(t, won)
	require.NoError(t, ReleaseAsyncAccount("account", "wrong-owner"))
	won, e = ClaimAsyncAccount("account", "worker-b", now+121)
	require.NoError(t, e)
	require.True(t, won)
	require.NoError(t, TransitionAsync(j, map[string]any{"status": AsyncReconciling, "error_code": "submission_unknown"}))
	require.NoError(t, DB.First(&u, u.Id).Error)
	require.Equal(t, 900, u.Quota)
}
func TestAsyncInsufficientTokenRollsBackWallet(t *testing.T) {
	u, k := asyncFixture(t)
	require.NoError(t, DB.Model(&k).UpdateColumn("remain_quota", 10).Error)
	_, e := CreateAsyncJob(testAsyncJob(u, k, "job", "key"), 100)
	require.ErrorIs(t, e, ErrAsyncFunds)
	require.NoError(t, DB.First(&u, u.Id).Error)
	require.Equal(t, 1000, u.Quota)
}
