package model

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// Opt-in real PostgreSQL test; no existing schema/data is touched. All objects
// live in a fresh schema that is dropped on completion.
func TestAsyncPostgresConcurrency(t *testing.T) {
	dsn := os.Getenv("APERA_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set APERA_TEST_POSTGRES_DSN for real PostgreSQL concurrency validation")
	}
	admin, e := gorm.Open(postgres.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, e)
	schema := fmt.Sprintf("apera_async_test_%d", time.Now().UnixNano())
	require.NoError(t, admin.Exec("CREATE SCHEMA "+schema).Error)
	defer admin.Exec("DROP SCHEMA " + schema + " CASCADE")
	u, e := url.Parse(dsn)
	require.NoError(t, e)
	q := u.Query()
	q.Set("options", "-c search_path="+schema)
	u.RawQuery = q.Encode()
	db, e := gorm.Open(postgres.Open(u.String()), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, e)
	sqlDB, e := db.DB()
	require.NoError(t, e)
	sqlDB.SetMaxOpenConns(12)
	defer sqlDB.Close()
	saved := DB
	DB = db
	defer func() { DB = saved; common.UsingPostgreSQL = false; common.UsingSQLite = true; initCol() }()
	common.UsingSQLite = false
	common.UsingPostgreSQL = true
	initCol()
	require.NoError(t, RunAperaMigrations())
	user := User{Username: "pg-async", Status: common.UserStatusEnabled, Quota: 1000}
	require.NoError(t, DB.Create(&user).Error)
	token := Token{UserId: user.Id, Key: "pg-async-key", RemainQuota: 1000, Status: common.TokenStatusEnabled, ExpiredTime: -1}
	require.NoError(t, DB.Create(&token).Error)
	var wg sync.WaitGroup
	var mu sync.Mutex
	created := 0
	errs := []error{}
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			j := testAsyncJob(user, token, fmt.Sprintf("pg-job-%d", i), "same-pg-key")
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
	require.NoError(t, DB.First(&user, user.Id).Error)
	require.Equal(t, 900, user.Quota)
	var job AsyncJob
	require.NoError(t, DB.Where("idempotency_hash = ?", "same-pg-key").First(&job).Error)
	successes := 0
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			copy := job
			e := FinishAsync(&copy, false, "test_failure", "", "")
			mu.Lock()
			defer mu.Unlock()
			if e == nil {
				successes++
			}
		}()
	}
	wg.Wait()
	require.Equal(t, 1, successes)
	require.NoError(t, DB.First(&user, user.Id).Error)
	require.Equal(t, 1000, user.Quota)
	created = 0
	errs = nil
	for i := 0; i < 25; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			j := testAsyncJob(user, token, fmt.Sprintf("bounded-%d", i), fmt.Sprintf("bounded-key-%d", i))
			j.Account = fmt.Sprintf("account-%d", i%2)
			j.Quota = 1
			ok, e := CreateAsyncJob(j, 100)
			mu.Lock()
			defer mu.Unlock()
			if ok {
				created++
			}
			if e != nil && !errors.Is(e, ErrAsyncCapacity) {
				errs = append(errs, e)
			}
		}(i)
	}
	wg.Wait()
	require.Empty(t, errs)
	require.Equal(t, 20, created)
	require.NoError(t, DB.First(&user, user.Id).Error)
	require.Equal(t, 980, user.Quota)

}
