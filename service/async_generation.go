package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/model"
	"github.com/QuantumNous/new-api/pkg/asyncprovider"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

type AsyncAccountConfig struct {
	Provider   string `json:"provider"`
	Key        string `json:"key"`
	MaxRunning int    `json:"max_running"`
	MaxQueued  int    `json:"max_queued"`
	SubmitRPM  int    `json:"submit_rpm"`
	PollRPM    int    `json:"poll_rpm"`
}
type AsyncModelConfig struct {
	Account string   `json:"account"`
	Version string   `json:"version"`
	Quota   int      `json:"quota"`
	Groups  []string `json:"groups"`
}
type AsyncConfig struct {
	Enabled  bool                          `json:"enabled"`
	Accounts map[string]AsyncAccountConfig `json:"accounts"`
	Models   map[string]AsyncModelConfig   `json:"models"`
}

var ErrAsyncRequest = errors.New("invalid async generation request")

var AsyncSettings AsyncConfig

func LoadAsyncConfig() error {
	raw := os.Getenv("ASYNC_PROVIDERS_JSON")
	if raw == "" {
		AsyncSettings = AsyncConfig{}
		return nil
	}
	var c AsyncConfig
	if common.Unmarshal([]byte(raw), &c) != nil {
		return fmt.Errorf("invalid ASYNC_PROVIDERS_JSON")
	}
	if c.Enabled {
		if os.Getenv("BATCH_UPDATE_ENABLED") == "true" || os.Getenv("LOG_SQL_DSN") != "" {
			return fmt.Errorf("async billing requires BATCH_UPDATE_ENABLED=false and a shared log database")
		}
		if len(common.CryptoSecret) < 32 || (os.Getenv("CRYPTO_SECRET") == "" && len(os.Getenv("SESSION_SECRET")) < 32) || !common.RedisEnabled {
			return fmt.Errorf("async tasks require Redis and a stable CRYPTO_SECRET of at least 32 bytes")
		}
		if len(c.Accounts) == 0 || len(c.Accounts) > 32 || len(c.Models) == 0 {
			return fmt.Errorf("async provider registry requires 1–32 accounts and at least one model")
		}
		keys := map[string]bool{}
		for name, a := range c.Accounts {
			if name == "" || len(name) > 64 || a.Provider != "tripo" || a.Key == "" || strings.ContainsAny(a.Key, "\r\n") || a.MaxRunning < 1 || a.MaxRunning > 10000 || a.MaxQueued < a.MaxRunning || a.MaxQueued > 100000 || a.SubmitRPM < 1 || a.SubmitRPM > 10000 || a.PollRPM < 1 || a.PollRPM > 10000 {
				return fmt.Errorf("invalid async account configuration: %s", name)
			}
			// One account entry per real provider account; multiple API keys do not increase concurrency.
			h := AsyncHash(a.Key)
			if keys[h] {
				return fmt.Errorf("duplicate provider credential configured under different accounts")
			}
			keys[h] = true
		}
		for alias, m := range c.Models {
			a, ok := c.Accounts[m.Account]
			if !ok || len(alias) == 0 || len(alias) > 128 || m.Quota < 1 || m.Quota > 1000000000 || len(m.Groups) == 0 {
				return fmt.Errorf("invalid async model configuration: %s", alias)
			}
			if _, e := NewAsyncProvider(a.Provider, a.Key).Prepare(asyncprovider.Request{}, m.Version); e != nil {
				return fmt.Errorf("invalid async model version for %s", alias)
			}
		}
	}
	AsyncSettings = c
	return nil
}
func AsyncHash(s string) string { h := sha256.Sum256([]byte(s)); return hex.EncodeToString(h[:]) }
func NewAsyncProvider(provider, key string) asyncprovider.Provider {
	if provider == "tripo" {
		return asyncprovider.Tripo{Key: key, HTTP: &http.Client{Timeout: 15 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}
	}
	return nil
}

// Fixed-window counters are atomic across replicas. Redis failure fails closed;
// the durable account lease, not Redis, owns execution of each account's work.
func AsyncRate(ctx context.Context, key string, limit int) (bool, error) {
	if common.RDB == nil {
		return false, fmt.Errorf("Redis unavailable")
	}
	n, e := common.RDB.Eval(ctx, `local n=redis.call('INCR',KEYS[1]); if n==1 then redis.call('EXPIRE',KEYS[1],120) end; return n`, []string{fmt.Sprintf("apera:async:%s:%d", key, time.Now().Unix()/60)}).Int64()
	return n <= int64(limit), e
}
func CreateAsyncGeneration(ctx context.Context, userID, tokenID int, group, idem string, r asyncprovider.Request) (*model.AsyncJob, bool, error) {
	canonical, e := common.Marshal(r)
	if e != nil {
		return nil, false, e
	}
	ih := AsyncHash(fmt.Sprintf("%d:%d:%s", userID, tokenID, idem))
	rh := AsyncHash(string(canonical))
	if old, e := model.AsyncFindIdempotency(ih, rh); e == nil {
		return old, false, nil
	} else if !errors.Is(e, model.ErrAsyncConflict) && !errors.Is(e, gorm.ErrRecordNotFound) {
		return nil, false, e
	} else if errors.Is(e, model.ErrAsyncConflict) {
		return nil, false, e
	}
	m, ok := AsyncSettings.Models[r.Model]
	if !ok {
		return nil, false, fmt.Errorf("%w: model unavailable", ErrAsyncRequest)
	}
	allowed := false
	for _, g := range m.Groups {
		if g == group {
			allowed = true
		}
	}
	if !allowed {
		return nil, false, fmt.Errorf("%w: model unavailable for group", ErrAsyncRequest)
	}
	a := AsyncSettings.Accounts[m.Account]
	body, e := NewAsyncProvider(a.Provider, a.Key).Prepare(r, m.Version)
	if e != nil {
		return nil, false, e
	}
	j := &model.AsyncJob{ID: "gen_" + uuid.NewString(), UserID: userID, TokenID: tokenID, Account: m.Account, Provider: a.Provider, Model: r.Model, Kind: r.Type, Quota: m.Quota, IdempotencyHash: ih, RequestHash: rh, RequestBody: string(body), CredentialHash: AsyncHash(a.Key)}
	j.Credential, e = asyncprovider.Seal(common.CryptoSecret, j.ID, a.Key)
	if e != nil {
		return nil, false, e
	}
	created, e := model.CreateAsyncJob(j, a.MaxQueued)
	return j, created, e
}

// AsyncWorkerStep runs bounded work per account; two workers cannot process the
// same account concurrently. Submission state is durable BEFORE any network call.
func AsyncWorkerStep(ctx context.Context) error {
	if !AsyncSettings.Enabled {
		return nil
	}
	if e := model.FlushAsyncCaches(); e != nil {
		return e
	}
	names := make([]string, 0, len(AsyncSettings.Accounts))
	for name := range AsyncSettings.Accounts {
		names = append(names, name)
	}
	sort.Strings(names)
	stepCtx, stop := context.WithTimeout(ctx, 60*time.Second)
	defer stop()
	queue := make(chan string, len(names))
	for _, name := range names {
		queue <- name
	}
	close(queue)
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for name := range queue {
				if stepCtx.Err() != nil {
					return
				}
				owner := uuid.NewString()
				won, e := model.ClaimAsyncAccount(name, owner, time.Now().Unix())
				if e != nil {
					common.SysError("async account claim failed")
					continue
				}
				if !won {
					continue
				}
				func() {
					defer model.ReleaseAsyncAccount(name, owner)
					step, cancel := context.WithTimeout(stepCtx, 45*time.Second)
					defer cancel()
					if e := runAsyncAccount(step, name, owner, AsyncSettings.Accounts[name]); e != nil {
						common.SysError("async account processing failed: " + name + ": " + e.Error())
					}
				}()
			}
		}()
	}
	wg.Wait()
	return nil
}
func runAsyncAccount(ctx context.Context, name, owner string, a AsyncAccountConfig) error {
	now := time.Now().Unix()
	// A previous process may have died between upstream acceptance and DB ack.
	if e := model.AsyncDB(ctx).Model(&model.AsyncJob{}).Where("account = ? AND status = ? AND updated_at < ?", name, model.AsyncSubmitting, now-120).Updates(map[string]any{"status": model.AsyncReconciling, "error_code": "submission_unknown", "version": gorm.Expr("version + 1"), "updated_at": now}).Error; e != nil {
		return e
	}
	// Poll one credential-compatible batch, independently of new submissions.
	var due []model.AsyncJob
	if e := model.AsyncDB(ctx).Where("account = ? AND billing = ? AND upstream_id <> ? AND next_attempt <= ?", name, "held", "", now).Order("next_attempt ASC").Limit(100).Find(&due).Error; e != nil {
		return e
	}
	if len(due) > 0 {
		first := due[0]
		batch := make([]model.AsyncJob, 0, len(due))
		ids := []string{}
		for _, j := range due {
			if j.CredentialHash == first.CredentialHash && j.Provider == first.Provider {
				batch = append(batch, j)
				ids = append(ids, j.UpstreamID)
			}
		}
		ok, e := AsyncRate(ctx, "poll:"+name, a.PollRPM)
		if e != nil {
			return e
		}
		if ok {
			key, e := asyncprovider.Open(common.CryptoSecret, first.ID, first.Credential)
			if e != nil {
				return e
			}
			if e := checkAsyncLease(ctx, name, owner); e != nil {
				return e
			}
			results, queryErr := NewAsyncProvider(first.Provider, key).Poll(ctx, ids)
			for i := range batch {
				j := &batch[i]
				result, found := results[j.UpstreamID]
				if queryErr != nil || !found || result.ID != j.UpstreamID {
					// Missing can mean wrong key/expired; neither proves no charge.
					if e := model.TransitionAsync(j, map[string]any{"next_attempt": now + 60, "status": model.AsyncReconciling, "error_code": "query_unconfirmed"}); e != nil {
						return e
					}
					continue
				}
				if e := ApplyAsyncResult(j, result, now); e != nil {
					return e
				}
			}
		}
	}
	// Expire never-submitted queued work even if all upstream slots are occupied.
	var expired []model.AsyncJob
	if e := model.AsyncDB(ctx).Where("account = ? AND status = ? AND created_at < ?", name, model.AsyncQueued, now-1800).Limit(20).Find(&expired).Error; e != nil {
		return e
	}
	for i := range expired {
		if e := model.FinishAsync(&expired[i], false, "queue_expired", "", ""); e != nil {
			return e
		}
	}
	var active int64
	if e := model.AsyncDB(ctx).Model(&model.AsyncJob{}).Where("account = ? AND billing = ? AND status <> ?", name, "held", model.AsyncQueued).Count(&active).Error; e != nil {
		return e
	}
	if active >= int64(a.MaxRunning) {
		return nil
	}
	var j model.AsyncJob
	e := model.AsyncDB(ctx).Where("account = ? AND status = ? AND next_attempt <= ?", name, model.AsyncQueued, now).Order("created_at ASC").First(&j).Error
	if errors.Is(e, gorm.ErrRecordNotFound) {
		return nil
	}
	if e != nil {
		return e
	}
	// Only local, never-submitted work has an automatic queue deadline.
	if now-j.CreatedAt > 1800 {
		return model.FinishAsync(&j, false, "queue_expired", "", "")
	}
	ok, e := AsyncRate(ctx, "submit:"+name, a.SubmitRPM)
	if e != nil || !ok {
		return e
	}
	key, e := asyncprovider.Open(common.CryptoSecret, j.ID, j.Credential)
	if e != nil {
		return e
	}
	if e = model.TransitionAsync(&j, map[string]any{"status": model.AsyncSubmitting, "attempts": gorm.Expr("attempts + 1")}); e != nil {
		return e
	}
	if e := checkAsyncLease(ctx, name, owner); e != nil {
		return e
	}
	logAsyncEvent("async_submission_started", j, "")
	result, submitErr := NewAsyncProvider(j.Provider, key).Submit(ctx, j.Kind, []byte(j.RequestBody))
	if submitErr != nil {
		if result.Rejected && result.RetryAfter > 0 && j.Attempts < 5 {
			return model.TransitionAsync(&j, map[string]any{"status": model.AsyncQueued, "next_attempt": now + int64(result.RetryAfter)})
		}
		if result.Rejected {
			common.SysError("async provider rejected request: " + j.Account)
			return model.FinishAsync(&j, false, "provider_rejected", "", "")
		}
		logAsyncEvent("async_submission_unknown", j, "")
		return model.TransitionAsync(&j, map[string]any{"status": model.AsyncReconciling, "error_code": "submission_unknown"})
	}
	logAsyncEvent("async_submission_accepted", j, result.ID)
	return model.TransitionAsync(&j, map[string]any{"status": model.AsyncSubmitted, "upstream_id": result.ID, "submitted_at": now, "next_attempt": now + 15})
}
func ApplyAsyncResult(j *model.AsyncJob, r asyncprovider.Result, now int64) error {
	switch r.Status {
	case "success":
		if !asyncprovider.HTTPS(r.Output.ModelURL) {
			return model.TransitionAsync(j, map[string]any{"status": model.AsyncReconciling, "error_code": "invalid_output", "next_attempt": now + 60})
		}
		if r.Output.PreviewURL != "" && !asyncprovider.HTTPS(r.Output.PreviewURL) {
			r.Output.PreviewURL = ""
		}
		// Only allowlisted public fields are returned; never persist provider error bodies.
		out, e := common.Marshal(r.Output)
		if e != nil {
			return e
		}
		return model.FinishAsync(j, true, "", string(out), string(r.Credits))
	case "failed", "cancelled":
		return model.FinishAsync(j, false, "provider_"+r.Status, "", string(r.Credits))
	case "queued", "running":
		progress := r.Progress
		if progress < 0 {
			progress = 0
		}
		if progress > 99 {
			progress = 99
		}
		return model.TransitionAsync(j, map[string]any{"status": model.AsyncRunning, "progress": progress, "error_code": "", "next_attempt": now + 15 + int64(j.Version%6)})
	default:
		return model.TransitionAsync(j, map[string]any{"status": model.AsyncReconciling, "error_code": "provider_state_unconfirmed", "next_attempt": now + 120})
	}
}

func checkAsyncLease(ctx context.Context, account, owner string) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	var count int64
	e := model.AsyncDB(ctx).Model(&model.AsyncAccount{}).Where("id = ? AND lease_owner = ? AND lease_until > ?", account, owner, time.Now().Unix()+20).Count(&count).Error
	if e != nil {
		return e
	}
	if count != 1 {
		return fmt.Errorf("account lease expired")
	}
	return nil
}

// LogAsyncMetrics publishes aggregate backlog snapshots as structured logs.
// Multiple workers report the same DB snapshot: dashboards/alarms use MAX, not SUM.
func LogAsyncMetrics(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	var states []struct {
		Status    string
		TaskCount int64
	}
	if e := model.AsyncDB(ctx).Model(&model.AsyncJob{}).Select("status, COUNT(*) AS task_count").Where("billing = ?", "held").Group("status").Scan(&states).Error; e != nil {
		return e
	}
	snapshot := map[string]any{"event": "async_queue_metrics", "queued": int64(0), "reconciling": int64(0), "oldest_queued_seconds": int64(0)}
	for _, s := range states {
		snapshot[s.Status] = s.TaskCount
	}
	var oldest model.AsyncJob
	e := model.AsyncDB(ctx).Select("created_at").Where("status = ? AND billing = ?", model.AsyncQueued, "held").Order("created_at ASC").First(&oldest).Error
	if e != nil && !errors.Is(e, gorm.ErrRecordNotFound) {
		return e
	}
	if e == nil {
		age := time.Now().Unix() - oldest.CreatedAt
		if age > 0 {
			snapshot["oldest_queued_seconds"] = age
		}
	}
	body, e := common.Marshal(snapshot)
	if e != nil {
		return e
	}
	fmt.Println(string(body))
	return nil
}

func logAsyncEvent(event string, j model.AsyncJob, upstreamID string) {
	body, e := common.Marshal(map[string]any{"event": event, "task_id": j.ID, "account": j.Account, "provider": j.Provider, "upstream_task_id": upstreamID, "version": os.Getenv("VERSION")})
	if e == nil {
		fmt.Println(string(body))
	}
}
