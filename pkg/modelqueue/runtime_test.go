package modelqueue

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/model"
	"github.com/QuantumNous/new-api/pkg/asyncprovider"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/glebarez/sqlite"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type objects struct {
	mu   sync.Mutex
	data map[string][]byte
}

func (o *objects) PutObject(ctx context.Context, in *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	b, e := io.ReadAll(in.Body)
	o.mu.Lock()
	defer o.mu.Unlock()
	o.data[*in.Key] = b
	return &s3.PutObjectOutput{}, e
}
func (o *objects) GetObject(ctx context.Context, in *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	b, ok := o.data[*in.Key]
	if !ok {
		return nil, errors.New("not found")
	}
	return &s3.GetObjectOutput{Body: io.NopCloser(bytes.NewReader(b))}, nil
}

type queue struct{ sent chan string }

func (q *queue) SendMessage(ctx context.Context, in *sqs.SendMessageInput, _ ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
	select {
	case q.sent <- *in.MessageBody:
		return &sqs.SendMessageOutput{}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
func (q *queue) ReceiveMessage(context.Context, *sqs.ReceiveMessageInput, ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	return nil, errors.New("unused")
}
func (q *queue) DeleteMessage(context.Context, *sqs.DeleteMessageInput, ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
	return &sqs.DeleteMessageOutput{}, nil
}
func (q *queue) ChangeMessageVisibility(context.Context, *sqs.ChangeMessageVisibilityInput, ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error) {
	return &sqs.ChangeMessageVisibilityOutput{}, nil
}

type bus struct {
	mu         sync.Mutex
	events     map[string][]redis.XMessage
	fail       bool
	failAfter  int
	dropMiddle bool
}

func (b *bus) publish(ctx context.Context, id string, e Event) error {
	if b.fail {
		return errors.New("redis unavailable")
	}
	data, err := common.Marshal(e)
	if err != nil {
		return err
	}
	sealed, err := asyncprovider.Seal(common.CryptoSecret, id, string(data))
	if err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.events[id] = append(b.events[id], redis.XMessage{ID: strconv.Itoa(len(b.events[id]) + 1), Values: map[string]any{"event": sealed}})
	return nil
}
func (b *bus) read(ctx context.Context, id, cursor string) ([]redis.XStream, error) {
	if b.fail {
		return nil, errors.New("redis unavailable")
	}
	offset, _ := strconv.Atoi(cursor)
	if b.dropMiddle && offset == 2 {
		offset++
	}
	if b.failAfter > 0 && offset >= b.failAfter {
		return nil, errors.New("redis stream lost after partial delivery")
	}
	for {
		b.mu.Lock()
		events := b.events[id]
		if offset < len(events) {
			message := events[offset]
			b.mu.Unlock()
			return []redis.XStream{{Stream: streamKey(id), Messages: []redis.XMessage{message}}}, nil
		}
		b.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Millisecond):
		}
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func fixture(t *testing.T) *Runtime {
	t.Helper()
	previous := model.DB
	redisEnabled := common.RedisEnabled
	secret := common.CryptoSecret
	db, e := gorm.Open(sqlite.Open("file:"+strings.ReplaceAll(t.Name(), "/", "_")+"?mode=memory&cache=shared"), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, e)
	sql, e := db.DB()
	require.NoError(t, e)
	sql.SetMaxOpenConns(1)
	model.DB = db
	common.RedisEnabled = false
	common.CryptoSecret = "test-model-queue-stable-encryption-secret"
	require.NoError(t, db.AutoMigrate(&model.QueueExecution{}, &model.QueueOutbox{}, &model.QueueHold{}))
	t.Cleanup(func() {
		model.DB = previous
		common.RedisEnabled = redisEnabled
		common.CryptoSecret = secret
		sql.Close()
	})
	return &Runtime{Queue: &queue{sent: make(chan string, 8)}, Objects: &objects{data: map[string][]byte{}}, bus: &bus{events: map[string][]redis.XMessage{}}, QueueURL: "queue", Bucket: "bucket", Concurrency: 1}
}
func drive(t *testing.T, r *Runtime, ctx context.Context) <-chan error {
	t.Helper()
	done := make(chan error, 1)
	go func() {
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case id := <-r.Queue.(*queue).sent:
				done <- r.execute(ctx, id, "receipt")
				return
			case <-ctx.Done():
				done <- ctx.Err()
				return
			case <-ticker.C:
				r.publish(ctx)
			}
		}
	}()
	return done
}
func TestQueueOrdinaryResponseAndDuplicateDelivery(t *testing.T) {
	r := fixture(t)
	var calls atomic.Int32
	r.Direct = roundTripFunc(func(req *http.Request) (*http.Response, error) {
		calls.Add(1)
		require.Equal(t, "Bearer secret", req.Header.Get("Authorization"))
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(`{"ok":true}`))}, nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ctx, trace := WithTrace(ctx)
	done := drive(t, r, ctx)
	req, e := http.NewRequestWithContext(ctx, "POST", "https://supplier.example/chat", strings.NewReader(`{"prompt":"private"}`))
	require.NoError(t, e)
	req.Header.Set("Authorization", "Bearer secret")
	resp, e := (Transport{Runtime: r}).RoundTrip(req)
	require.NoError(t, e)
	body, e := io.ReadAll(resp.Body)
	require.NoError(t, e)
	require.NoError(t, resp.Body.Close())
	require.JSONEq(t, `{"ok":true}`, string(body))
	require.NoError(t, <-done)
	require.True(t, trace.Resolve(ctx))
	require.False(t, trace.Unknown())
	require.Len(t, trace.IDs, 1)
	require.NoError(t, r.execute(ctx, trace.IDs[0], "duplicate"))
	require.EqualValues(t, 1, calls.Load())
	for _, raw := range r.Objects.(*objects).data {
		require.NotContains(t, string(raw), "Bearer secret")
		require.NotContains(t, string(raw), "private")
	}
}
func TestQueueStreamsBeforeProviderCompletes(t *testing.T) {
	r := fixture(t)
	upstream, writer := io.Pipe()
	defer upstream.Close()
	r.Direct = roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"text/event-stream"}}, Body: upstream}, nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := drive(t, r, ctx)
	go func() { _, _ = writer.Write([]byte("data: first\n\n")) }()
	req, _ := http.NewRequestWithContext(ctx, "POST", "https://supplier.example/chat", nil)
	resp, e := (Transport{Runtime: r}).RoundTrip(req)
	require.NoError(t, e)
	first := make([]byte, len("data: first\n\n"))
	_, e = io.ReadFull(resp.Body, first)
	require.NoError(t, e)
	require.Equal(t, "data: first\n\n", string(first))
	var execution model.QueueExecution
	require.NoError(t, model.DB.First(&execution).Error)
	require.Equal(t, model.QueueRunning, execution.State)
	go func() { _, _ = writer.Write([]byte("data: second\n\n")); writer.Close() }()
	rest, e := io.ReadAll(resp.Body)
	require.NoError(t, e)
	resp.Body.Close()
	require.Equal(t, "data: second\n\n", string(rest))
	require.NoError(t, <-done)
}
func TestQueueRedisLossRecoversDurableResult(t *testing.T) {
	r := fixture(t)
	r.bus.(*bus).fail = true
	r.Direct = roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader("complete response"))}, nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	done := drive(t, r, ctx)
	req, _ := http.NewRequestWithContext(ctx, "GET", "https://supplier.example/result", nil)
	resp, e := (Transport{Runtime: r}).RoundTrip(req)
	require.NoError(t, e)
	body, e := io.ReadAll(resp.Body)
	require.NoError(t, e)
	resp.Body.Close()
	require.Equal(t, "complete response", string(body))
	require.NoError(t, <-done)
}
func TestQueueAmbiguousProviderDisconnectDoesNotReexecute(t *testing.T) {
	r := fixture(t)
	var calls atomic.Int32
	r.bus.(*bus).fail = true
	r.Direct = roundTripFunc(func(*http.Request) (*http.Response, error) {
		calls.Add(1)
		return nil, errors.New("connection lost after request was sent")
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ctx, trace := WithTrace(ctx)
	done := drive(t, r, ctx)
	req, _ := http.NewRequestWithContext(ctx, "POST", "https://supplier.example/task", nil)
	_, e := (Transport{Runtime: r}).RoundTrip(req)
	require.ErrorContains(t, e, "unknown")
	require.NoError(t, <-done)
	require.False(t, trace.Resolve(ctx))
	require.True(t, trace.Unknown())
	require.NoError(t, r.execute(ctx, trace.IDs[0], "duplicate"))
	require.EqualValues(t, 1, calls.Load())
}
func TestQueueDestinationRestrictions(t *testing.T) {
	for _, raw := range []string{"http://supplier.example", "https://name:secret@supplier.example", "https://supplier.example:8443"} {
		require.Error(t, destination(raw), raw)
	}
	for _, ip := range []string{"127.0.0.1", "169.254.169.254", "10.0.0.1", "100.100.100.200", "::1", "fc00::1", "64:ff9b::a00:1"} {
		require.False(t, publicIP(net.ParseIP(ip)), ip)
	}
	require.True(t, publicIP(net.ParseIP("8.8.8.8")))
	require.NoError(t, destination("https://supplier.example:443/v1"))
}
func TestQueueCancelledBeforeDispatchNeverStarts(t *testing.T) {
	r := fixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx, trace := WithTrace(ctx)
	go func() {
		for !trace.Accepted() {
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Millisecond):
			}
		}
		cancel()
	}()
	req, _ := http.NewRequestWithContext(ctx, "POST", "https://supplier.example/chat", nil)
	_, e := (Transport{Runtime: r}).RoundTrip(req)
	require.Error(t, e)
	require.Len(t, trace.IDs, 1)
	var j model.QueueExecution
	require.NoError(t, model.DB.First(&j, "id = ?", trace.IDs[0]).Error)
	require.Equal(t, model.QueueExpired, j.State)
	won, e := model.ClaimQueueExecution(context.Background(), j.ID, "worker")
	require.NoError(t, e)
	require.False(t, won)
	require.False(t, trace.Unknown())
}

func TestQueueRedisLossAfterPartialDeliveryDoesNotDuplicateBytes(t *testing.T) {
	r := fixture(t)
	r.bus.(*bus).failAfter = 2
	body := strings.Repeat("a", 32<<10) + strings.Repeat("b", 32<<10) + "tail"
	r.Direct = roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(body))}, nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	done := drive(t, r, ctx)
	req, _ := http.NewRequestWithContext(ctx, "POST", "https://supplier.example/chat", nil)
	resp, e := (Transport{Runtime: r}).RoundTrip(req)
	require.NoError(t, e)
	got, e := io.ReadAll(resp.Body)
	require.NoError(t, e)
	resp.Body.Close()
	require.Equal(t, body, string(got))
	require.NoError(t, <-done)
}

func TestQueueMissingStreamChunkFallsBackWithoutSilentTruncation(t *testing.T) {
	r := fixture(t)
	r.bus.(*bus).dropMiddle = true
	body := strings.Repeat("a", 32<<10) + strings.Repeat("b", 32<<10) + strings.Repeat("c", 32<<10) + "tail"
	r.Direct = roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(body))}, nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	done := drive(t, r, ctx)
	req, _ := http.NewRequestWithContext(ctx, "POST", "https://supplier.example/chat", nil)
	resp, e := (Transport{Runtime: r}).RoundTrip(req)
	require.NoError(t, e)
	got, e := io.ReadAll(resp.Body)
	require.NoError(t, e)
	resp.Body.Close()
	require.Equal(t, body, string(got))
	require.NoError(t, <-done)
}

func TestQueueMissingFinalChunkCannotReturnEarlyEOF(t *testing.T) {
	r := fixture(t)
	r.bus.(*bus).dropMiddle = true
	body := strings.Repeat("a", 32<<10) + strings.Repeat("b", 32<<10)
	r.Direct = roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(body))}, nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	done := drive(t, r, ctx)
	req, _ := http.NewRequestWithContext(ctx, "POST", "https://supplier.example/chat", nil)
	resp, e := (Transport{Runtime: r}).RoundTrip(req)
	require.NoError(t, e)
	got, e := io.ReadAll(resp.Body)
	require.NoError(t, e)
	resp.Body.Close()
	require.Equal(t, body, string(got))
	require.NoError(t, <-done)
}
