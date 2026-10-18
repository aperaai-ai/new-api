// Package modelqueue dispatches supplier HTTP requests through SQS. AWS control
// plane clients deliberately use their own HTTP transport to avoid recursion.
package modelqueue

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/model"
	"github.com/QuantumNous/new-api/pkg/asyncprovider"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
)

const maxInput = 8 << 20
const maxOutput = 32 << 20
const retention = time.Hour

var Default *Runtime

func Enabled() bool { return os.Getenv("MODEL_QUEUE_ENABLED") == "true" }

type QueueAPI interface {
	SendMessage(context.Context, *sqs.SendMessageInput, ...func(*sqs.Options)) (*sqs.SendMessageOutput, error)
	ReceiveMessage(context.Context, *sqs.ReceiveMessageInput, ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
	DeleteMessage(context.Context, *sqs.DeleteMessageInput, ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error)
	ChangeMessageVisibility(context.Context, *sqs.ChangeMessageVisibilityInput, ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error)
}
type ObjectAPI interface {
	PutObject(context.Context, *s3.PutObjectInput, ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	GetObject(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error)
}
type eventBus interface {
	publish(context.Context, string, Event) error
	read(context.Context, string, string) ([]redis.XStream, error)
}
type Runtime struct {
	bus              eventBus
	Queue            QueueAPI
	Objects          ObjectAPI
	Redis            *redis.Client
	QueueURL, Bucket string
	Direct           http.RoundTripper
	Concurrency      int
}
type Request struct {
	Method, URL, Host string
	Header            http.Header
	Body              []byte
}
type Response struct {
	Status int
	Header http.Header
	Body   []byte
	Error  string
}
type Event struct {
	Offset int
	Kind   string
	Status int
	Header http.Header
	Data   []byte
}
type traceKey struct{}

// Trace prevents channel retries and refunds when an accepted execution's outcome
// is unknown. It is shared with the request transport via the request context.
type Trace struct {
	mu        sync.Mutex
	IDs       []string
	Uncertain bool
	pending   int
	HoldID    string
}

func TraceFrom(ctx context.Context) *Trace { t, _ := ctx.Value(traceKey{}).(*Trace); return t }
func (t *Trace) SetHold(id string)         { t.mu.Lock(); defer t.mu.Unlock(); t.HoldID = id }
func (t *Trace) hold() string              { t.mu.Lock(); defer t.mu.Unlock(); return t.HoldID }

// Resolve uses durable state rather than an SSE parser's stopping point.
func (t *Trace) Resolve(ctx context.Context) bool {
	t.mu.Lock()
	ids := append([]string(nil), t.IDs...)
	t.mu.Unlock()
	for {
		complete := true
		for _, id := range ids {
			var j model.QueueExecution
			if model.AsyncDB(ctx).First(&j, "id = ?", id).Error != nil {
				return false
			}
			switch j.State {
			case model.QueueSucceeded, model.QueueFailed, model.QueueExpired:
			case model.QueueUnknown:
				return false
			default:
				complete = false
			}
		}
		if complete {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(50 * time.Millisecond):
		}
	}
}
func WithTrace(ctx context.Context) (context.Context, *Trace) {
	t := &Trace{}
	return context.WithValue(ctx, traceKey{}, t), t
}
func (t *Trace) Accepted() bool { t.mu.Lock(); defer t.mu.Unlock(); return len(t.IDs) > 0 }
func (t *Trace) Unknown() bool  { t.mu.Lock(); defer t.mu.Unlock(); return t.Uncertain }
func (t *Trace) add(id string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.IDs = append(t.IDs, id)
	t.Uncertain = true
	t.pending++
}

func (t *Trace) complete() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.pending--
	t.Uncertain = t.pending > 0
}

// Only definitive local non-execution can clear uncertainty after an error.
func (t *Trace) notStarted() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.IDs) == 1 {
		t.Uncertain = false
	}
}

func Init(ctx context.Context) error {
	role := os.Getenv("APERA_ROLE")
	if (role == "api" || role == "worker") && !Enabled() {
		return errors.New("distributed API/Worker deployment requires MODEL_QUEUE_ENABLED=true")
	}
	if !Enabled() {
		return nil
	}
	if os.Getenv("MODEL_QUEUE_URL") == "" || os.Getenv("MODEL_PAYLOAD_BUCKET") == "" || !common.RedisEnabled || common.RDB == nil || len(os.Getenv("CRYPTO_SECRET")) < 32 {
		return errors.New("model queue requires queue URL, payload bucket, Redis and stable CRYPTO_SECRET")
	}
	cfg, e := config.LoadDefaultConfig(ctx)
	if e != nil {
		return errors.New("AWS model queue configuration failed")
	}
	opt := common.ParseRedisOption()
	opt.PoolSize = 128
	opt.ReadTimeout = 5 * time.Second
	r := &Runtime{Queue: sqs.NewFromConfig(cfg), Objects: s3.NewFromConfig(cfg), Redis: redis.NewClient(opt), QueueURL: os.Getenv("MODEL_QUEUE_URL"), Bucket: os.Getenv("MODEL_PAYLOAD_BUCKET"), Concurrency: 4}
	if n, e := strconv.Atoi(os.Getenv("MODEL_WORKER_CONCURRENCY")); e == nil && n >= 1 && n <= 32 {
		r.Concurrency = n
	}
	r.Direct = &http.Transport{MaxIdleConns: 128, MaxIdleConnsPerHost: 32, ForceAttemptHTTP2: true, DialContext: publicDial}
	Default = r
	return nil
}

// Resolve and validate on each new connection; proxies and redirects cannot be
// used to reach ECS credentials, IMDS, the database or private network services.
func publicDial(ctx context.Context, network, address string) (net.Conn, error) {
	host, port, e := net.SplitHostPort(address)
	if e != nil || port != "443" {
		return nil, errors.New("model destinations require public HTTPS port 443")
	}
	ips, e := net.DefaultResolver.LookupIPAddr(ctx, host)
	if e != nil {
		return nil, errors.New("model DNS resolution failed")
	}
	if len(ips) == 0 {
		return nil, errors.New("empty model DNS result")
	}
	for _, ip := range ips {
		if !publicIP(ip.IP) {
			return nil, errors.New("private model destination blocked")
		}
	}
	var last error
	for _, ip := range ips {
		var conn net.Conn
		conn, last = (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}).DialContext(ctx, network, net.JoinHostPort(ip.IP.String(), port))
		if last == nil {
			return conn, nil
		}
	}
	return nil, last
}
func publicIP(ip net.IP) bool {
	if !ip.IsGlobalUnicast() || ip.IsPrivate() || ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsUnspecified() {
		return false
	}
	for _, cidr := range []string{"100.64.0.0/10", "192.0.0.0/24", "192.0.2.0/24", "198.18.0.0/15", "198.51.100.0/24", "203.0.113.0/24", "240.0.0.0/4", "2001:db8::/32", "64:ff9b::/96"} {
		_, block, _ := net.ParseCIDR(cidr)
		if block.Contains(ip) {
			return false
		}
	}
	return true
}
func destination(s string) error {
	u, e := url.Parse(s)
	if e != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.Fragment != "" || (u.Port() != "" && u.Port() != "443") {
		return errors.New("model destinations require public HTTPS")
	}
	return nil
}
func (r *Runtime) put(ctx context.Context, key, id string, v any) error {
	b, e := common.Marshal(v)
	if e != nil {
		return e
	}
	sealed, e := asyncprovider.Seal(common.CryptoSecret, id, string(b))
	if e != nil {
		return e
	}
	_, e = r.Objects.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(r.Bucket), Key: aws.String(key), Body: strings.NewReader(sealed), ContentType: aws.String("application/octet-stream")})
	return e
}
func (r *Runtime) get(ctx context.Context, key, id string, v any) error {
	o, e := r.Objects.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(r.Bucket), Key: aws.String(key)})
	if e != nil {
		return e
	}
	defer o.Body.Close()
	b, e := io.ReadAll(io.LimitReader(o.Body, 96<<20))
	if e != nil {
		return e
	}
	plain, e := asyncprovider.Open(common.CryptoSecret, id, string(b))
	if e != nil {
		return e
	}
	return common.Unmarshal([]byte(plain), v)
}
func streamKey(id string) string { return "apera:model:result:" + id }
func (r *Runtime) emit(ctx context.Context, id string, event Event) error {
	if r.bus != nil {
		return r.bus.publish(ctx, id, event)
	}
	b, e := common.Marshal(event)
	if e != nil {
		return e
	}
	sealed, e := asyncprovider.Seal(common.CryptoSecret, id, string(b))
	if e != nil {
		return e
	}
	pipe := r.Redis.TxPipeline()
	pipe.XAdd(ctx, &redis.XAddArgs{Stream: streamKey(id), Values: map[string]any{"event": sealed}})
	pipe.Expire(ctx, streamKey(id), retention)
	_, e = pipe.Exec(ctx)
	return e
}

// RunPublisher retries the transactional outbox. Duplicate sends are expected and
// harmless: only the SQL execution claim authorizes a provider request.
func (r *Runtime) RunPublisher(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		r.publish(ctx)
		if e := model.ReapQueueExecutions(ctx); e != nil {
			common.SysError("model execution reaper failed")
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
func (r *Runtime) publish(ctx context.Context) {
	var pending []model.QueueOutbox
	if model.AsyncDB(ctx).Where("published = ? AND next_attempt <= ?", false, time.Now().Unix()).Order("created_at ASC").Limit(100).Find(&pending).Error != nil {
		return
	}
	for _, p := range pending {
		if ctx.Err() != nil {
			return
		}
		claim := model.AsyncDB(ctx).Model(&model.QueueOutbox{}).Where("id = ? AND published = ? AND next_attempt <= ?", p.ID, false, time.Now().Unix()).UpdateColumn("next_attempt", time.Now().Unix()+30)
		if claim.Error != nil || claim.RowsAffected != 1 {
			continue
		}
		send, cancel := context.WithTimeout(ctx, 10*time.Second)
		_, e := r.Queue.SendMessage(send, &sqs.SendMessageInput{QueueUrl: aws.String(r.QueueURL), MessageBody: aws.String(p.ID)})
		cancel()
		update := map[string]any{"published": true}
		if e != nil {
			delay := int64(1) << min(p.Attempts, 6)
			update = map[string]any{"attempts": p.Attempts + 1, "next_attempt": time.Now().Unix() + delay}
			common.SysError("model queue publish failed")
		}
		if model.AsyncDB(ctx).Model(&model.QueueOutbox{}).Where("id = ? AND published = ?", p.ID, false).Updates(update).Error != nil {
			common.SysError("model queue outbox update failed")
		}
	}
}

func (r *Runtime) RunWorker(ctx context.Context) {
	var wg sync.WaitGroup
	for i := 0; i < r.Concurrency; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); r.consume(ctx) }()
	}
	wg.Wait()
}
func (r *Runtime) consume(ctx context.Context) {
	for ctx.Err() == nil {
		received, e := r.Queue.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: aws.String(r.QueueURL), MaxNumberOfMessages: 1, WaitTimeSeconds: 20, VisibilityTimeout: 120})
		if e != nil {
			if ctx.Err() != nil {
				return
			}
			common.SysError("model queue receive failed")
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
			continue
		}
		for _, msg := range received.Messages {
			id := aws.ToString(msg.Body)
			receipt := aws.ToString(msg.ReceiptHandle)
			if len(id) > 64 || !strings.HasPrefix(id, "exec_") {
				common.SysError("invalid model queue message")
				continue
			}
			if e := r.execute(ctx, id, receipt); e != nil {
				common.SysError("model execution failed; retained for recovery")
				continue
			}
			if _, e := r.Queue.DeleteMessage(ctx, &sqs.DeleteMessageInput{QueueUrl: aws.String(r.QueueURL), ReceiptHandle: aws.String(receipt)}); e != nil {
				common.SysError("model queue acknowledgement failed")
			}
		}
	}
}
func (r *Runtime) execute(parent context.Context, id, receipt string) error {
	owner := uuid.NewString()
	won, e := model.ClaimQueueExecution(parent, id, owner)
	if e != nil {
		return e
	}
	if !won {
		var existing model.QueueExecution
		if e := model.AsyncDB(parent).First(&existing, "id = ?", id).Error; e != nil {
			return e
		}
		// Never acknowledge a live duplicate: the original worker may still die.
		if existing.State == model.QueueQueued || existing.State == model.QueueRunning {
			return errors.New("execution not available")
		}
		return nil
	}
	var j model.QueueExecution
	if e := model.AsyncDB(parent).First(&j, "id = ?", id).Error; e != nil {
		return e
	}
	ctx, cancel := context.WithDeadline(parent, time.Unix(j.Deadline, 0))
	defer cancel()
	var request Request
	if e := r.get(ctx, j.InputKey, id, &request); e != nil {
		return e
	}
	if e := destination(request.URL); e != nil {
		return r.reject(ctx, id, owner, "unsupported model destination")
	}
	req, e := http.NewRequestWithContext(ctx, request.Method, request.URL, bytes.NewReader(request.Body))
	if e != nil {
		return r.reject(ctx, id, owner, "invalid model request")
	}
	req.Header = request.Header
	req.Host = request.Host
	done := make(chan struct{})
	defer close(done)
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				alive, e := model.HeartbeatQueueExecution(ctx, id, owner)
				if e != nil || !alive {
					cancel()
					return
				}
				if _, e := r.Queue.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{QueueUrl: aws.String(r.QueueURL), ReceiptHandle: aws.String(receipt), VisibilityTimeout: 120}); e != nil {
					common.SysError("model queue visibility heartbeat failed")
				}
			}
		}
	}()
	// Transport.RoundTrip performs exactly one HTTP request and does not follow redirects.
	resp, e := r.Direct.RoundTrip(req)
	if e != nil {
		return r.unknown(id, owner)
	}
	defer resp.Body.Close()
	result := Response{Status: resp.StatusCode, Header: resp.Header.Clone()}
	_ = r.emit(ctx, id, Event{Kind: "headers", Status: result.Status, Header: result.Header})
	buf := make([]byte, 32<<10)
	for {
		n, readErr := resp.Body.Read(buf)
		if len(result.Body)+n > maxOutput {
			return r.unknown(id, owner)
		}
		if n > 0 {
			result.Body = append(result.Body, buf[:n]...)
			_ = r.emit(ctx, id, Event{Kind: "chunk", Data: buf[:n], Offset: len(result.Body) - n})
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return r.unknown(id, owner)
		}
	}
	key := "output/" + id
	if e := r.put(ctx, key, id, result); e != nil {
		return e
	}
	completed, e := model.CompleteQueueExecution(ctx, id, owner, model.QueueSucceeded, key, resp.StatusCode)
	if e != nil {
		return e
	}
	if !completed {
		return errors.New("execution lease lost before completion")
	}
	_ = r.emit(ctx, id, Event{Kind: "done", Offset: len(result.Body)})
	return nil
}
func (r *Runtime) unknown(id, owner string) error {
	common.SysError("model execution outcome unknown: " + id)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, e := model.CompleteQueueExecution(ctx, id, owner, model.QueueUnknown, "")
	return e
}
func (r *Runtime) reject(ctx context.Context, id, owner, message string) error {
	key := "output/" + id
	if e := r.put(ctx, key, id, Response{Error: message}); e != nil {
		return e
	}
	_, e := model.CompleteQueueExecution(ctx, id, owner, model.QueueFailed, key)
	return e
}

// Transport enqueues HTTP executions rather than opening supplier connections.
type Transport struct{ Runtime *Runtime }

func (t Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	r := t.Runtime
	if r == nil {
		return nil, errors.New("model queue unavailable")
	}
	if e := destination(req.URL.String()); e != nil {
		return nil, e
	}
	var body []byte
	if req.Body != nil {
		defer req.Body.Close()
		var e error
		body, e = io.ReadAll(io.LimitReader(req.Body, maxInput+1))
		if e != nil {
			return nil, e
		}
		if len(body) > maxInput {
			return nil, errors.New("queued model request exceeds 8 MiB")
		}
	}
	ctx, cancel := context.WithTimeout(req.Context(), 10*time.Minute)
	id := "exec_" + uuid.NewString()
	deadline, _ := ctx.Deadline()
	input := "input/" + id
	if e := r.put(ctx, input, id, Request{Method: req.Method, URL: req.URL.String(), Host: req.Host, Header: req.Header.Clone(), Body: body}); e != nil {
		cancel()
		return nil, errors.New("model payload storage unavailable")
	}
	trace := TraceFrom(req.Context())
	j := &model.QueueExecution{ID: id, Kind: "http", InputKey: input, Deadline: deadline.Unix()}
	if trace != nil {
		j.HoldID = trace.hold()
	}
	if e := model.CreateQueueExecution(ctx, j); e != nil {
		cancel()
		return nil, errors.New("model execution admission failed")
	}
	trace = TraceFrom(req.Context())
	if trace != nil {
		trace.add(id)
	}
	reader := &resultBody{runtime: r, ctx: ctx, cancel: cancel, id: id, cursor: "0-0", trace: trace}
	for {
		event, e := reader.next()
		if e != nil {
			reader.Close()
			return nil, e
		}
		if event.Kind == "headers" {
			reader.headerSeen = true
			return &http.Response{StatusCode: event.Status, Status: fmt.Sprintf("%d %s", event.Status, http.StatusText(event.Status)), Header: event.Header, Body: reader, ContentLength: -1, Request: req, Proto: "HTTP/1.1", ProtoMajor: 1, ProtoMinor: 1}, nil
		}
		reader.durable = true
	}
}

type resultBody struct {
	headerSeen bool
	durable    bool
	runtime    *Runtime
	ctx        context.Context
	cancel     context.CancelFunc
	id, cursor string
	pending    []byte
	offset     int
	ended      bool
	trace      *Trace
}

func (b *resultBody) next() (Event, error) {
	for {
		if e := b.ctx.Err(); e != nil {
			b.expireQueued()
			return Event{}, errors.New("model result wait expired; check execution before retry")
		}
		var streams []redis.XStream
		var e error
		if !b.durable {
			if b.runtime.bus != nil {
				streams, e = b.runtime.bus.read(b.ctx, b.id, b.cursor)
			} else {
				streams, e = b.runtime.Redis.XRead(b.ctx, &redis.XReadArgs{Streams: []string{streamKey(b.id), b.cursor}, Count: 1, Block: time.Second}).Result()
			}
		}
		if e == nil && len(streams) > 0 && len(streams[0].Messages) > 0 {
			m := streams[0].Messages[0]
			b.cursor = m.ID
			s, ok := m.Values["event"].(string)
			if !ok {
				return Event{}, errors.New("invalid model result event")
			}
			plain, e := asyncprovider.Open(common.CryptoSecret, b.id, s)
			if e != nil {
				return Event{}, errors.New("model result authentication failed")
			}
			var event Event
			if e := common.Unmarshal([]byte(plain), &event); e != nil {
				return Event{}, e
			}
			return event, nil
		}
		// Redis is a notification path. SQL plus S3 remain authoritative and let a
		// surviving API complete even after Redis loses its transient stream.
		var j model.QueueExecution
		if e := model.AsyncDB(b.ctx).First(&j, "id = ?", b.id).Error; e != nil {
			return Event{}, errors.New("model execution status unavailable")
		}
		switch j.State {
		case model.QueueExpired:
			if b.trace != nil {
				b.trace.notStarted()
			}
			return Event{}, errors.New("model execution expired before starting")
		case model.QueueUnknown:
			return Event{}, errors.New("model execution outcome unknown; automatic retry disabled")
		case model.QueueSucceeded, model.QueueFailed:
			var response Response
			if e := b.runtime.get(b.ctx, j.OutputKey, b.id, &response); e != nil {
				return Event{}, errors.New("durable model result unavailable")
			}
			if response.Error != "" {
				if b.trace != nil {
					b.trace.notStarted()
				}
				return Event{}, errors.New(response.Error)
			}
			b.durable = true
			if !b.headerSeen {
				b.headerSeen = true
				b.cursor = "durable"
				return Event{Kind: "headers", Status: response.Status, Header: response.Header}, nil
			}
			if b.offset < len(response.Body) {
				return Event{Kind: "chunk", Data: response.Body[b.offset:], Offset: b.offset}, nil
			}
			return Event{Kind: "done", Offset: len(response.Body)}, nil
		}
		if e != nil && e != redis.Nil {
			select {
			case <-b.ctx.Done():
			case <-time.After(time.Second):
			}
		}
	}
}
func (b *resultBody) Read(p []byte) (int, error) {
	for len(b.pending) == 0 {
		if b.ended {
			return 0, io.EOF
		}
		event, e := b.next()
		if e != nil {
			return 0, e
		}
		switch event.Kind {
		case "chunk":
			if event.Offset != b.offset {
				b.durable = true
				continue
			}
			b.pending = event.Data
		case "done":
			if event.Offset != b.offset {
				b.durable = true
				continue
			}
			if b.trace != nil {
				b.trace.complete()
			}
			b.ended = true
			return 0, io.EOF
		}
	}
	n := copy(p, b.pending)
	b.pending = b.pending[n:]
	b.offset += n
	return n, nil
}
func (b *resultBody) expireQueued() {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	r := model.AsyncDB(ctx).Model(&model.QueueExecution{}).Where("id = ? AND state = ?", b.id, model.QueueQueued).Updates(map[string]any{"state": model.QueueExpired, "updated_at": time.Now().Unix()})
	if r.Error == nil && r.RowsAffected == 1 && b.trace != nil {
		b.trace.notStarted()
	}
}
func (b *resultBody) Close() error {
	if !b.ended {
		b.expireQueued()
	}
	b.cancel()
	return nil
}
