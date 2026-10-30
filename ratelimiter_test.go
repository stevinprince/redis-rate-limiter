package ratelimiter

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stevin/ratelimiter/internal/algorithms"
	ipolicy "github.com/stevin/ratelimiter/internal/policy"
	iredis "github.com/stevin/ratelimiter/internal/redis"
)

// ---------------------------------------------------------------------------
// Mock Redis client
// ---------------------------------------------------------------------------

// mockClient implements iredis.Client for unit testing. It provides an
// in-memory key-value store and a configurable Eval callback so tests
// can simulate Lua script responses without a running Redis instance.
type mockClient struct {
	mu   sync.Mutex
	data map[string]string

	// evalFn is called for both Eval and EvalSha. Tests set this to
	// return the desired Lua script result.
	evalFn func(ctx context.Context, script string, keys []string, args ...interface{}) (interface{}, error)

	closed bool
}

func newMockClient() *mockClient {
	return &mockClient{
		data: make(map[string]string),
	}
}

func (m *mockClient) Eval(ctx context.Context, script string, keys []string, args ...interface{}) (interface{}, error) {
	if m.evalFn != nil {
		return m.evalFn(ctx, script, keys, args...)
	}
	return nil, nil
}

func (m *mockClient) EvalSha(ctx context.Context, sha string, keys []string, args ...interface{}) (interface{}, error) {
	// Delegate to evalFn directly — in real Redis, scriptRunner would
	// try EvalSha first and fall back to Eval on NOSCRIPT. In the mock
	// we skip that dance and just return the configured response.
	if m.evalFn != nil {
		return m.evalFn(ctx, sha, keys, args...)
	}
	return nil, nil
}

func (m *mockClient) ScriptLoad(ctx context.Context, script string) (string, error) {
	return "mock-sha", nil
}

func (m *mockClient) Get(ctx context.Context, key string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	val, ok := m.data[key]
	if !ok {
		return "", iredis.ErrNil
	}
	return val, nil
}

func (m *mockClient) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[key] = fmt.Sprintf("%v", value)
	return nil
}

func (m *mockClient) Del(ctx context.Context, keys ...string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var count int64
	for _, k := range keys {
		if _, ok := m.data[k]; ok {
			delete(m.data, k)
			count++
		}
	}
	return count, nil
}

func (m *mockClient) Ping(ctx context.Context) error {
	return nil
}

func (m *mockClient) Close() error {
	m.closed = true
	return nil
}

// ---------------------------------------------------------------------------
// Test helper: construct a RateLimiter with the mock client
// ---------------------------------------------------------------------------

func newTestLimiter(t *testing.T, mock *mockClient, opts ...func(*RateLimiter)) *RateLimiter {
	t.Helper()

	defPolicy := Policy{
		Algorithm: TokenBucket,
		Rate:      10,
		Window:    time.Minute,
	}

	policyMgr := ipolicy.NewManager(ipolicy.Options{
		Client:    mock,
		KeyPrefix: "rl:",
		CacheTTL:  time.Minute,
	})

	rl := &RateLimiter{
		client:        mock,
		policyMgr:     policyMgr,
		defaultPolicy: defPolicy,
		keyPrefix:     "rl:",
		logger:        slog.New(discardHandler{}),
		algos: map[AlgorithmType]algorithms.Algorithm{
			TokenBucket:   algorithms.NewTokenBucket(mock),
			SlidingWindow: algorithms.NewSlidingWindow(mock),
			FixedWindow:   algorithms.NewFixedWindow(mock),
			LeakyBucket:   algorithms.NewLeakyBucket(mock),
		},
	}

	for _, opt := range opts {
		opt(rl)
	}
	return rl
}

// luaAllowedResult returns a mock Lua script response indicating the request
// was allowed with the given remaining count.
func luaAllowedResult(remaining int, resetAt int64) []interface{} {
	return []interface{}{int64(1), int64(remaining), resetAt, int64(0)}
}

// luaDeniedResult returns a mock Lua script response indicating the request
// was denied with the given retry-after duration in milliseconds.
func luaDeniedResult(resetAt int64, retryAfterMs int64) []interface{} {
	return []interface{}{int64(0), int64(0), resetAt, retryAfterMs}
}

// ---------------------------------------------------------------------------
// Config validation tests
// ---------------------------------------------------------------------------

func TestNew_MissingRedisAddr(t *testing.T) {
	_, err := New(Config{})
	if err == nil {
		t.Fatal("expected error for missing RedisAddr")
	}
	if !errors.Is(err, ErrInvalidConfig) {
		t.Errorf("expected ErrInvalidConfig, got: %v", err)
	}
}

func TestNew_InvalidDefaultPolicy(t *testing.T) {
	_, err := New(Config{
		RedisAddr: "localhost:6379",
		DefaultPolicy: &Policy{
			Algorithm: TokenBucket,
			Rate:      -1,
			Window:    time.Minute,
		},
	})
	if err == nil {
		t.Fatal("expected error for invalid default policy")
	}
	if !errors.Is(err, ErrInvalidPolicy) {
		t.Errorf("expected ErrInvalidPolicy, got: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Allow tests — Token Bucket
// ---------------------------------------------------------------------------

func TestAllow_TokenBucket_Allowed(t *testing.T) {
	mock := newMockClient()
	resetAt := time.Now().Add(time.Minute).Unix()
	mock.evalFn = func(_ context.Context, _ string, _ []string, _ ...interface{}) (interface{}, error) {
		return luaAllowedResult(9, resetAt), nil
	}

	rl := newTestLimiter(t, mock)
	result, err := rl.Allow(context.Background(), "user:1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.Allowed {
		t.Error("expected request to be allowed")
	}
	if result.Remaining != 9 {
		t.Errorf("expected 9 remaining, got %d", result.Remaining)
	}
	if result.RetryAfter != 0 {
		t.Errorf("expected zero RetryAfter, got %v", result.RetryAfter)
	}
	if result.Limit != 10 {
		t.Errorf("expected limit 10, got %d", result.Limit)
	}
}

func TestAllow_TokenBucket_Denied(t *testing.T) {
	mock := newMockClient()
	resetAt := time.Now().Add(time.Minute).Unix()
	mock.evalFn = func(_ context.Context, _ string, _ []string, _ ...interface{}) (interface{}, error) {
		return luaDeniedResult(resetAt, 5000), nil
	}

	rl := newTestLimiter(t, mock)
	result, err := rl.Allow(context.Background(), "user:1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Allowed {
		t.Error("expected request to be denied")
	}
	if result.Remaining != 0 {
		t.Errorf("expected 0 remaining, got %d", result.Remaining)
	}
	if result.RetryAfter != 5*time.Second {
		t.Errorf("expected 5s RetryAfter, got %v", result.RetryAfter)
	}
}

// ---------------------------------------------------------------------------
// Allow tests — per-algorithm via WithPolicy
// ---------------------------------------------------------------------------

func TestAllow_SlidingWindow(t *testing.T) {
	mock := newMockClient()
	resetAt := time.Now().Add(time.Minute).Unix()
	mock.evalFn = func(_ context.Context, _ string, _ []string, _ ...interface{}) (interface{}, error) {
		return luaAllowedResult(4, resetAt), nil
	}

	rl := newTestLimiter(t, mock)
	result, err := rl.Allow(context.Background(), "user:2",
		WithPolicy(Policy{
			Algorithm: SlidingWindow,
			Rate:      5,
			Window:    time.Minute,
		}),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.Allowed {
		t.Error("expected request to be allowed")
	}
	if result.Remaining != 4 {
		t.Errorf("expected 4 remaining, got %d", result.Remaining)
	}
	if result.Limit != 5 {
		t.Errorf("expected limit 5, got %d", result.Limit)
	}
}

func TestAllow_FixedWindow(t *testing.T) {
	mock := newMockClient()
	resetAt := time.Now().Add(time.Minute).Unix()
	mock.evalFn = func(_ context.Context, _ string, _ []string, _ ...interface{}) (interface{}, error) {
		return luaAllowedResult(19, resetAt), nil
	}

	rl := newTestLimiter(t, mock)
	result, err := rl.Allow(context.Background(), "user:3",
		WithPolicy(Policy{
			Algorithm: FixedWindow,
			Rate:      20,
			Window:    time.Minute,
		}),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.Allowed {
		t.Error("expected request to be allowed")
	}
	if result.Remaining != 19 {
		t.Errorf("expected 19 remaining, got %d", result.Remaining)
	}
}

func TestAllow_LeakyBucket(t *testing.T) {
	mock := newMockClient()
	resetAt := time.Now().Add(time.Minute).Unix()
	mock.evalFn = func(_ context.Context, _ string, _ []string, _ ...interface{}) (interface{}, error) {
		return luaAllowedResult(7, resetAt), nil
	}

	rl := newTestLimiter(t, mock)
	result, err := rl.Allow(context.Background(), "user:4",
		WithPolicy(Policy{
			Algorithm: LeakyBucket,
			Rate:      8,
			Window:    time.Minute,
		}),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.Allowed {
		t.Error("expected request to be allowed")
	}
	if result.Remaining != 7 {
		t.Errorf("expected 7 remaining, got %d", result.Remaining)
	}
}

// ---------------------------------------------------------------------------
// WithCost tests
// ---------------------------------------------------------------------------

func TestAllow_WithCost(t *testing.T) {
	mock := newMockClient()
	resetAt := time.Now().Add(time.Minute).Unix()

	var capturedCost interface{}
	mock.evalFn = func(_ context.Context, _ string, _ []string, args ...interface{}) (interface{}, error) {
		// For token bucket: ARGV[3] = cost
		if len(args) >= 3 {
			capturedCost = args[2]
		}
		return luaAllowedResult(5, resetAt), nil
	}

	rl := newTestLimiter(t, mock)
	_, err := rl.Allow(context.Background(), "user:5", WithCost(5))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if capturedCost != 5 {
		t.Errorf("expected cost 5 passed to algorithm, got %v", capturedCost)
	}
}

// ---------------------------------------------------------------------------
// GetStatus tests
// ---------------------------------------------------------------------------

func TestGetStatus(t *testing.T) {
	mock := newMockClient()
	resetAt := time.Now().Add(time.Minute).Unix()
	mock.evalFn = func(_ context.Context, _ string, _ []string, _ ...interface{}) (interface{}, error) {
		return luaAllowedResult(8, resetAt), nil
	}

	rl := newTestLimiter(t, mock)
	status, err := rl.GetStatus(context.Background(), "user:1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if status.Remaining != 8 {
		t.Errorf("expected 8 remaining, got %d", status.Remaining)
	}
	if status.Limit != 10 {
		t.Errorf("expected limit 10, got %d", status.Limit)
	}
	if status.ResetAt.Unix() != resetAt {
		t.Errorf("expected reset at %d, got %d", resetAt, status.ResetAt.Unix())
	}
}

// ---------------------------------------------------------------------------
// Reset tests
// ---------------------------------------------------------------------------

func TestReset(t *testing.T) {
	mock := newMockClient()
	mock.data["rl:user:1"] = "some-value"

	rl := newTestLimiter(t, mock)
	err := rl.Reset(context.Background(), "user:1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Close tests
// ---------------------------------------------------------------------------

func TestClose(t *testing.T) {
	mock := newMockClient()
	rl := newTestLimiter(t, mock)

	if err := rl.Close(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !mock.closed {
		t.Error("expected Redis client to be closed")
	}
}

func TestClose_DoubleClose(t *testing.T) {
	mock := newMockClient()
	rl := newTestLimiter(t, mock)

	_ = rl.Close()
	err := rl.Close()
	if !errors.Is(err, ErrLimiterClosed) {
		t.Errorf("expected ErrLimiterClosed on double close, got: %v", err)
	}
}

func TestAllowAfterClose(t *testing.T) {
	mock := newMockClient()
	rl := newTestLimiter(t, mock)
	_ = rl.Close()

	_, err := rl.Allow(context.Background(), "user:1")
	if !errors.Is(err, ErrLimiterClosed) {
		t.Errorf("expected ErrLimiterClosed, got: %v", err)
	}
}

func TestGetStatusAfterClose(t *testing.T) {
	mock := newMockClient()
	rl := newTestLimiter(t, mock)
	_ = rl.Close()

	_, err := rl.GetStatus(context.Background(), "user:1")
	if !errors.Is(err, ErrLimiterClosed) {
		t.Errorf("expected ErrLimiterClosed, got: %v", err)
	}
}

func TestResetAfterClose(t *testing.T) {
	mock := newMockClient()
	rl := newTestLimiter(t, mock)
	_ = rl.Close()

	err := rl.Reset(context.Background(), "user:1")
	if !errors.Is(err, ErrLimiterClosed) {
		t.Errorf("expected ErrLimiterClosed, got: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Policy management tests
// ---------------------------------------------------------------------------

func TestSetPolicy_InvalidPolicy(t *testing.T) {
	mock := newMockClient()
	rl := newTestLimiter(t, mock)

	err := rl.SetPolicy("bad", Policy{Rate: -1, Window: time.Minute})
	if !errors.Is(err, ErrInvalidPolicy) {
		t.Errorf("expected ErrInvalidPolicy, got: %v", err)
	}
}

func TestSetAndGetPolicy(t *testing.T) {
	mock := newMockClient()
	rl := newTestLimiter(t, mock)

	p := Policy{
		Algorithm: SlidingWindow,
		Rate:      50,
		Window:    5 * time.Minute,
	}
	if err := rl.SetPolicy("api", p); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got, err := rl.GetPolicy("api")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Algorithm != SlidingWindow {
		t.Errorf("expected SlidingWindow, got %s", got.Algorithm)
	}
	if got.Rate != 50 {
		t.Errorf("expected rate 50, got %d", got.Rate)
	}
	if got.Window != 5*time.Minute {
		t.Errorf("expected 5m window, got %v", got.Window)
	}
}

func TestGetPolicy_NotFound(t *testing.T) {
	mock := newMockClient()
	rl := newTestLimiter(t, mock)

	_, err := rl.GetPolicy("nonexistent")
	if !errors.Is(err, ErrPolicyNotFound) {
		t.Errorf("expected ErrPolicyNotFound, got: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Unknown algorithm tests
// ---------------------------------------------------------------------------

func TestAllow_UnknownAlgorithm(t *testing.T) {
	mock := newMockClient()
	rl := newTestLimiter(t, mock)

	_, err := rl.Allow(context.Background(), "user:1",
		WithPolicy(Policy{
			Algorithm: "unknown_algo",
			Rate:      10,
			Window:    time.Minute,
		}),
	)
	if !errors.Is(err, ErrInvalidPolicy) {
		t.Errorf("expected ErrInvalidPolicy, got: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Redis error propagation
// ---------------------------------------------------------------------------

func TestAllow_RedisError(t *testing.T) {
	mock := newMockClient()
	mock.evalFn = func(_ context.Context, _ string, _ []string, _ ...interface{}) (interface{}, error) {
		return nil, errors.New("connection refused")
	}

	rl := newTestLimiter(t, mock)
	_, err := rl.Allow(context.Background(), "user:1")
	if err == nil {
		t.Fatal("expected error from Redis")
	}
}

// ---------------------------------------------------------------------------
// Concurrent access test
// ---------------------------------------------------------------------------

func TestAllow_Concurrent(t *testing.T) {
	mock := newMockClient()
	resetAt := time.Now().Add(time.Minute).Unix()
	mock.evalFn = func(_ context.Context, _ string, _ []string, _ ...interface{}) (interface{}, error) {
		return luaAllowedResult(5, resetAt), nil
	}

	rl := newTestLimiter(t, mock)

	var wg sync.WaitGroup
	errs := make(chan error, 100)

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := rl.Allow(context.Background(), "user:concurrent")
			if err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		t.Errorf("unexpected error in concurrent Allow: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Options tests
// ---------------------------------------------------------------------------

func TestResolveOptions_Defaults(t *testing.T) {
	o := resolveOptions(nil)
	if o.cost != 1 {
		t.Errorf("expected default cost 1, got %d", o.cost)
	}
	if o.policy != nil {
		t.Error("expected nil policy by default")
	}
}

func TestResolveOptions_WithCost(t *testing.T) {
	o := resolveOptions([]Option{WithCost(5)})
	if o.cost != 5 {
		t.Errorf("expected cost 5, got %d", o.cost)
	}
}

func TestResolveOptions_WithPolicy(t *testing.T) {
	p := Policy{Algorithm: FixedWindow, Rate: 20, Window: time.Hour}
	o := resolveOptions([]Option{WithPolicy(p)})
	if o.policy == nil {
		t.Fatal("expected non-nil policy")
	}
	if o.policy.Algorithm != FixedWindow {
		t.Errorf("expected FixedWindow, got %s", o.policy.Algorithm)
	}
}

// ---------------------------------------------------------------------------
// Conversion helpers tests
// ---------------------------------------------------------------------------

func TestFromAlgoResult(t *testing.T) {
	now := time.Now().Unix()
	r := fromAlgoResult(&algorithms.Result{
		Allowed:      true,
		Remaining:    42,
		ResetAtUnix:  now,
		RetryAfterMs: 0,
		Limit:        100,
	})
	if !r.Allowed {
		t.Error("expected allowed")
	}
	if r.Remaining != 42 {
		t.Errorf("expected 42, got %d", r.Remaining)
	}
	if r.ResetAt.Unix() != now {
		t.Errorf("expected reset at %d, got %d", now, r.ResetAt.Unix())
	}
	if r.Limit != 100 {
		t.Errorf("expected limit 100, got %d", r.Limit)
	}
}

func TestFromAlgoResult_Denied(t *testing.T) {
	r := fromAlgoResult(&algorithms.Result{
		Allowed:      false,
		Remaining:    0,
		ResetAtUnix:  time.Now().Unix(),
		RetryAfterMs: 3000,
		Limit:        10,
	})
	if r.Allowed {
		t.Error("expected denied")
	}
	if r.RetryAfter != 3*time.Second {
		t.Errorf("expected 3s, got %v", r.RetryAfter)
	}
}
