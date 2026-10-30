//go:build integration

package ratelimiter_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stevin/ratelimiter"
)

// redisAddr returns the Redis address for integration tests.
// It defaults to localhost:6379 but can be overridden via REDIS_ADDR.
func redisAddr() string {
	if addr := os.Getenv("REDIS_ADDR"); addr != "" {
		return addr
	}
	return "localhost:6379"
}

// newIntegrationLimiter creates a RateLimiter connected to a real Redis
// instance. It calls t.Fatal if the connection fails.
func newIntegrationLimiter(t *testing.T, policy *ratelimiter.Policy) *ratelimiter.RateLimiter {
	t.Helper()

	cfg := ratelimiter.Config{
		RedisAddr:     redisAddr(),
		KeyPrefix:     fmt.Sprintf("rl:test:%d:", time.Now().UnixNano()),
		DefaultPolicy: policy,
	}
	rl, err := ratelimiter.New(cfg)
	if err != nil {
		t.Fatalf("failed to create rate limiter: %v", err)
	}
	t.Cleanup(func() { _ = rl.Close() })
	return rl
}

// ---------------------------------------------------------------------------
// Token Bucket integration tests
// ---------------------------------------------------------------------------

func TestIntegration_TokenBucket_BasicFlow(t *testing.T) {
	policy := &ratelimiter.Policy{
		Algorithm: ratelimiter.TokenBucket,
		Rate:      5,
		Window:    time.Minute,
		Burst:     5,
	}
	rl := newIntegrationLimiter(t, policy)
	ctx := context.Background()

	key := "token_bucket_basic"

	// Reset to start clean
	if err := rl.Reset(ctx, key); err != nil {
		t.Fatalf("reset: %v", err)
	}

	// First 5 requests should be allowed
	for i := 0; i < 5; i++ {
		result, err := rl.Allow(ctx, key)
		if err != nil {
			t.Fatalf("request %d: %v", i+1, err)
		}
		if !result.Allowed {
			t.Errorf("request %d: expected allowed", i+1)
		}
		if result.Remaining != 5-i-1 {
			t.Errorf("request %d: expected %d remaining, got %d", i+1, 5-i-1, result.Remaining)
		}
	}

	// 6th request should be denied
	result, err := rl.Allow(ctx, key)
	if err != nil {
		t.Fatalf("request 6: %v", err)
	}
	if result.Allowed {
		t.Error("request 6: expected denied")
	}
	if result.RetryAfter <= 0 {
		t.Error("expected positive RetryAfter on denial")
	}
}

func TestIntegration_TokenBucket_GetStatus(t *testing.T) {
	policy := &ratelimiter.Policy{
		Algorithm: ratelimiter.TokenBucket,
		Rate:      10,
		Window:    time.Minute,
		Burst:     10,
	}
	rl := newIntegrationLimiter(t, policy)
	ctx := context.Background()

	key := "token_bucket_status"
	_ = rl.Reset(ctx, key)

	// Consume 3 tokens
	for i := 0; i < 3; i++ {
		_, _ = rl.Allow(ctx, key)
	}

	status, err := rl.GetStatus(ctx, key)
	if err != nil {
		t.Fatalf("get status: %v", err)
	}
	if status.Limit != 10 {
		t.Errorf("expected limit 10, got %d", status.Limit)
	}
	if status.Remaining > 7 {
		t.Errorf("expected remaining <= 7 after 3 requests, got %d", status.Remaining)
	}
}

// ---------------------------------------------------------------------------
// Sliding Window integration tests
// ---------------------------------------------------------------------------

func TestIntegration_SlidingWindow_BasicFlow(t *testing.T) {
	policy := ratelimiter.Policy{
		Algorithm: ratelimiter.SlidingWindow,
		Rate:      5,
		Window:    time.Minute,
	}
	rl := newIntegrationLimiter(t, nil)
	ctx := context.Background()

	key := "sliding_window_basic"
	_ = rl.Reset(ctx, key)

	// 5 requests should be allowed
	for i := 0; i < 5; i++ {
		result, err := rl.Allow(ctx, key, ratelimiter.WithPolicy(policy))
		if err != nil {
			t.Fatalf("request %d: %v", i+1, err)
		}
		if !result.Allowed {
			t.Errorf("request %d: expected allowed", i+1)
		}
	}

	// 6th should be denied
	result, err := rl.Allow(ctx, key, ratelimiter.WithPolicy(policy))
	if err != nil {
		t.Fatalf("request 6: %v", err)
	}
	if result.Allowed {
		t.Error("request 6: expected denied")
	}
}

// ---------------------------------------------------------------------------
// Fixed Window integration tests
// ---------------------------------------------------------------------------

func TestIntegration_FixedWindow_BasicFlow(t *testing.T) {
	policy := ratelimiter.Policy{
		Algorithm: ratelimiter.FixedWindow,
		Rate:      5,
		Window:    time.Minute,
	}
	rl := newIntegrationLimiter(t, nil)
	ctx := context.Background()

	key := "fixed_window_basic"
	_ = rl.Reset(ctx, key)

	for i := 0; i < 5; i++ {
		result, err := rl.Allow(ctx, key, ratelimiter.WithPolicy(policy))
		if err != nil {
			t.Fatalf("request %d: %v", i+1, err)
		}
		if !result.Allowed {
			t.Errorf("request %d: expected allowed", i+1)
		}
	}

	result, err := rl.Allow(ctx, key, ratelimiter.WithPolicy(policy))
	if err != nil {
		t.Fatalf("request 6: %v", err)
	}
	if result.Allowed {
		t.Error("request 6: expected denied")
	}
}

// ---------------------------------------------------------------------------
// Leaky Bucket integration tests
// ---------------------------------------------------------------------------

func TestIntegration_LeakyBucket_BasicFlow(t *testing.T) {
	policy := ratelimiter.Policy{
		Algorithm: ratelimiter.LeakyBucket,
		Rate:      5,
		Window:    time.Minute,
	}
	rl := newIntegrationLimiter(t, nil)
	ctx := context.Background()

	key := "leaky_bucket_basic"
	_ = rl.Reset(ctx, key)

	for i := 0; i < 5; i++ {
		result, err := rl.Allow(ctx, key, ratelimiter.WithPolicy(policy))
		if err != nil {
			t.Fatalf("request %d: %v", i+1, err)
		}
		if !result.Allowed {
			t.Errorf("request %d: expected allowed", i+1)
		}
	}

	result, err := rl.Allow(ctx, key, ratelimiter.WithPolicy(policy))
	if err != nil {
		t.Fatalf("request 6: %v", err)
	}
	if result.Allowed {
		t.Error("request 6: expected denied")
	}
}

// ---------------------------------------------------------------------------
// WithCost integration test
// ---------------------------------------------------------------------------

func TestIntegration_WithCost(t *testing.T) {
	policy := &ratelimiter.Policy{
		Algorithm: ratelimiter.TokenBucket,
		Rate:      10,
		Window:    time.Minute,
		Burst:     10,
	}
	rl := newIntegrationLimiter(t, policy)
	ctx := context.Background()

	key := "with_cost"
	_ = rl.Reset(ctx, key)

	// Consume 5 tokens at once
	result, err := rl.Allow(ctx, key, ratelimiter.WithCost(5))
	if err != nil {
		t.Fatalf("cost-5 request: %v", err)
	}
	if !result.Allowed {
		t.Error("cost-5 request: expected allowed")
	}
	if result.Remaining != 5 {
		t.Errorf("expected 5 remaining, got %d", result.Remaining)
	}

	// Consume 6 more — should be denied (only 5 remain)
	result, err = rl.Allow(ctx, key, ratelimiter.WithCost(6))
	if err != nil {
		t.Fatalf("cost-6 request: %v", err)
	}
	if result.Allowed {
		t.Error("cost-6 request: expected denied (only 5 tokens remain)")
	}
}

// ---------------------------------------------------------------------------
// Concurrent atomicity test — the critical integration test
// ---------------------------------------------------------------------------

func TestIntegration_ConcurrentAtomicity(t *testing.T) {
	limit := 50
	policy := &ratelimiter.Policy{
		Algorithm: ratelimiter.TokenBucket,
		Rate:      limit,
		Window:    time.Minute,
		Burst:     limit,
	}
	rl := newIntegrationLimiter(t, policy)
	ctx := context.Background()

	key := "concurrent_atomicity"
	_ = rl.Reset(ctx, key)

	goroutines := 100
	var allowed atomic.Int64
	var denied atomic.Int64
	var wg sync.WaitGroup

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			result, err := rl.Allow(ctx, key)
			if err != nil {
				t.Errorf("concurrent allow: %v", err)
				return
			}
			if result.Allowed {
				allowed.Add(1)
			} else {
				denied.Add(1)
			}
		}()
	}
	wg.Wait()

	a := allowed.Load()
	d := denied.Load()

	if a != int64(limit) {
		t.Errorf("expected exactly %d allowed, got %d (denied: %d)", limit, a, d)
	}
	if a+d != int64(goroutines) {
		t.Errorf("expected %d total, got %d", goroutines, a+d)
	}
}

func TestIntegration_ConcurrentAtomicity_SlidingWindow(t *testing.T) {
	limit := 30
	policy := ratelimiter.Policy{
		Algorithm: ratelimiter.SlidingWindow,
		Rate:      limit,
		Window:    time.Minute,
	}
	rl := newIntegrationLimiter(t, nil)
	ctx := context.Background()

	key := "concurrent_sliding"
	_ = rl.Reset(ctx, key)

	goroutines := 60
	var allowed atomic.Int64
	var wg sync.WaitGroup

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			result, err := rl.Allow(ctx, key, ratelimiter.WithPolicy(policy))
			if err != nil {
				t.Errorf("concurrent allow: %v", err)
				return
			}
			if result.Allowed {
				allowed.Add(1)
			}
		}()
	}
	wg.Wait()

	a := allowed.Load()
	if a != int64(limit) {
		t.Errorf("expected exactly %d allowed, got %d", limit, a)
	}
}

// ---------------------------------------------------------------------------
// Policy management integration test
// ---------------------------------------------------------------------------

func TestIntegration_PolicyManagement(t *testing.T) {
	rl := newIntegrationLimiter(t, nil)

	p := ratelimiter.Policy{
		Algorithm: ratelimiter.SlidingWindow,
		Rate:      100,
		Window:    5 * time.Minute,
	}
	if err := rl.SetPolicy("api_default", p); err != nil {
		t.Fatalf("set policy: %v", err)
	}

	got, err := rl.GetPolicy("api_default")
	if err != nil {
		t.Fatalf("get policy: %v", err)
	}
	if got.Algorithm != ratelimiter.SlidingWindow {
		t.Errorf("expected SlidingWindow, got %s", got.Algorithm)
	}
	if got.Rate != 100 {
		t.Errorf("expected rate 100, got %d", got.Rate)
	}
	if got.Window != 5*time.Minute {
		t.Errorf("expected 5m window, got %v", got.Window)
	}
}

// ---------------------------------------------------------------------------
// Reset integration test
// ---------------------------------------------------------------------------

func TestIntegration_Reset(t *testing.T) {
	policy := &ratelimiter.Policy{
		Algorithm: ratelimiter.TokenBucket,
		Rate:      3,
		Window:    time.Minute,
		Burst:     3,
	}
	rl := newIntegrationLimiter(t, policy)
	ctx := context.Background()

	key := "reset_test"
	_ = rl.Reset(ctx, key)

	// Exhaust the limit
	for i := 0; i < 3; i++ {
		_, _ = rl.Allow(ctx, key)
	}
	result, _ := rl.Allow(ctx, key)
	if result.Allowed {
		t.Error("should be denied after exhaustion")
	}

	// Reset and try again
	if err := rl.Reset(ctx, key); err != nil {
		t.Fatalf("reset: %v", err)
	}
	result, err := rl.Allow(ctx, key)
	if err != nil {
		t.Fatalf("after reset: %v", err)
	}
	if !result.Allowed {
		t.Error("expected allowed after reset")
	}
}

// ---------------------------------------------------------------------------
// HTTP Middleware integration test
// ---------------------------------------------------------------------------

func TestIntegration_HTTPMiddleware(t *testing.T) {
	rl := newIntegrationLimiter(t, nil)

	policy := ratelimiter.Policy{
		Algorithm: ratelimiter.TokenBucket,
		Rate:      3,
		Window:    time.Minute,
		Burst:     3,
	}

	handler := rl.HTTPMiddleware(policy, ratelimiter.KeyByIP)(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("OK"))
		}),
	)

	// Use a unique IP to avoid collisions
	clientIP := fmt.Sprintf("10.%d.%d.%d:1234",
		time.Now().Nanosecond()%256,
		time.Now().UnixMicro()%256,
		time.Now().UnixNano()%256,
	)

	// First 3 should pass
	for i := 0; i < 3; i++ {
		req := httptest.NewRequest("GET", "/", nil)
		req.RemoteAddr = clientIP
		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, req)

		if rr.Code != http.StatusOK {
			t.Errorf("request %d: expected 200, got %d", i+1, rr.Code)
		}
		if rr.Header().Get("X-RateLimit-Limit") == "" {
			t.Errorf("request %d: missing X-RateLimit-Limit header", i+1)
		}
	}

	// 4th should get 429
	req := httptest.NewRequest("GET", "/", nil)
	req.RemoteAddr = clientIP
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusTooManyRequests {
		t.Errorf("request 4: expected 429, got %d", rr.Code)
	}
	if rr.Header().Get("Retry-After") == "" {
		t.Error("request 4: missing Retry-After header")
	}
}

// ---------------------------------------------------------------------------
// Close integration test
// ---------------------------------------------------------------------------

func TestIntegration_Close(t *testing.T) {
	rl := newIntegrationLimiter(t, nil)

	if err := rl.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	_, err := rl.Allow(context.Background(), "test")
	if err == nil {
		t.Error("expected error after close")
	}
}
