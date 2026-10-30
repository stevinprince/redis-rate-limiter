package ratelimiter

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Key extractor tests
// ---------------------------------------------------------------------------

func TestKeyByIP_RemoteAddr(t *testing.T) {
	r := httptest.NewRequest("GET", "/", nil)
	r.RemoteAddr = "192.168.1.1:12345"

	key := KeyByIP(r)
	if key != "ip:192.168.1.1" {
		t.Errorf("expected ip:192.168.1.1, got %s", key)
	}
}

func TestKeyByIP_XForwardedFor(t *testing.T) {
	r := httptest.NewRequest("GET", "/", nil)
	r.RemoteAddr = "127.0.0.1:12345"
	r.Header.Set("X-Forwarded-For", "10.0.0.1, 10.0.0.2")

	key := KeyByIP(r)
	if key != "ip:10.0.0.1" {
		t.Errorf("expected ip:10.0.0.1, got %s", key)
	}
}

func TestKeyByIP_XRealIP(t *testing.T) {
	r := httptest.NewRequest("GET", "/", nil)
	r.RemoteAddr = "127.0.0.1:12345"
	r.Header.Set("X-Real-IP", "172.16.0.1")

	key := KeyByIP(r)
	if key != "ip:172.16.0.1" {
		t.Errorf("expected ip:172.16.0.1, got %s", key)
	}
}

func TestKeyByIP_XForwardedForTakesPrecedence(t *testing.T) {
	r := httptest.NewRequest("GET", "/", nil)
	r.RemoteAddr = "127.0.0.1:12345"
	r.Header.Set("X-Forwarded-For", "10.0.0.1")
	r.Header.Set("X-Real-IP", "172.16.0.1")

	key := KeyByIP(r)
	if key != "ip:10.0.0.1" {
		t.Errorf("expected X-Forwarded-For to take precedence, got %s", key)
	}
}

func TestKeyByIP_NoPort(t *testing.T) {
	r := httptest.NewRequest("GET", "/", nil)
	r.RemoteAddr = "192.168.1.1"

	key := KeyByIP(r)
	if key != "ip:192.168.1.1" {
		t.Errorf("expected ip:192.168.1.1, got %s", key)
	}
}

func TestKeyByHeader_Present(t *testing.T) {
	r := httptest.NewRequest("GET", "/", nil)
	r.RemoteAddr = "127.0.0.1:12345"
	r.Header.Set("X-API-Key", "abc123")

	keyFn := KeyByHeader("X-API-Key")
	key := keyFn(r)
	if key != "header:abc123" {
		t.Errorf("expected header:abc123, got %s", key)
	}
}

func TestKeyByHeader_Missing_FallsBackToIP(t *testing.T) {
	r := httptest.NewRequest("GET", "/", nil)
	r.RemoteAddr = "192.168.1.1:12345"

	keyFn := KeyByHeader("X-API-Key")
	key := keyFn(r)
	if key != "ip:192.168.1.1" {
		t.Errorf("expected fallback to ip:192.168.1.1, got %s", key)
	}
}

func TestKeyByEndpoint(t *testing.T) {
	r := httptest.NewRequest("POST", "/api/users", nil)
	r.RemoteAddr = "10.0.0.5:9999"

	key := KeyByEndpoint(r)
	expected := "ip:10.0.0.5:POST:/api/users"
	if key != expected {
		t.Errorf("expected %s, got %s", expected, key)
	}
}

// ---------------------------------------------------------------------------
// Middleware tests
// ---------------------------------------------------------------------------

// okHandler responds with 200 OK and a body.
var okHandler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("OK"))
})

func TestHTTPMiddleware_Allowed(t *testing.T) {
	mock := newMockClient()
	resetAt := time.Now().Add(time.Minute).Unix()
	mock.evalFn = func(_ context.Context, _ string, _ []string, _ ...interface{}) (interface{}, error) {
		return luaAllowedResult(9, resetAt), nil
	}

	rl := newTestLimiter(t, mock)

	policy := Policy{
		Algorithm: TokenBucket,
		Rate:      10,
		Window:    time.Minute,
	}
	middleware := rl.HTTPMiddleware(policy, KeyByIP)
	handler := middleware(okHandler)

	req := httptest.NewRequest("GET", "/", nil)
	req.RemoteAddr = "192.168.1.1:1234"
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rr.Code)
	}

	// Check rate limit headers
	if got := rr.Header().Get("X-RateLimit-Limit"); got != "10" {
		t.Errorf("expected X-RateLimit-Limit=10, got %s", got)
	}
	if got := rr.Header().Get("X-RateLimit-Remaining"); got != "9" {
		t.Errorf("expected X-RateLimit-Remaining=9, got %s", got)
	}
	if got := rr.Header().Get("X-RateLimit-Reset"); got == "" {
		t.Error("expected X-RateLimit-Reset header to be set")
	}
	if got := rr.Header().Get("Retry-After"); got != "" {
		t.Errorf("expected no Retry-After header on allowed request, got %s", got)
	}
}

func TestHTTPMiddleware_RateLimited(t *testing.T) {
	mock := newMockClient()
	resetAt := time.Now().Add(time.Minute).Unix()
	mock.evalFn = func(_ context.Context, _ string, _ []string, _ ...interface{}) (interface{}, error) {
		return luaDeniedResult(resetAt, 30000), nil
	}

	rl := newTestLimiter(t, mock)

	policy := Policy{
		Algorithm: TokenBucket,
		Rate:      10,
		Window:    time.Minute,
	}
	middleware := rl.HTTPMiddleware(policy, KeyByIP)
	handler := middleware(okHandler)

	req := httptest.NewRequest("GET", "/", nil)
	req.RemoteAddr = "192.168.1.1:1234"
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusTooManyRequests {
		t.Errorf("expected 429, got %d", rr.Code)
	}

	// Check headers
	if got := rr.Header().Get("X-RateLimit-Remaining"); got != "0" {
		t.Errorf("expected X-RateLimit-Remaining=0, got %s", got)
	}
	if got := rr.Header().Get("Retry-After"); got != "30" {
		t.Errorf("expected Retry-After=30, got %s", got)
	}
	if got := rr.Header().Get("Content-Type"); got != "application/json" {
		t.Errorf("expected Content-Type=application/json, got %s", got)
	}

	// Check JSON body
	var body rateLimitResponse
	if err := json.NewDecoder(rr.Body).Decode(&body); err != nil {
		t.Fatalf("failed to decode response body: %v", err)
	}
	if body.Error != "rate limit exceeded" {
		t.Errorf("expected error message 'rate limit exceeded', got %q", body.Error)
	}
	if body.RetryAfter != 30 {
		t.Errorf("expected retry_after=30, got %d", body.RetryAfter)
	}
}

func TestHTTPMiddleware_RedisError_FailsOpen(t *testing.T) {
	mock := newMockClient()
	mock.evalFn = func(_ context.Context, _ string, _ []string, _ ...interface{}) (interface{}, error) {
		return nil, errors.New("connection refused")
	}

	rl := newTestLimiter(t, mock)

	policy := Policy{
		Algorithm: TokenBucket,
		Rate:      10,
		Window:    time.Minute,
	}
	middleware := rl.HTTPMiddleware(policy, KeyByIP)

	var handlerCalled bool
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handlerCalled = true
		w.WriteHeader(http.StatusOK)
	})
	handler := middleware(inner)

	req := httptest.NewRequest("GET", "/", nil)
	req.RemoteAddr = "192.168.1.1:1234"
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	if !handlerCalled {
		t.Error("expected handler to be called (fail open) on Redis error")
	}
	if rr.Code != http.StatusOK {
		t.Errorf("expected 200 on fail open, got %d", rr.Code)
	}
}

func TestHTTPMiddleware_CustomKeyFunc(t *testing.T) {
	mock := newMockClient()
	resetAt := time.Now().Add(time.Minute).Unix()

	var capturedKey string
	mock.evalFn = func(_ context.Context, _ string, keys []string, _ ...interface{}) (interface{}, error) {
		if len(keys) > 0 {
			capturedKey = keys[0]
		}
		return luaAllowedResult(9, resetAt), nil
	}

	rl := newTestLimiter(t, mock)

	policy := Policy{
		Algorithm: TokenBucket,
		Rate:      10,
		Window:    time.Minute,
	}
	customKey := func(r *http.Request) string {
		return "tenant:" + r.Header.Get("X-Tenant-ID")
	}
	middleware := rl.HTTPMiddleware(policy, customKey)
	handler := middleware(okHandler)

	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("X-Tenant-ID", "acme")
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rr.Code)
	}
	// The key should be prefixed: "rl:" + "tenant:acme"
	expected := "rl:tenant:acme"
	if capturedKey != expected {
		t.Errorf("expected Redis key %q, got %q", expected, capturedKey)
	}
}

func TestHTTPMiddleware_RetryAfter_MinimumOneSecond(t *testing.T) {
	mock := newMockClient()
	resetAt := time.Now().Add(time.Second).Unix()
	// retryAfterMs = 100 → less than 1 second → should be clamped to 1
	mock.evalFn = func(_ context.Context, _ string, _ []string, _ ...interface{}) (interface{}, error) {
		return luaDeniedResult(resetAt, 100), nil
	}

	rl := newTestLimiter(t, mock)
	policy := Policy{Algorithm: TokenBucket, Rate: 1, Window: time.Second}
	middleware := rl.HTTPMiddleware(policy, KeyByIP)
	handler := middleware(okHandler)

	req := httptest.NewRequest("GET", "/", nil)
	req.RemoteAddr = "10.0.0.1:5555"
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	if got := rr.Header().Get("Retry-After"); got != "1" {
		t.Errorf("expected Retry-After clamped to 1, got %s", got)
	}
}
