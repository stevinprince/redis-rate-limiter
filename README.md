# ratelimiter

Distributed rate limiting for Go applications, backed by Redis.

[![Go Reference](https://pkg.go.dev/badge/github.com/stevin/ratelimiter.svg)](https://pkg.go.dev/github.com/stevin/ratelimiter)
[![Go Report Card](https://goreportcard.com/badge/github.com/stevin/ratelimiter)](https://goreportcard.com/report/github.com/stevin/ratelimiter)

## Install

```bash
go get github.com/stevin/ratelimiter
```

Requires Go 1.21+ and a running Redis instance.

## Quick Start

```go
package main

import (
    "context"
    "fmt"
    "time"

    "github.com/stevin/ratelimiter"
)

func main() {
    rl, err := ratelimiter.New(ratelimiter.Config{
        RedisAddr: "localhost:6379",
    })
    if err != nil {
        panic(err)
    }
    defer rl.Close()

    result, _ := rl.Allow(context.Background(), "user:123")
    fmt.Printf("allowed=%v remaining=%d\n", result.Allowed, result.Remaining)
}
```

## Algorithms

Four rate limiting strategies are available, selectable per-policy:

| Algorithm | Constant | Behavior | Best For |
|-----------|----------|----------|----------|
| **Token Bucket** | `ratelimiter.TokenBucket` | Refills tokens at a steady rate; allows bursts up to capacity | APIs that allow short bursts but enforce average rate |
| **Sliding Window** | `ratelimiter.SlidingWindow` | Counts requests in a rolling time window with millisecond precision | Precise enforcement without edge-burst problems |
| **Fixed Window** | `ratelimiter.FixedWindow` | Counts requests in fixed time intervals | Simple use cases where boundary bursts are acceptable |
| **Leaky Bucket** | `ratelimiter.LeakyBucket` | Processes requests at a constant drain rate | Smoothing bursty traffic into a steady stream |

All algorithms use **atomic Redis Lua scripts** — no race conditions under concurrent access.

## API Reference

### Constructor

```go
rl, err := ratelimiter.New(ratelimiter.Config{
    RedisAddr:     "localhost:6379",
    RedisPassword: "",              // optional
    RedisDB:       0,               // optional
    KeyPrefix:     "rl:",           // optional, default "rl:"
    DefaultPolicy: &ratelimiter.Policy{
        Algorithm: ratelimiter.TokenBucket,
        Rate:      100,
        Window:    time.Minute,
    },
    PolicyCacheTTL: time.Minute,    // optional
    Logger:         slog.Default(), // optional, *slog.Logger
})
```

### Core Operations

```go
// Check if a request is allowed (consumes 1 token)
result, err := rl.Allow(ctx, "user:123")

// Check with a specific policy
result, err := rl.Allow(ctx, "user:123", ratelimiter.WithPolicy(policy))

// Check with custom cost (e.g., batch operations)
result, err := rl.Allow(ctx, "user:123", ratelimiter.WithCost(5))

// Peek at current status without consuming
status, err := rl.GetStatus(ctx, "user:123")

// Reset a key's rate limit state
err := rl.Reset(ctx, "user:123")

// Clean up
err := rl.Close()
```

### Result & Status Types

```go
type Result struct {
    Allowed    bool          // whether the request is permitted
    Remaining  int           // requests remaining in current window
    ResetAt    time.Time     // when the window resets
    RetryAfter time.Duration // how long to wait (zero if allowed)
    Limit      int           // total requests allowed per window
}

type Status struct {
    Remaining int       // requests remaining
    Limit     int       // total allowed per window
    ResetAt   time.Time // when the window resets
    Used      int       // requests consumed so far
}
```

## HTTP Middleware

The middleware works with any `net/http` compatible router.

### With `net/http` (stdlib)

```go
policy := ratelimiter.Policy{
    Algorithm: ratelimiter.TokenBucket,
    Rate:      100,
    Window:    time.Minute,
    Burst:     20,
}

mux := http.NewServeMux()
mux.HandleFunc("/api/data", handleData)

handler := rl.HTTPMiddleware(policy, ratelimiter.KeyByIP)(mux)
http.ListenAndServe(":8080", handler)
```

### With chi

```go
r := chi.NewRouter()
r.Use(rl.HTTPMiddleware(policy, ratelimiter.KeyByIP))
r.Get("/api/data", handleData)
```

### With gorilla/mux

```go
r := mux.NewRouter()
r.Use(rl.HTTPMiddleware(policy, ratelimiter.KeyByIP))
r.HandleFunc("/api/data", handleData)
```

### Response Headers

The middleware automatically sets these headers on **every** response:

| Header | Description |
|--------|-------------|
| `X-RateLimit-Limit` | Total requests allowed per window |
| `X-RateLimit-Remaining` | Requests remaining |
| `X-RateLimit-Reset` | Unix timestamp when the window resets |
| `Retry-After` | Seconds to wait (only on 429 responses) |

When rate limited, the response is `429 Too Many Requests` with a JSON body:

```json
{
    "error": "rate limit exceeded",
    "retry_after": 30
}
```

### Key Extractors

Built-in key extractors for common patterns:

```go
// By client IP (respects X-Forwarded-For, X-Real-IP)
ratelimiter.KeyByIP

// By HTTP header value (e.g., API key)
ratelimiter.KeyByHeader("X-API-Key")

// By IP + method + path (per-endpoint limiting)
ratelimiter.KeyByEndpoint

// Custom key function
func myKeyFunc(r *http.Request) string {
    userID := getUserID(r)
    return "user:" + userID
}
```

## Policy Configuration

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `Algorithm` | `AlgorithmType` | `TokenBucket` | Rate limiting algorithm |
| `Rate` | `int` | **required** | Max requests per window |
| `Window` | `time.Duration` | **required** | Time window duration |
| `Burst` | `int` | `Rate` | Max burst size (Token Bucket only) |
| `RefillRate` | `float64` | `Rate/Window` | Tokens added per second (Token Bucket only) |
| `DrainRate` | `float64` | `Rate/Window` | Requests drained per second (Leaky Bucket only) |

### Named Policies

Register policies by name so all instances share them via Redis:

```go
// Register
rl.SetPolicy("premium", ratelimiter.Policy{
    Algorithm: ratelimiter.TokenBucket,
    Rate:      1000,
    Window:    time.Minute,
    Burst:     100,
})

// Retrieve (from any instance)
policy, err := rl.GetPolicy("premium")
```

## Advanced Usage

### Per-Route Policies

```go
apiPolicy := ratelimiter.Policy{
    Algorithm: ratelimiter.SlidingWindow,
    Rate:      100,
    Window:    time.Minute,
}
uploadPolicy := ratelimiter.Policy{
    Algorithm: ratelimiter.LeakyBucket,
    Rate:      10,
    Window:    time.Minute,
}

mux := http.NewServeMux()
mux.Handle("/api/", rl.HTTPMiddleware(apiPolicy, ratelimiter.KeyByIP)(apiHandler))
mux.Handle("/upload", rl.HTTPMiddleware(uploadPolicy, ratelimiter.KeyByIP)(uploadHandler))
```

### Cost-Based Limiting

Charge different costs for different operations:

```go
// Normal request — costs 1 token
result, _ := rl.Allow(ctx, "user:123")

// Expensive operation — costs 10 tokens
result, _ := rl.Allow(ctx, "user:123", ratelimiter.WithCost(10))

// Batch operation — costs N tokens
result, _ := rl.Allow(ctx, "user:123", ratelimiter.WithCost(len(items)))
```

### Custom Key Functions

```go
// Composite key: user + endpoint
keyFunc := func(r *http.Request) string {
    user := r.Header.Get("X-User-ID")
    return fmt.Sprintf("user:%s:%s:%s", user, r.Method, r.URL.Path)
}

handler := rl.HTTPMiddleware(policy, keyFunc)(mux)
```

### Structured Logging

Pass a `*slog.Logger` to see rate limiter activity:

```go
logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
    Level: slog.LevelDebug,
}))

rl, _ := ratelimiter.New(ratelimiter.Config{
    RedisAddr: "localhost:6379",
    Logger:    logger,
})
```

## Error Handling

The library returns sentinel errors that can be matched with `errors.Is()`:

```go
result, err := rl.Allow(ctx, key)
if errors.Is(err, ratelimiter.ErrLimiterClosed) {
    // limiter was closed
}
if errors.Is(err, ratelimiter.ErrRedisUnavailable) {
    // Redis connection failed
}
if errors.Is(err, ratelimiter.ErrInvalidPolicy) {
    // bad policy configuration
}
if errors.Is(err, ratelimiter.ErrPolicyNotFound) {
    // named policy doesn't exist
}
```

## Testing

### Unit Tests

Unit tests use a mock Redis client and don't require a running Redis instance:

```bash
go test -v ./...
```

### Integration Tests

Integration tests require a running Redis instance. Use the provided Docker Compose file:

```bash
# Start Redis
docker compose up -d

# Run integration tests
go test -tags integration -v -count=1 ./...

# Stop Redis
docker compose down
```

The integration tests verify Lua script atomicity under concurrent access — 100 goroutines hitting the same key with a limit of 50 must result in exactly 50 allowed requests.

## Project Structure

```
ratelimiter/
├── ratelimiter.go          # Public API: types, constructor, core methods
├── middleware.go            # HTTP middleware and key extractors
├── options.go              # Functional options (WithPolicy, WithCost)
├── internal/
│   ├── redis/
│   │   └── client.go       # Redis client interface and wrapper
│   ├── algorithms/
│   │   ├── algorithm.go    # Algorithm interface
│   │   ├── token_bucket.go # Token bucket + Lua script
│   │   ├── sliding_window.go
│   │   ├── fixed_window.go
│   │   └── leaky_bucket.go
│   └── policy/
│       └── manager.go      # Policy storage and caching
├── ratelimiter_test.go     # Unit tests (mock Redis)
├── middleware_test.go      # Middleware and key extractor tests
├── integration_test.go     # Integration tests (real Redis)
├── example_test.go         # Testable examples for godoc
├── docker-compose.yml      # Redis for integration tests
├── go.mod
└── go.sum
```

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/my-feature`)
3. Write tests for your changes
4. Ensure all tests pass (`go test ./...`)
5. Run `go vet ./...` and fix any issues
6. Commit your changes (`git commit -am 'Add my feature'`)
7. Push to the branch (`git push origin feature/my-feature`)
8. Open a Pull Request

### Running the Full Test Suite

```bash
# Unit tests
go test -v ./...

# Integration tests (requires Docker)
docker compose up -d
go test -tags integration -v -count=1 ./...
docker compose down

# Vet and lint
go vet ./...
```

## License

MIT
