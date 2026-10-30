# ratelimiter

Distributed rate limiting for Go applications, backed by Redis.

A **library** that any Go service can import via `go get` and start rate limiting in minutes. Whether you're building an HTTP API, a gRPC service, a queue consumer, or a CLI tool — if it talks to Redis, it can rate limit.

[![Go Reference](https://pkg.go.dev/badge/github.com/stevin/ratelimiter.svg)](https://pkg.go.dev/github.com/stevin/ratelimiter)
[![Go Report Card](https://goreportcard.com/badge/github.com/stevin/ratelimiter)](https://goreportcard.com/report/github.com/stevin/ratelimiter)

## Install

```bash
go get github.com/stevin/ratelimiter
```

Requires **Go 1.21+** and a running **Redis** instance.

## Quick Start

```go
package main

import (
    "context"
    "fmt"

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

That's it — 10 lines to distributed rate limiting. The default policy is **100 requests per minute** using the **Token Bucket** algorithm.

---

## Integration Guide

This library is designed to be imported by **any Go service**. There are three main integration patterns depending on what you're building.

### Prerequisites

Every service that uses this library needs access to a **Redis instance**. Redis is the shared backend that makes rate limiting distributed — multiple instances of your service all share the same counters.

```
┌──────────────────┐     ┌──────────────────┐     ┌──────────────────┐
│  Service A (x3)  │     │  Service B (x2)  │     │  Service C (x1)  │
│  go get ratelim  │     │  go get ratelim  │     │  go get ratelim  │
└────────┬─────────┘     └────────┬─────────┘     └────────┬─────────┘
         │                        │                        │
         └────────────────────────┼────────────────────────┘
                                  │
                          ┌───────▼───────┐
                          │     Redis     │
                          │  (shared)     │
                          └───────────────┘
```

### 1. Direct / Programmatic Usage (Any Go Service)

Use the `Allow()` method directly in any business logic — HTTP handlers, gRPC methods, background workers, CLI commands, queue consumers, or anywhere you need to throttle operations.

```go
package main

import (
    "context"
    "log"
    "time"

    "github.com/stevin/ratelimiter"
)

func main() {
    // Create once at startup (e.g., in main() or your DI container)
    rl, err := ratelimiter.New(ratelimiter.Config{
        RedisAddr: "your-redis:6379",
        KeyPrefix: "myservice:",  // isolate keys per service
        DefaultPolicy: &ratelimiter.Policy{
            Algorithm: ratelimiter.TokenBucket,
            Rate:      100,
            Window:    time.Minute,
        },
    })
    if err != nil {
        log.Fatal(err)
    }
    defer rl.Close()

    // Use anywhere — safe for concurrent goroutines
    ctx := context.Background()

    // Rate limit by user
    result, err := rl.Allow(ctx, "user:123")
    if err != nil {
        log.Printf("rate limiter error: %v", err)
        // Decide: fail open (allow) or fail closed (deny)
        return
    }

    if result.Allowed {
        log.Printf("request allowed, %d remaining", result.Remaining)
        // ... process the request
    } else {
        log.Printf("rate limited, retry after %v", result.RetryAfter)
        // ... reject, queue, or delay the request
    }
}
```

**Key design point**: The key string (`"user:123"`, `"ip:10.0.0.1"`, `"tenant:acme"`) is entirely up to you. Pass whatever identifies the entity being rate-limited.

#### Use different policies for different operations

```go
// Strict policy for writes
writePolicy := ratelimiter.Policy{
    Algorithm: ratelimiter.SlidingWindow,
    Rate:      10,
    Window:    time.Minute,
}

// Generous policy for reads
readPolicy := ratelimiter.Policy{
    Algorithm: ratelimiter.TokenBucket,
    Rate:      1000,
    Window:    time.Minute,
    Burst:     50,
}

// Apply per-call
writeResult, _ := rl.Allow(ctx, "user:123:write", ratelimiter.WithPolicy(writePolicy))
readResult, _ := rl.Allow(ctx, "user:123:read", ratelimiter.WithPolicy(readPolicy))
```

#### Cost-based limiting for batch operations

```go
// Single item — costs 1 token (default)
result, _ := rl.Allow(ctx, "user:123")

// Batch of 50 items — costs 50 tokens
result, _ := rl.Allow(ctx, "user:123", ratelimiter.WithCost(50))

// Expensive operation — costs 10 tokens
result, _ := rl.Allow(ctx, "user:123", ratelimiter.WithCost(10))
```

---

### 2. HTTP Service Integration (Middleware)

For HTTP APIs, the library provides **plug-and-play middleware** that works with any `net/http` compatible router. You don't need to call `Allow()` manually — the middleware handles everything.

#### With `net/http` (stdlib)

```go
package main

import (
    "log"
    "net/http"
    "time"

    "github.com/stevin/ratelimiter"
)

func main() {
    rl, err := ratelimiter.New(ratelimiter.Config{
        RedisAddr: "localhost:6379",
    })
    if err != nil {
        log.Fatal(err)
    }
    defer rl.Close()

    apiPolicy := ratelimiter.Policy{
        Algorithm: ratelimiter.TokenBucket,
        Rate:      100,
        Window:    time.Minute,
        Burst:     20,
    }

    mux := http.NewServeMux()
    mux.HandleFunc("/api/users", handleUsers)
    mux.HandleFunc("/api/orders", handleOrders)

    // Wrap the entire mux with rate limiting
    handler := rl.HTTPMiddleware(apiPolicy, ratelimiter.KeyByIP)(mux)

    log.Println("server starting on :8080")
    http.ListenAndServe(":8080", handler)
}
```

#### With chi

```go
r := chi.NewRouter()
r.Use(rl.HTTPMiddleware(policy, ratelimiter.KeyByIP))
r.Get("/api/data", handleData)
```

#### With gorilla/mux

```go
r := mux.NewRouter()
r.Use(rl.HTTPMiddleware(policy, ratelimiter.KeyByIP))
r.HandleFunc("/api/data", handleData)
```

#### Per-route policies

Apply different limits to different endpoints:

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

#### Rate limit by API key instead of IP

```go
handler := rl.HTTPMiddleware(policy, ratelimiter.KeyByHeader("X-API-Key"))(mux)
```

#### Custom key extraction

```go
// Composite key: authenticated user + endpoint
keyFunc := func(r *http.Request) string {
    userID := r.Header.Get("X-User-ID")
    return fmt.Sprintf("user:%s:%s:%s", userID, r.Method, r.URL.Path)
}

handler := rl.HTTPMiddleware(policy, keyFunc)(mux)
```

#### What the middleware does automatically

On **every response**, the middleware sets these headers:

| Header | Description |
|--------|-------------|
| `X-RateLimit-Limit` | Total requests allowed per window |
| `X-RateLimit-Remaining` | Requests remaining |
| `X-RateLimit-Reset` | Unix timestamp when the window resets |
| `Retry-After` | Seconds to wait (only on 429 responses) |

When rate limited, it responds with **`429 Too Many Requests`** and a JSON body:

```json
{
    "error": "rate limit exceeded",
    "retry_after": 30
}
```

On **Redis errors**, the middleware **fails open** — requests pass through rather than blocking all traffic.

---

### 3. gRPC Service Integration

For gRPC services, use the `Allow()` method directly in your RPC handlers or in a **unary server interceptor**.

#### In individual RPC handlers

```go
package server

import (
    "context"
    "time"

    "github.com/stevin/ratelimiter"
    "google.golang.org/grpc/codes"
    "google.golang.org/grpc/status"

    pb "your/proto/package"
)

type UserService struct {
    pb.UnimplementedUserServiceServer
    limiter *ratelimiter.RateLimiter
}

func NewUserService(redisAddr string) (*UserService, error) {
    rl, err := ratelimiter.New(ratelimiter.Config{
        RedisAddr: redisAddr,
        KeyPrefix: "user-svc:",
        DefaultPolicy: &ratelimiter.Policy{
            Algorithm: ratelimiter.SlidingWindow,
            Rate:      100,
            Window:    time.Minute,
        },
    })
    if err != nil {
        return nil, err
    }
    return &UserService{limiter: rl}, nil
}

func (s *UserService) GetUser(ctx context.Context, req *pb.GetUserRequest) (*pb.GetUserResponse, error) {
    // Rate limit by the calling user or client identity
    result, err := s.limiter.Allow(ctx, "grpc:GetUser:"+req.GetUserId())
    if err != nil {
        return nil, status.Errorf(codes.Internal, "rate limiter error: %v", err)
    }
    if !result.Allowed {
        return nil, status.Errorf(codes.ResourceExhausted,
            "rate limit exceeded, retry after %v", result.RetryAfter)
    }

    // ... handle the request
    return &pb.GetUserResponse{}, nil
}

func (s *UserService) Close() error {
    return s.limiter.Close()
}
```

#### As a gRPC unary interceptor (all RPCs at once)

```go
package main

import (
    "context"
    "log"
    "net"
    "time"

    "github.com/stevin/ratelimiter"
    "google.golang.org/grpc"
    "google.golang.org/grpc/codes"
    "google.golang.org/grpc/metadata"
    "google.golang.org/grpc/peer"
    "google.golang.org/grpc/status"
)

func rateLimitInterceptor(rl *ratelimiter.RateLimiter) grpc.UnaryServerInterceptor {
    return func(
        ctx context.Context,
        req interface{},
        info *grpc.UnaryServerInfo,
        handler grpc.UnaryHandler,
    ) (interface{}, error) {
        // Extract client identity for the rate limit key
        key := grpcClientKey(ctx, info.FullMethod)

        result, err := rl.Allow(ctx, key)
        if err != nil {
            // Fail open on rate limiter errors
            log.Printf("rate limiter error: %v", err)
            return handler(ctx, req)
        }

        if !result.Allowed {
            return nil, status.Errorf(codes.ResourceExhausted,
                "rate limit exceeded, retry after %v", result.RetryAfter)
        }

        // Set rate limit info in response headers (gRPC metadata)
        header := metadata.Pairs(
            "x-ratelimit-limit", fmt.Sprintf("%d", result.Limit),
            "x-ratelimit-remaining", fmt.Sprintf("%d", result.Remaining),
            "x-ratelimit-reset", fmt.Sprintf("%d", result.ResetAt.Unix()),
        )
        grpc.SetHeader(ctx, header)

        return handler(ctx, req)
    }
}

// grpcClientKey builds a rate limit key from the gRPC context.
func grpcClientKey(ctx context.Context, method string) string {
    // Try to get client IP from peer info
    if p, ok := peer.FromContext(ctx); ok {
        return fmt.Sprintf("grpc:%s:%s", p.Addr.String(), method)
    }
    return "grpc:unknown:" + method
}

func main() {
    rl, err := ratelimiter.New(ratelimiter.Config{
        RedisAddr: "localhost:6379",
        KeyPrefix: "mygrpc:",
    })
    if err != nil {
        log.Fatal(err)
    }
    defer rl.Close()

    server := grpc.NewServer(
        grpc.UnaryInterceptor(rateLimitInterceptor(rl)),
    )

    lis, _ := net.Listen("tcp", ":50051")
    log.Println("gRPC server starting on :50051")
    server.Serve(lis)
}
```

---

### 4. Background Workers & Queue Consumers

Rate limit outbound calls, database writes, or third-party API requests in workers:

```go
func processQueue(rl *ratelimiter.RateLimiter, queue <-chan Job) {
    ctx := context.Background()

    // Limit to 30 API calls per minute to a third-party service
    apiPolicy := ratelimiter.Policy{
        Algorithm: ratelimiter.LeakyBucket,  // smooth, constant rate
        Rate:      30,
        Window:    time.Minute,
    }

    for job := range queue {
        for {
            result, err := rl.Allow(ctx, "external-api:stripe",
                ratelimiter.WithPolicy(apiPolicy),
            )
            if err != nil {
                log.Printf("rate limiter error: %v", err)
                break // fail open
            }
            if result.Allowed {
                processJob(job)
                break
            }
            // Wait and retry
            time.Sleep(result.RetryAfter)
        }
    }
}
```

---

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

### Built-In Key Extractors (HTTP)

```go
// By client IP (respects X-Forwarded-For, X-Real-IP)
ratelimiter.KeyByIP

// By HTTP header value (e.g., API key)
ratelimiter.KeyByHeader("X-API-Key")

// By IP + method + path (per-endpoint limiting)
ratelimiter.KeyByEndpoint
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

### Named Policies (Shared Across Instances)

Register policies by name so all service instances share them via Redis:

```go
// Register (from any instance)
rl.SetPolicy("premium", ratelimiter.Policy{
    Algorithm: ratelimiter.TokenBucket,
    Rate:      1000,
    Window:    time.Minute,
    Burst:     100,
})

// Retrieve (from any other instance)
policy, err := rl.GetPolicy("premium")
```

## Advanced Usage

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

### Multi-Service Key Isolation

When multiple services share a Redis instance, use `KeyPrefix` to isolate keys:

```go
// Service A
rlA, _ := ratelimiter.New(ratelimiter.Config{
    RedisAddr: "shared-redis:6379",
    KeyPrefix: "svc-a:",
})

// Service B
rlB, _ := ratelimiter.New(ratelimiter.Config{
    RedisAddr: "shared-redis:6379",
    KeyPrefix: "svc-b:",
})
```

Both services can rate limit `"user:123"` independently — the actual Redis keys will be `svc-a:user:123` and `svc-b:user:123`.

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
