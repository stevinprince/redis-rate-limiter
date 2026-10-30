package ratelimiter_test

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/stevin/ratelimiter"
)

// ExampleNew demonstrates creating a rate limiter with default settings.
// The default policy is 100 requests per minute using the token bucket algorithm.
func ExampleNew() {
	limiter, err := ratelimiter.New(ratelimiter.Config{
		RedisAddr: "localhost:6379",
	})
	if err != nil {
		fmt.Println("failed to create limiter:", err)
		return
	}
	defer limiter.Close()

	fmt.Println("rate limiter created")
}

// ExampleNew_withCustomPolicy demonstrates creating a rate limiter with a
// custom default policy using the sliding window algorithm.
func ExampleNew_withCustomPolicy() {
	limiter, err := ratelimiter.New(ratelimiter.Config{
		RedisAddr: "localhost:6379",
		KeyPrefix: "myapp:",
		DefaultPolicy: &ratelimiter.Policy{
			Algorithm: ratelimiter.SlidingWindow,
			Rate:      200,
			Window:    time.Minute,
		},
	})
	if err != nil {
		fmt.Println("failed to create limiter:", err)
		return
	}
	defer limiter.Close()

	fmt.Println("rate limiter created with custom policy")
}

// ExampleRateLimiter_Allow demonstrates checking if a request is allowed.
func ExampleRateLimiter_Allow() {
	limiter, err := ratelimiter.New(ratelimiter.Config{
		RedisAddr: "localhost:6379",
	})
	if err != nil {
		fmt.Println("failed:", err)
		return
	}
	defer limiter.Close()

	ctx := context.Background()
	result, err := limiter.Allow(ctx, "user:123")
	if err != nil {
		fmt.Println("error:", err)
		return
	}

	if result.Allowed {
		fmt.Println("request allowed")
	} else {
		fmt.Printf("rate limited, retry after %v\n", result.RetryAfter)
	}
}

// ExampleRateLimiter_Allow_withPolicy demonstrates using a per-call policy
// override with the WithPolicy option.
func ExampleRateLimiter_Allow_withPolicy() {
	limiter, err := ratelimiter.New(ratelimiter.Config{
		RedisAddr: "localhost:6379",
	})
	if err != nil {
		fmt.Println("failed:", err)
		return
	}
	defer limiter.Close()

	ctx := context.Background()
	result, err := limiter.Allow(ctx, "api:endpoint:/users",
		ratelimiter.WithPolicy(ratelimiter.Policy{
			Algorithm: ratelimiter.SlidingWindow,
			Rate:      30,
			Window:    time.Minute,
		}),
	)
	if err != nil {
		fmt.Println("error:", err)
		return
	}

	fmt.Printf("allowed=%v remaining=%d\n", result.Allowed, result.Remaining)
}

// ExampleRateLimiter_Allow_withCost demonstrates cost-based rate limiting
// where a single operation consumes multiple tokens.
func ExampleRateLimiter_Allow_withCost() {
	limiter, err := ratelimiter.New(ratelimiter.Config{
		RedisAddr: "localhost:6379",
	})
	if err != nil {
		fmt.Println("failed:", err)
		return
	}
	defer limiter.Close()

	ctx := context.Background()

	// A batch operation that costs 10 tokens
	result, err := limiter.Allow(ctx, "user:123", ratelimiter.WithCost(10))
	if err != nil {
		fmt.Println("error:", err)
		return
	}

	fmt.Printf("batch allowed=%v remaining=%d\n", result.Allowed, result.Remaining)
}

// ExampleRateLimiter_HTTPMiddleware demonstrates using the HTTP middleware
// with the standard library's net/http ServeMux.
func ExampleRateLimiter_HTTPMiddleware() {
	limiter, err := ratelimiter.New(ratelimiter.Config{
		RedisAddr: "localhost:6379",
	})
	if err != nil {
		fmt.Println("failed:", err)
		return
	}
	defer limiter.Close()

	apiPolicy := ratelimiter.Policy{
		Algorithm: ratelimiter.TokenBucket,
		Rate:      100,
		Window:    time.Minute,
		Burst:     20,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/data", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"data": "hello"}`))
	})

	// Wrap the mux with the rate limiting middleware
	handler := limiter.HTTPMiddleware(apiPolicy, ratelimiter.KeyByIP)(mux)

	_ = handler
	// http.ListenAndServe(":8080", handler)
}

// ExampleRateLimiter_HTTPMiddleware_byHeader demonstrates rate limiting
// by API key header instead of IP address.
func ExampleRateLimiter_HTTPMiddleware_byHeader() {
	limiter, err := ratelimiter.New(ratelimiter.Config{
		RedisAddr: "localhost:6379",
	})
	if err != nil {
		fmt.Println("failed:", err)
		return
	}
	defer limiter.Close()

	apiPolicy := ratelimiter.Policy{
		Algorithm: ratelimiter.SlidingWindow,
		Rate:      1000,
		Window:    time.Hour,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/data", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"data": "hello"}`))
	})

	// Rate limit by API key header
	handler := limiter.HTTPMiddleware(apiPolicy, ratelimiter.KeyByHeader("X-API-Key"))(mux)

	_ = handler
	// http.ListenAndServe(":8080", handler)
}

// ExampleRateLimiter_SetPolicy demonstrates registering and retrieving
// named policies that are shared across instances via Redis.
func ExampleRateLimiter_SetPolicy() {
	limiter, err := ratelimiter.New(ratelimiter.Config{
		RedisAddr: "localhost:6379",
	})
	if err != nil {
		fmt.Println("failed:", err)
		return
	}
	defer limiter.Close()

	// Register a named policy
	err = limiter.SetPolicy("premium", ratelimiter.Policy{
		Algorithm: ratelimiter.TokenBucket,
		Rate:      1000,
		Window:    time.Minute,
		Burst:     100,
	})
	if err != nil {
		fmt.Println("error:", err)
		return
	}

	// Retrieve it later (possibly from another instance)
	policy, err := limiter.GetPolicy("premium")
	if err != nil {
		fmt.Println("error:", err)
		return
	}

	fmt.Printf("policy: rate=%d window=%v\n", policy.Rate, policy.Window)
}
