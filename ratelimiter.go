// Package ratelimiter provides distributed rate limiting backed by Redis.
//
// It supports four algorithms — Token Bucket, Sliding Window, Fixed Window,
// and Leaky Bucket — selectable per-policy. All operations are atomic
// (implemented via Redis Lua scripts) and safe for concurrent use by
// multiple goroutines.
//
// Basic usage:
//
//	limiter, err := ratelimiter.New(ratelimiter.Config{
//	    RedisAddr: "localhost:6379",
//	})
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer limiter.Close()
//
//	result, err := limiter.Allow(ctx, "user:123")
//	if result.Allowed {
//	    // serve request
//	}
package ratelimiter
