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

import (
	"context"
	"errors"
	"log/slog"
	"time"
)

// AlgorithmType identifies a rate limiting algorithm.
type AlgorithmType string

const (
	// TokenBucket refills tokens at a steady rate and allows bursts up to capacity.
	TokenBucket AlgorithmType = "token_bucket"

	// SlidingWindow counts requests in a rolling time window for precise enforcement.
	SlidingWindow AlgorithmType = "sliding_window"

	// FixedWindow counts requests in fixed intervals; simple but allows edge bursts.
	FixedWindow AlgorithmType = "fixed_window"

	// LeakyBucket processes requests at a constant rate, smoothing traffic spikes.
	LeakyBucket AlgorithmType = "leaky_bucket"
)

// Sentinel errors returned by the library. Callers can match them with [errors.Is].
var (
	// ErrRedisUnavailable is returned when the Redis connection fails.
	ErrRedisUnavailable = errors.New("ratelimiter: redis unavailable")

	// ErrPolicyNotFound is returned when a named policy cannot be resolved.
	ErrPolicyNotFound = errors.New("ratelimiter: policy not found")

	// ErrInvalidConfig is returned when the provided Config is invalid.
	ErrInvalidConfig = errors.New("ratelimiter: invalid config")

	// ErrInvalidPolicy is returned when a Policy has invalid field values.
	ErrInvalidPolicy = errors.New("ratelimiter: invalid policy")

	// ErrLimiterClosed is returned when operations are attempted after Close.
	ErrLimiterClosed = errors.New("ratelimiter: limiter closed")
)

// Config holds the configuration for creating a new [RateLimiter].
type Config struct {
	// RedisAddr is the address of the Redis server (e.g. "localhost:6379").
	RedisAddr string

	// RedisPassword is the optional password for Redis authentication.
	RedisPassword string

	// RedisDB is the Redis database number to use (default 0).
	RedisDB int

	// KeyPrefix is prepended to all Redis keys to avoid collisions (default "rl:").
	KeyPrefix string

	// DefaultPolicy is the policy used when no policy is specified in Allow().
	// If nil, a default of 100 requests per minute using TokenBucket is used.
	DefaultPolicy *Policy

	// PolicyCacheTTL controls how long named policies are cached locally
	// before being re-fetched from Redis. Default is 1 minute.
	PolicyCacheTTL time.Duration

	// Logger is an optional structured logger. If nil, logging is disabled.
	Logger *slog.Logger
}

// Policy defines a rate limiting strategy and its parameters.
type Policy struct {
	// Algorithm selects the rate limiting algorithm.
	// Defaults to TokenBucket if empty.
	Algorithm AlgorithmType

	// Rate is the maximum number of requests allowed in the given Window.
	Rate int

	// Window is the time window for rate limiting (e.g. 1 minute, 1 hour).
	Window time.Duration

	// Burst is the maximum burst size (only applicable to TokenBucket).
	// Defaults to Rate if zero.
	Burst int

	// RefillRate is the number of tokens added per second (only applicable
	// to TokenBucket). Defaults to Rate/Window.Seconds() if zero.
	RefillRate float64

	// DrainRate is the number of requests processed per second (only
	// applicable to LeakyBucket). Defaults to Rate/Window.Seconds() if zero.
	DrainRate float64
}

// Result is returned by [RateLimiter.Allow] and describes the outcome of a
// rate limit check.
type Result struct {
	// Allowed is true if the request is permitted.
	Allowed bool

	// Remaining is the number of requests remaining in the current window.
	Remaining int

	// ResetAt is the time when the rate limit window resets.
	ResetAt time.Time

	// RetryAfter is the duration the caller should wait before retrying.
	// Zero if the request was allowed.
	RetryAfter time.Duration

	// Limit is the total number of requests allowed in the window.
	Limit int
}

// Status describes the current quota state for a key without consuming
// a request.
type Status struct {
	// Remaining is the number of requests remaining in the current window.
	Remaining int

	// Limit is the total number of requests allowed in the window.
	Limit int

	// ResetAt is the time when the rate limit window resets.
	ResetAt time.Time

	// Used is the number of requests consumed in the current window.
	Used int
}

// RateLimiter is the main entry point for distributed rate limiting.
// It is safe for concurrent use by multiple goroutines.
type RateLimiter struct {
	// unexported fields will be added during wiring (Step 9)
	closed bool
}

// New creates a new [RateLimiter] with the given configuration.
//
// It establishes a connection to Redis and validates the config.
// The caller must call [RateLimiter.Close] when done to release resources.
func New(cfg Config) (*RateLimiter, error) {
	// TODO: implement in Step 9
	return nil, nil
}

// Allow checks whether a request identified by key is allowed under the
// applicable rate limiting policy. It atomically updates the counter in Redis.
//
// The key should uniquely identify the entity being rate-limited
// (e.g. "user:123", "ip:10.0.0.1").
//
// Options can override the policy or request cost for this call.
func (rl *RateLimiter) Allow(ctx context.Context, key string, opts ...Option) (*Result, error) {
	// TODO: implement in Step 9
	return nil, nil
}

// GetStatus returns the current rate limit status for the given key without
// consuming a request.
func (rl *RateLimiter) GetStatus(ctx context.Context, key string) (*Status, error) {
	// TODO: implement in Step 9
	return nil, nil
}

// Reset clears the rate limit state for the given key, restoring the full quota.
func (rl *RateLimiter) Reset(ctx context.Context, key string) error {
	// TODO: implement in Step 9
	return nil
}

// Close releases all resources held by the RateLimiter, including the Redis
// connection. After Close is called, all other methods return [ErrLimiterClosed].
func (rl *RateLimiter) Close() error {
	// TODO: implement in Step 9
	return nil
}

// SetPolicy registers a named policy that can be referenced later.
// The policy is cached locally and stored in Redis so all instances share it.
func (rl *RateLimiter) SetPolicy(name string, policy Policy) error {
	// TODO: implement in Step 9
	return nil
}

// GetPolicy retrieves a previously registered named policy.
func (rl *RateLimiter) GetPolicy(name string) (*Policy, error) {
	// TODO: implement in Step 9
	return nil, nil
}
