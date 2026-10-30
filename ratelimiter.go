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
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/stevin/ratelimiter/internal/algorithms"
	ipolicy "github.com/stevin/ratelimiter/internal/policy"
	iredis "github.com/stevin/ratelimiter/internal/redis"
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

// defaultPolicy is used when no policy is specified in Config or per-call options.
var defaultPolicy = Policy{
	Algorithm: TokenBucket,
	Rate:      100,
	Window:    time.Minute,
}

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
	client        iredis.Client
	policyMgr     *ipolicy.Manager
	defaultPolicy Policy
	keyPrefix     string
	logger        *slog.Logger

	algos map[AlgorithmType]algorithms.Algorithm

	mu     sync.RWMutex
	closed bool
}

// New creates a new [RateLimiter] with the given configuration.
//
// It establishes a connection to Redis and validates the config.
// The caller must call [RateLimiter.Close] when done to release resources.
func New(cfg Config) (*RateLimiter, error) {
	if cfg.RedisAddr == "" {
		return nil, fmt.Errorf("%w: RedisAddr is required", ErrInvalidConfig)
	}

	keyPrefix := cfg.KeyPrefix
	if keyPrefix == "" {
		keyPrefix = "rl:"
	}

	defPolicy := defaultPolicy
	if cfg.DefaultPolicy != nil {
		defPolicy = *cfg.DefaultPolicy
	}
	if defPolicy.Algorithm == "" {
		defPolicy.Algorithm = TokenBucket
	}
	if defPolicy.Rate <= 0 {
		return nil, fmt.Errorf("%w: DefaultPolicy.Rate must be positive", ErrInvalidPolicy)
	}
	if defPolicy.Window <= 0 {
		return nil, fmt.Errorf("%w: DefaultPolicy.Window must be positive", ErrInvalidPolicy)
	}

	ctx := context.Background()
	client, err := iredis.NewClient(ctx, iredis.Options{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDB,
	})
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrRedisUnavailable, err)
	}

	cacheTTL := cfg.PolicyCacheTTL
	if cacheTTL <= 0 {
		cacheTTL = time.Minute
	}

	policyMgr := ipolicy.NewManager(ipolicy.Options{
		Client:    client,
		KeyPrefix: keyPrefix,
		CacheTTL:  cacheTTL,
	})

	algoMap := map[AlgorithmType]algorithms.Algorithm{
		TokenBucket:   algorithms.NewTokenBucket(client),
		SlidingWindow: algorithms.NewSlidingWindow(client),
		FixedWindow:   algorithms.NewFixedWindow(client),
		LeakyBucket:   algorithms.NewLeakyBucket(client),
	}

	logger := cfg.Logger
	if logger == nil {
		logger = slog.New(discardHandler{})
	}

	return &RateLimiter{
		client:        client,
		policyMgr:     policyMgr,
		defaultPolicy: defPolicy,
		keyPrefix:     keyPrefix,
		logger:        logger,
		algos:         algoMap,
	}, nil
}

// Allow checks whether a request identified by key is allowed under the
// applicable rate limiting policy. It atomically updates the counter in Redis.
//
// The key should uniquely identify the entity being rate-limited
// (e.g. "user:123", "ip:10.0.0.1").
//
// Options can override the policy or request cost for this call.
func (rl *RateLimiter) Allow(ctx context.Context, key string, opts ...Option) (*Result, error) {
	if err := rl.checkClosed(); err != nil {
		return nil, err
	}

	o := resolveOptions(opts)

	pol := rl.defaultPolicy
	if o.policy != nil {
		pol = *o.policy
	}
	if pol.Algorithm == "" {
		pol.Algorithm = TokenBucket
	}

	algo, ok := rl.algos[pol.Algorithm]
	if !ok {
		return nil, fmt.Errorf("%w: unknown algorithm %q", ErrInvalidPolicy, pol.Algorithm)
	}

	fullKey := rl.keyPrefix + key

	params := toParams(pol, o.cost)

	rl.logger.Debug("rate limit check",
		"key", key,
		"algorithm", pol.Algorithm,
		"rate", pol.Rate,
		"cost", o.cost,
	)

	res, err := algo.Allow(ctx, fullKey, params)
	if err != nil {
		return nil, err
	}

	return fromAlgoResult(res), nil
}

// GetStatus returns the current rate limit status for the given key without
// consuming a request.
func (rl *RateLimiter) GetStatus(ctx context.Context, key string) (*Status, error) {
	if err := rl.checkClosed(); err != nil {
		return nil, err
	}

	pol := rl.defaultPolicy
	algo := rl.algos[pol.Algorithm]

	fullKey := rl.keyPrefix + key
	params := toParams(pol, 0)

	st, err := algo.Status(ctx, fullKey, params)
	if err != nil {
		return nil, err
	}

	return fromAlgoStatus(st), nil
}

// Reset clears the rate limit state for the given key, restoring the full quota.
func (rl *RateLimiter) Reset(ctx context.Context, key string) error {
	if err := rl.checkClosed(); err != nil {
		return err
	}

	fullKey := rl.keyPrefix + key

	// Reset across all algorithms since we don't know which was used
	for _, algo := range rl.algos {
		if err := algo.Reset(ctx, fullKey); err != nil {
			return err
		}
	}

	rl.logger.Debug("rate limit reset", "key", key)
	return nil
}

// Close releases all resources held by the RateLimiter, including the Redis
// connection. After Close is called, all other methods return [ErrLimiterClosed].
func (rl *RateLimiter) Close() error {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	if rl.closed {
		return ErrLimiterClosed
	}
	rl.closed = true

	rl.logger.Debug("rate limiter closing")
	return rl.client.Close()
}

// SetPolicy registers a named policy that can be referenced later.
// The policy is cached locally and stored in Redis so all instances share it.
func (rl *RateLimiter) SetPolicy(name string, p Policy) error {
	if err := rl.checkClosed(); err != nil {
		return err
	}
	if p.Rate <= 0 || p.Window <= 0 {
		return fmt.Errorf("%w: Rate and Window must be positive", ErrInvalidPolicy)
	}

	internal := toInternalPolicy(p)
	return rl.policyMgr.Set(context.Background(), name, internal)
}

// GetPolicy retrieves a previously registered named policy.
func (rl *RateLimiter) GetPolicy(name string) (*Policy, error) {
	if err := rl.checkClosed(); err != nil {
		return nil, err
	}

	internal, found, err := rl.policyMgr.Get(context.Background(), name)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("%w: %q", ErrPolicyNotFound, name)
	}

	p := fromInternalPolicy(internal)
	return &p, nil
}

// checkClosed returns ErrLimiterClosed if the limiter has been closed.
func (rl *RateLimiter) checkClosed() error {
	rl.mu.RLock()
	defer rl.mu.RUnlock()
	if rl.closed {
		return ErrLimiterClosed
	}
	return nil
}

// toParams converts a public Policy and cost into internal algorithm Params.
func toParams(p Policy, cost int) algorithms.Params {
	if cost <= 0 {
		cost = 1
	}
	return algorithms.Params{
		Rate:       p.Rate,
		Window:     p.Window,
		Burst:      p.Burst,
		RefillRate: p.RefillRate,
		DrainRate:  p.DrainRate,
		Cost:       cost,
	}
}

// fromAlgoResult converts an internal algorithm Result to the public Result.
func fromAlgoResult(r *algorithms.Result) *Result {
	return &Result{
		Allowed:    r.Allowed,
		Remaining:  r.Remaining,
		ResetAt:    time.Unix(r.ResetAtUnix, 0),
		RetryAfter: time.Duration(r.RetryAfterMs) * time.Millisecond,
		Limit:      r.Limit,
	}
}

// fromAlgoStatus converts an internal algorithm Status to the public Status.
func fromAlgoStatus(s *algorithms.Status) *Status {
	return &Status{
		Remaining: s.Remaining,
		Limit:     s.Limit,
		ResetAt:   time.Unix(s.ResetAtUnix, 0),
		Used:      s.Used,
	}
}

// toInternalPolicy converts a public Policy to the internal policy type.
func toInternalPolicy(p Policy) ipolicy.Policy {
	return ipolicy.Policy{
		Algorithm:  string(p.Algorithm),
		Rate:       p.Rate,
		Window:     p.Window,
		Burst:      p.Burst,
		RefillRate: p.RefillRate,
		DrainRate:  p.DrainRate,
	}
}

// fromInternalPolicy converts an internal policy type to a public Policy.
func fromInternalPolicy(p ipolicy.Policy) Policy {
	return Policy{
		Algorithm:  AlgorithmType(p.Algorithm),
		Rate:       p.Rate,
		Window:     p.Window,
		Burst:      p.Burst,
		RefillRate: p.RefillRate,
		DrainRate:  p.DrainRate,
	}
}

// discardHandler is a slog.Handler that discards all log records.
// Used when no logger is configured.
type discardHandler struct{}

func (discardHandler) Enabled(context.Context, slog.Level) bool  { return false }
func (discardHandler) Handle(context.Context, slog.Record) error { return nil }
func (d discardHandler) WithAttrs([]slog.Attr) slog.Handler      { return d }
func (d discardHandler) WithGroup(string) slog.Handler            { return d }
