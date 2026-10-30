// Package algorithms implements the rate limiting strategies used by the
// ratelimiter library. Each algorithm is backed by an atomic Redis Lua script
// to guarantee correctness under concurrent access.
package algorithms

import (
	"context"
	"time"

	iredis "github.com/stevin/ratelimiter/internal/redis"
)

// Result holds the outcome of a rate limit check performed by an algorithm.
type Result struct {
	// Allowed is true if the request was permitted.
	Allowed bool

	// Remaining is the number of requests remaining in the current window.
	Remaining int

	// ResetAt is the Unix timestamp (seconds) when the window resets.
	ResetAtUnix int64

	// RetryAfterMs is the number of milliseconds the caller should wait
	// before retrying. Zero if the request was allowed.
	RetryAfterMs int64

	// Limit is the total number of requests allowed per window.
	Limit int
}

// Status holds the current quota state for a key without consuming a request.
type Status struct {
	// Remaining is the number of requests remaining in the current window.
	Remaining int

	// Limit is the total number of requests allowed per window.
	Limit int

	// ResetAtUnix is the Unix timestamp (seconds) when the window resets.
	ResetAtUnix int64

	// Used is the number of requests consumed in the current window.
	Used int
}

// Params holds the resolved policy parameters passed to an algorithm.
type Params struct {
	// Rate is the maximum number of requests allowed in Window.
	Rate int

	// Window is the time window duration.
	Window time.Duration

	// Burst is the maximum burst capacity (token bucket).
	Burst int

	// RefillRate is tokens added per second (token bucket).
	RefillRate float64

	// DrainRate is requests drained per second (leaky bucket).
	DrainRate float64

	// Cost is the number of tokens/requests consumed by this call.
	Cost int
}

// Algorithm is the interface that each rate limiting strategy implements.
// All methods must be safe for concurrent use.
type Algorithm interface {
	// Allow atomically checks and updates the rate limit for key.
	Allow(ctx context.Context, key string, p Params) (*Result, error)

	// Status returns the current quota state for key without consuming
	// a request.
	Status(ctx context.Context, key string, p Params) (*Status, error)

	// Reset removes all rate limit state for key.
	Reset(ctx context.Context, key string) error
}

// scriptRunner is a helper that handles the EVALSHA-with-fallback pattern.
// It first tries EVALSHA (fast path), and if the script is not cached in
// Redis, it falls back to EVAL which also caches the script.
func scriptRunner(
	ctx context.Context,
	client iredis.Client,
	sha string,
	script string,
	keys []string,
	args ...interface{},
) (interface{}, error) {
	res, err := client.EvalSha(ctx, sha, keys, args...)
	if err != nil && iredis.IsNoScriptErr(err) {
		return client.Eval(ctx, script, keys, args...)
	}
	return res, err
}
