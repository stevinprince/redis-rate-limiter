package algorithms

import (
	"context"
	"crypto/sha1"
	"fmt"
	"time"

	iredis "github.com/stevin/ratelimiter/internal/redis"
)

// fixedWindowScript is the Lua script that atomically performs a fixed window
// rate limit check. The key incorporates the current time bucket so each
// window gets its own counter.
//
// KEYS[1] = rate limit key (already includes the window bucket suffix)
// ARGV[1] = limit (max requests per window)
// ARGV[2] = cost (number of requests this call represents)
// ARGV[3] = window duration (seconds) — used for TTL
// ARGV[4] = window end (Unix seconds) — when this window resets
//
// Returns: {allowed(0/1), remaining, reset_at_unix_s, retry_after_ms}
const fixedWindowScript = `
local key = KEYS[1]
local limit = tonumber(ARGV[1])
local cost = tonumber(ARGV[2])
local window_s = tonumber(ARGV[3])
local window_end_s = tonumber(ARGV[4])

local current = tonumber(redis.call('GET', key) or "0")

local allowed = 0
local remaining = limit - current
local retry_after_ms = 0

if current + cost <= limit then
    redis.call('INCRBY', key, cost)
    -- Set TTL only if the key has no expiry yet (first request in window)
    if redis.call('TTL', key) == -1 then
        redis.call('EXPIRE', key, window_s)
    end
    allowed = 1
    remaining = limit - current - cost
else
    -- Denied — retry after the current window expires
    local ttl = redis.call('TTL', key)
    if ttl > 0 then
        retry_after_ms = ttl * 1000
    else
        retry_after_ms = window_s * 1000
    end
    remaining = 0
end

return {allowed, remaining, window_end_s, retry_after_ms}
`

var fixedWindowSHA = func() string {
	h := sha1.New()
	h.Write([]byte(fixedWindowScript))
	return fmt.Sprintf("%x", h.Sum(nil))
}()

// Compile-time check that FixedWindowAlgo implements Algorithm.
var _ Algorithm = (*FixedWindowAlgo)(nil)

// FixedWindowAlgo implements the fixed window rate limiting algorithm.
// It divides time into fixed-size buckets and counts requests per bucket.
// Simple and efficient, but allows up to 2× burst at window boundaries.
type FixedWindowAlgo struct {
	client iredis.Client
}

// NewFixedWindow creates a new fixed window algorithm backed by the given
// Redis client.
func NewFixedWindow(client iredis.Client) *FixedWindowAlgo {
	return &FixedWindowAlgo{client: client}
}

// windowKey returns the Redis key for the current time bucket.
// The bucket is identified by dividing the current Unix timestamp by the
// window duration, ensuring all requests within the same interval share
// a counter.
func windowKey(baseKey string, window time.Duration, now time.Time) (key string, windowEndUnix int64) {
	windowSec := int64(window.Seconds())
	if windowSec <= 0 {
		windowSec = 1
	}
	bucket := now.Unix() / windowSec
	windowEnd := (bucket + 1) * windowSec
	return fmt.Sprintf("%s:%d", baseKey, bucket), windowEnd
}

// Allow atomically checks whether the request identified by key is within
// the rate limit for the current fixed window.
func (fw *FixedWindowAlgo) Allow(ctx context.Context, key string, p Params) (*Result, error) {
	now := time.Now()
	bucketKey, windowEnd := windowKey(key, p.Window, now)
	windowSec := int64(p.Window.Seconds())
	if windowSec <= 0 {
		windowSec = 1
	}

	raw, err := scriptRunner(ctx, fw.client, fixedWindowSHA, fixedWindowScript,
		[]string{bucketKey},
		p.Rate,
		p.Cost,
		windowSec,
		windowEnd,
	)
	if err != nil {
		return nil, fmt.Errorf("fixed_window allow: %w", err)
	}

	vals := raw.([]interface{})
	return &Result{
		Allowed:      vals[0].(int64) == 1,
		Remaining:    int(vals[1].(int64)),
		ResetAtUnix:  vals[2].(int64),
		RetryAfterMs: vals[3].(int64),
		Limit:        p.Rate,
	}, nil
}

// Status returns the current request count in the fixed window without
// recording a new request.
func (fw *FixedWindowAlgo) Status(ctx context.Context, key string, p Params) (*Status, error) {
	now := time.Now()
	bucketKey, windowEnd := windowKey(key, p.Window, now)
	windowSec := int64(p.Window.Seconds())
	if windowSec <= 0 {
		windowSec = 1
	}

	// Run with cost=0 to inspect without incrementing
	raw, err := scriptRunner(ctx, fw.client, fixedWindowSHA, fixedWindowScript,
		[]string{bucketKey},
		p.Rate,
		0, // zero cost — status only
		windowSec,
		windowEnd,
	)
	if err != nil {
		return nil, fmt.Errorf("fixed_window status: %w", err)
	}

	vals := raw.([]interface{})
	remaining := int(vals[1].(int64))

	return &Status{
		Remaining:   remaining,
		Limit:       p.Rate,
		ResetAtUnix: vals[2].(int64),
		Used:        p.Rate - remaining,
	}, nil
}

// Reset removes the fixed window counter for the current bucket.
// Note: this only clears the current window; previous (already expired)
// windows are cleaned up automatically by Redis TTL.
func (fw *FixedWindowAlgo) Reset(ctx context.Context, key string) error {
	// Delete both the base key and the current bucket key
	now := time.Now()
	// Try a few recent buckets to be thorough
	windowDurations := []time.Duration{
		time.Second, time.Minute, time.Hour,
		5 * time.Minute, 10 * time.Minute, 30 * time.Minute,
	}
	keys := []string{key}
	for _, w := range windowDurations {
		bk, _ := windowKey(key, w, now)
		keys = append(keys, bk)
	}
	_, err := fw.client.Del(ctx, keys...)
	if err != nil {
		return fmt.Errorf("fixed_window reset: %w", err)
	}
	return nil
}
