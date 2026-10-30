package algorithms

import (
	"context"
	"crypto/sha1"
	"fmt"
	"time"

	iredis "github.com/stevin/ratelimiter/internal/redis"
)

// leakyBucketScript is the Lua script that atomically performs a leaky bucket
// rate limit check. It models a queue that drains at a constant rate.
//
// State is stored in a Redis hash with fields:
//   - queue_size: current number of items in the queue (float for partial drains)
//   - last_drip:  Unix timestamp (ms) of the last drain calculation
//
// KEYS[1] = rate limit key
// ARGV[1] = capacity (max queue size, equals rate limit)
// ARGV[2] = drain rate (requests processed per second)
// ARGV[3] = cost (number of requests this call represents)
// ARGV[4] = current time (Unix ms)
// ARGV[5] = window duration (ms) — used for TTL
//
// Returns: {allowed(0/1), remaining, reset_at_unix_s, retry_after_ms}
const leakyBucketScript = `
local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local drain_rate = tonumber(ARGV[2])
local cost = tonumber(ARGV[3])
local now_ms = tonumber(ARGV[4])
local window_ms = tonumber(ARGV[5])

local data = redis.call('HMGET', key, 'queue_size', 'last_drip')
local queue_size = tonumber(data[1])
local last_drip = tonumber(data[2])

if queue_size == nil then
    queue_size = 0
    last_drip = now_ms
end

-- Drain items based on elapsed time
local elapsed_ms = now_ms - last_drip
local drained = (elapsed_ms / 1000.0) * drain_rate
queue_size = math.max(0, queue_size - drained)
last_drip = now_ms

local allowed = 0
local remaining = math.floor(capacity - queue_size)
local retry_after_ms = 0

if queue_size + cost <= capacity then
    queue_size = queue_size + cost
    allowed = 1
    remaining = math.floor(capacity - queue_size)
else
    -- Calculate how long until enough space drains
    local excess = (queue_size + cost) - capacity
    retry_after_ms = math.ceil((excess / drain_rate) * 1000)
    remaining = 0
end

-- Update state
redis.call('HSET', key, 'queue_size', tostring(queue_size), 'last_drip', tostring(last_drip))

-- Set TTL so stale keys are cleaned up
local ttl_s = math.ceil(window_ms / 1000) * 2
redis.call('EXPIRE', key, ttl_s)

-- Reset time: when queue will be fully drained
local drain_time_ms = math.ceil((queue_size / drain_rate) * 1000)
local reset_at_s = math.ceil((now_ms + drain_time_ms) / 1000)

return {allowed, remaining, reset_at_s, retry_after_ms}
`

var leakyBucketSHA = func() string {
	h := sha1.New()
	h.Write([]byte(leakyBucketScript))
	return fmt.Sprintf("%x", h.Sum(nil))
}()

// Compile-time check that LeakyBucketAlgo implements Algorithm.
var _ Algorithm = (*LeakyBucketAlgo)(nil)

// LeakyBucketAlgo implements the leaky bucket rate limiting algorithm.
// It models a queue with a fixed capacity that drains at a constant rate,
// smoothing bursty traffic into a steady flow.
type LeakyBucketAlgo struct {
	client iredis.Client
}

// NewLeakyBucket creates a new leaky bucket algorithm backed by the given
// Redis client.
func NewLeakyBucket(client iredis.Client) *LeakyBucketAlgo {
	return &LeakyBucketAlgo{client: client}
}

// Allow atomically checks whether there is room in the leaky bucket for
// the request identified by key. If space is available the request is
// queued; otherwise it is rejected with a retry hint.
func (lb *LeakyBucketAlgo) Allow(ctx context.Context, key string, p Params) (*Result, error) {
	drainRate := p.DrainRate
	if drainRate <= 0 {
		drainRate = float64(p.Rate) / p.Window.Seconds()
	}

	nowMs := time.Now().UnixMilli()
	windowMs := p.Window.Milliseconds()

	raw, err := scriptRunner(ctx, lb.client, leakyBucketSHA, leakyBucketScript,
		[]string{key},
		p.Rate,
		drainRate,
		p.Cost,
		nowMs,
		windowMs,
	)
	if err != nil {
		return nil, fmt.Errorf("leaky_bucket allow: %w", err)
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

// Status returns the current queue depth without adding a new request.
func (lb *LeakyBucketAlgo) Status(ctx context.Context, key string, p Params) (*Status, error) {
	drainRate := p.DrainRate
	if drainRate <= 0 {
		drainRate = float64(p.Rate) / p.Window.Seconds()
	}

	nowMs := time.Now().UnixMilli()
	windowMs := p.Window.Milliseconds()

	raw, err := scriptRunner(ctx, lb.client, leakyBucketSHA, leakyBucketScript,
		[]string{key},
		p.Rate,
		drainRate,
		0, // zero cost — status only
		nowMs,
		windowMs,
	)
	if err != nil {
		return nil, fmt.Errorf("leaky_bucket status: %w", err)
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

// Reset removes all leaky bucket state for the given key.
func (lb *LeakyBucketAlgo) Reset(ctx context.Context, key string) error {
	_, err := lb.client.Del(ctx, key)
	if err != nil {
		return fmt.Errorf("leaky_bucket reset: %w", err)
	}
	return nil
}
