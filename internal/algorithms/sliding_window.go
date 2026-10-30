package algorithms

import (
	"context"
	"crypto/sha1"
	"fmt"
	"time"

	iredis "github.com/stevin/ratelimiter/internal/redis"
)

// slidingWindowScript is the Lua script that atomically performs a sliding
// window rate limit check using a Redis sorted set (ZSET).
//
// Each request is stored as a member scored by its timestamp (Unix ms).
// On every check the script removes expired members, counts the remaining
// ones, and conditionally adds the new request.
//
// KEYS[1] = rate limit key
// ARGV[1] = window duration (ms)
// ARGV[2] = limit (max requests per window)
// ARGV[3] = current time (Unix ms)
// ARGV[4] = cost (number of requests this call represents)
// ARGV[5] = unique member ID (now_ms concatenated with a random suffix)
//
// Returns: {allowed(0/1), remaining, reset_at_unix_s, retry_after_ms}
const slidingWindowScript = `
local key = KEYS[1]
local window_ms = tonumber(ARGV[1])
local limit = tonumber(ARGV[2])
local now_ms = tonumber(ARGV[3])
local cost = tonumber(ARGV[4])
local member_id = ARGV[5]

-- Remove all entries outside the current window
local window_start = now_ms - window_ms
redis.call('ZREMRANGEBYSCORE', key, '-inf', window_start)

-- Count current requests in the window
local current = redis.call('ZCARD', key)

local allowed = 0
local remaining = limit - current
local retry_after_ms = 0

if current + cost <= limit then
    -- Add cost number of members with the current timestamp as score
    for i = 1, cost do
        redis.call('ZADD', key, now_ms, member_id .. ':' .. i)
    end
    allowed = 1
    remaining = limit - current - cost
else
    -- Find the oldest entry to calculate when space opens up
    local oldest = redis.call('ZRANGE', key, 0, 0, 'WITHSCORES')
    if #oldest > 0 then
        local oldest_score = tonumber(oldest[2])
        retry_after_ms = math.max(0, (oldest_score + window_ms) - now_ms)
    end
    remaining = 0
end

-- Set TTL slightly beyond window so stale keys are cleaned up
local ttl_s = math.ceil(window_ms / 1000) + 1
redis.call('EXPIRE', key, ttl_s)

-- Reset time is when the oldest entry in the window expires
local reset_at_s = math.ceil((now_ms + window_ms) / 1000)

return {allowed, remaining, reset_at_s, retry_after_ms}
`

var slidingWindowSHA = func() string {
	h := sha1.New()
	h.Write([]byte(slidingWindowScript))
	return fmt.Sprintf("%x", h.Sum(nil))
}()

// Compile-time check that SlidingWindowAlgo implements Algorithm.
var _ Algorithm = (*SlidingWindowAlgo)(nil)

// SlidingWindowAlgo implements the sliding window rate limiting algorithm.
// It uses a Redis sorted set to track individual requests with millisecond
// precision, providing accurate enforcement at window boundaries.
type SlidingWindowAlgo struct {
	client iredis.Client
}

// NewSlidingWindow creates a new sliding window algorithm backed by the
// given Redis client.
func NewSlidingWindow(client iredis.Client) *SlidingWindowAlgo {
	return &SlidingWindowAlgo{client: client}
}

// Allow atomically checks whether the request identified by key is within
// the rate limit. If allowed, it records the request in the sliding window.
func (sw *SlidingWindowAlgo) Allow(ctx context.Context, key string, p Params) (*Result, error) {
	nowMs := time.Now().UnixMilli()
	windowMs := p.Window.Milliseconds()

	// Use timestamp + nanosecond suffix for unique member IDs
	memberID := fmt.Sprintf("%d:%d", nowMs, time.Now().UnixNano())

	raw, err := scriptRunner(ctx, sw.client, slidingWindowSHA, slidingWindowScript,
		[]string{key},
		windowMs,
		p.Rate,
		nowMs,
		p.Cost,
		memberID,
	)
	if err != nil {
		return nil, fmt.Errorf("sliding_window allow: %w", err)
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

// Status returns the current request count in the sliding window without
// recording a new request.
func (sw *SlidingWindowAlgo) Status(ctx context.Context, key string, p Params) (*Status, error) {
	// Run the script with cost=0 so no member is added
	nowMs := time.Now().UnixMilli()
	windowMs := p.Window.Milliseconds()

	raw, err := scriptRunner(ctx, sw.client, slidingWindowSHA, slidingWindowScript,
		[]string{key},
		windowMs,
		p.Rate,
		nowMs,
		0, // zero cost — status only
		"status_noop",
	)
	if err != nil {
		return nil, fmt.Errorf("sliding_window status: %w", err)
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

// Reset removes all sliding window state for the given key.
func (sw *SlidingWindowAlgo) Reset(ctx context.Context, key string) error {
	_, err := sw.client.Del(ctx, key)
	if err != nil {
		return fmt.Errorf("sliding_window reset: %w", err)
	}
	return nil
}
