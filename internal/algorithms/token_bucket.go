package algorithms

import (
	"context"
	"crypto/sha1"
	"fmt"
	"time"

	iredis "github.com/stevin/ratelimiter/internal/redis"
)

// tokenBucketScript is the Lua script that atomically performs a token bucket
// rate limit check. It stores state in a Redis hash with fields:
//   - tokens:     current number of tokens (float for fractional refill)
//   - last_refill: Unix timestamp (ms) of the last refill
//
// KEYS[1] = rate limit key
// ARGV[1] = bucket capacity (burst)
// ARGV[2] = refill rate (tokens per second)
// ARGV[3] = cost (tokens to consume)
// ARGV[4] = current time (Unix ms)
// ARGV[5] = window duration (ms) — used for TTL and reset time
//
// Returns: {allowed(0/1), remaining, reset_at_unix_s, retry_after_ms}
const tokenBucketScript = `
local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local refill_rate = tonumber(ARGV[2])
local cost = tonumber(ARGV[3])
local now_ms = tonumber(ARGV[4])
local window_ms = tonumber(ARGV[5])

local data = redis.call('HMGET', key, 'tokens', 'last_refill')
local tokens = tonumber(data[1])
local last_refill = tonumber(data[2])

if tokens == nil then
    tokens = capacity
    last_refill = now_ms
end

-- Refill tokens based on elapsed time
local elapsed_ms = now_ms - last_refill
local refill = (elapsed_ms / 1000.0) * refill_rate
tokens = math.min(capacity, tokens + refill)
last_refill = now_ms

local allowed = 0
local remaining = math.floor(tokens)
local retry_after_ms = 0

if tokens >= cost then
    tokens = tokens - cost
    allowed = 1
    remaining = math.floor(tokens)
else
    -- Calculate how long until enough tokens are available
    local deficit = cost - tokens
    retry_after_ms = math.ceil((deficit / refill_rate) * 1000)
end

-- Update state
redis.call('HSET', key, 'tokens', tostring(tokens), 'last_refill', tostring(last_refill))

-- Set TTL to window duration so stale keys are cleaned up
local ttl_s = math.ceil(window_ms / 1000) * 2
redis.call('EXPIRE', key, ttl_s)

-- Reset time: when bucket will be full again
local deficit_to_full = capacity - tokens
local reset_ms = now_ms + math.ceil((deficit_to_full / refill_rate) * 1000)
local reset_at_s = math.ceil(reset_ms / 1000)

return {allowed, remaining, reset_at_s, retry_after_ms}
`

// tokenBucketSHA is the precomputed SHA1 of the Lua script.
var tokenBucketSHA = computeSHA(tokenBucketScript)

// Compile-time check that TokenBucketAlgo implements Algorithm.
var _ Algorithm = (*TokenBucketAlgo)(nil)

// TokenBucketAlgo implements the token bucket rate limiting algorithm.
type TokenBucketAlgo struct {
	client iredis.Client
}

// NewTokenBucket creates a new token bucket algorithm backed by the given
// Redis client.
func NewTokenBucket(client iredis.Client) *TokenBucketAlgo {
	return &TokenBucketAlgo{client: client}
}

// Allow atomically checks whether cost tokens are available in the bucket
// identified by key. If enough tokens are present they are consumed and
// the request is allowed; otherwise the request is denied and RetryAfterMs
// indicates when to retry.
func (tb *TokenBucketAlgo) Allow(ctx context.Context, key string, p Params) (*Result, error) {
	capacity := p.Burst
	if capacity <= 0 {
		capacity = p.Rate
	}
	refillRate := p.RefillRate
	if refillRate <= 0 {
		refillRate = float64(p.Rate) / p.Window.Seconds()
	}

	nowMs := time.Now().UnixMilli()
	windowMs := p.Window.Milliseconds()

	raw, err := scriptRunner(ctx, tb.client, tokenBucketSHA, tokenBucketScript,
		[]string{key},
		capacity,
		refillRate,
		p.Cost,
		nowMs,
		windowMs,
	)
	if err != nil {
		return nil, fmt.Errorf("token_bucket allow: %w", err)
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

// Status returns the current token count without consuming any tokens.
func (tb *TokenBucketAlgo) Status(ctx context.Context, key string, p Params) (*Status, error) {
	capacity := p.Burst
	if capacity <= 0 {
		capacity = p.Rate
	}
	refillRate := p.RefillRate
	if refillRate <= 0 {
		refillRate = float64(p.Rate) / p.Window.Seconds()
	}

	// Run Allow with cost=0 to get current state without consuming
	nowMs := time.Now().UnixMilli()
	windowMs := p.Window.Milliseconds()

	raw, err := scriptRunner(ctx, tb.client, tokenBucketSHA, tokenBucketScript,
		[]string{key},
		capacity,
		refillRate,
		0, // zero cost — status only
		nowMs,
		windowMs,
	)
	if err != nil {
		return nil, fmt.Errorf("token_bucket status: %w", err)
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

// Reset removes all token bucket state for the given key.
func (tb *TokenBucketAlgo) Reset(ctx context.Context, key string) error {
	_, err := tb.client.Del(ctx, key)
	if err != nil {
		return fmt.Errorf("token_bucket reset: %w", err)
	}
	return nil
}

// computeSHA returns the hex-encoded SHA1 digest of s.
func computeSHA(s string) string {
	h := sha1.New()
	h.Write([]byte(s))
	return fmt.Sprintf("%x", h.Sum(nil))
}
