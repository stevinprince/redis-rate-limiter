// Package policy manages named rate limiting policies with a local cache
// backed by Redis for cross-instance sharing.
package policy

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	iredis "github.com/stevin/ratelimiter/internal/redis"
)

// Policy mirrors the public ratelimiter.Policy type for internal use.
// It is serialized to/from JSON when stored in Redis.
type Policy struct {
	Algorithm  string        `json:"algorithm"`
	Rate       int           `json:"rate"`
	Window     time.Duration `json:"window"`
	Burst      int           `json:"burst,omitempty"`
	RefillRate float64       `json:"refill_rate,omitempty"`
	DrainRate  float64       `json:"drain_rate,omitempty"`
}

// cacheEntry holds a cached policy alongside its expiration time.
type cacheEntry struct {
	policy    Policy
	expiresAt time.Time
}

// Manager stores and retrieves named policies. It caches policies locally
// with a configurable TTL and persists them to Redis so all rate limiter
// instances in a distributed deployment share the same configuration.
type Manager struct {
	client    iredis.Client
	keyPrefix string
	cacheTTL  time.Duration

	mu    sync.RWMutex
	cache map[string]cacheEntry
}

// Options configures a new policy [Manager].
type Options struct {
	// Client is the Redis client used for persistent storage.
	Client iredis.Client

	// KeyPrefix is prepended to all policy keys in Redis.
	// The final Redis key is: "<KeyPrefix>policy:<name>".
	KeyPrefix string

	// CacheTTL controls how long policies are cached locally before being
	// re-fetched from Redis. Zero or negative values disable caching.
	CacheTTL time.Duration
}

// NewManager creates a new policy [Manager] with the given options.
func NewManager(opts Options) *Manager {
	prefix := opts.KeyPrefix
	if prefix == "" {
		prefix = "rl:"
	}
	cacheTTL := opts.CacheTTL
	if cacheTTL <= 0 {
		cacheTTL = time.Minute
	}
	return &Manager{
		client:    opts.Client,
		keyPrefix: prefix,
		cacheTTL:  cacheTTL,
		cache:     make(map[string]cacheEntry),
	}
}

// redisKey returns the Redis key for a named policy.
func (m *Manager) redisKey(name string) string {
	return fmt.Sprintf("%spolicy:%s", m.keyPrefix, name)
}

// Set stores a named policy both locally and in Redis.
func (m *Manager) Set(ctx context.Context, name string, p Policy) error {
	data, err := json.Marshal(p)
	if err != nil {
		return fmt.Errorf("policy marshal: %w", err)
	}

	if err := m.client.Set(ctx, m.redisKey(name), string(data), 0); err != nil {
		return fmt.Errorf("policy redis set: %w", err)
	}

	m.mu.Lock()
	m.cache[name] = cacheEntry{
		policy:    p,
		expiresAt: time.Now().Add(m.cacheTTL),
	}
	m.mu.Unlock()

	return nil
}

// Get retrieves a named policy. It first checks the local cache, falling
// back to Redis if the cache entry is missing or expired.
//
// Returns the policy and true if found, or a zero Policy and false if the
// name has never been registered.
func (m *Manager) Get(ctx context.Context, name string) (Policy, bool, error) {
	// Check local cache first
	m.mu.RLock()
	entry, ok := m.cache[name]
	m.mu.RUnlock()

	if ok && time.Now().Before(entry.expiresAt) {
		return entry.policy, true, nil
	}

	// Cache miss or expired — fetch from Redis
	data, err := m.client.Get(ctx, m.redisKey(name))
	if err != nil {
		if iredis.IsNilErr(err) {
			return Policy{}, false, nil
		}
		return Policy{}, false, fmt.Errorf("policy redis get: %w", err)
	}

	var p Policy
	if err := json.Unmarshal([]byte(data), &p); err != nil {
		return Policy{}, false, fmt.Errorf("policy unmarshal: %w", err)
	}

	// Update local cache
	m.mu.Lock()
	m.cache[name] = cacheEntry{
		policy:    p,
		expiresAt: time.Now().Add(m.cacheTTL),
	}
	m.mu.Unlock()

	return p, true, nil
}

// Delete removes a named policy from both the local cache and Redis.
func (m *Manager) Delete(ctx context.Context, name string) error {
	m.mu.Lock()
	delete(m.cache, name)
	m.mu.Unlock()

	_, err := m.client.Del(ctx, m.redisKey(name))
	if err != nil {
		return fmt.Errorf("policy redis del: %w", err)
	}
	return nil
}

// InvalidateCache clears all locally cached policies, forcing the next
// Get call to fetch from Redis.
func (m *Manager) InvalidateCache() {
	m.mu.Lock()
	m.cache = make(map[string]cacheEntry)
	m.mu.Unlock()
}
