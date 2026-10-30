// Package redis provides an abstracted Redis client for the ratelimiter internals.
//
// It defines a [Client] interface that covers only the operations needed by the
// rate limiting algorithms and policy manager. This allows the algorithms to be
// tested with a mock implementation that does not require a running Redis instance.
package redis

import (
	"context"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// Client is the interface that rate limiting algorithms and the policy manager
// use to interact with Redis. It is intentionally narrow — only the operations
// needed by this library are included.
type Client interface {
	// Eval executes a Lua script against Redis. keys and args are passed to
	// the script as KEYS and ARGV arrays respectively.
	Eval(ctx context.Context, script string, keys []string, args ...interface{}) (interface{}, error)

	// EvalSha executes a previously loaded Lua script by its SHA1 digest.
	EvalSha(ctx context.Context, sha string, keys []string, args ...interface{}) (interface{}, error)

	// ScriptLoad loads a Lua script into the Redis script cache and returns
	// its SHA1 digest.
	ScriptLoad(ctx context.Context, script string) (string, error)

	// Get retrieves the value of a key. Returns ("", ErrNil) if the key
	// does not exist.
	Get(ctx context.Context, key string) (string, error)

	// Set sets a key to a value with an optional expiration.
	// Pass 0 for expiration to set no TTL.
	Set(ctx context.Context, key string, value interface{}, expiration time.Duration) error

	// Del deletes one or more keys and returns the number of keys removed.
	Del(ctx context.Context, keys ...string) (int64, error)

	// Ping checks the connection to Redis.
	Ping(ctx context.Context) error

	// Close closes the underlying connection.
	Close() error
}

// ErrNil is returned by Get when a key does not exist in Redis.
var ErrNil = goredis.Nil

// IsNilErr reports whether err is a Redis nil error (key not found).
func IsNilErr(err error) bool {
	return err == goredis.Nil
}

// IsNoScriptErr reports whether err is a NOSCRIPT error, indicating the Lua
// script has not been cached by Redis and needs to be reloaded.
func IsNoScriptErr(err error) bool {
	return goredis.HasErrorPrefix(err, "NOSCRIPT")
}

// Options holds configuration for creating a new Redis client.
type Options struct {
	// Addr is the Redis server address (e.g. "localhost:6379").
	Addr string

	// Password is the optional authentication password.
	Password string

	// DB is the Redis database number.
	DB int
}

// redisClient is a concrete implementation of [Client] backed by go-redis.
type redisClient struct {
	rdb *goredis.Client
}

// NewClient creates a new Redis [Client] connected to the server specified
// in opts. It pings the server to verify the connection.
func NewClient(ctx context.Context, opts Options) (Client, error) {
	rdb := goredis.NewClient(&goredis.Options{
		Addr:     opts.Addr,
		Password: opts.Password,
		DB:       opts.DB,
	})

	if err := rdb.Ping(ctx).Err(); err != nil {
		_ = rdb.Close()
		return nil, fmt.Errorf("redis ping: %w", err)
	}

	return &redisClient{rdb: rdb}, nil
}

func (c *redisClient) Eval(ctx context.Context, script string, keys []string, args ...interface{}) (interface{}, error) {
	return c.rdb.Eval(ctx, script, keys, args...).Result()
}

func (c *redisClient) EvalSha(ctx context.Context, sha string, keys []string, args ...interface{}) (interface{}, error) {
	return c.rdb.EvalSha(ctx, sha, keys, args...).Result()
}

func (c *redisClient) ScriptLoad(ctx context.Context, script string) (string, error) {
	return c.rdb.ScriptLoad(ctx, script).Result()
}

func (c *redisClient) Get(ctx context.Context, key string) (string, error) {
	return c.rdb.Get(ctx, key).Result()
}

func (c *redisClient) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) error {
	return c.rdb.Set(ctx, key, value, expiration).Err()
}

func (c *redisClient) Del(ctx context.Context, keys ...string) (int64, error) {
	return c.rdb.Del(ctx, keys...).Result()
}

func (c *redisClient) Ping(ctx context.Context) error {
	return c.rdb.Ping(ctx).Err()
}

func (c *redisClient) Close() error {
	return c.rdb.Close()
}
