package ratelimiter

// options holds the resolved per-call overrides for a rate limit check.
type options struct {
	policy *Policy
	cost   int
}

// Option is a functional option that customizes a single [RateLimiter.Allow] call.
type Option func(*options)

// WithPolicy overrides the rate limiting policy for this call.
// This takes precedence over any default or named policy.
func WithPolicy(p Policy) Option {
	return func(o *options) {
		o.policy = &p
	}
}

// WithCost sets the cost (number of tokens/requests consumed) for this call.
// The default cost is 1. Use this for operations that should count as more
// than one request (e.g. batch operations or expensive endpoints).
func WithCost(n int) Option {
	return func(o *options) {
		o.cost = n
	}
}

// resolveOptions applies all functional options and returns the resolved values.
func resolveOptions(opts []Option) options {
	o := options{
		cost: 1,
	}
	for _, opt := range opts {
		opt(&o)
	}
	return o
}
