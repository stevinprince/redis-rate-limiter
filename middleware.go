package ratelimiter

import "net/http"

// HTTPMiddleware returns an http.Handler middleware that rate limits incoming
// requests using the given policy.
//
// keyFunc extracts the rate limit key from each request (e.g. client IP,
// API key header, user ID). The returned middleware is compatible with any
// net/http router (stdlib, chi, gorilla/mux, etc.).
//
// When a request is rate limited, the middleware responds with 429 Too Many
// Requests and a JSON body. On every response it sets the following headers:
//
//   - X-RateLimit-Limit: total requests allowed in the window
//   - X-RateLimit-Remaining: requests remaining
//   - X-RateLimit-Reset: Unix timestamp when the window resets
//   - Retry-After: seconds to wait (only on 429 responses)
func (rl *RateLimiter) HTTPMiddleware(policy Policy, keyFunc func(r *http.Request) string) func(http.Handler) http.Handler {
	// TODO: implement in Step 10
	return func(next http.Handler) http.Handler {
		return next
	}
}

// KeyByIP returns a key extractor that uses the client's IP address.
// It respects the X-Forwarded-For and X-Real-IP headers, falling back
// to [http.Request.RemoteAddr].
func KeyByIP(r *http.Request) string {
	// TODO: implement in Step 10
	return ""
}

// KeyByHeader returns a key extractor that uses the value of the specified
// HTTP header as the rate limit key. This is useful for API key or
// authorization-based rate limiting.
func KeyByHeader(header string) func(r *http.Request) string {
	// TODO: implement in Step 10
	return func(r *http.Request) string {
		return ""
	}
}
