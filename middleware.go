package ratelimiter

import (
	"encoding/json"
	"net"
	"net/http"
	"strconv"
	"strings"
)

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
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			key := keyFunc(r)

			result, err := rl.Allow(r.Context(), key, WithPolicy(policy))
			if err != nil {
				rl.logger.Error("rate limiter error",
					"key", key,
					"error", err,
				)
				// On error, fail open — let the request through
				next.ServeHTTP(w, r)
				return
			}

			// Always set rate limit headers
			w.Header().Set("X-RateLimit-Limit", strconv.Itoa(result.Limit))
			w.Header().Set("X-RateLimit-Remaining", strconv.Itoa(result.Remaining))
			w.Header().Set("X-RateLimit-Reset", strconv.FormatInt(result.ResetAt.Unix(), 10))

			if !result.Allowed {
				retryAfterSec := int(result.RetryAfter.Seconds())
				if retryAfterSec <= 0 {
					retryAfterSec = 1
				}
				w.Header().Set("Retry-After", strconv.Itoa(retryAfterSec))
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusTooManyRequests)

				body := rateLimitResponse{
					Error:      "rate limit exceeded",
					RetryAfter: retryAfterSec,
				}
				_ = json.NewEncoder(w).Encode(body)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

// rateLimitResponse is the JSON body returned when a request is rate limited.
type rateLimitResponse struct {
	Error      string `json:"error"`
	RetryAfter int    `json:"retry_after"`
}

// KeyByIP returns a key extractor that uses the client's IP address.
// It checks the X-Forwarded-For and X-Real-IP headers first (for clients
// behind proxies/load balancers), falling back to [http.Request.RemoteAddr].
func KeyByIP(r *http.Request) string {
	// X-Forwarded-For may contain a comma-separated list; take the first
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		parts := strings.SplitN(xff, ",", 2)
		ip := strings.TrimSpace(parts[0])
		if ip != "" {
			return "ip:" + ip
		}
	}

	if realIP := r.Header.Get("X-Real-IP"); realIP != "" {
		return "ip:" + strings.TrimSpace(realIP)
	}

	// RemoteAddr is "host:port"; strip the port
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return "ip:" + r.RemoteAddr
	}
	return "ip:" + host
}

// KeyByHeader returns a key extractor that uses the value of the specified
// HTTP header as the rate limit key. This is useful for API key or
// authorization-based rate limiting.
//
// If the header is missing or empty, the extractor falls back to the
// client IP address.
func KeyByHeader(header string) func(r *http.Request) string {
	return func(r *http.Request) string {
		val := r.Header.Get(header)
		if val == "" {
			return KeyByIP(r)
		}
		return "header:" + val
	}
}

// KeyByEndpoint returns a key extractor that combines the client IP with
// the request path, enabling per-endpoint rate limiting.
func KeyByEndpoint(r *http.Request) string {
	ip := KeyByIP(r)
	return ip + ":" + r.Method + ":" + r.URL.Path
}

// KeyFunc is the function signature for extracting a rate limit key from
// an HTTP request. Provided for documentation clarity — it is identical to
// func(r *http.Request) string.
type KeyFunc = func(r *http.Request) string

