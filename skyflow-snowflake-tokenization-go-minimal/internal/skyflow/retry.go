package skyflow

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"strconv"
	"time"
)

// Retrier handles retry logic with exponential backoff and jitter
type Retrier struct {
	maxRetries  int
	baseDelayMs int
	rng         *rand.Rand
}

// NewRetrier creates a new Retrier
func NewRetrier(maxRetries, baseDelayMs int) *Retrier {
	return &Retrier{
		maxRetries:  maxRetries,
		baseDelayMs: baseDelayMs,
		rng:         rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// HTTPError represents an HTTP error with status code
type HTTPError struct {
	StatusCode int
	Message    string
	Response   *http.Response
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("HTTP %d: %s", e.StatusCode, e.Message)
}

// Do executes a function with retry logic
func (r *Retrier) Do(ctx context.Context, operation func() error) error {
	var lastErr error

	for attempt := 1; attempt <= r.maxRetries; attempt++ {
		// Execute operation
		err := operation()
		if err == nil {
			return nil
		}

		lastErr = err

		// Check if we should retry
		if !r.shouldRetry(err) {
			return err
		}

		// Last attempt - don't sleep
		if attempt == r.maxRetries {
			break
		}

		// Calculate backoff with jitter
		delay := r.calculateBackoff(attempt, lastErr)

		// Sleep with context cancellation support
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
			// Continue to next attempt
		}
	}

	return fmt.Errorf("operation failed after %d attempts: %w", r.maxRetries, lastErr)
}

// calculateBackoff calculates the backoff delay with jitter and special handling for rate limiting
func (r *Retrier) calculateBackoff(attempt int, lastErr error) time.Duration {
	// Base exponential backoff: delay * 2^(attempt-1)
	baseDelay := float64(r.baseDelayMs) * math.Pow(2, float64(attempt-1))

	// Special handling for rate limiting (429)
	if httpErr, ok := lastErr.(*HTTPError); ok && httpErr.StatusCode == 429 {
		// Check for Retry-After header
		if httpErr.Response != nil {
			if retryAfter := httpErr.Response.Header.Get("Retry-After"); retryAfter != "" {
				// Try parsing as seconds
				if seconds, err := strconv.Atoi(retryAfter); err == nil {
					return time.Duration(seconds) * time.Second
				}
				// Could also parse HTTP-date format, but seconds is most common
			}
		}
		// Use longer backoff for rate limiting
		baseDelay *= 2
	}

	// Add jitter: random value between 0.5x and 1.5x base delay
	// This prevents thundering herd problem
	jitter := 0.5 + r.rng.Float64() // Random between 0.5 and 1.5
	delayMs := int(baseDelay * jitter)

	// Cap at 30 seconds
	if delayMs > 30000 {
		delayMs = 30000
	}

	return time.Duration(delayMs) * time.Millisecond
}

// shouldRetry determines if an error is retryable
func (r *Retrier) shouldRetry(err error) bool {
	// Check if it's an HTTP error
	httpErr, ok := err.(*HTTPError)
	if !ok {
		// Network errors, timeouts, and other non-HTTP errors are retryable
		return true
	}

	// Don't retry client errors (4xx) except 429 (rate limit) and 408 (timeout)
	if httpErr.StatusCode >= 400 && httpErr.StatusCode < 500 {
		return httpErr.StatusCode == 429 || httpErr.StatusCode == 408
	}

	// Retry 5xx server errors
	if httpErr.StatusCode >= 500 {
		return true
	}

	// Don't retry 2xx and 3xx
	return false
}
