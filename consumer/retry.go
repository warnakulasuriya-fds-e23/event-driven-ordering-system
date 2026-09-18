package main

import (
	"fmt"
	"time"
)

// RetryConfig holds the retry parameters for transient operation retries.
type RetryConfig struct {
	MaxRetries int
	BaseDelay  time.Duration
}

// ExecuteWithRetry calls fn up to cfg.MaxRetries times. It retries only when
// the error is transient (determined by isTransient). Between retries it waits
// an exponentially-increasing duration: baseDelay * 2^attempt.
//
// Returns the first non-transient error, or the last error if all retries are
// exhausted. Returns nil when fn succeeds.
func ExecuteWithRetry(cfg RetryConfig, fn func() error, isTransient func(error) bool) error {
	var lastErr error
	for attempt := 0; attempt < cfg.MaxRetries; attempt++ {
		lastErr = fn()
		if lastErr == nil {
			return nil
		}
		if !isTransient(lastErr) {
			return lastErr
		}
		if attempt < cfg.MaxRetries-1 {
			delay := cfg.BaseDelay * time.Duration(1<<attempt)
			time.Sleep(delay)
		}
	}
	return lastErr
}

// kafkaTransientError returns true when err is a kafka.Error that is temporary.
// For non-kafka errors it returns false (treated as permanent).
func kafkaTransientError(err error) bool {
	type temporary interface {
		Temporary() bool
	}
	if t, ok := err.(temporary); ok {
		return t.Temporary()
	}
	return false
}

// formatRetryAttempt logs a human-readable retry attempt message.
func formatRetryAttempt(attempt, maxRetries int, err error) string {
	return fmt.Sprintf("retry attempt %d/%d after transient error: %s", attempt+1, maxRetries, err.Error())
}
