package goqueue

import (
	"math"
	"math/rand/v2"
	"time"
)

// calculateBackoff computes an exponentially increasing wait duration based on the attempt number.
//
// To prevent a thundering herd problem when a downstream service or database is recovering,
// this function applies up to 25% random jitter to the wait time.
//
// Note: Because this function uses a soft ceiling, if the calculated backoff reaches cfg.BackoffMax,
// the returned duration will be between cfg.BackoffMax and cfg.BackoffMax + (cfg.BackoffMax * 25%).
// BackoffMax is only applied if it is greater than zero.
func calculateBackoff(attempt int, cfg Config) time.Duration {

	currentBackoff := float64(cfg.BackoffBase) * math.Pow(cfg.BackoffMulti, float64(attempt))

	if cfg.BackoffMax > 0 && currentBackoff >= float64(cfg.BackoffMax) {
		currentBackoff = float64(cfg.BackoffMax)
	}

	currentBackoff += currentBackoff * 0.25 * rand.Float64()

	return time.Duration(currentBackoff)
}
