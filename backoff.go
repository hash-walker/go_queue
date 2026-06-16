package main

import (
	"math"
	"math/rand/v2"
	"time"
)

// BackoffConfig defines the parameters for calculating exponential backoff.
// It utilizes a "soft-ceiling" architecture, meaning the final calculated duration
// may exceed the Max duration by up to 25% due to the application of randomized jitter.
// This design ensures distributed workers maintain scattered wake times during mass-recovery events.

type BackoffConfig struct {
	Base       time.Duration // 1s
	Max        time.Duration // 5m
	Multiplier float64       // 2.0
}

func NewBackoffConfig(base time.Duration, max time.Duration, multiplier float64) *BackoffConfig {
	return &BackoffConfig{
		Base:       base,
		Max:        max,
		Multiplier: multiplier,
	}
}

// calculateBackoff computes an exponentially increasing wait duration based on the attempt number.
//
// To prevent a thundering herd problem when a downstream service or database is recovering,
// this function applies up to 25% random jitter to the wait time.
//
// Note: Because this function uses a soft ceiling, if the calculated backoff reaches cfg.Max,
// the returned duration will be between cfg.Max and cfg.Max + (cfg.Max * 25%).

func calculateBackoff(attempt int, cfg BackoffConfig) time.Duration {

	currentBackoff := float64(cfg.Base) * math.Pow(cfg.Multiplier, float64(attempt))
	var waitDuration time.Duration

	if currentBackoff >= float64(cfg.Max) {
		currentBackoff = float64(cfg.Max)
	}

	currentBackoff += currentBackoff * 0.25 * rand.Float64()
	waitDuration = time.Duration(currentBackoff)

	return waitDuration
}
