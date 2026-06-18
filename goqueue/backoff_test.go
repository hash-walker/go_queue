package goqueue

import (
	"math"
	"testing"
	"time"
)

// TestCalculateBackoff_ZeroBase verifies that a zero BackoffBase always returns 0
// regardless of attempt number — no delay if the base is not configured.
func TestCalculateBackoff_ZeroBase(t *testing.T) {
	cfg := Config{
		BackoffBase:  0,
		BackoffMulti: 2.0,
		BackoffMax:   time.Hour,
	}
	for attempt := 0; attempt <= 5; attempt++ {
		got := calculateBackoff(attempt, cfg)
		if got != 0 {
			t.Errorf("attempt %d: expected 0 with zero BackoffBase, got %v", attempt, got)
		}
	}
}

// TestCalculateBackoff_ExponentialGrowth verifies the delay doubles on each retry
// and that jitter stays within the [base, base*1.25] range.
func TestCalculateBackoff_ExponentialGrowth(t *testing.T) {
	cfg := Config{
		BackoffBase:  time.Second,
		BackoffMulti: 2.0,
		BackoffMax:   time.Hour,
	}

	tests := []struct {
		attempt     int
		base        time.Duration // expected minimum (before jitter)
	}{
		{0, 1 * time.Second},
		{1, 2 * time.Second},
		{2, 4 * time.Second},
		{3, 8 * time.Second},
		{4, 16 * time.Second},
	}

	for _, tt := range tests {
		got := calculateBackoff(tt.attempt, cfg)
		maxWithJitter := time.Duration(float64(tt.base) * 1.25)
		if got < tt.base {
			t.Errorf("attempt %d: got %v, expected >= %v (base)", tt.attempt, got, tt.base)
		}
		if got > maxWithJitter {
			t.Errorf("attempt %d: got %v, expected <= %v (base+25%% jitter)", tt.attempt, got, maxWithJitter)
		}
	}
}

// TestCalculateBackoff_MaxCap verifies that once the computed value exceeds BackoffMax,
// it is clamped to BackoffMax (plus up to 25% jitter on the capped value).
func TestCalculateBackoff_MaxCap(t *testing.T) {
	cfg := Config{
		BackoffBase:  time.Second,
		BackoffMulti: 2.0,
		BackoffMax:   10 * time.Second, // attempt 4 would be 16s — exceeds the cap
	}

	got := calculateBackoff(4, cfg) // raw = 16s, should be capped to 10s
	minExpected := 10 * time.Second
	maxExpected := time.Duration(float64(10*time.Second) * 1.25) // 12.5s

	if got < minExpected || got > maxExpected {
		t.Errorf("got %v, expected in [%v, %v] after capping at BackoffMax", got, minExpected, maxExpected)
	}
}

// TestCalculateBackoff_ZeroMaxNoCap verifies that when BackoffMax is 0 (unset),
// no cap is applied and the full exponential value is returned.
// This was the critical bug: BackoffMax=0 used to clamp *everything* to 0.
func TestCalculateBackoff_ZeroMaxNoCap(t *testing.T) {
	cfg := Config{
		BackoffBase:  time.Second,
		BackoffMulti: 2.0,
		BackoffMax:   0, // deliberately unset
	}

	// attempt 10: 1s * 2^10 = 1024s — must NOT be clamped to 0
	expectedBase := time.Duration(float64(time.Second) * math.Pow(2.0, 10))
	got := calculateBackoff(10, cfg)

	if got < expectedBase {
		t.Errorf("BackoffMax=0 wrongly capped result: got %v, expected >= %v", got, expectedBase)
	}
}

// TestCalculateBackoff_JitterBounds runs calculateBackoff 200 times and verifies
// the returned value is always in [base, base * 1.25] — i.e. jitter is non-negative
// and never exceeds 25%.
func TestCalculateBackoff_JitterBounds(t *testing.T) {
	cfg := Config{
		BackoffBase:  time.Second,
		BackoffMulti: 2.0,
		BackoffMax:   time.Hour,
	}
	base := time.Second
	max := time.Duration(float64(base) * 1.25)

	for i := 0; i < 200; i++ {
		got := calculateBackoff(0, cfg)
		if got < base {
			t.Errorf("run %d: jitter went negative — got %v < base %v", i, got, base)
		}
		if got > max {
			t.Errorf("run %d: jitter exceeded 25%% — got %v > max %v", i, got, max)
		}
	}
}

// TestCalculateBackoff_ZeroMultiplier_AfterFirstAttempt verifies that a zero
// BackoffMulti produces 0 for every attempt > 0 (math.Pow(0,n) = 0 for n > 0).
// This documents the expected behaviour when the multiplier is not configured.
func TestCalculateBackoff_ZeroMultiplier_AfterFirstAttempt(t *testing.T) {
	cfg := Config{
		BackoffBase:  time.Second,
		BackoffMulti: 0, // not configured — no exponential growth
		BackoffMax:   time.Hour,
	}

	// attempt 0: 1s * 0^0 = 1s * 1 = 1s (math.Pow(0,0) == 1.0)
	got0 := calculateBackoff(0, cfg)
	if got0 < time.Second {
		t.Errorf("attempt 0: got %v, expected >= 1s", got0)
	}

	// attempt 1+: 1s * 0^n = 0 (no delay — this is the broken behaviour fixed by defaults)
	for attempt := 1; attempt <= 3; attempt++ {
		got := calculateBackoff(attempt, cfg)
		if got != 0 {
			t.Errorf("attempt %d with zero multiplier: got %v, expected 0", attempt, got)
		}
	}
}
