type BackoffConfig struct {
	Base       time.Duration // 1s
	Max        time.Duration // 5m
	Multiplier float64       // 2.0
}

// calculateBackoff returns: min(base * mult^attempt + jitter, max)
func calculateBackoff(attempt int, cfg BackoffConfig) time.Duration

// jitter: ±25% randomization to prevent thundering herd
