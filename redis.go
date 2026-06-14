package main

type RedisConfig struct {
	Addr     string
	Password string
	DB       int
}

type redisNotifier struct { /* redis client */
}

// On Enqueue: PUBLISH "goqueue:notify" jobID
// Workers:    SUBSCRIBE "goqueue:notify" → wake up immediately
// Counters:   INCR/DECR "goqueue:depth:{status}" for fast metrics
// Fallback:   If Redis is down, workers revert to PollInterval polling
