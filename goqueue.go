package main

import (
	"log/slog"
	"time"
)

type Config struct {
	// Required
	DatabaseURL string

	// Pool
	Concurrency  int
	PollInterval time.Duration

	// Table
	SchemaName string
	TableName  string

	// Retry
	MaxRetries  int
	BackoffBase time.Duration
	BackoffMax  time.Duration
	BackoffMult float64

	// Logging
	Logger *slog.Logger
}
