package main

import (
	"context"
	"log/slog"
	"net/http"
	"time"
)

type Config struct {
	// Required
	DatabaseURL string

	// Pool
	Concurrency  int           // default: 5
	PollInterval time.Duration // default: 200ms (ignored if Redis enabled)

	// Table
	SchemaName string // default: "public"
	TableName  string // default: "goqueue_jobs"

	// Retry
	MaxRetries  int           // default: 3
	BackoffBase time.Duration // default: 1s
	BackoffMax  time.Duration // default: 5m
	BackoffMult float64       // default: 2.0

	// Optional
	Redis          *RedisConfig // nil = polling mode
	MetricsEnabled bool         // default: false
	AutoMigrate    bool         // default: true

	// Logging
	Logger *slog.Logger // default: slog.Default()
}

type WorkerPool struct { /* unexported fields */
}

func New(cfg Config) (*WorkerPool, error)
func (wp *WorkerPool) Register(jobType string, handler HandlerFunc)

// func (wp *WorkerPool) Enqueue(ctx context.Context, jobType string, payload any, opts ...Option) (uuid.UUID, error)
func (wp *WorkerPool) Start(ctx context.Context) error    // blocks, runs workers
func (wp *WorkerPool) Shutdown(ctx context.Context) error // graceful drain
func (wp *WorkerPool) DashboardHandler() http.Handler
func (wp *WorkerPool) StatsHandler() http.Handler

// func (wp *WorkerPool) Stats(ctx context.Context) (QueueStats, error)
