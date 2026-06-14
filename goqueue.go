package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type PoolInterface interface {
	Begin(ctx context.Context) (pgx.Tx, error)
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

type Config struct {
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

type WorkerPool struct {
	// database

	db PoolInterface

	// config and state
	cfg    Config
	logger *slog.Logger

	handlers   map[string]HandlerFunc
	handlerMut sync.Mutex

	// Lifecycle state
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func NewWorkerPool(db PoolInterface, cfg Config) *WorkerPool {
	return &WorkerPool{
		db:       db,
		cfg:      cfg,
		logger:   cfg.Logger,
		handlers: make(map[string]HandlerFunc),
	}
}

func (wp *WorkerPool) Enqueue(ctx context.Context, db PoolInterface, jobType string, payload any, opts ...JobOption) (uuid.UUID, error) {

	jsonBytes, err := json.Marshal(payload)

	if err != nil {
		return uuid.Nil, fmt.Errorf("failed to marshal job payload: %w", err)
	}

	job := Job{
		Type:       jobType,
		Payload:    jsonBytes,
		Status:     StatusPending,
		Priority:   0,
		RunAt:      time.Now(),
		RetryCount: 0,
		MaxRetries: wp.cfg.MaxRetries,
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
	}

	for _, opt := range opts {
		opt(&job)
	}

	return insertJob(ctx, db, job)
}
