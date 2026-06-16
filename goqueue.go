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
	MaxRetries   int
	BackoffBase  time.Duration
	BackoffMax   time.Duration
	BackoffMulti float64

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
	handlerMut sync.RWMutex

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

func (wp *WorkerPool) Register(jobType string, handler HandlerFunc) error {

	wp.handlerMut.Lock()
	defer wp.handlerMut.Unlock()

	if wp.handlers == nil {
		wp.handlers = make(map[string]HandlerFunc)
	}

	if _, exists := wp.handlers[jobType]; exists {
		return fmt.Errorf("handler for job type '%s' already registered", jobType)
	}

	wp.handlers[jobType] = handler

	return nil
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

func (wp *WorkerPool) Start(ctx context.Context) error {
	for i := range wp.cfg.Concurrency {
		go func(workerID int) {
			ticker := time.NewTicker(wp.cfg.BackoffBase)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					wp.processNextJob(ctx)
				}
			}
		}(i)
	}
	return nil
}

func (wp *WorkerPool) processNextJob(ctx context.Context) {

	job, err := fetchJob(ctx, wp.db)

	cfg := NewBackoffConfig(1*time.Second, 1*time.Minute, 2.0)

	if err != nil {
		return
	}

	handler := wp.handlers[job.Type]

	processErr := handler(ctx, job)

	if processErr != nil {

		runAt := calculateBackoff(job.RetryCount, *cfg)

		runTime := time.Now().Add(runAt)

		failedJob(ctx, wp.db, job.ID, runTime, processErr)
	} else {
		err = completeJob(ctx, wp.db, job.ID)
	}

}
