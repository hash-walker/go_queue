package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type HandlerFunc func(ctx context.Context, job Job) error

type PoolInterface interface {
	Begin(ctx context.Context) (pgx.Tx, error)
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Config holds the tuning parameters of the WorkerPool
// It dictates the concurrency levels, database targets, and how aggressively
// the system should backoff during mass failure events.
type Config struct {
	// Concurrency defines the number of parallel background workers spawned by Start().
	Concurrency  int
	PollInterval time.Duration // The baseline sleep duration when the queue is empty.

	// Table
	SchemaName string
	TableName  string

	// Retry
	MaxRetries   int
	BackoffBase  time.Duration
	BackoffMax   time.Duration // The soft-ceiling limit for exponential backoff.
	BackoffMulti float64

	// Logging
	Logger *slog.Logger
}

// WorkerPool is a highly concurrent, stateless job processing engine.
// It continuously polls the database for pending jobs and execute them via registered handlers
//
// The pool relies on PostgresSQL for state management, allowing multiple applications
// to run safely concurrent without duplicate job execution
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

// Start launches the background workers and begins processing jobs.
// It spawns a number of goroutines equal to cfg.Concurrency.
//
// To maximize throughput, workers bypass the ticker and continuously
// drain the queue as long as jobs are successfully processed. A worker
// only falls asleep and waits for the next tick if the queue is empty.
//
// To perform a graceful shutdown, cancel the provided context. Workers
// will finish their active job and cleanly exit their goroutine.
func (wp *WorkerPool) Start(ctx context.Context) error {
	for i := range wp.cfg.Concurrency {

		go func(workerID int) {
			ticker := time.NewTicker(wp.cfg.PollInterval)
			defer ticker.Stop()

			for {

				if wp.processNextJob(ctx) {
					continue
				} else {

					select {
					case <-ctx.Done():
						return
					case <-ticker.C:
					}

				}

			}

		}(i)
	}
	return nil
}

func (wp *WorkerPool) processNextJob(ctx context.Context) bool {

	job, err := fetchJob(ctx, wp.db)

	if err != nil {

		if errors.Is(err, sql.ErrNoRows) {
			return false
		}

		log.Printf("Database failure: %v", err)
		return false
	}

	handler := wp.handlers[job.Type]

	processErr := handler(ctx, job)

	if processErr != nil {

		runAt := calculateBackoff(job.RetryCount, wp.cfg)

		runTime := time.Now().Add(runAt)

		failedJob(ctx, wp.db, job.ID, runTime, processErr)
	} else {
		err = completeJob(ctx, wp.db, job.ID)
	}

	return true

}
