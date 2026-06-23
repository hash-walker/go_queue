package goqueue

import (
	"context"
	"encoding/json"
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
	SchemaName  string
	TableName   string
	AutoMigrate bool

	// Retry
	MaxRetries   int
	BackoffBase  time.Duration
	BackoffMax   time.Duration // The soft-ceiling limit for exponential backoff.
	BackoffMulti float64

	// Logging
	Logger *slog.Logger

	// Pruning
	DeleteOnComplete bool          // If true, jobs are deleted instead of marked 'complete'
	RetentionPeriod  time.Duration // If > 0, background pruner deletes completed/failed jobs older than this
	PruneInterval    time.Duration // How often the background pruner runs (default: 1 hour)
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

func NewWorkerPool(ctx context.Context, db PoolInterface, cfg Config) (*WorkerPool, error) {

	if cfg.SchemaName == "" {
		cfg.SchemaName = "public"
	}
	if cfg.TableName == "" {
		cfg.TableName = "goqueue_jobs"
	}
	if cfg.Concurrency == 0 {
		cfg.Concurrency = 1
	}
	if cfg.PollInterval == 0 {
		cfg.PollInterval = 5 * time.Second
	}
	if cfg.MaxRetries == 0 {
		cfg.MaxRetries = 3
	}
	if cfg.BackoffBase == 0 {
		cfg.BackoffBase = 1 * time.Second
	}
	if cfg.BackoffMulti == 0 {
		cfg.BackoffMulti = 2.0 // doubles the delay on each retry: 1s, 2s, 4s, 8s...
	}
	if cfg.BackoffMax == 0 {
		cfg.BackoffMax = 1 * time.Hour
	}
	if cfg.PruneInterval == 0 {
		cfg.PruneInterval = 1 * time.Hour
	}

	if cfg.AutoMigrate {
		if err := runMigrations(ctx, db, cfg.SchemaName, cfg.TableName); err != nil {
			return nil, err
		}
	}

	return &WorkerPool{
		db:       db,
		cfg:      cfg,
		logger:   cfg.Logger,
		handlers: make(map[string]HandlerFunc),
	}, nil
}

// tableFQN returns the fully-qualified table name as "schema.table".
func (wp *WorkerPool) tableFQN() string {
	return wp.cfg.SchemaName + "." + wp.cfg.TableName
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

	return insertJob(ctx, db, wp.tableFQN(), job)
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
	if wp.cfg.RetentionPeriod > 0 {
		wp.wg.Add(1)
		go func() {
			defer wp.wg.Done()
			ticker := time.NewTicker(wp.cfg.PruneInterval)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					pruneCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					if err := pruneJobs(pruneCtx, wp.db, wp.tableFQN(), wp.cfg.RetentionPeriod); err != nil {
						if wp.logger != nil {
							wp.logger.Error("failed to prune jobs", "error", err)
						} else {
							log.Printf("failed to prune jobs: %v", err)
						}
					}
					cancel()
				}
			}
		}()
	}

	for i := range wp.cfg.Concurrency {

		wp.wg.Add(1)

		go func(workerID int) {
			defer wp.wg.Done()

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

// Shutdown gracefully waits for all active workers to finish their current jobs.
// It blocks until all workers have exited, or until the provided context expires.
func (wp *WorkerPool) Shutdown(ctx context.Context) error {
	waitChan := make(chan struct{})

	go func() {
		wp.wg.Wait()
		close(waitChan)
	}()

	select {
	case <-waitChan:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}

}

func (wp *WorkerPool) processNextJob(ctx context.Context) bool {

	job, err := fetchJob(ctx, wp.db, wp.tableFQN())

	if err != nil {
		log.Printf("Database failure: %v", err)
		return false
	}

	// fetchJob returns an empty Job (zero-value ID) when the queue is empty
	if job.ID == (uuid.UUID{}) {
		return false
	}

	// Look up handler under read lock to avoid data race with Register
	wp.handlerMut.RLock()
	handler, ok := wp.handlers[job.Type]
	wp.handlerMut.RUnlock()

	if !ok || handler == nil {
		// No handler registered — fail the job immediately so it doesn't block the queue
		log.Printf("No handler registered for job type '%s' (job %s)", job.Type, job.ID)
		failedJob(ctx, wp.db, wp.tableFQN(), job.ID, time.Now(), fmt.Errorf("no handler registered for job type '%s'", job.Type))
		return true
	}

	processErr := handler(ctx, job)

	if processErr != nil {

		runAt := calculateBackoff(job.RetryCount, wp.cfg)

		runTime := time.Now().Add(runAt)

		failedJob(ctx, wp.db, wp.tableFQN(), job.ID, runTime, processErr)
	} else {
		if err := completeJob(ctx, wp.db, wp.tableFQN(), job.ID, wp.cfg.DeleteOnComplete); err != nil {
			log.Printf("Failed to mark job %s complete: %v", job.ID, err)
		}
	}

	return true

}
