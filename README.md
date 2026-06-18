# goqueue

A simple, reliable, PostgreSQL-backed background job queue for Go.

`goqueue` uses your existing PostgreSQL database as a durable queue backend — no extra infrastructure (Redis, RabbitMQ, etc.) required. It leverages `FOR UPDATE SKIP LOCKED` to guarantee that each job is processed by exactly one worker, even across multiple application instances.

---

## Features

- **PostgreSQL-backed** — durable, transactional, no extra services
- **At-most-once delivery** using `FOR UPDATE SKIP LOCKED`
- **Configurable concurrency** — spawn as many parallel workers as you need
- **Priority queuing** — higher-priority jobs are always processed first
- **Delayed jobs** — schedule work to run at a future time
- **Automatic retries** with exponential backoff and jitter
- **Auto-migration** — creates the jobs table on first startup
- **Graceful shutdown** — workers finish their active job before exiting
- **Concurrent-safe** — safe to call `Register` and `Enqueue` from multiple goroutines

---

## Installation

```bash
go get github.com/hash-walker/goqueue
```

Requires Go 1.25+ and PostgreSQL.

---

## Quick Start

```go
package main

import (
    "context"
    "log"
    "os"
    "time"

    "github.com/hash-walker/goqueue"
    "github.com/jackc/pgx/v5/pgxpool"
)

type EmailPayload struct {
    To      string
    Subject string
    Body    string
}

func main() {
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    // 1. Connect to PostgreSQL
    db, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
    if err != nil {
        log.Fatalf("database connection failed: %v", err)
    }
    defer db.Close()

    // 2. Create the worker pool
    pool, err := goqueue.NewWorkerPool(ctx, db, goqueue.Config{
        Concurrency: 5,
        AutoMigrate: true, // creates the goqueue_jobs table if it doesn't exist
    })
    if err != nil {
        log.Fatalf("failed to create pool: %v", err)
    }

    // 3. Register job handlers
    if err := pool.Register("send_email", func(ctx context.Context, job goqueue.Job) error {
        var p EmailPayload
        if err := job.UnmarshalPayload(&p); err != nil {
            return err
        }
        log.Printf("Sending email to %s: %s", p.To, p.Subject)
        return nil
    }); err != nil {
        log.Fatalf("register failed: %v", err)
    }

    // 4. Enqueue a job
    jobID, err := pool.Enqueue(ctx, db, "send_email", EmailPayload{
        To:      "user@example.com",
        Subject: "Welcome!",
        Body:    "Thanks for signing up.",
    })
    if err != nil {
        log.Fatalf("enqueue failed: %v", err)
    }
    log.Printf("Enqueued job: %s", jobID)

    // 5. Start workers (non-blocking)
    if err := pool.Start(ctx); err != nil {
        log.Fatalf("start failed: %v", err)
    }

    // 6. Graceful shutdown on signal / timeout
    time.Sleep(10 * time.Second)
    shutdownCtx, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel2()
    pool.Shutdown(shutdownCtx)
}
```

---

## Configuration

Pass a `Config` struct to `NewWorkerPool`. All fields have sensible defaults — you only need to set what you want to override.

```go
pool, err := goqueue.NewWorkerPool(ctx, db, goqueue.Config{
    // Worker concurrency
    Concurrency:  5,             // default: 1
    PollInterval: 2 * time.Second, // default: 5s — how long a worker sleeps when queue is empty

    // Database table
    SchemaName:  "public",      // default: "public"
    TableName:   "goqueue_jobs", // default: "goqueue_jobs"
    AutoMigrate: true,           // default: false — set true to create the table automatically

    // Retry behaviour
    MaxRetries:   5,             // default: 3 — max retries per job (0 = no retries)
    BackoffBase:  time.Second,   // default: 1s — base delay for exponential backoff
    BackoffMulti: 2.0,           // default: 2.0 — multiplier (2.0 = doubles each attempt)
    BackoffMax:   time.Hour,     // default: 1h — soft ceiling on backoff delay

    // Logging
    Logger: slog.Default(),     // default: nil (falls back to log.Printf)
})
```

### Default values summary

| Field | Default | Description |
|---|---|---|
| `Concurrency` | `1` | Number of parallel worker goroutines |
| `PollInterval` | `5s` | Sleep duration when queue is empty |
| `SchemaName` | `"public"` | PostgreSQL schema for the jobs table |
| `TableName` | `"goqueue_jobs"` | Name of the jobs table |
| `AutoMigrate` | `false` | Create the table on startup if missing |
| `MaxRetries` | `3` | Maximum retry attempts before marking a job failed |
| `BackoffBase` | `1s` | Starting delay for retry backoff |
| `BackoffMulti` | `2.0` | Exponential multiplier (`2.0` = 1s → 2s → 4s → 8s…) |
| `BackoffMax` | `1h` | Soft ceiling; delay will not exceed this value |

---

## API Reference

### `NewWorkerPool`

```go
func NewWorkerPool(ctx context.Context, db PoolInterface, cfg Config) (*WorkerPool, error)
```

Creates and initialises a new `WorkerPool`. If `cfg.AutoMigrate` is `true`, the jobs table is created/updated in the database before returning.

---

### `Register`

```go
func (wp *WorkerPool) Register(jobType string, handler HandlerFunc) error
```

Registers a handler function for a specific job type. Returns an error if a handler for that type is already registered.

```go
type HandlerFunc func(ctx context.Context, job Job) error
```

The handler receives the full `Job` struct. Use `job.UnmarshalPayload(&target)` to deserialise the JSON payload:

```go
pool.Register("resize_image", func(ctx context.Context, job goqueue.Job) error {
    var payload ResizePayload
    if err := job.UnmarshalPayload(&payload); err != nil {
        return err
    }
    return resizeImage(payload.ImageURL, payload.Width, payload.Height)
})
```

> **Note**: Register can be called safely from multiple goroutines.

---

### `Enqueue`

```go
func (wp *WorkerPool) Enqueue(
    ctx      context.Context,
    db       PoolInterface,
    jobType  string,
    payload  any,
    opts     ...JobOption,
) (uuid.UUID, error)
```

Inserts a new job into the queue. The `payload` is marshalled to JSON automatically. Returns the UUID of the newly created job.

#### Job Options

| Option | Description |
|---|---|
| `WithPriority(n int)` | Higher value = processed sooner. Default: `0` |
| `WithMaxRetries(n int)` | Override the pool-level `MaxRetries` for this job |
| `WithDelay(d time.Duration)` | Delay execution by `d` from now |
| `WithRunAt(t time.Time)` | Schedule execution at an exact time |

```go
// High-priority job with custom retry count, delayed by 5 minutes
jobID, err := pool.Enqueue(ctx, db, "send_email", payload,
    goqueue.WithPriority(10),
    goqueue.WithMaxRetries(5),
    goqueue.WithDelay(5 * time.Minute),
)
```

---

### `Start`

```go
func (wp *WorkerPool) Start(ctx context.Context) error
```

Launches `cfg.Concurrency` background worker goroutines. This call is **non-blocking** — it returns immediately after all goroutines are started.

Workers continuously drain the queue. When the queue is empty, a worker sleeps for `PollInterval` before checking again. To stop workers, cancel the `ctx` passed to `Start`.

---

### `Shutdown`

```go
func (wp *WorkerPool) Shutdown(ctx context.Context) error
```

Blocks until all active workers finish their current job and exit, or until the provided context expires. Use this after cancelling the `Start` context to ensure a clean exit:

```go
// Signal workers to stop
cancel()

// Wait up to 30 seconds for them to finish
shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
defer shutdownCancel()
if err := pool.Shutdown(shutdownCtx); err != nil {
    log.Printf("shutdown timed out: %v", err)
}
```

---

## Retry & Backoff

When a handler returns an error, the job is rescheduled for a future attempt. The retry delay is calculated using exponential backoff with random jitter:

```
delay = BackoffBase × BackoffMulti^retryCount
delay += delay × rand(0, 0.25)   // up to 25% random jitter
delay = min(delay, BackoffMax)   // soft ceiling
```

### Default retry schedule (with defaults)

| Retry attempt | Base delay | Max delay (with 25% jitter) |
|---|---|---|
| 1 | 2s | 2.5s |
| 2 | 4s | 5s |
| 3 | 8s | 10s → job marked `failed` |

After `MaxRetries` attempts, the job is permanently marked `failed` in the database. It is not deleted, so you can inspect or requeue failed jobs manually.

The jitter prevents a **thundering herd** — when a downstream service recovers, all retrying jobs don't slam it at the same instant.

---

## Job Lifecycle

```
           Enqueue()
               │
           [pending]
               │  Worker picks it up (FOR UPDATE SKIP LOCKED)
               ▼
           [running]
          ╱          ╲
    Success          Error
       │                │
   [complete]      retry_count < max_retries?
                   ╱             ╲
                 Yes              No
                  │                │
             [pending]         [failed]
          (run_at = future)
```

---

## Database Schema

The jobs table is created automatically when `AutoMigrate: true`:

```sql
CREATE TABLE IF NOT EXISTS public.goqueue_jobs (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_type    VARCHAR(100) NOT NULL,
    payload     JSONB NOT NULL DEFAULT '{}',
    status      VARCHAR(20) NOT NULL DEFAULT 'pending',  -- pending | running | complete | failed
    priority    INT NOT NULL DEFAULT 0,
    run_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    retry_count INT NOT NULL DEFAULT 0,
    max_retries INT NOT NULL DEFAULT 3,
    last_error  TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Partial index for efficient worker polling
CREATE INDEX IF NOT EXISTS idx_goqueue_jobs_fetch
    ON public.goqueue_jobs(status, priority DESC, run_at)
    WHERE status NOT IN ('complete', 'failed');
```

> The table and schema name are configurable via `Config.TableName` and `Config.SchemaName`.

---

## Running the Example

The `example/` directory contains a fully working demo using Docker Compose.

```bash
cd goqueue/example
docker compose up --build
```

This starts:
- A **PostgreSQL 16** instance
- The **worker** application which enqueues and processes sample jobs

The `DATABASE_URL` environment variable is read automatically from the compose environment. For local development without Docker:

```bash
export DATABASE_URL="postgres://postgres:secret@localhost:5432/goqueue_demo"
go run ./example/
```

---

## Running Tests

The test suite uses a mock `PoolInterface` and requires no database connection:

```bash
go test ./... -race
```

To run with verbose output:

```bash
go test ./... -v -race
```

### What is tested

| Area | Tests |
|---|---|
| `calculateBackoff` | Zero base, exponential growth, max capping, zero-max no-cap regression, jitter bounds, zero multiplier behaviour |
| `NewWorkerPool` | All default values, custom value preservation, table FQN formatting |
| `Register` | Success, duplicate error, multiple types, concurrent safety (race detector) |
| `processNextJob` | Empty queue, DB error, nil handler (immediate fail), handler success → complete, handler failure → fail with backoff time and error text |
| Job options | `WithPriority`, `WithMaxRetries`, `WithDelay`, `WithRunAt`, combined options |
| Payload | `UnmarshalPayload` success and invalid JSON |
| Helpers | `TimeToPgTime`, `StringToText` (valid/invalid paths) |

---

## Design Notes

- **No polling loop overhead**: Workers bypass the ticker and spin continuously while jobs are available. They only sleep after an empty fetch.
- **Stateless workers**: All state lives in PostgreSQL. Multiple application instances can run simultaneously without coordination — `FOR UPDATE SKIP LOCKED` ensures no job is processed twice.
- **Unregistered handlers**: If a job is dequeued but no handler is registered for its type, the job is immediately marked as failed (with the error message recorded) rather than crashing or re-queuing silently.
- **`PoolInterface` compatibility**: Any type satisfying `PoolInterface` works — `*pgxpool.Pool` from `github.com/jackc/pgx/v5/pgxpool` is the intended production implementation. The interface is narrow enough to be mocked easily in tests.

---

## License

MIT