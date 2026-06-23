package goqueue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

// ---------------------------------------------------------------------------
// Mock types
// ---------------------------------------------------------------------------

// mockRow implements pgx.Row for testing without a real database.
type mockRow struct {
	scanFn func(dest ...any) error
}

func (r *mockRow) Scan(dest ...any) error {
	if r.scanFn != nil {
		return r.scanFn(dest...)
	}
	return pgx.ErrNoRows
}

// mockDB implements PoolInterface for testing. Set only the fields relevant
// to each test; unused methods return safe zero values.
type mockDB struct {
	queryRowFn func(ctx context.Context, sql string, args ...any) pgx.Row
	execFn     func(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
}

func (m *mockDB) Begin(ctx context.Context) (pgx.Tx, error)                               { return nil, nil }
func (m *mockDB) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)    { return nil, nil }
func (m *mockDB) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	if m.queryRowFn != nil {
		return m.queryRowFn(ctx, sql, args...)
	}
	return &mockRow{} // returns pgx.ErrNoRows by default
}
func (m *mockDB) Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
	if m.execFn != nil {
		return m.execFn(ctx, sql, arguments...)
	}
	return pgconn.NewCommandTag("UPDATE 1"), nil
}

// populateFetchJobScan fills the Scan destination slice with the provided job
// fields, simulating what PostgreSQL would return for a fetchJob query.
// Destination order must match the RETURNING clause in fetchJob.
func populateFetchJobScan(dest []any, id uuid.UUID, jobType string, retryCount, maxRetries int) error {
	*dest[0].(*uuid.UUID) = id
	*dest[1].(*string) = jobType
	*dest[2].(*json.RawMessage) = json.RawMessage(`{"msg":"test"}`)
	*dest[3].(*JobStatus) = StatusRunning
	*dest[4].(*int) = 0 // priority
	*dest[5].(*time.Time) = time.Now()
	*dest[6].(*int) = retryCount
	*dest[7].(*int) = maxRetries
	*dest[8].(*time.Time) = time.Now()
	*dest[9].(*time.Time) = time.Now()
	return nil
}

// newTestPool creates a WorkerPool with nil DB (safe when AutoMigrate is false).
func newTestPool(t *testing.T, cfg Config) *WorkerPool {
	t.Helper()
	pool, err := NewWorkerPool(context.Background(), nil, cfg)
	if err != nil {
		t.Fatalf("NewWorkerPool: unexpected error: %v", err)
	}
	return pool
}

// ---------------------------------------------------------------------------
// NewWorkerPool — default values
// ---------------------------------------------------------------------------

func TestNewWorkerPool_AppliesDefaults(t *testing.T) {
	pool := newTestPool(t, Config{}) // all zero values

	type check struct {
		name string
		got  any
		want any
	}
	checks := []check{
		{"SchemaName", pool.cfg.SchemaName, "public"},
		{"TableName", pool.cfg.TableName, "goqueue_jobs"},
		{"Concurrency", pool.cfg.Concurrency, 1},
		{"PollInterval", pool.cfg.PollInterval, 5 * time.Second},
		{"MaxRetries", pool.cfg.MaxRetries, 3},
		{"BackoffBase", pool.cfg.BackoffBase, time.Second},
		{"BackoffMulti", pool.cfg.BackoffMulti, 2.0},
		{"BackoffMax", pool.cfg.BackoffMax, time.Hour},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("Config.%s = %v, want %v", c.name, c.got, c.want)
		}
	}
}

func TestNewWorkerPool_PreservesCustomValues(t *testing.T) {
	custom := Config{
		SchemaName:   "myschema",
		TableName:    "myjobs",
		Concurrency:  10,
		PollInterval: 30 * time.Second,
		MaxRetries:   7,
		BackoffBase:  500 * time.Millisecond,
		BackoffMulti: 3.0,
		BackoffMax:   30 * time.Minute,
	}
	pool := newTestPool(t, custom)

	if pool.cfg.SchemaName != "myschema" {
		t.Errorf("SchemaName = %q, want %q", pool.cfg.SchemaName, "myschema")
	}
	if pool.cfg.TableName != "myjobs" {
		t.Errorf("TableName = %q, want %q", pool.cfg.TableName, "myjobs")
	}
	if pool.cfg.Concurrency != 10 {
		t.Errorf("Concurrency = %d, want 10", pool.cfg.Concurrency)
	}
	if pool.cfg.PollInterval != 30*time.Second {
		t.Errorf("PollInterval = %v, want 30s", pool.cfg.PollInterval)
	}
	if pool.cfg.MaxRetries != 7 {
		t.Errorf("MaxRetries = %d, want 7", pool.cfg.MaxRetries)
	}
	if pool.cfg.BackoffBase != 500*time.Millisecond {
		t.Errorf("BackoffBase = %v, want 500ms", pool.cfg.BackoffBase)
	}
	if pool.cfg.BackoffMulti != 3.0 {
		t.Errorf("BackoffMulti = %v, want 3.0", pool.cfg.BackoffMulti)
	}
	if pool.cfg.BackoffMax != 30*time.Minute {
		t.Errorf("BackoffMax = %v, want 30m", pool.cfg.BackoffMax)
	}
}

func TestNewWorkerPool_TableFQN(t *testing.T) {
	pool := newTestPool(t, Config{SchemaName: "myschema", TableName: "myjobs"})
	want := "myschema.myjobs"
	got := pool.tableFQN()
	if got != want {
		t.Errorf("tableFQN() = %q, want %q", got, want)
	}
}

// ---------------------------------------------------------------------------
// Register
// ---------------------------------------------------------------------------

func TestRegister_Success(t *testing.T) {
	pool := newTestPool(t, Config{})
	handler := func(ctx context.Context, job Job) error { return nil }

	err := pool.Register("send_email", handler)
	if err != nil {
		t.Fatalf("Register returned unexpected error: %v", err)
	}

	pool.handlerMut.RLock()
	_, ok := pool.handlers["send_email"]
	pool.handlerMut.RUnlock()

	if !ok {
		t.Error("handler was not stored after Register")
	}
}

func TestRegister_DuplicateReturnsError(t *testing.T) {
	pool := newTestPool(t, Config{})
	handler := func(ctx context.Context, job Job) error { return nil }

	pool.Register("send_email", handler) //nolint

	err := pool.Register("send_email", handler)
	if err == nil {
		t.Error("expected error when registering duplicate job type, got nil")
	}
	if !strings.Contains(err.Error(), "send_email") {
		t.Errorf("error message should mention the job type, got: %v", err)
	}
}

func TestRegister_MultipleTypesSucceed(t *testing.T) {
	pool := newTestPool(t, Config{})
	handler := func(ctx context.Context, job Job) error { return nil }

	types := []string{"send_email", "process_payment", "send_sms"}
	for _, jobType := range types {
		if err := pool.Register(jobType, handler); err != nil {
			t.Errorf("Register(%q): unexpected error: %v", jobType, err)
		}
	}

	pool.handlerMut.RLock()
	count := len(pool.handlers)
	pool.handlerMut.RUnlock()

	if count != len(types) {
		t.Errorf("expected %d handlers, got %d", len(types), count)
	}
}

// TestRegister_Concurrent verifies there are no data races when Register is
// called concurrently for different job types. Run with -race to validate.
func TestRegister_Concurrent(t *testing.T) {
	pool := newTestPool(t, Config{})
	handler := func(ctx context.Context, job Job) error { return nil }

	const n = 20
	var wg sync.WaitGroup
	wg.Add(n)

	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			pool.Register(fmt.Sprintf("job_type_%d", i), handler) //nolint
		}(i)
	}
	wg.Wait()

	pool.handlerMut.RLock()
	count := len(pool.handlers)
	pool.handlerMut.RUnlock()

	if count != n {
		t.Errorf("expected %d registered handlers, got %d", n, count)
	}
}

// ---------------------------------------------------------------------------
// processNextJob
// ---------------------------------------------------------------------------

func TestProcessNextJob_EmptyQueue_ReturnsFalse(t *testing.T) {
	pool := newTestPool(t, Config{})
	pool.db = &mockDB{
		queryRowFn: func(ctx context.Context, sql string, args ...any) pgx.Row {
			return &mockRow{
				// pgx.ErrNoRows causes fetchJob to return Job{}, nil
				scanFn: func(dest ...any) error { return pgx.ErrNoRows },
			}
		},
	}

	got := pool.processNextJob(context.Background())
	if got != false {
		t.Error("expected false for empty queue, got true")
	}
}

func TestProcessNextJob_DBError_ReturnsFalse(t *testing.T) {
	pool := newTestPool(t, Config{})
	pool.db = &mockDB{
		queryRowFn: func(ctx context.Context, sql string, args ...any) pgx.Row {
			return &mockRow{
				scanFn: func(dest ...any) error {
					return errors.New("connection refused")
				},
			}
		},
	}

	got := pool.processNextJob(context.Background())
	if got != false {
		t.Error("expected false on DB error, got true")
	}
}

func TestProcessNextJob_NoHandlerRegistered_FailsJobAndReturnsTrue(t *testing.T) {
	testJobID := uuid.New()
	execCalled := false

	pool := newTestPool(t, Config{})
	pool.db = &mockDB{
		queryRowFn: func(ctx context.Context, sql string, args ...any) pgx.Row {
			return &mockRow{
				scanFn: func(dest ...any) error {
					return populateFetchJobScan(dest, testJobID, "unknown_type", 0, 3)
				},
			}
		},
		execFn: func(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
			execCalled = true
			return pgconn.NewCommandTag("UPDATE 1"), nil
		},
	}
	// Note: no handlers registered for "unknown_type"

	got := pool.processNextJob(context.Background())
	if !got {
		t.Error("expected true (job dequeued and failed), got false")
	}
	if !execCalled {
		t.Error("expected failedJob (Exec) to be called for unregistered handler, but Exec was not called")
	}
}

func TestProcessNextJob_HandlerSuccess_CompletesJob(t *testing.T) {
	testJobID := uuid.New()
	handlerCalled := false
	var execSQL string

	pool := newTestPool(t, Config{})
	pool.db = &mockDB{
		queryRowFn: func(ctx context.Context, sql string, args ...any) pgx.Row {
			return &mockRow{
				scanFn: func(dest ...any) error {
					return populateFetchJobScan(dest, testJobID, "test_job", 0, 3)
				},
			}
		},
		execFn: func(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
			execSQL = sql
			return pgconn.NewCommandTag("UPDATE 1"), nil
		},
	}
	pool.Register("test_job", func(ctx context.Context, job Job) error { //nolint
		handlerCalled = true
		return nil // success
	})

	got := pool.processNextJob(context.Background())
	if !got {
		t.Error("expected true (job processed), got false")
	}
	if !handlerCalled {
		t.Error("expected handler to be called, but it was not")
	}
	// completeJob SQL sets status = 'complete'
	if !strings.Contains(execSQL, "complete") {
		t.Errorf("expected completeJob SQL to contain 'complete', got: %s", execSQL)
	}
}

func TestProcessNextJob_HandlerFailure_FailsJobWithBackoff(t *testing.T) {
	testJobID := uuid.New()
	handlerCalled := false
	var execSQL string
	var execArgs []any

	pool := newTestPool(t, Config{
		BackoffBase:  10 * time.Second, // attempt 0: 10s base → run_at is at least 10s from now
		BackoffMulti: 2.0,
		BackoffMax:   time.Hour,
		MaxRetries:   3,
	})
	pool.db = &mockDB{
		queryRowFn: func(ctx context.Context, sql string, args ...any) pgx.Row {
			return &mockRow{
				scanFn: func(dest ...any) error {
					return populateFetchJobScan(dest, testJobID, "test_job", 0, 3)
				},
			}
		},
		execFn: func(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
			execSQL = sql
			execArgs = arguments
			return pgconn.NewCommandTag("UPDATE 1"), nil
		},
	}
	pool.Register("test_job", func(ctx context.Context, job Job) error { //nolint
		handlerCalled = true
		return errors.New("downstream service unavailable")
	})

	now := time.Now()
	got := pool.processNextJob(context.Background())

	if !got {
		t.Error("expected true (job dequeued even if handler failed), got false")
	}
	if !handlerCalled {
		t.Error("expected handler to be called, but it was not")
	}
	// failedJob SQL checks retry_count < max_retries
	if !strings.Contains(execSQL, "retry_count") {
		t.Errorf("expected failedJob SQL to reference retry_count, got: %s", execSQL)
	}
	// Verify run_at (execArgs[1]) is at least BackoffBase in the future
	if len(execArgs) < 2 {
		t.Fatalf("expected at least 2 exec args ($1=jobID, $2=runAt), got %d", len(execArgs))
	}
	runAt, ok := execArgs[1].(pgtype.Timestamp)
	if !ok {
		t.Fatalf("execArgs[1] expected pgtype.Timestamp, got %T", execArgs[1])
	}
	if runAt.Time.Before(now.Add(10 * time.Second)) {
		t.Errorf("expected runAt to be >= now+10s (BackoffBase), got %v (now=%v)", runAt.Time, now)
	}
}

func TestProcessNextJob_HandlerFailure_ErrorRecordedInJob(t *testing.T) {
	testJobID := uuid.New()
	handlerErr := errors.New("payment gateway timeout")

	var execArgs []any
	pool := newTestPool(t, Config{})
	pool.db = &mockDB{
		queryRowFn: func(ctx context.Context, sql string, args ...any) pgx.Row {
			return &mockRow{
				scanFn: func(dest ...any) error {
					return populateFetchJobScan(dest, testJobID, "payment_job", 0, 3)
				},
			}
		},
		execFn: func(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
			execArgs = arguments
			return pgconn.NewCommandTag("UPDATE 1"), nil
		},
	}
	pool.Register("payment_job", func(ctx context.Context, job Job) error { //nolint
		return handlerErr
	})

	pool.processNextJob(context.Background())

	// execArgs: $1=jobID, $2=runAt, $3=errTxt
	if len(execArgs) < 3 {
		t.Fatalf("expected 3 exec args, got %d", len(execArgs))
	}
	errTxt, ok := execArgs[2].(pgtype.Text)
	if !ok {
		t.Fatalf("execArgs[2] expected pgtype.Text, got %T", execArgs[2])
	}
	if !strings.Contains(errTxt.String, handlerErr.Error()) {
		t.Errorf("expected error text to contain %q, got %q", handlerErr.Error(), errTxt.String)
	}
	if !errTxt.Valid {
		t.Error("expected errTxt.Valid = true, got false")
	}
}

// ---------------------------------------------------------------------------
// Job options
// ---------------------------------------------------------------------------

func TestWithPriority(t *testing.T) {
	job := Job{Priority: 0}
	WithPriority(99)(&job)
	if job.Priority != 99 {
		t.Errorf("WithPriority(99): got %d, want 99", job.Priority)
	}
}

func TestWithMaxRetries(t *testing.T) {
	job := Job{MaxRetries: 0}
	WithMaxRetries(7)(&job)
	if job.MaxRetries != 7 {
		t.Errorf("WithMaxRetries(7): got %d, want 7", job.MaxRetries)
	}
}

func TestWithDelay(t *testing.T) {
	job := Job{RunAt: time.Now()}
	before := time.Now()
	WithDelay(30 * time.Second)(&job)
	after := time.Now()

	minExpected := before.Add(30 * time.Second)
	maxExpected := after.Add(30 * time.Second)

	if job.RunAt.Before(minExpected) || job.RunAt.After(maxExpected) {
		t.Errorf("WithDelay(30s): RunAt=%v not in expected range [%v, %v]", job.RunAt, minExpected, maxExpected)
	}
}

func TestWithRunAt(t *testing.T) {
	target := time.Date(2030, 1, 1, 12, 0, 0, 0, time.UTC)
	job := Job{}
	WithRunAt(target)(&job)
	if !job.RunAt.Equal(target) {
		t.Errorf("WithRunAt: got %v, want %v", job.RunAt, target)
	}
}

// TestJobOptions_Combined verifies options compose correctly on a single job.
func TestJobOptions_Combined(t *testing.T) {
	job := Job{RunAt: time.Now()}
	before := time.Now()
	opts := []JobOption{
		WithPriority(5),
		WithMaxRetries(10),
		WithDelay(1 * time.Minute),
	}
	for _, opt := range opts {
		opt(&job)
	}

	if job.Priority != 5 {
		t.Errorf("Priority = %d, want 5", job.Priority)
	}
	if job.MaxRetries != 10 {
		t.Errorf("MaxRetries = %d, want 10", job.MaxRetries)
	}
	if job.RunAt.Before(before.Add(1 * time.Minute)) {
		t.Errorf("RunAt %v is too early; expected >= %v", job.RunAt, before.Add(time.Minute))
	}
}

// ---------------------------------------------------------------------------
// UnmarshalPayload
// ---------------------------------------------------------------------------

func TestJob_UnmarshalPayload_Success(t *testing.T) {
	type Payload struct {
		Name  string `json:"name"`
		Score int    `json:"score"`
	}
	orig := Payload{Name: "Alice", Score: 42}
	data, _ := json.Marshal(orig)

	job := Job{Payload: json.RawMessage(data)}
	var got Payload
	if err := job.UnmarshalPayload(&got); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Name != "Alice" || got.Score != 42 {
		t.Errorf("got %+v, want {Name:Alice Score:42}", got)
	}
}

func TestJob_UnmarshalPayload_InvalidJSON(t *testing.T) {
	job := Job{Payload: json.RawMessage(`not-valid-json`)}
	var got map[string]any
	err := job.UnmarshalPayload(&got)
	if err == nil {
		t.Error("expected error for invalid JSON payload, got nil")
	}
}

// ---------------------------------------------------------------------------
// TimeToPgTime / StringToText helpers
// ---------------------------------------------------------------------------

func TestTimeToPgTime_NonZero(t *testing.T) {
	now := time.Now()
	result := TimeToPgTime(now)
	if !result.Valid {
		t.Error("TimeToPgTime: Valid should be true for non-zero time")
	}
	if !result.Time.Equal(now) {
		t.Errorf("TimeToPgTime: Time = %v, want %v", result.Time, now)
	}
}

func TestTimeToPgTime_ZeroTime(t *testing.T) {
	result := TimeToPgTime(time.Time{})
	if result.Valid {
		t.Error("TimeToPgTime: Valid should be false for zero time")
	}
}

func TestStringToText_NonEmpty(t *testing.T) {
	result := StringToText("hello")
	if !result.Valid {
		t.Error("StringToText: Valid should be true for non-empty string")
	}
	if result.String != "hello" {
		t.Errorf("StringToText: String = %q, want %q", result.String, "hello")
	}
}

func TestStringToText_Empty(t *testing.T) {
	result := StringToText("")
	if result.Valid {
		t.Error("StringToText: Valid should be false for empty string")
	}
}

// ---------------------------------------------------------------------------
// Pruning and Completion helpers
// ---------------------------------------------------------------------------

func TestCompleteJob_Update(t *testing.T) {
	var execSQL string
	db := &mockDB{
		execFn: func(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
			execSQL = sql
			return pgconn.NewCommandTag("UPDATE 1"), nil
		},
	}
	err := completeJob(context.Background(), db, "jobs", uuid.New(), false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(execSQL, "UPDATE") || strings.Contains(execSQL, "DELETE") {
		t.Errorf("expected UPDATE statement, got: %s", execSQL)
	}
}

func TestCompleteJob_Delete(t *testing.T) {
	var execSQL string
	db := &mockDB{
		execFn: func(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
			execSQL = sql
			return pgconn.NewCommandTag("DELETE 1"), nil
		},
	}
	err := completeJob(context.Background(), db, "jobs", uuid.New(), true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(execSQL, "DELETE") || strings.Contains(execSQL, "UPDATE") {
		t.Errorf("expected DELETE statement, got: %s", execSQL)
	}
}

func TestPruneJobs(t *testing.T) {
	var execSQL string
	db := &mockDB{
		execFn: func(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
			execSQL = sql
			return pgconn.NewCommandTag("DELETE 10"), nil
		},
	}
	err := pruneJobs(context.Background(), db, "jobs", 24*time.Hour)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(execSQL, "DELETE") {
		t.Errorf("expected DELETE statement, got: %s", execSQL)
	}
	if !strings.Contains(execSQL, "86400") { // 24 hours in seconds
		t.Errorf("expected 86400 seconds in interval, got: %s", execSQL)
	}
}
