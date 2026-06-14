package goqueue

import (
	"encoding/json"
	"time"

	uuid "github.com/jackc/pgx/pgtype/ext/satori-uuid"
)

type JobStatus string

const (
	StatusPending   JobStatus = "pending"
	StatusRunning   JobStatus = "running"
	StatusCompleted JobStatus = "completed"
	StatusFailed    JobStatus = "failed"
)

type Job struct {
	ID         uuid.UUID       `json:"id"`
	Type       string          `json:"type"`
	Payload    json.RawMessage `json:"payload"`
	Status     JobStatus       `json:"status"`
	Priority   int             `json:"priority"`
	RunAt      time.Time       `json:"run_at"`
	RetryCount int             `json:"retry_count"`
	MaxRetries int             `json:"max_retries"`
	LastError  string          `json:"last_error,omitempty"`
	CreatedAt  time.Time       `json:"created_at"`
	UpdatedAt  time.Time       `json:"updated_at"`
}

func (j Job) UnmarshalPayload(v any) error // convenience

// Functional options
type Option func(*enqueueOpts)

func WithPriority(p int) Option
func WithMaxRetries(n int) Option
func WithDelay(d time.Duration) Option
