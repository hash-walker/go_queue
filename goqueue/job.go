package main

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
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

type JobOption func(*Job)

func WithPriority(priority int) JobOption {
	return func(j *Job) {
		j.Priority = priority
	}
}

func WithRunAt(runAt time.Time) JobOption {
	return func(j *Job) {
		j.RunAt = runAt
	}
}

func WithMaxRetries(maxRetries int) JobOption {
	return func(j *Job) {
		j.MaxRetries = maxRetries
	}
}

func (j Job) UnmarshalPayload(v any) error {
	return json.Unmarshal(j.Payload, v)
}
