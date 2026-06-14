package main

import (
	"context"
	"fmt"

	"github.com/google/uuid"
)

func insertJob(ctx context.Context, db PoolInterface, job Job) (uuid.UUID, error) {

	const sql = `
		INSERT INTO goqueue_jobs (
		    job_type, payload, status, priority,
		    run_at, retry_count, max_retries, created_at, updated_at
		) VALUES (
		    $1, $2, $3, $4, $5, $6, $7, $8, $9
		) RETURNING id;
`

	var returnedID uuid.UUID

	err := db.QueryRow(ctx, sql, job.Type, job.Payload, job.Status, job.Priority, job.RunAt, job.RetryCount, job.MaxRetries, job.CreatedAt, job.UpdatedAt).Scan(&returnedID)

	if err != nil {
		// Return the error so the caller knows the insert failed
		return uuid.Nil, fmt.Errorf("failed to insert job: %w", err)
	}

	return returnedID, nil
}
