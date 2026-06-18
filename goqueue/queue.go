package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

var ErrJobNotFoundOrInvalidState = fmt.Errorf("job not found or not in 'running' state")

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

func fetchJob(ctx context.Context, db PoolInterface) (Job, error) {

	const sql = `
		UPDATE goqueue_jobs 
		SET status = 'running', updated_at = NOW()
		WHERE id = (
		    SELECT id
		    FROM goqueue_jobs
		    WHERE status = 'pending' AND run_at <= NOW()
		    ORDER BY priority DESC
		    LIMIT 1
		    FOR UPDATE SKIP LOCKED
		)
		RETURNING id, job_type, payload, status, priority, 
                 run_at, retry_count, max_retries, created_at, updated_at;
`
	var job Job
	err := db.QueryRow(ctx, sql).Scan(&job.ID, &job.Type, &job.Payload, &job.Status, &job.Priority,
		&job.RunAt, &job.RetryCount, &job.MaxRetries, &job.CreatedAt, &job.UpdatedAt)

	if err != nil {
		// Handle the normal case: the queue is empty
		if errors.Is(err, pgx.ErrNoRows) {
			return Job{}, nil
		}

		// Handle actual database failures
		return Job{}, fmt.Errorf("failed to fetch job: %w", err)
	}

	return job, nil
}

func completeJob(ctx context.Context, db PoolInterface, jobID uuid.UUID) error {
	const sql = `
		UPDATE goqueue_jobs
		SET status = 'complete', updated_at = NOW()
		WHERE id = $1 AND status = 'running';
`

	tag, err := db.Exec(ctx, sql, jobID)

	if err != nil {
		return fmt.Errorf("failed to complete job %s: %w", jobID, err)
	}

	if tag.RowsAffected() == 0 {
		return ErrJobNotFoundOrInvalidState
	}

	return nil
}

func failedJob(ctx context.Context, db PoolInterface, jobID uuid.UUID, runAt time.Time, err error) {
	errTxt := StringToText(err.Error())

	const sql = `
				UPDATE goqueue_jobs
				SET status = CASE WHEN retry_count < max_retries THEN 'pending' ELSE 'failed' END,
				    last_error = $3,
					retry_count = retry_count + 1,
				run_at = CASE 
				WHEN retry_count < max_retries THEN $2
				ELSE run_at
				END,
				update_at = NOW()
			WHERE id = $1;
`

	runTime := TimeToPgTime(runAt)

	_, err = db.Exec(ctx, sql, jobID, runTime, errTxt)

}

func TimeToPgTime(goTime time.Time) pgtype.Timestamp {
	return pgtype.Timestamp{Time: goTime, Valid: !goTime.IsZero()}
}

func StringToText(s string) pgtype.Text {
	return pgtype.Text{
		String: s,
		Valid:  s != "",
	}
}
