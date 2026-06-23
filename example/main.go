package main

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/hash-walker/go_queue"
	"github.com/jackc/pgx/v5/pgxpool"
)

type EmailPayload struct {
	To      string
	Subject string
	Body    string
}

func sendEmail(to, subject, body string) error {
	log.Printf("[MOCK EMAIL] To: %s | Subject: %s | Body: %s", to, subject, body)
	return nil
}

func handlePayment(ctx context.Context, job goqueue.Job) error {
	log.Printf("[PAYMENT HANDLER] Processing payment job ID: %s", job.ID)
	return nil
}

func main() {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://postgres:secret@localhost:5432/goqueue_demo"
	}

	// Connect to PostgreSQL database
	db, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		log.Fatalf("failed to connect to database: %v", err)
	}
	defer db.Close()

	// Initialize the WorkerPool
	pool, err := goqueue.NewWorkerPool(ctx, db, goqueue.Config{
		Concurrency:  5,
		PollInterval: 1 * time.Second,
		AutoMigrate:  true, // creates the table if it doesn't exist
	})
	if err != nil {
		log.Fatalf("failed to create worker pool: %v", err)
	}

	// Register handlers
	if err := pool.Register("send_email", func(ctx context.Context, job goqueue.Job) error {
		var payload EmailPayload
		if err := job.UnmarshalPayload(&payload); err != nil {
			return err
		}
		return sendEmail(payload.To, payload.Subject, payload.Body)
	}); err != nil {
		log.Fatalf("failed to register send_email handler: %v", err)
	}

	if err := pool.Register("process_payment", handlePayment); err != nil {
		log.Fatalf("failed to register process_payment handler: %v", err)
	}

	// Enqueue jobs
	payload1 := EmailPayload{
		To:      "user@example.com",
		Subject: "Hello",
		Body:    "Welcome to goqueue!",
	}

	jobID1, err := pool.Enqueue(ctx, db, "send_email", payload1)

	if err != nil {
		log.Fatalf("failed to enqueue job 1: %v", err)
	}
	log.Printf("Enqueued email job: %s", jobID1)

	// With options (e.g. WithPriority, WithMaxRetries, WithDelay)
	payload2 := EmailPayload{
		To:      "admin@example.com",
		Subject: "System Alert",
		Body:    "This is a high priority delayed alert.",
	}
	jobID2, err := pool.Enqueue(ctx, db, "send_email", payload2,
		goqueue.WithPriority(10),         // higher = processed first
		goqueue.WithMaxRetries(5),        // override default
		goqueue.WithDelay(5*time.Minute), // delayed execution
	)
	if err != nil {
		log.Fatalf("failed to enqueue job 2: %v", err)
	}
	log.Printf("Enqueued high priority delayed job: %s", jobID2)

	// Start processing (non-blocking)
	if err := pool.Start(ctx); err != nil {
		log.Fatalf("failed to start pool: %v", err)
	}
	log.Println("Worker pool started. Waiting for 3 seconds...")

	// Keep the main function running for a few seconds to let any pending jobs run
	time.Sleep(3 * time.Second)

	log.Println("Shutting down worker pool...")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := pool.Shutdown(shutdownCtx); err != nil {
		log.Printf("Shutdown error: %v", err)
	}
	log.Println("Shutdown complete.")
}
