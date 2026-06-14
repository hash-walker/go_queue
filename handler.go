

type HandlerFunc func(ctx context.Context, job Job) error

// Internal registry: map[string]HandlerFunc
// Panics if duplicate job type registered (fail-fast)
