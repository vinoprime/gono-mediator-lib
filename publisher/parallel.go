package publisher

import (
	"context"
	"errors"
	"sync"
)

// ParallelPublisher invokes all notification handlers concurrently using goroutines.
//
// Behaviour:
//   - ALL handlers are started simultaneously via goroutines.
//   - Publish blocks until ALL handlers have completed (or all have been attempted).
//   - All errors are collected and returned as a combined error (errors.Join).
//   - A single handler failure does NOT cancel other handlers.
//
// When to use:
//   - Handler order does NOT matter.
//   - Handlers are independent and should not block each other.
//   - You want the fastest possible fan-out (total time ≈ slowest handler, not sum).
//
// All handlers run concurrently and their errors are aggregated.
//
// Note: Handlers receive the same context. If you need to cancel all handlers on
// first failure, use a derived context with cancel — see WithContext option.
type ParallelPublisher struct{}

// NewParallelPublisher creates a parallel publishing strategy.
func NewParallelPublisher() *ParallelPublisher {
	return &ParallelPublisher{}
}

// Publish fans out to all handlers concurrently and waits for all to complete.
// All errors are aggregated via errors.Join (Go 1.20+).
func (p *ParallelPublisher) Publish(ctx context.Context, handlers []HandlerFunc) error {
	if len(handlers) == 0 {
		return nil
	}

	// errs collects errors from all goroutines.
	errs := make([]error, len(handlers))
	var wg sync.WaitGroup

	for i, handle := range handlers {
		wg.Add(1)
		// Capture loop variables before goroutine — Go 1.22+ loop-variable semantics
		// handle each iteration's variable, but we explicit-capture for Go 1.21 compat.
		idx, h := i, handle
		go func() {
			defer wg.Done()
			errs[idx] = h(ctx)
		}()
	}

	// Block until every goroutine finishes — mirrors Task.WhenAll semantics.
	wg.Wait()

	// errors.Join returns nil if all entries are nil — no manual nil check needed.
	return errors.Join(errs...)
}
