package pipeline

import (
	"context"
	"time"

	"github.com/gono-mediator-lib/contracts"
)

// TimeoutBehavior injects a deadline into the context before calling the handler.
//
// Why: Prevents runaway handlers from holding resources indefinitely.
// Each request type may require a different timeout — configure accordingly.
//
// If the parent context already has a SHORTER deadline, it is preserved
// (context.WithTimeout respects the minimum of parent deadline and new timeout).
//
// The timeout applies to the ENTIRE remaining pipeline (all inner behaviors + handler).
//
// Example — 500ms timeout for all queries:
//
//	mediator.WithBehavior(pipeline.NewTimeoutBehavior[MyQuery, MyResult](500 * time.Millisecond))
type TimeoutBehavior[TRequest any, TResponse any] struct {
	// timeout is the maximum duration allowed for the inner pipeline to complete.
	timeout time.Duration
}

// NewTimeoutBehavior creates a TimeoutBehavior with the given duration.
// Pass 0 to disable (behavior becomes a no-op pass-through).
func NewTimeoutBehavior[TRequest any, TResponse any](timeout time.Duration) *TimeoutBehavior[TRequest, TResponse] {
	return &TimeoutBehavior[TRequest, TResponse]{timeout: timeout}
}

// Handle wraps the inner pipeline with a context deadline.
// The cancel function is always called to free context resources.
func (b *TimeoutBehavior[TRequest, TResponse]) Handle(
	ctx context.Context,
	request TRequest,
	next contracts.Next[TResponse],
) (TResponse, error) {
	// Disabled — pass through with no modification.
	if b.timeout <= 0 {
		return next(ctx)
	}

	// context.WithTimeout creates a derived context that expires after b.timeout.
	// cancel() releases the timer resources — MUST be called to prevent context leak.
	timeoutCtx, cancel := context.WithTimeout(ctx, b.timeout)
	defer cancel()

	return next(timeoutCtx)
}

