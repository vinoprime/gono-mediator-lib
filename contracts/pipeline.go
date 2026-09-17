// Package contracts — pipeline behavior extension contracts.
//
// Think of pipeline behaviors like airport security checkpoints:
//   - Your request (passenger) passes through each checkpoint in order.
//   - Each checkpoint can inspect, modify, or reject the request before passing it forward.
//   - The final destination (handler) only runs after all checkpoints approve.
package contracts

import "context"

// Next is a function type representing the remainder of the pipeline.
//
// A PipelineBehavior calls Next to pass control to the next behavior (or the
// final request handler). This mirrors the "next middleware" pattern used in
// HTTP frameworks like Gin or Echo.
//
// Example usage inside a behavior:
//
//	func (b *LoggingBehavior[TReq, TRes]) Handle(ctx context.Context, req TReq, next Next[TRes]) (TRes, error) {
//	    start := time.Now()
//	    res, err := next(ctx)        // call the rest of the pipeline
//	    log.Printf("took %s", time.Since(start))
//	    return res, err
//	}
type Next[TResponse any] func(ctx context.Context) (TResponse, error)

// PipelineBehavior is cross-cutting middleware executed around every request handler.
//
// Behaviors are composable and ordered — they form an onion around the handler:
//
//	[ Logging [ Recovery [ Validation [ Handler ] ] ] ]
//
// This is equivalent to MediatR's IPipelineBehavior<TRequest, TResponse>.
// Behaviors should be registered in the order they should execute (outermost first).
//
// Common use-cases:
//   - Logging & tracing
//   - Panic recovery
//   - Input validation
//   - Caching
//   - Authorization
//   - Retry/circuit-breaker
type PipelineBehavior[TRequest any, TResponse any] interface {
	// Handle wraps the pipeline execution.
	// Call next(ctx) to invoke the inner pipeline.
	// Do NOT call next(ctx) to short-circuit (e.g., return cached result).
	Handle(ctx context.Context, request TRequest, next Next[TResponse]) (TResponse, error)
}

