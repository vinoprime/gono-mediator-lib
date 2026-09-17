package pipeline

import (
	"context"
	"fmt"
	"runtime/debug"

	"github.com/gono-mediator-lib/contracts"
	"github.com/gono-mediator-lib/mediator"
)

// RecoveryBehavior wraps the pipeline in a panic recovery guard.
//
// Why: A panicking handler would crash the entire goroutine (and potentially the
// process if unrecovered). Recovery converts the panic to a *mediator.PanicError,
// allowing the caller to handle it gracefully and log a full stack trace.
//
// This behavior should be registered OUTERMOST (first) so it catches panics from
// ALL subsequent behaviors and the handler itself.
//
// The returned error is *mediator.PanicError, which callers can inspect:
//
//	var pe *mediator.PanicError
//	if errors.As(err, &pe) {
//	    log.Error("panic recovered", "value", pe.Value, "stack", string(pe.Stack))
//	}
type RecoveryBehavior[TRequest any, TResponse any] struct{}

// NewRecoveryBehavior creates a RecoveryBehavior.
func NewRecoveryBehavior[TRequest any, TResponse any]() *RecoveryBehavior[TRequest, TResponse] {
	return &RecoveryBehavior[TRequest, TResponse]{}
}

// Handle wraps next(ctx) in a deferred recover to catch and convert panics.
func (b *RecoveryBehavior[TRequest, TResponse]) Handle(
	ctx context.Context,
	request TRequest,
	next contracts.Next[TResponse],
) (response TResponse, err error) {
	defer func() {
		if r := recover(); r != nil {
			// Capture the full goroutine stack at the point of panic.
			// debug.Stack() is safe to call inside a recover block.
			stack := debug.Stack()

			// Wrap the panic value in a typed error so callers can use errors.As.
			err = &mediator.PanicError{
				Value: r,
				Stack: stack,
			}

			// response is the zero value for TResponse — safe to return.
			response = *new(TResponse)

			// Format a diagnostic message for any upstream logger.
			_ = fmt.Sprintf("gono-mediator: recovered panic: %v\n%s", r, stack)
		}
	}()

	return next(ctx)
}

