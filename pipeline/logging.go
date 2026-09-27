// Package pipeline provides built-in PipelineBehavior implementations for
// gono-mediator-lib. All behaviors are opt-in — include only what you need.
//
// Built-in behaviors:
//   - LoggingBehavior   — structured request/response logging via slog
//   - RecoveryBehavior  — converts panics to errors (never let panics escape handlers)
//   - ValidationBehavior — runs Validate() on requests before the handler
//   - TimeoutBehavior   — injects a per-request deadline into the context
//
// Behaviors are composable and executed in registration order (outermost first):
//
//	[ Recovery [ Logging [ Validation [ Timeout [ Handler ] ] ] ] ]
package pipeline

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/vinoprime/gono-mediator-lib/contracts"
)

// LoggingBehavior logs the request name, outcome, and duration for every request.
//
// Uses Go's stdlib slog for structured, zero-allocation logging.
// No external logger dependency — wiring to any slog.Handler is the caller's responsibility.
//
// Log fields:
//   - "request"  — Go type name of the request (e.g., "GetUserQuery")
//   - "duration" — time elapsed from before calling next to after (ms precision)
//   - "error"    — present and non-nil only when the handler chain returned an error
type LoggingBehavior[TRequest any, TResponse any] struct {
	// logger is the slog.Logger to write to. If nil, slog.Default() is used.
	logger *slog.Logger
}

// NewLoggingBehavior creates a LoggingBehavior using the given logger.
// Pass nil to use slog.Default().
func NewLoggingBehavior[TRequest any, TResponse any](logger *slog.Logger) *LoggingBehavior[TRequest, TResponse] {
	if logger == nil {
		logger = slog.Default()
	}
	return &LoggingBehavior[TRequest, TResponse]{logger: logger}
}

// Handle logs before and after the pipeline executes.
// The log level is INFO on success, ERROR on failure.
func (b *LoggingBehavior[TRequest, TResponse]) Handle(
	ctx context.Context,
	request TRequest,
	next contracts.Next[TResponse],
) (TResponse, error) {
	// fmt.Sprintf("%T", request) gives the full type name — used only for logging,
	// not for dispatch, so reflect overhead here is acceptable.
	requestName := fmt.Sprintf("%T", request)

	b.logger.InfoContext(ctx, "handling request", slog.String("request", requestName))

	start := time.Now()
	response, err := next(ctx)
	elapsed := time.Since(start)

	if err != nil {
		b.logger.ErrorContext(ctx, "request failed",
			slog.String("request", requestName),
			slog.Duration("duration", elapsed),
			slog.Any("error", err),
		)
	} else {
		b.logger.InfoContext(ctx, "request handled",
			slog.String("request", requestName),
			slog.Duration("duration", elapsed),
		)
	}

	return response, err
}
