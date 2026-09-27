package publisher

import (
	"context"
)

// ErrorCallback is called when a fire-and-forget handler returns an error.
// Receives the handler index (0-based, registration order) and the error.
// This is the ONLY way to observe errors from fire-and-forget handlers.
//
// Example:
//
//	func myErrorCallback(idx int, err error) {
//	    slog.Error("notification handler failed", "handlerIndex", idx, "error", err)
//	}
type ErrorCallback func(handlerIndex int, err error)

// FireAndForgetPublisher spawns a goroutine per handler and returns immediately.
//
// Behaviour:
//   - Publish returns nil as soon as all goroutines are launched (does NOT wait).
//   - Handler errors are reported asynchronously via the ErrorCallback.
//   - A detached context (context.WithoutCancel) is used so handlers are NOT
//     cancelled when the caller's context is cancelled — true fire-and-forget.
//
// When to use:
//   - Side-effects that MUST complete even if the caller request is done
//     (e.g., audit logging, analytics, cache warming).
//   - You can tolerate occasional handler failures (errors are observed, not propagated).
//
// ⚠️  Production Warning:
//   - No backpressure — if handlers are slow, goroutines accumulate.
//   - Handlers may outlive the Go process if the program exits abruptly.
//   - Prefer a proper background queue (channels + worker pool) for high-volume events.
type FireAndForgetPublisher struct {
	// onError is called asynchronously when a handler returns an error.
	// If nil, errors are silently discarded.
	onError ErrorCallback
}

// NewFireAndForgetPublisher creates a fire-and-forget publisher.
// Pass an ErrorCallback to observe handler errors; pass nil to discard them.
func NewFireAndForgetPublisher(onError ErrorCallback) *FireAndForgetPublisher {
	return &FireAndForgetPublisher{onError: onError}
}

// Publish launches a goroutine for each handler and returns immediately (nil error always).
// The detached context ensures handlers run even after the caller's request context ends.
func (p *FireAndForgetPublisher) Publish(ctx context.Context, handlers []HandlerFunc) error {
	// context.WithoutCancel (Go 1.21+) detaches the handler context from the caller's
	// cancellation — fire-and-forget handlers must not be aborted when the HTTP request ends.
	detachedCtx := context.WithoutCancel(ctx)

	for i, handle := range handlers {
		// Capture loop variables explicitly (Go 1.21 compatibility).
		idx, h := i, handle
		go func() {
			if err := h(detachedCtx); err != nil && p.onError != nil {
				p.onError(idx, err)
			}
		}()
	}

	// Always return nil — errors are async and reported via callback.
	return nil
}
