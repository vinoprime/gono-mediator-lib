package publisher

import "context"

// SequentialPublisher invokes notification handlers one by one in registration order.
//
// Behaviour on error:
//   - If a handler returns an error, the remaining handlers are SKIPPED.
//   - The first error is returned immediately.
//
// When to use (default strategy):
//   - Handler order matters (e.g., audit log must run before email).
//   - You want fail-fast semantics — one failure stops the chain.
//   - Simplest to reason about; easiest to debug.
//
// Analogous to MediatR's ForeachAwaitPublisher.
type SequentialPublisher struct{}

// NewSequentialPublisher creates the default sequential publishing strategy.
func NewSequentialPublisher() *SequentialPublisher {
	return &SequentialPublisher{}
}

// Publish calls each handler in order, stopping at the first error.
func (p *SequentialPublisher) Publish(ctx context.Context, handlers []HandlerFunc) error {
	for _, handle := range handlers {
		// Check context cancellation before each handler to avoid work after abort.
		if err := ctx.Err(); err != nil {
			return err
		}

		if err := handle(ctx); err != nil {
			return err
		}
	}
	return nil
}

