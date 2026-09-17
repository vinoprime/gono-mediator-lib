// Package publisher defines the strategy interface and all built-in notification
// publishing strategies for gono-mediator-lib.
//
// Think of publisher strategies like different ways to announce a school closure:
//   - Sequential: Tell each teacher one at a time (stop if one doesn't answer).
//   - Parallel: Text all teachers at once (wait for all replies).
//   - FireAndForget: Post on the school board and walk away (don't wait for anyone).
package publisher

import "context"

// HandlerFunc is the type-erased wrapper for a notification handler invocation.
// The publisher receives a slice of these — one per registered handler — and
// decides how to call them.
//
// Why a func instead of an interface?
// Using a func allows the mediator to close over the typed handler and notification,
// giving the publisher a simple, uniform API regardless of the notification type.
type HandlerFunc func(ctx context.Context) error

// Publisher is the strategy interface for notification dispatch.
//
// Implementing this interface lets you define custom publishing behavior:
//   - Retry on failure
//   - Publish to external message bus (outbox pattern)
//   - Priority-ordered execution
//
// The mediator uses the configured Publisher whenever Publish() is called.
type Publisher interface {
	// Publish invokes all handlers for a notification.
	// ctx is propagated to every handler — respect cancellation.
	// handlers is an ordered slice of handler invocation funcs (registration order).
	Publish(ctx context.Context, handlers []HandlerFunc) error
}

