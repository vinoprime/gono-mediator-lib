// Package contracts — notification extension contracts.
//
// Think of notifications like a school bell:
//   - The bell rings once (Publish).
//   - Every classroom (NotificationHandler) hears it and reacts independently.
package contracts

import "context"

// NotificationMarker is an embeddable struct that types embed to declare
// themselves as Notification types — satisfying the Notification interface
// from external packages.
//
// Example:
//
//	type UserCreatedEvent struct {
//	    contracts.NotificationMarker // declares: "I am a notification"
//	    UserID string
//	    Email  string
//	}
type NotificationMarker struct{}

// Notification is a marker interface satisfied by embedding NotificationMarker.
//
// Unlike Request, a Notification has NO response — it is fire-and-observe.
// Multiple handlers can be registered for the same notification type.
type Notification interface {
	isNotification()
}

// isNotification makes NotificationMarker satisfy Notification.
func (NotificationMarker) isNotification() {}

// NotificationHandler handles a single notification type.
//
// Multiple handlers can be registered for the same TNotification.
// The publishing strategy (sequential, parallel, fire-and-forget) determines
// HOW these handlers are invoked — see the publisher package.
//
// Implementations should:
//   - Be idempotent where possible (notifications can be re-delivered in some systems).
//   - Respect context cancellation.
//   - Never block indefinitely without checking ctx.Done().
type NotificationHandler[TNotification Notification] interface {
	// Handle processes the notification.
	// Returning an error signals the publisher; behavior depends on the strategy.
	Handle(ctx context.Context, notification TNotification) error
}
