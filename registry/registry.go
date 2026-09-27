// Package registry provides thread-safe handler registration and resolution
// for gono-mediator-lib.
//
// Think of the registry like a phone book:
//   - You register a handler by "name" (the Go type of the request).
//   - The mediator looks up the handler by name when a request comes in.
//
// Design: handlers are stored as `any` internally (type erasure), but the
// public generic API ensures type safety at the registration call site.
// This is the same pattern used by encoding/json and sync.Map.
package registry

import (
	"fmt"
	"reflect"
	"sync"

	"github.com/vinoprime/gono-mediator-lib/contracts"
)

// handlerEntry wraps a registered handler with its reflection type for
// diagnostics and error messages. The actual handler value is type-erased to `any`.
type handlerEntry struct {
	// handler is the concrete handler (RequestHandler, NotificationHandler, StreamRequestHandler).
	handler any
	// typeName is the human-readable request type name, used only in error messages.
	typeName string
}

// Registry is a thread-safe store for all handler registrations.
//
// It maintains three separate maps:
//   - requestHandlers:      one handler per request type (enforced)
//   - notificationHandlers: N handlers per notification type (slice)
//   - streamHandlers:       one handler per stream request type (enforced)
//
// All maps are keyed by reflect.Type of the request/notification, giving us a
// stable, unique, collision-free key that mirrors the type system.
type Registry struct {
	mu sync.RWMutex

	// requestHandlers maps request type → single handler.
	// Key: reflect.Type of the TRequest type parameter.
	requestHandlers map[reflect.Type]handlerEntry

	// notificationHandlers maps notification type → ordered slice of handlers.
	// Key: reflect.Type of the TNotification type parameter.
	notificationHandlers map[reflect.Type][]handlerEntry

	// streamHandlers maps stream request type → single handler.
	// Key: reflect.Type of the TRequest type parameter.
	streamHandlers map[reflect.Type]handlerEntry
}

// New creates an empty, ready-to-use Registry.
func New() *Registry {
	return &Registry{
		requestHandlers:      make(map[reflect.Type]handlerEntry),
		notificationHandlers: make(map[reflect.Type][]handlerEntry),
		streamHandlers:       make(map[reflect.Type]handlerEntry),
	}
}

// ── Request handler registration ───────────────────────────────────────────────

// RegisterRequestHandler registers a single handler for request type TRequest.
//
// Why only one handler per request type?
// The Command/Query Responsibility pattern mandates a single authoritative handler
// per request. Multiple handlers would create ambiguity about which executes and
// in what order — use Notifications for fan-out scenarios instead.
//
// Returns ErrHandlerAlreadyRegistered if a handler already exists for TRequest.
func RegisterRequestHandler[TRequest any, TResponse any](
	r *Registry,
	handler contracts.RequestHandler[TRequest, TResponse],
) error {
	// reflect.TypeOf((*TRequest)(nil)).Elem() obtains the reflect.Type of TRequest
	// without requiring an instance — a standard Go generics idiom.
	key := reflect.TypeOf((*TRequest)(nil)).Elem()

	r.mu.Lock()
	defer r.mu.Unlock()

	if existing, ok := r.requestHandlers[key]; ok {
		return fmt.Errorf(
			"gono-mediator: handler already registered for %q (existing: %T, attempted: %T)",
			existing.typeName, existing.handler, handler,
		)
	}

	r.requestHandlers[key] = handlerEntry{
		handler:  handler,
		typeName: key.String(),
	}
	return nil
}

// ResolveRequestHandler retrieves the handler for the given request type.
// Returns (handler, true) if found; (nil, false) otherwise.
// The returned value is type-erased and must be asserted by the caller.
func (r *Registry) ResolveRequestHandler(requestType reflect.Type) (any, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	entry, ok := r.requestHandlers[requestType]
	if !ok {
		return nil, false
	}
	return entry.handler, true
}

// ── Notification handler registration ─────────────────────────────────────────

// RegisterNotificationHandler appends a handler for notification type TNotification.
//
// Multiple handlers can be registered for the same notification type.
// They are stored in registration order and invoked according to the publisher strategy.
func RegisterNotificationHandler[TNotification contracts.Notification](
	r *Registry,
	handler contracts.NotificationHandler[TNotification],
) {
	key := reflect.TypeOf((*TNotification)(nil)).Elem()

	r.mu.Lock()
	defer r.mu.Unlock()

	r.notificationHandlers[key] = append(r.notificationHandlers[key], handlerEntry{
		handler:  handler,
		typeName: key.String(),
	})
}

// ResolveNotificationHandlers retrieves all handlers registered for notificationType.
// Returns nil if no handlers are registered (this is valid — zero handlers means no-op).
func (r *Registry) ResolveNotificationHandlers(notificationType reflect.Type) []any {
	r.mu.RLock()
	defer r.mu.RUnlock()

	entries := r.notificationHandlers[notificationType]
	if len(entries) == 0 {
		return nil
	}

	// Copy to avoid returning a slice backed by the internal map's slice.
	handlers := make([]any, len(entries))
	for i, e := range entries {
		handlers[i] = e.handler
	}
	return handlers
}

// ── Stream handler registration ────────────────────────────────────────────────

// RegisterStreamHandler registers a single stream handler for TRequest.
// Returns an error if a stream handler already exists for TRequest.
func RegisterStreamHandler[TRequest any, TResponse any](
	r *Registry,
	handler contracts.StreamRequestHandler[TRequest, TResponse],
) error {
	key := reflect.TypeOf((*TRequest)(nil)).Elem()

	r.mu.Lock()
	defer r.mu.Unlock()

	if existing, ok := r.streamHandlers[key]; ok {
		return fmt.Errorf(
			"gono-mediator: stream handler already registered for %q (existing: %T, attempted: %T)",
			existing.typeName, existing.handler, handler,
		)
	}

	r.streamHandlers[key] = handlerEntry{
		handler:  handler,
		typeName: key.String(),
	}
	return nil
}

// ResolveStreamHandler retrieves the stream handler for the given request type.
// Returns (handler, true) if found; (nil, false) otherwise.
func (r *Registry) ResolveStreamHandler(requestType reflect.Type) (any, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	entry, ok := r.streamHandlers[requestType]
	if !ok {
		return nil, false
	}
	return entry.handler, true
}
