// Package mediator is the entry point for gono-mediator-lib.
//
// It provides three top-level functions for the core mediator operations:
//
//   - Send[TResponse]  — dispatch a request to exactly one handler
//   - Publish[TNotif]  — broadcast a notification to all registered handlers
//   - CreateStream[TResponse] — start a streaming request, returns a channel
//
// Quick start:
//
//	// 1. Define a request and handler
//	type GetUserQuery struct { ID string }
//	func (GetUserQuery) requestMarker() {}
//
//	type GetUserHandler struct{ db *sql.DB }
//	func (h *GetUserHandler) Handle(ctx context.Context, q GetUserQuery) (User, error) {
//	    return fetchUser(ctx, h.db, q.ID)
//	}
//
//	// 2. Wire up
//	reg := registry.New()
//	registry.RegisterRequestHandler[GetUserQuery, User](reg, &GetUserHandler{db: db})
//
//	m := mediator.New(mediator.WithRegistry(reg))
//
//	// 3. Dispatch
//	user, err := mediator.Send[User](ctx, m, GetUserQuery{ID: "123"})
package mediator

import (
	"context"
	"fmt"
	"reflect"

	"github.com/vinoprime/gono-mediator-lib/contracts"
	"github.com/vinoprime/gono-mediator-lib/publisher"
)

// Mediator is the central dispatcher. It is safe for concurrent use.
//
// Create via New() — do NOT use zero value (maps will be nil).
// Mediator is intentionally a struct (not an interface) — use it directly.
// If you need to mock in tests, define a local interface with Send/Publish/CreateStream.
type Mediator struct {
	opts options
}

// New creates a Mediator with the given options applied over the defaults.
//
// Example — default (sequential publisher, no behaviors):
//
//	m := mediator.New()
//
// Example — with pre-built registry and parallel publisher:
//
//	m := mediator.New(
//	    mediator.WithRegistry(reg),
//	    mediator.WithPublisher(publisher.NewParallelPublisher()),
//	)
func New(opts ...Option) *Mediator {
	o := defaultOptions()
	for _, apply := range opts {
		apply(&o)
	}
	return &Mediator{opts: o}
}

// Registry returns the underlying registry for direct handler registration.
// This is useful when you want to register handlers after creating the mediator.
func (m *Mediator) Registry() interface {
	ResolveRequestHandler(reflect.Type) (any, bool)
	ResolveNotificationHandlers(reflect.Type) []any
	ResolveStreamHandler(reflect.Type) (any, bool)
} {
	return m.opts.registry
}

// ── Send ───────────────────────────────────────────────────────────────────────

// Send dispatches a request to its single registered handler, passing it through
// all registered pipeline behaviors (outermost first).
//
// Type parameters:
//   - TResponse: the expected response type (must be specified explicitly)
//
// TRequest is inferred from the request argument.
//
// Returns ErrNoHandler (as *NoHandlerError) if no handler is registered for TRequest.
//
// Example:
//
//	user, err := mediator.Send[User](ctx, m, GetUserQuery{ID: "42"})
func Send[TResponse any, TRequest any](ctx context.Context, m *Mediator, request TRequest) (TResponse, error) {
	requestType := reflect.TypeOf(request)
	// Handle pointer types: if request is *MyQuery, key should be *MyQuery.
	// reflect.TypeOf already handles this correctly.

	// Resolve the handler from the registry (type-erased).
	raw, ok := m.opts.registry.ResolveRequestHandler(requestType)
	if !ok {
		var zero TResponse
		return zero, &NoHandlerError{TypeName: requestType.String()}
	}

	// Assert the handler back to its typed contract.
	// This assertion WILL succeed if registration was done via RegisterRequestHandler[TRequest, TResponse].
	// If it fails, there is a bug in the registration code — panic is appropriate (programmer error).
	handler, ok := raw.(contracts.RequestHandler[TRequest, TResponse])
	if !ok {
		var zero TResponse
		return zero, fmt.Errorf(
			"gono-mediator: handler type mismatch for %q (registered: %T, expected RequestHandler[%T, %T])",
			requestType, raw, request, zero,
		)
	}

	// Build the pipeline: wrap behaviors around the handler (outermost first).
	// The innermost function calls the actual handler.
	pipeline := buildPipeline(ctx, request, handler, m.opts.behaviors)

	return pipeline(ctx)
}

// ── Publish ────────────────────────────────────────────────────────────────────

// Publish broadcasts a notification to all registered handlers using the
// configured Publisher strategy (default: Sequential).
//
// If no handlers are registered for TNotification, Publish is a no-op (returns nil).
// Zero handlers is not an error for notifications.
//
// Example:
//
//	err := mediator.Publish[UserCreatedEvent](ctx, m, UserCreatedEvent{UserID: "42"})
func Publish[TNotification contracts.Notification](ctx context.Context, m *Mediator, notification TNotification) error {
	notifType := reflect.TypeOf(notification)

	// Retrieve all registered handlers (type-erased).
	rawHandlers := m.opts.registry.ResolveNotificationHandlers(notifType)
	if len(rawHandlers) == 0 {
		return nil // No handlers — silent no-op (not an error for notifications).
	}

	// Convert type-erased handlers into publisher.HandlerFunc closures.
	// Each closure captures the typed handler and notification, giving the publisher
	// a uniform func(ctx) error interface.
	handlerFuncs := make([]publisher.HandlerFunc, 0, len(rawHandlers))
	for _, raw := range rawHandlers {
		h, ok := raw.(contracts.NotificationHandler[TNotification])
		if !ok {
			return fmt.Errorf(
				"gono-mediator: notification handler type mismatch for %q (got: %T)",
				notifType, raw,
			)
		}
		// Capture h by value in the closure (Go 1.22+ loop-var fix; also explicit).
		capturedHandler := h
		handlerFuncs = append(handlerFuncs, func(ctx context.Context) error {
			return capturedHandler.Handle(ctx, notification)
		})
	}

	return m.opts.publisher.Publish(ctx, handlerFuncs)
}

// ── CreateStream ───────────────────────────────────────────────────────────────

// CreateStream dispatches a streaming request to its single registered handler
// and returns a read-only channel of TResponse values.
//
// The channel is closed by the handler when the stream ends or when ctx is cancelled.
// Callers should always drain the channel OR cancel ctx to prevent goroutine leaks.
//
// Example:
//
//	ch, err := mediator.CreateStream[PriceUpdate](ctx, m, LivePriceQuery{Symbol: "AAPL"})
//	if err != nil { return err }
//	for update := range ch {
//	    fmt.Println(update.Price)
//	}
func CreateStream[TResponse any, TRequest any](ctx context.Context, m *Mediator, request TRequest) (<-chan TResponse, error) {
	requestType := reflect.TypeOf(request)

	raw, ok := m.opts.registry.ResolveStreamHandler(requestType)
	if !ok {
		return nil, &NoHandlerError{TypeName: requestType.String()}
	}

	handler, ok := raw.(contracts.StreamRequestHandler[TRequest, TResponse])
	if !ok {
		return nil, fmt.Errorf(
			"gono-mediator: stream handler type mismatch for %q (registered: %T)",
			requestType, raw,
		)
	}

	return handler.Handle(ctx, request)
}

// ── Pipeline builder ───────────────────────────────────────────────────────────

// buildPipeline constructs the execution chain by wrapping behaviors around the handler.
//
// Strategy (reverse iteration):
//   - Start with innermost: the actual handler call as a Next[TResponse] func.
//   - Wrap each behavior from last-registered to first-registered.
//   - Result: first registered behavior is outermost (executed first).
//
// This is the standard "onion model" used in all middleware frameworks.
func buildPipeline[TRequest any, TResponse any](
	_ context.Context,
	request TRequest,
	handler contracts.RequestHandler[TRequest, TResponse],
	behaviors []anyBehavior,
) contracts.Next[TResponse] {
	// Innermost: the actual handler.
	pipeline := contracts.Next[TResponse](func(ctx context.Context) (TResponse, error) {
		return handler.Handle(ctx, request)
	})

	// Wrap from innermost outward (reverse slice iteration).
	for i := len(behaviors) - 1; i >= 0; i-- {
		b := behaviors[i]

		// Type-assert to the typed behavior interface.
		// Only behaviors registered for [TRequest, TResponse] will match.
		typedBehavior, ok := b.(contracts.PipelineBehavior[TRequest, TResponse])
		if !ok {
			// Behavior is not applicable to this request/response type — skip it.
			// This enables "open generic" behaviors (registered once, applied where they match).
			continue
		}

		// Capture for closure (prevents loop-variable aliasing).
		currentBehavior := typedBehavior
		inner := pipeline

		pipeline = func(ctx context.Context) (TResponse, error) {
			return currentBehavior.Handle(ctx, request, inner)
		}
	}

	return pipeline
}
