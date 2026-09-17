// Package contracts defines the core extension interfaces for gono-mediator-lib.
//
// Think of contracts like a menu at a restaurant:
//   - "Request" is an order you place.
//   - "RequestHandler" is the kitchen that cooks exactly one dish per order type.
//
// These interfaces are the Single Source of Truth (SSoT) for all handler contracts.
// Nothing outside this package should re-define these shapes.
package contracts

import "context"

// ── Request ────────────────────────────────────────────────────────────────────

// RequestMarker is an embeddable struct that types embed to declare themselves
// as Request types. This is the idiomatic Go alternative to unexported marker methods —
// it prevents accidental satisfaction while remaining embeddable from external packages.
//
// Example:
//
//	type GetUserQuery struct {
//	    contracts.RequestMarker[UserDTO] // declares: "I produce UserDTO"
//	    ID string
//	}
type RequestMarker[TResponse any] struct{}

// Request is a marker interface satisfied by embedding RequestMarker[TResponse].
//
// The TResponse type parameter encodes what the mediator returns when this request
// is sent — visible at the Send[TResponse] call site.
type Request[TResponse any] interface {
	isRequest(TResponse)
}

// isRequest makes RequestMarker[TResponse] satisfy Request[TResponse].
func (RequestMarker[TResponse]) isRequest(TResponse) {}

// RequestHandler processes a single Request and returns a TResponse or an error.
//
// Each request type must have exactly ONE handler registered — analogous to
// MediatR's IRequestHandler<TRequest, TResponse>. This enforces the Command/Query
// Responsibility pattern: one handler owns the logic for one request type.
//
// Implementations should:
//   - Respect context cancellation (check ctx.Done() in long operations).
//   - Return domain errors, not panic.
//   - Be stateless — all dependencies should be injected at construction time.
type RequestHandler[TRequest any, TResponse any] interface {
	// Handle executes the business logic for the given request.
	// ctx carries deadlines, cancellation signals, and request-scoped values.
	Handle(ctx context.Context, request TRequest) (TResponse, error)
}

