// Package mediator — functional options for Mediator configuration.
//
// The Options pattern (also called "functional options" or "configuration functions")
// is the idiomatic Go way to provide optional, named configuration without breaking
// backward compatibility when new options are added.
//
// Reference: Dave Cheney's "Functional options for friendly APIs" (2014).
package mediator

import (
	"context"
	"fmt"

	"github.com/gono-mediator-lib/contracts"
	"github.com/gono-mediator-lib/publisher"
	"github.com/gono-mediator-lib/registry"
)

// options holds all configurable settings for the Mediator.
// Fields are unexported — callers interact only via Option functions.
type options struct {
	// registry is the handler store. Defaults to a new empty Registry.
	registry *registry.Registry

	// publisher determines how notifications are dispatched to handlers.
	// Defaults to SequentialPublisher.
	publisher publisher.Publisher

	// behaviors is the ordered list of type-erased pipeline behavior wrappers.
	// Applied outermost-first (index 0 = first executed).
	behaviors []anyBehavior
}

// anyBehavior is a type-erased wrapper that allows behaviors to be stored in
// a uniform slice regardless of their TRequest/TResponse type parameters.
// The concrete wrapping happens in the WithBehavior option functions.
type anyBehavior interface {
	// typeName returns the behavior's string name for diagnostics.
	typeName() string
}

// Option is a function that configures a Mediator.
// Apply options via mediator.New(option1, option2, ...).
type Option func(*options)

// WithRegistry replaces the default empty Registry with a pre-configured one.
//
// Use this when you want to register all handlers before creating the Mediator:
//
//	reg := registry.New()
//	registry.RegisterRequestHandler[MyQuery, MyResult](reg, myHandler)
//	m := mediator.New(mediator.WithRegistry(reg))
func WithRegistry(r *registry.Registry) Option {
	return func(o *options) {
		o.registry = r
	}
}

// WithPublisher replaces the default SequentialPublisher with a custom strategy.
//
// Built-in strategies: publisher.NewSequentialPublisher(),
// publisher.NewParallelPublisher(), publisher.NewFireAndForgetPublisher(cb).
//
// Example:
//
//	m := mediator.New(mediator.WithPublisher(publisher.NewParallelPublisher()))
func WithPublisher(p publisher.Publisher) Option {
	return func(o *options) {
		o.publisher = p
	}
}

// defaultOptions returns the out-of-the-box configuration:
//   - New empty registry
//   - Sequential publisher (safest default)
//   - No behaviors (opt-in)
func defaultOptions() options {
	return options{
		registry:  registry.New(),
		publisher: publisher.NewSequentialPublisher(),
		behaviors: nil,
	}
}

// ── Behavior registration ──────────────────────────────────────────────────────

// behaviorWrapper wraps a typed PipelineBehavior[TReq, TRes] as an anyBehavior.
//
// It implements:
//   - anyBehavior: so it can be stored in the uniform []anyBehavior slice.
//   - contracts.PipelineBehavior[TReq, TRes]: so buildPipeline can type-assert it
//     and call Handle directly.
//
// This "dual interface" pattern is the bridge between the type-erased store and
// the fully-typed pipeline execution — the only place in the library we use this trick.
type behaviorWrapper[TRequest any, TResponse any] struct {
	behavior contracts.PipelineBehavior[TRequest, TResponse]
	name     string
}

// typeName returns the behavior's Go type name — used only for diagnostics.
func (w *behaviorWrapper[TRequest, TResponse]) typeName() string { return w.name }

// Handle satisfies contracts.PipelineBehavior[TRequest, TResponse] by delegating
// to the wrapped behavior. buildPipeline type-asserts anyBehavior to this interface
// at dispatch time.
func (w *behaviorWrapper[TRequest, TResponse]) Handle(
	ctx context.Context,
	request TRequest,
	next contracts.Next[TResponse],
) (TResponse, error) {
	return w.behavior.Handle(ctx, request, next)
}

// WithBehavior adds a PipelineBehavior[TRequest, TResponse] to the mediator's pipeline.
//
// Behaviors are executed in the order they are added (first added = outermost/first executed).
// The behavior only applies to Send calls where TRequest and TResponse match exactly.
//
// Example — add logging around all GetUserQuery → User dispatches:
//
//	m := mediator.New(
//	    mediator.WithBehavior(pipeline.NewLoggingBehavior[GetUserQuery, User](logger)),
//	    mediator.WithBehavior(pipeline.NewValidationBehavior[GetUserQuery, User]()),
//	)
func WithBehavior[TRequest any, TResponse any](b contracts.PipelineBehavior[TRequest, TResponse]) Option {
	return func(o *options) {
		o.behaviors = append(o.behaviors, &behaviorWrapper[TRequest, TResponse]{
			behavior: b,
			name:     fmt.Sprintf("%T", b),
		})
	}
}

