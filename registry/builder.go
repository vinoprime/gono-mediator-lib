// Package registry — fluent builder for ergonomic handler registration.
//
// The Builder wraps a Registry and provides a chainable API so you can
// wire up all handlers in one readable block during application startup.
//
// Example:
//
//	reg := registry.New()
//	err := registry.NewBuilder(reg).
//	    Handle(handlers.NewGetUserHandler(db)).
//	    Handle(handlers.NewCreateUserHandler(db)).
//	    Notify(handlers.NewSendWelcomeEmailHandler(mailer)).
//	    Notify(handlers.NewAuditLogHandler(logger)).
//	    Build()
package registry

import (
	"errors"

	"github.com/vinoprime/gono-mediator-lib/contracts"
)

// Builder provides a fluent API for registering handlers into a Registry.
//
// All errors are collected and returned together by Build(), so you can
// register all handlers and see all misconfiguration at once (fail-fast at startup).
type Builder struct {
	registry *Registry
	errs     []error
}

// NewBuilder creates a Builder wrapping the given Registry.
func NewBuilder(r *Registry) *Builder {
	return &Builder{registry: r}
}

// Handle registers a RequestHandler[TRequest, TResponse] and returns the Builder
// for chaining. Errors are accumulated and returned by Build().
//
// Usage: builder.Handle(myRequestHandler)
func Handle[TRequest any, TResponse any](
	b *Builder,
	handler contracts.RequestHandler[TRequest, TResponse],
) *Builder {
	if err := RegisterRequestHandler[TRequest, TResponse](b.registry, handler); err != nil {
		b.errs = append(b.errs, err)
	}
	return b
}

// Notify registers a NotificationHandler[TNotification] and returns the Builder
// for chaining. Multiple handlers per notification type are allowed.
//
// Usage: builder.Notify(myNotificationHandler)
func Notify[TNotification contracts.Notification](
	b *Builder,
	handler contracts.NotificationHandler[TNotification],
) *Builder {
	RegisterNotificationHandler[TNotification](b.registry, handler)
	return b
}

// Stream registers a StreamRequestHandler[TRequest, TResponse] and returns the
// Builder for chaining. Errors are accumulated and returned by Build().
//
// Usage: builder.Stream(myStreamHandler)
func Stream[TRequest any, TResponse any](
	b *Builder,
	handler contracts.StreamRequestHandler[TRequest, TResponse],
) *Builder {
	if err := RegisterStreamHandler[TRequest, TResponse](b.registry, handler); err != nil {
		b.errs = append(b.errs, err)
	}
	return b
}

// Build finalises the builder and returns all accumulated registration errors.
// Returns nil if all registrations succeeded.
// Callers should call Build() during application startup and halt if it returns an error.
func (b *Builder) Build() error {
	if len(b.errs) == 0 {
		return nil
	}
	// errors.Join (Go 1.20+) combines multiple errors into one, preserving all messages.
	return errors.Join(b.errs...)
}
