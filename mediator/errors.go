// Package mediator provides the core errors for gono-mediator-lib.
//
// All errors are sentinel values or typed structs, enabling callers to use
// errors.Is / errors.As for precise error handling — no string matching needed.
package mediator

import "fmt"

// ── Sentinel errors ────────────────────────────────────────────────────────────

// ErrNoHandler is returned when Send or CreateStream is called for a request type
// that has no registered handler.
//
// Why: Fail-fast and explicit. A missing handler is always a programmer error
// (misconfigured registry), so we surface it immediately rather than silently ignoring.
var ErrNoHandler = fmt.Errorf("gono-mediator: no handler registered for request type")

// ErrHandlerAlreadyRegistered is returned when a second RequestHandler is registered
// for the same request type.
//
// Why: Enforces the "one handler per request type" invariant at registration time,
// not at dispatch time. Better to catch this during startup than during a live request.
var ErrHandlerAlreadyRegistered = fmt.Errorf("gono-mediator: handler already registered for request type")

// ── Typed errors ───────────────────────────────────────────────────────────────

// PanicError wraps a recovered panic value as a standard error.
//
// This is produced by the recovery pipeline behavior when a handler panics.
// Callers can use errors.As to unwrap and inspect the original panic value.
//
// Example:
//
//	var pe *mediator.PanicError
//	if errors.As(err, &pe) {
//	    log.Printf("recovered panic: %v\nstack: %s", pe.Value, pe.Stack)
//	}
type PanicError struct {
	// Value is the value that was passed to panic().
	Value any
	// Stack is the goroutine stack trace captured at the point of panic.
	Stack []byte
}

// Error implements the error interface.
func (e *PanicError) Error() string {
	return fmt.Sprintf("gono-mediator: recovered panic: %v", e.Value)
}

// Unwrap returns nil — PanicError is a terminal error, not a wrapper.
// The original value is accessible via the Value field.
func (e *PanicError) Unwrap() error { return nil }

// NoHandlerError is the typed variant of ErrNoHandler, carrying the type name.
//
// Returned instead of the sentinel when additional context is available.
type NoHandlerError struct {
	// TypeName is the fully-qualified request type name (reflect.TypeOf is only
	// used here, in error reporting — not in the hot dispatch path).
	TypeName string
}

// Error implements the error interface.
func (e *NoHandlerError) Error() string {
	return fmt.Sprintf("gono-mediator: no handler registered for %q", e.TypeName)
}

// Is enables errors.Is(err, ErrNoHandler) to match NoHandlerError instances.
func (e *NoHandlerError) Is(target error) bool {
	return target == ErrNoHandler
}
