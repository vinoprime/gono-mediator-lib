package pipeline

import (
	"context"

	"github.com/gono-mediator-lib/contracts"
)

// Validator is an optional interface that request types can implement to
// enable automatic validation via ValidationBehavior.
//
// This follows the "interface on the consumer side" Go idiom — the validation
// behavior defines what it needs, and requests opt-in by implementing it.
//
// Example:
//
//	type CreateUserCommand struct {
//	    Email string
//	    Age   int
//	}
//
//	func (c CreateUserCommand) Validate() error {
//	    if c.Email == "" {
//	        return errors.New("email is required")
//	    }
//	    if c.Age < 0 {
//	        return errors.New("age cannot be negative")
//	    }
//	    return nil
//	}
type Validator interface {
	// Validate returns a non-nil error if the request is invalid.
	// Return a descriptive error; callers should NOT need to check error type.
	Validate() error
}

// ValidationBehavior checks whether a request implements Validator and, if so,
// calls Validate() before passing the request to the handler.
//
// Short-circuit semantics:
//   - If Validate() returns an error, next() is NOT called — handler is skipped.
//   - If the request does not implement Validator, it passes through silently.
//
// This behavior should run AFTER RecoveryBehavior but BEFORE the handler,
// so validation errors are clean (not panics) and observed by logging.
type ValidationBehavior[TRequest any, TResponse any] struct{}

// NewValidationBehavior creates a ValidationBehavior.
func NewValidationBehavior[TRequest any, TResponse any]() *ValidationBehavior[TRequest, TResponse] {
	return &ValidationBehavior[TRequest, TResponse]{}
}

// Handle checks if the request implements Validator and calls Validate().
// Skips handler execution if validation fails.
func (b *ValidationBehavior[TRequest, TResponse]) Handle(
	ctx context.Context,
	request TRequest,
	next contracts.Next[TResponse],
) (TResponse, error) {
	// Type assertion to Validator — zero overhead if request doesn't implement it.
	if v, ok := any(request).(Validator); ok {
		if err := v.Validate(); err != nil {
			var zero TResponse
			return zero, err
		}
	}

	return next(ctx)
}

