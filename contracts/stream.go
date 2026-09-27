// Package contracts — streaming request extension contracts.
//
// Think of streaming like a water tap:
//   - You turn it on (send the StreamRequest).
//   - Water flows out continuously (channel receives values).
//   - You turn it off (cancel context or drain the channel).
package contracts

import "context"

// StreamMarker is an embeddable struct that types embed to declare themselves
// as StreamRequest types — satisfying the StreamRequest[TResponse] interface
// from external packages.
//
// Example:
//
//	type LivePriceQuery struct {
//	    contracts.StreamMarker[PriceUpdate] // declares: "I stream PriceUpdate"
//	    Symbol string
//	}
type StreamMarker[TResponse any] struct{}

// StreamRequest is a marker interface satisfied by embedding StreamMarker[TResponse].
//
// Unlike Request[TResponse] which returns a single value, StreamRequest[TResponse]
// causes the mediator to return a read-only channel that emits zero or more TResponse
// values over time.
type StreamRequest[TResponse any] interface {
	isStream(TResponse)
}

// isStream makes StreamMarker[TResponse] satisfy StreamRequest[TResponse].
func (StreamMarker[TResponse]) isStream(TResponse) {}

// StreamRequestHandler handles a StreamRequest and produces a channel of responses.
//
// The handler is responsible for:
//   - Spawning a goroutine that sends values to the channel.
//   - Closing the channel when production is complete or context is cancelled.
//   - Returning an error immediately if the stream cannot be started.
//
// Callers should always drain the channel or cancel ctx to prevent goroutine leaks.
type StreamRequestHandler[TRequest any, TResponse any] interface {
	// Handle starts the stream and returns the read channel.
	// The channel MUST be closed by the handler when the stream ends.
	Handle(ctx context.Context, request TRequest) (<-chan TResponse, error)
}
