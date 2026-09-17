// Package stream provides the streaming support for gono-mediator-lib.
//
// While the mediator.CreateStream function is the public API for dispatching
// stream requests, this package provides utilities for implementing StreamRequestHandlers.
//
// Think of a StreamRequestHandler like a garden hose:
//   - You open the tap (call Handle) → water starts flowing (channel starts receiving).
//   - You close the tap (cancel ctx or handler finishes) → channel is closed automatically.
package stream

import "context"

// SafeSend attempts to send a value to the channel, but gracefully aborts if
// either the context is cancelled or the channel is full/closed.
//
// Use this helper inside StreamRequestHandler implementations to avoid panics
// when sending on a closed channel (which would happen if the consumer cancelled).
//
// Returns:
//   - true  if the value was successfully sent
//   - false if ctx was cancelled (caller should stop producing values)
func SafeSend[T any](ctx context.Context, ch chan<- T, value T) bool {
	select {
	case <-ctx.Done():
		// Context cancelled — consumer no longer listening, stop producing.
		return false
	case ch <- value:
		// Value delivered successfully.
		return true
	}
}

// NewStream creates a buffered or unbuffered channel and starts the producer
// goroutine. The channel is automatically closed when the producer returns.
//
// Parameters:
//   - ctx: cancellation signal — producer goroutine exits when ctx is done.
//   - bufferSize: channel buffer size (0 = unbuffered/synchronous).
//   - producer: function that writes values to the channel; receives the writable end.
//
// The caller receives the read-only channel end. Cancelling ctx stops production.
//
// Example:
//
//	ch, err := stream.NewStream[int](ctx, 10, func(ctx context.Context, out chan<- int) error {
//	    for i := 0; i < 100; i++ {
//	        if !stream.SafeSend(ctx, out, i) {
//	            return nil // context cancelled
//	        }
//	    }
//	    return nil
//	})
func NewStream[T any](
	ctx context.Context,
	bufferSize int,
	producer func(ctx context.Context, out chan<- T) error,
) (<-chan T, error) {
	ch := make(chan T, bufferSize)

	go func() {
		// Always close the channel when producer exits — this signals to the consumer
		// that the range loop can terminate (range over channel stops on close).
		defer close(ch)
		// Errors from the producer are lost here; use a separate error channel or
		// embed errors in T if you need producer error propagation.
		_ = producer(ctx, ch)
	}()

	return ch, nil
}

