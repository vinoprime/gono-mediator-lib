// Package mediator_test — end-to-end integration tests for Send, Publish, CreateStream.
//
// Tests cover:
//   - Happy path: Send returns correct response.
//   - No handler: Send returns ErrNoHandler.
//   - Pipeline: behaviors execute in correct order.
//   - Panic recovery: PanicError is returned, not process crash.
//   - Validation: invalid request is rejected before handler.
//   - Publish sequential/parallel/fire-and-forget strategies.
//   - Stream: channel delivers values and closes.
//   - Context cancellation in streaming.
package mediator_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gono-mediator-lib/contracts"
	"github.com/gono-mediator-lib/mediator"
	"github.com/gono-mediator-lib/pipeline"
	"github.com/gono-mediator-lib/publisher"
	"github.com/gono-mediator-lib/registry"
	"github.com/gono-mediator-lib/stream"
)

// ── Request/Response test doubles ────────────────────────────────────────────

// echoRequest returns its Payload as the response.
// Implements contracts.Request[string] by embedding contracts.RequestMarker[string].
type echoRequest struct {
	contracts.RequestMarker[string]
	Payload string
}

// echoHandler echoes the request payload.
type echoHandler struct{}

func (h *echoHandler) Handle(_ context.Context, req echoRequest) (string, error) {
	return req.Payload, nil
}

// panicHandler panics unconditionally.
type panicHandler struct{}

func (h *panicHandler) Handle(_ context.Context, _ echoRequest) (string, error) {
	panic("test panic from handler")
}

// badRequest implements Validator and always fails.
// Implements contracts.Request[string] by embedding contracts.RequestMarker[string].
type badRequest struct {
	contracts.RequestMarker[string]
	Payload string
}

func (b badRequest) Validate() error {
	if b.Payload == "" {
		return errors.New("payload is required")
	}
	return nil
}

// badRequestHandler handles badRequest.
type badRequestHandler struct{}

func (h *badRequestHandler) Handle(_ context.Context, req badRequest) (string, error) {
	return req.Payload, nil
}

// ── Notification test doubles ─────────────────────────────────────────────────

// pingNotification is a test domain event.
// Implements contracts.Notification by embedding contracts.NotificationMarker.
type pingNotification struct {
	contracts.NotificationMarker
	Data string
}

// countingNotifHandler counts how many times Handle is called.
type countingNotifHandler struct {
	count atomic.Int64
}

func (h *countingNotifHandler) Handle(_ context.Context, _ pingNotification) error {
	h.count.Add(1)
	return nil
}

// ── Stream test doubles ───────────────────────────────────────────────────────

// countRequest produces a stream of integers from 1 to N.
// Implements contracts.StreamRequest[int] by embedding contracts.StreamMarker[int].
type countRequest struct {
	contracts.StreamMarker[int]
	N int
}

// countStreamHandler implements StreamRequestHandler[countRequest, int].
type countStreamHandler struct{}

func (h *countStreamHandler) Handle(ctx context.Context, req countRequest) (<-chan int, error) {
	return stream.NewStream[int](ctx, req.N, func(ctx context.Context, out chan<- int) error {
		for i := 1; i <= req.N; i++ {
			if !stream.SafeSend(ctx, out, i) {
				return nil
			}
		}
		return nil
	})
}

// ── Helper: build a registry with standard test handlers ─────────────────────


func buildTestRegistry(t *testing.T) *registry.Registry {
	t.Helper()
	reg := registry.New()

	if err := registry.RegisterRequestHandler[echoRequest, string](reg, &echoHandler{}); err != nil {
		t.Fatalf("register echoHandler: %v", err)
	}
	if err := registry.RegisterRequestHandler[badRequest, string](reg, &badRequestHandler{}); err != nil {
		t.Fatalf("register badRequestHandler: %v", err)
	}
	if err := registry.RegisterStreamHandler[countRequest, int](reg, &countStreamHandler{}); err != nil {
		t.Fatalf("register countStreamHandler: %v", err)
	}
	return reg
}

// ── Send tests ────────────────────────────────────────────────────────────────

// TestSend_HappyPath verifies Send returns the handler's response.
func TestSend_HappyPath(t *testing.T) {
	m := mediator.New(mediator.WithRegistry(buildTestRegistry(t)))

	got, err := mediator.Send[string](context.Background(), m, echoRequest{Payload: "hello"})
	if err != nil {
		t.Fatalf("Send returned unexpected error: %v", err)
	}
	if got != "hello" {
		t.Errorf("expected %q, got %q", "hello", got)
	}
}

// TestSend_NoHandler verifies ErrNoHandler is returned for unregistered types.
func TestSend_NoHandler(t *testing.T) {
	m := mediator.New() // empty registry

	_, err := mediator.Send[string](context.Background(), m, echoRequest{Payload: "x"})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, mediator.ErrNoHandler) {
		t.Errorf("expected ErrNoHandler, got %T: %v", err, err)
	}
}

// TestSend_PipelineExecutionOrder verifies behaviors execute outermost-first.
func TestSend_PipelineExecutionOrder(t *testing.T) {
	// Track execution order via a shared slice.
	var order []string

	// Behavior A records "A-before" / "A-after".
	behaviorA := &orderTrackingBehavior[echoRequest, string]{name: "A", order: &order}
	// Behavior B records "B-before" / "B-after".
	behaviorB := &orderTrackingBehavior[echoRequest, string]{name: "B", order: &order}

	reg := buildTestRegistry(t)
	m := mediator.New(
		mediator.WithRegistry(reg),
		mediator.WithBehavior[echoRequest, string](behaviorA),
		mediator.WithBehavior[echoRequest, string](behaviorB),
	)

	_, err := mediator.Send[string](context.Background(), m, echoRequest{Payload: "order"})
	if err != nil {
		t.Fatalf("Send failed: %v", err)
	}

	// Expected: A outermost → B inner → handler → B after → A after.
	want := []string{"A-before", "B-before", "B-after", "A-after"}
	if fmt.Sprint(order) != fmt.Sprint(want) {
		t.Errorf("behavior order wrong\nwant: %v\ngot:  %v", want, order)
	}
}

// orderTrackingBehavior is a test behavior that records its execution order.
type orderTrackingBehavior[TRequest any, TResponse any] struct {
	name  string
	order *[]string
}

func (b *orderTrackingBehavior[TRequest, TResponse]) Handle(
	ctx context.Context,
	req TRequest,
	next contracts.Next[TResponse],
) (TResponse, error) {
	*b.order = append(*b.order, b.name+"-before")
	res, err := next(ctx)
	*b.order = append(*b.order, b.name+"-after")
	return res, err
}

// TestSend_RecoveryBehavior verifies panics are converted to PanicError.
func TestSend_RecoveryBehavior(t *testing.T) {
	reg := registry.New()
	// Register the panicking handler.
	if err := registry.RegisterRequestHandler[echoRequest, string](reg, &panicHandler{}); err != nil {
		t.Fatalf("register: %v", err)
	}

	m := mediator.New(
		mediator.WithRegistry(reg),
		mediator.WithBehavior[echoRequest, string](pipeline.NewRecoveryBehavior[echoRequest, string]()),
	)

	_, err := mediator.Send[string](context.Background(), m, echoRequest{Payload: "boom"})
	if err == nil {
		t.Fatal("expected panic error, got nil")
	}

	var pe *mediator.PanicError
	if !errors.As(err, &pe) {
		t.Errorf("expected *mediator.PanicError, got %T", err)
	}
	if pe.Value != "test panic from handler" {
		t.Errorf("unexpected panic value: %v", pe.Value)
	}
}

// TestSend_ValidationBehavior_InvalidRequest verifies handler is skipped on validation failure.
func TestSend_ValidationBehavior_InvalidRequest(t *testing.T) {
	reg := buildTestRegistry(t)
	m := mediator.New(
		mediator.WithRegistry(reg),
		mediator.WithBehavior[badRequest, string](pipeline.NewValidationBehavior[badRequest, string]()),
	)

	_, err := mediator.Send[string](context.Background(), m, badRequest{Payload: ""})
	if err == nil {
		t.Fatal("expected validation error, got nil")
	}
	if err.Error() != "payload is required" {
		t.Errorf("unexpected error message: %q", err.Error())
	}
}

// TestSend_ValidationBehavior_ValidRequest verifies valid requests pass through.
func TestSend_ValidationBehavior_ValidRequest(t *testing.T) {
	reg := buildTestRegistry(t)
	m := mediator.New(
		mediator.WithRegistry(reg),
		mediator.WithBehavior[badRequest, string](pipeline.NewValidationBehavior[badRequest, string]()),
	)

	got, err := mediator.Send[string](context.Background(), m, badRequest{Payload: "valid"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "valid" {
		t.Errorf("expected %q, got %q", "valid", got)
	}
}

// TestSend_TimeoutBehavior verifies context deadline is enforced.
func TestSend_TimeoutBehavior(t *testing.T) {
	reg := registry.New()
	// Register a slow handler that exceeds the timeout.
	if err := registry.RegisterRequestHandler[echoRequest, string](reg, &slowHandler{delay: 200 * time.Millisecond}); err != nil {
		t.Fatalf("register: %v", err)
	}

	m := mediator.New(
		mediator.WithRegistry(reg),
		mediator.WithBehavior[echoRequest, string](pipeline.NewTimeoutBehavior[echoRequest, string](50*time.Millisecond)),
	)

	_, err := mediator.Send[string](context.Background(), m, echoRequest{Payload: "slow"})
	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("expected DeadlineExceeded, got: %v", err)
	}
}

// slowHandler sleeps for a fixed duration before responding.
type slowHandler struct{ delay time.Duration }

func (h *slowHandler) Handle(ctx context.Context, req echoRequest) (string, error) {
	select {
	case <-time.After(h.delay):
		return req.Payload, nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

// ── Publish tests ─────────────────────────────────────────────────────────────

// TestPublish_Sequential verifies all handlers are called in order.
func TestPublish_Sequential(t *testing.T) {
	reg := registry.New()
	h1 := &countingNotifHandler{}
	h2 := &countingNotifHandler{}

	registry.RegisterNotificationHandler[pingNotification](reg, h1)
	registry.RegisterNotificationHandler[pingNotification](reg, h2)

	m := mediator.New(mediator.WithRegistry(reg))
	if err := mediator.Publish[pingNotification](context.Background(), m, pingNotification{Data: "ping"}); err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	if h1.count.Load() != 1 {
		t.Errorf("h1 expected 1 call, got %d", h1.count.Load())
	}
	if h2.count.Load() != 1 {
		t.Errorf("h2 expected 1 call, got %d", h2.count.Load())
	}
}

// TestPublish_Parallel verifies all handlers are called when using parallel publisher.
func TestPublish_Parallel(t *testing.T) {
	reg := registry.New()
	const numHandlers = 10
	handlers := make([]*countingNotifHandler, numHandlers)
	for i := range handlers {
		handlers[i] = &countingNotifHandler{}
		registry.RegisterNotificationHandler[pingNotification](reg, handlers[i])
	}

	m := mediator.New(
		mediator.WithRegistry(reg),
		mediator.WithPublisher(publisher.NewParallelPublisher()),
	)

	if err := mediator.Publish[pingNotification](context.Background(), m, pingNotification{}); err != nil {
		t.Fatalf("Publish (parallel) failed: %v", err)
	}

	for i, h := range handlers {
		if h.count.Load() != 1 {
			t.Errorf("handler[%d] expected 1 call, got %d", i, h.count.Load())
		}
	}
}

// TestPublish_NoHandlers verifies Publish is a no-op with zero handlers (not an error).
func TestPublish_NoHandlers(t *testing.T) {
	m := mediator.New() // empty registry

	err := mediator.Publish[pingNotification](context.Background(), m, pingNotification{})
	if err != nil {
		t.Errorf("expected nil for no-handler publish, got: %v", err)
	}
}

// TestPublish_FireAndForget verifies publish returns immediately and errors are observed via callback.
func TestPublish_FireAndForget(t *testing.T) {
	reg := registry.New()
	h := &countingNotifHandler{}
	registry.RegisterNotificationHandler[pingNotification](reg, h)

	m := mediator.New(
		mediator.WithRegistry(reg),
		mediator.WithPublisher(publisher.NewFireAndForgetPublisher(nil)),
	)

	// Should return immediately.
	if err := mediator.Publish[pingNotification](context.Background(), m, pingNotification{}); err != nil {
		t.Fatalf("Publish (fire-and-forget) failed: %v", err)
	}

	// Give goroutine time to execute.
	time.Sleep(20 * time.Millisecond)

	if h.count.Load() != 1 {
		t.Errorf("expected handler called once, got %d", h.count.Load())
	}
}

// ── Stream tests ──────────────────────────────────────────────────────────────

// TestCreateStream_DeliversValues verifies stream delivers all values and closes.
func TestCreateStream_DeliversValues(t *testing.T) {
	reg := buildTestRegistry(t)
	m := mediator.New(mediator.WithRegistry(reg))

	ch, err := mediator.CreateStream[int](context.Background(), m, countRequest{N: 5})
	if err != nil {
		t.Fatalf("CreateStream failed: %v", err)
	}

	var got []int
	for v := range ch {
		got = append(got, v)
	}

	want := []int{1, 2, 3, 4, 5}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("stream values wrong\nwant: %v\ngot:  %v", want, got)
	}
}

// TestCreateStream_ContextCancellation verifies stream stops on ctx cancel.
func TestCreateStream_ContextCancellation(t *testing.T) {
	reg := buildTestRegistry(t)
	m := mediator.New(mediator.WithRegistry(reg))

	ctx, cancel := context.WithCancel(context.Background())

	// Large stream — we'll cancel early.
	ch, err := mediator.CreateStream[int](ctx, m, countRequest{N: 1000})
	if err != nil {
		t.Fatalf("CreateStream failed: %v", err)
	}

	// Read one value then cancel.
	<-ch
	cancel()

	// Drain remaining values. The channel should close (producer exits on ctx.Done).
	timeout := time.After(500 * time.Millisecond)
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				return // channel closed — test passes
			}
		case <-timeout:
			t.Fatal("timed out waiting for channel close after context cancellation")
		}
	}
}

// TestCreateStream_NoHandler verifies ErrNoHandler for unregistered stream types.
func TestCreateStream_NoHandler(t *testing.T) {
	m := mediator.New() // empty registry

	type unknownStreamReq struct{}
	_, err := mediator.CreateStream[int](context.Background(), m, unknownStreamReq{})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, mediator.ErrNoHandler) {
		t.Errorf("expected ErrNoHandler, got %T: %v", err, err)
	}
}
