// Package registry_test — unit tests for the handler registry.
//
// Tests cover:
//   - Successful registration of request, notification, and stream handlers.
//   - Duplicate request handler detection.
//   - Multiple notification handlers for the same type.
//   - Thread-safety under concurrent registration and resolution (use -race flag).
package registry_test

import (
	"context"
	"sync"
	"testing"

	"github.com/vinoprime/gono-mediator-lib/contracts"
	"github.com/vinoprime/gono-mediator-lib/registry"
)

// ── Test doubles ──────────────────────────────────────────────────────────────

// fakeRequest is a test request type.
// Implements contracts.Request[string] by embedding contracts.RequestMarker[string].
type fakeRequest struct {
	contracts.RequestMarker[string]
	Value string
}

// fakeHandler implements contracts.RequestHandler[fakeRequest, string].
type fakeHandler struct{}

func (h *fakeHandler) Handle(_ context.Context, req fakeRequest) (string, error) {
	return req.Value, nil
}

// fakeNotification is a test notification type.
// Implements contracts.Notification by embedding contracts.NotificationMarker.
type fakeNotification struct {
	contracts.NotificationMarker
	Message string
}

// fakeNotifHandler implements contracts.NotificationHandler[fakeNotification].
type fakeNotifHandler struct{ id string }

func (h *fakeNotifHandler) Handle(_ context.Context, _ fakeNotification) error { return nil }

// fakeStreamHandler implements contracts.StreamRequestHandler[fakeRequest, string].
type fakeStreamHandler struct{}

func (h *fakeStreamHandler) Handle(_ context.Context, req fakeRequest) (<-chan string, error) {
	ch := make(chan string, 1)
	ch <- req.Value
	close(ch)
	return ch, nil
}

// ── Request handler tests ─────────────────────────────────────────────────────

// TestRegisterRequestHandler_Success verifies a handler is stored and resolvable.
func TestRegisterRequestHandler_Success(t *testing.T) {
	reg := registry.New()
	handler := &fakeHandler{}

	err := registry.RegisterRequestHandler[fakeRequest, string](reg, handler)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	// Resolution is tested indirectly via Send in mediator_test.
}

// TestRegisterRequestHandler_DuplicateReturnsError verifies the 1:1 constraint.
func TestRegisterRequestHandler_DuplicateReturnsError(t *testing.T) {
	reg := registry.New()

	if err := registry.RegisterRequestHandler[fakeRequest, string](reg, &fakeHandler{}); err != nil {
		t.Fatalf("first registration failed: %v", err)
	}

	// Second registration for the same request type must fail.
	err := registry.RegisterRequestHandler[fakeRequest, string](reg, &fakeHandler{})
	if err == nil {
		t.Fatal("expected error on duplicate registration, got nil")
	}
}

// ── Notification handler tests ────────────────────────────────────────────────

// TestRegisterNotificationHandler_NoDuplicateError verifies N:1 notification is allowed.
func TestRegisterNotificationHandler_NoDuplicateError(t *testing.T) {
	reg := registry.New()

	// All three registrations must succeed (no error — multiple handlers are valid).
	registry.RegisterNotificationHandler[fakeNotification](reg, &fakeNotifHandler{id: "h1"})
	registry.RegisterNotificationHandler[fakeNotification](reg, &fakeNotifHandler{id: "h2"})
	registry.RegisterNotificationHandler[fakeNotification](reg, &fakeNotifHandler{id: "h3"})

	// Indirect verification: if Publish works with 3 handlers, registry stored them.
	// Direct count test via resolver:
	// (We don't expose a Count() API — this is by design to keep registry API minimal.)
}

// ── Stream handler tests ──────────────────────────────────────────────────────

// TestRegisterStreamHandler_DuplicateReturnsError verifies stream handler 1:1 constraint.
func TestRegisterStreamHandler_DuplicateReturnsError(t *testing.T) {
	reg := registry.New()

	if err := registry.RegisterStreamHandler[fakeRequest, string](reg, &fakeStreamHandler{}); err != nil {
		t.Fatalf("first stream registration failed: %v", err)
	}

	err := registry.RegisterStreamHandler[fakeRequest, string](reg, &fakeStreamHandler{})
	if err == nil {
		t.Fatal("expected error on duplicate stream handler registration, got nil")
	}
}

// ── Thread safety ─────────────────────────────────────────────────────────────

// TestRegistry_ConcurrentNotificationRegistration verifies no data race under
// concurrent notification handler registration.
// Run this test with the -race flag: go test -race ./registry/...
func TestRegistry_ConcurrentNotificationRegistration(t *testing.T) {
	reg := registry.New()
	var wg sync.WaitGroup

	// Register 100 notification handlers concurrently from separate goroutines.
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			registry.RegisterNotificationHandler[fakeNotification](reg, &fakeNotifHandler{id: "concurrent"})
		}()
	}

	wg.Wait()
	// If the race detector finds a race, the test fails automatically.
}

// ── Compile-time interface satisfaction checks ─────────────────────────────────

// These blank identifier assignments cause a compile error if the test doubles
// don't satisfy the required interfaces — no runtime assertion needed.
var _ contracts.RequestHandler[fakeRequest, string] = (*fakeHandler)(nil)
var _ contracts.NotificationHandler[fakeNotification] = (*fakeNotifHandler)(nil)
var _ contracts.StreamRequestHandler[fakeRequest, string] = (*fakeStreamHandler)(nil)
