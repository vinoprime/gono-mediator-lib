// Package main demonstrates the basic Send + Publish usage of gono-mediator-lib.
//
// This example shows:
//  1. Defining a request type (GetUserQuery → UserDTO)
//  2. Defining a notification type (UserFetchedEvent)
//  3. Registering handlers
//  4. Creating a Mediator
//  5. Sending a request and publishing a notification
package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"

	"github.com/gono-mediator-lib/contracts"
	"github.com/gono-mediator-lib/mediator"
	"github.com/gono-mediator-lib/registry"
)

// ── Domain model ─────────────────────────────────────────────────────────────

// UserDTO represents the response returned by GetUserQuery.
type UserDTO struct {
	ID   string
	Name string
	Age  int
}

// ── Request: GetUserQuery ─────────────────────────────────────────────────────

// GetUserQuery is a CQRS Query — asks for a user by ID.
// Implements contracts.Request[UserDTO] by embedding contracts.RequestMarker[UserDTO].
type GetUserQuery struct {
	contracts.RequestMarker[UserDTO]
	UserID string
}


// GetUserHandler processes GetUserQuery and returns a UserDTO.
// In production, this would query a database.
type GetUserHandler struct {
	// db *sql.DB  ← inject real dependency here
}

// Handle retrieves the user. Returns fake data for this example.
func (h *GetUserHandler) Handle(_ context.Context, q GetUserQuery) (UserDTO, error) {
	// Simulate a DB lookup.
	if q.UserID == "" {
		return UserDTO{}, fmt.Errorf("user ID is required")
	}
	return UserDTO{ID: q.UserID, Name: "Alice", Age: 30}, nil
}

// Compile-time check: GetUserHandler satisfies RequestHandler[GetUserQuery, UserDTO].
var _ contracts.RequestHandler[GetUserQuery, UserDTO] = (*GetUserHandler)(nil)

// ── Notification: UserFetchedEvent ───────────────────────────────────────────

// UserFetchedEvent is a domain event fired after a user is successfully retrieved.
// Implements contracts.Notification by embedding contracts.NotificationMarker.
type UserFetchedEvent struct {
	contracts.NotificationMarker
	UserID string
	Name   string
}

// AuditLogHandler logs that a user was fetched (one of potentially many handlers).
type AuditLogHandler struct {
	logger *slog.Logger
}

func (h *AuditLogHandler) Handle(_ context.Context, evt UserFetchedEvent) error {
	h.logger.Info("audit: user fetched", slog.String("userID", evt.UserID), slog.String("name", evt.Name))
	return nil
}

// MetricsHandler increments a counter when a user is fetched (second handler).
type MetricsHandler struct{}

func (h *MetricsHandler) Handle(_ context.Context, evt UserFetchedEvent) error {
	fmt.Printf("[metrics] user_fetch_count +1 (userID=%s)\n", evt.UserID)
	return nil
}

// ── Main ──────────────────────────────────────────────────────────────────────

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	// 1. Create and populate the registry.
	reg := registry.New()

	// Register the query handler (one handler per request type).
	if err := registry.RegisterRequestHandler[GetUserQuery, UserDTO](reg, &GetUserHandler{}); err != nil {
		log.Fatalf("register GetUserHandler: %v", err)
	}

	// Register multiple notification handlers for UserFetchedEvent.
	registry.RegisterNotificationHandler[UserFetchedEvent](reg, &AuditLogHandler{logger: logger})
	registry.RegisterNotificationHandler[UserFetchedEvent](reg, &MetricsHandler{})

	// 2. Create the mediator with the registry.
	m := mediator.New(mediator.WithRegistry(reg))

	ctx := context.Background()

	// 3. Send a query — returns a strongly-typed UserDTO.
	fmt.Println("=== Send: GetUserQuery ===")
	user, err := mediator.Send[UserDTO](ctx, m, GetUserQuery{UserID: "u-42"})
	if err != nil {
		log.Fatalf("Send failed: %v", err)
	}
	fmt.Printf("User: ID=%s, Name=%s, Age=%d\n\n", user.ID, user.Name, user.Age)

	// 4. Publish a notification — all handlers are called sequentially.
	fmt.Println("=== Publish: UserFetchedEvent ===")
	if err := mediator.Publish[UserFetchedEvent](ctx, m, UserFetchedEvent{
		UserID: user.ID,
		Name:   user.Name,
	}); err != nil {
		log.Fatalf("Publish failed: %v", err)
	}

	fmt.Println("\nDone.")
}
