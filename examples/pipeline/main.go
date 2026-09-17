// Package main demonstrates pipeline behaviors: logging + validation + panic recovery.
//
// This example shows:
//  1. How to attach multiple behaviors to a mediator
//  2. Behavior execution order (Recovery → Logging → Validation → Handler)
//  3. Short-circuit via ValidationBehavior on an invalid request
//  4. RecoveryBehavior converting a panic to a *mediator.PanicError
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"os"

	"github.com/gono-mediator-lib/contracts"
	"github.com/gono-mediator-lib/mediator"
	"github.com/gono-mediator-lib/pipeline"
	"github.com/gono-mediator-lib/registry"
)

// ── Domain ────────────────────────────────────────────────────────────────────

// CreateOrderCommand is a write-side CQRS command.
// Implements contracts.Request[OrderResult] by embedding contracts.RequestMarker[OrderResult].
type CreateOrderCommand struct {
	contracts.RequestMarker[OrderResult]
	ProductID string
	Quantity  int
}

// Validate implements pipeline.Validator — called by ValidationBehavior.
func (c CreateOrderCommand) Validate() error {
	if c.ProductID == "" {
		return errors.New("productID is required")
	}
	if c.Quantity <= 0 {
		return errors.New("quantity must be positive")
	}
	return nil
}

// OrderResult is the response from CreateOrderCommand.
type OrderResult struct {
	OrderID string
	Total   float64
}

// CreateOrderHandler processes CreateOrderCommand.
type CreateOrderHandler struct{}

func (h *CreateOrderHandler) Handle(_ context.Context, cmd CreateOrderCommand) (OrderResult, error) {
	// Simulate order creation logic.
	return OrderResult{
		OrderID: "ord-001",
		Total:   float64(cmd.Quantity) * 9.99,
	}, nil
}

// BuggyCommand always panics (to demonstrate recovery behavior).
// Implements contracts.Request[string] by embedding contracts.RequestMarker[string].
type BuggyCommand struct {
	contracts.RequestMarker[string]
}

// BuggyHandler simulates a handler that panics.
type BuggyHandler struct{}

func (h *BuggyHandler) Handle(_ context.Context, _ BuggyCommand) (string, error) {
	panic("unexpected nil pointer dereference in BuggyHandler")
}


// ── Main ──────────────────────────────────────────────────────────────────────

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	// Build registry.
	reg := registry.New()
	if err := registry.RegisterRequestHandler[CreateOrderCommand, OrderResult](reg, &CreateOrderHandler{}); err != nil {
		log.Fatal(err)
	}
	if err := registry.RegisterRequestHandler[BuggyCommand, string](reg, &BuggyHandler{}); err != nil {
		log.Fatal(err)
	}

	// Create mediator with pipeline behaviors applied in order:
	//   RecoveryBehavior (outermost) → LoggingBehavior → ValidationBehavior (innermost before handler)
	//
	// Execution onion:
	//   Recovery [ Logging [ Validation [ Handler ] ] ]
	m := mediator.New(
		mediator.WithRegistry(reg),
		mediator.WithBehavior[CreateOrderCommand, OrderResult](
			pipeline.NewRecoveryBehavior[CreateOrderCommand, OrderResult](),
		),
		mediator.WithBehavior[CreateOrderCommand, OrderResult](
			pipeline.NewLoggingBehavior[CreateOrderCommand, OrderResult](logger),
		),
		mediator.WithBehavior[CreateOrderCommand, OrderResult](
			pipeline.NewValidationBehavior[CreateOrderCommand, OrderResult](),
		),
		// Recovery for BuggyCommand too.
		mediator.WithBehavior[BuggyCommand, string](
			pipeline.NewRecoveryBehavior[BuggyCommand, string](),
		),
	)

	ctx := context.Background()

	// ── Case 1: Valid request → handler runs ──────────────────────────────────
	fmt.Println("=== Case 1: Valid CreateOrderCommand ===")
	result, err := mediator.Send[OrderResult](ctx, m, CreateOrderCommand{
		ProductID: "prod-007",
		Quantity:  3,
	})
	if err != nil {
		log.Fatalf("unexpected error: %v", err)
	}
	fmt.Printf("Order created: %s, total: $%.2f\n\n", result.OrderID, result.Total)

	// ── Case 2: Invalid request → ValidationBehavior short-circuits ──────────
	fmt.Println("=== Case 2: Invalid CreateOrderCommand (empty productID) ===")
	_, err = mediator.Send[OrderResult](ctx, m, CreateOrderCommand{Quantity: 1})
	if err != nil {
		fmt.Printf("Validation error (expected): %v\n\n", err)
	}

	// ── Case 3: Handler panics → RecoveryBehavior converts to PanicError ─────
	fmt.Println("=== Case 3: BuggyHandler panics → recovered as PanicError ===")
	_, err = mediator.Send[string](ctx, m, BuggyCommand{})
	if err != nil {
		var pe *mediator.PanicError
		if errors.As(err, &pe) {
			fmt.Printf("Panic recovered: %v\n", pe.Value)
		} else {
			fmt.Printf("Unexpected error type: %T: %v\n", err, err)
		}
	}

	fmt.Println("\nDone.")
}
