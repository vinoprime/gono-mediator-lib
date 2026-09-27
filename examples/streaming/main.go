// Package main demonstrates streaming requests using gono-mediator-lib.
//
// This example shows:
//  1. Defining a StreamRequest type (StockPriceQuery → PriceUpdate)
//  2. Implementing a StreamRequestHandler using stream.NewStream + stream.SafeSend
//  3. Using CreateStream to consume the channel
//  4. Graceful shutdown via context cancellation
package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/vinoprime/gono-mediator-lib/contracts"
	"github.com/vinoprime/gono-mediator-lib/mediator"
	"github.com/vinoprime/gono-mediator-lib/registry"
	"github.com/vinoprime/gono-mediator-lib/stream"
)

// ── Domain ────────────────────────────────────────────────────────────────────

// StockPriceQuery asks for a live price stream for a given stock symbol.
// Implements contracts.StreamRequest[PriceUpdate] by embedding contracts.StreamMarker[PriceUpdate].
type StockPriceQuery struct {
	contracts.StreamMarker[PriceUpdate]
	Symbol   string
	Interval time.Duration // how often to emit a new price
	Count    int           // 0 = infinite; > 0 = emit exactly Count prices then stop
}

// PriceUpdate is a single price tick emitted by the stream.
type PriceUpdate struct {
	Symbol string
	Price  float64
	At     time.Time
}

// ── Stream handler ────────────────────────────────────────────────────────────

// StockPriceHandler simulates a live price feed.
type StockPriceHandler struct{}

// Handle starts the price stream. Uses stream.NewStream for safe goroutine management.
func (h *StockPriceHandler) Handle(ctx context.Context, req StockPriceQuery) (<-chan PriceUpdate, error) {
	return stream.NewStream[PriceUpdate](ctx, 10, func(ctx context.Context, out chan<- PriceUpdate) error {
		// Simulate a starting price between $50 and $150.
		price := 50.0 + rand.Float64()*100

		emitted := 0
		for {
			// Stop if we've emitted the requested count.
			if req.Count > 0 && emitted >= req.Count {
				return nil
			}

			// Simulate price movement (random walk ±0.5%).
			price += price * (rand.Float64()*0.01 - 0.005)

			update := PriceUpdate{
				Symbol: req.Symbol,
				Price:  price,
				At:     time.Now(),
			}

			// SafeSend returns false when ctx is cancelled — gracefully exit.
			if !stream.SafeSend(ctx, out, update) {
				return nil
			}
			emitted++

			// Wait for the next tick (respects cancellation).
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(req.Interval):
			}
		}
	})
}

// ── Main ──────────────────────────────────────────────────────────────────────

func main() {
	// Register the stream handler.
	reg := registry.New()
	if err := registry.RegisterStreamHandler[StockPriceQuery, PriceUpdate](reg, &StockPriceHandler{}); err != nil {
		log.Fatal(err)
	}

	m := mediator.New(mediator.WithRegistry(reg))

	// ── Example 1: Finite stream (5 price updates) ───────────────────────────
	fmt.Println("=== Example 1: AAPL — 5 price updates ===")

	ctx := context.Background()
	ch, err := mediator.CreateStream[PriceUpdate](ctx, m, StockPriceQuery{
		Symbol:   "AAPL",
		Interval: 50 * time.Millisecond,
		Count:    5,
	})
	if err != nil {
		log.Fatalf("CreateStream failed: %v", err)
	}

	for update := range ch {
		fmt.Printf("  [%s] %s $%.2f\n", update.At.Format("15:04:05.000"), update.Symbol, update.Price)
	}

	// ── Example 2: Infinite stream cancelled after 200ms ─────────────────────
	fmt.Println("\n=== Example 2: GOOGL — infinite stream, cancelled after 200ms ===")

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	ch, err = mediator.CreateStream[PriceUpdate](ctx, m, StockPriceQuery{
		Symbol:   "GOOGL",
		Interval: 30 * time.Millisecond,
		Count:    0, // infinite
	})
	if err != nil {
		log.Fatalf("CreateStream failed: %v", err)
	}

	count := 0
	for update := range ch {
		fmt.Printf("  [%s] %s $%.2f\n", update.At.Format("15:04:05.000"), update.Symbol, update.Price)
		count++
	}
	fmt.Printf("  (received %d updates before timeout)\n", count)

	fmt.Println("\nDone.")
}
