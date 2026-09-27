# gono-mediator-lib

> A production-grade, type-safe mediator library for Go — built with Go 1.21 generics, zero external dependencies, and no global state.

[![Go Version](https://img.shields.io/badge/go-1.21+-blue.svg)](https://golang.org/dl/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

---

## Why?

`gono-mediator-lib` provides typed request dispatch, notifications, streaming, and composable pipeline behaviors for Go applications:

| Feature                   | gono-mediator-lib                             |
| ------------------------- | --------------------------------------------- |
| Request/Response dispatch | `mediator.Send[T]`                            |
| Notification fan-out      | `mediator.Publish[T]`                         |
| Streaming                 | `mediator.CreateStream[T]`                    |
| Pipeline middleware       | `contracts.PipelineBehavior[Req,Res]`         |
| Publisher strategies      | `publisher.Publisher` interface               |
| Type safety               | Go generics with no `reflect` in the hot path |
| Dependency injection      | **Not required** — explicit wiring            |

---

## Architecture

```
gono-mediator-lib/
│
├── contracts/        ← Extension interfaces (SSoT) — import this to build handlers
│   ├── request.go    ← RequestMarker[T], Request[T], RequestHandler[Req, Res]
│   ├── notification.go ← NotificationMarker, Notification, NotificationHandler[T]
│   ├── pipeline.go   ← Next[T] func type, PipelineBehavior[Req, Res]
│   └── stream.go     ← StreamMarker[T], StreamRequest[T], StreamRequestHandler[Req, Res]
│
├── mediator/         ← Public API — Send, Publish, CreateStream
│   ├── mediator.go   ← Mediator struct + top-level dispatch functions
│   ├── options.go    ← WithRegistry, WithPublisher, WithBehavior options
│   └── errors.go     ← ErrNoHandler, ErrHandlerAlreadyRegistered, PanicError
│
├── registry/         ← Thread-safe handler registration
│   ├── registry.go   ← Register/Resolve functions, RWMutex-guarded maps
│   └── builder.go    ← Fluent registration builder
│
├── publisher/        ← Notification publishing strategies (pluggable)
│   ├── publisher.go  ← Publisher interface + HandlerFunc type
│   ├── sequential.go ← Default: sequential, stop-on-first-error
│   ├── parallel.go   ← All handlers concurrently, errors.Join aggregation
│   └── fireandforget.go ← Goroutine per handler, async error callback
│
├── pipeline/         ← Built-in behaviors (opt-in)
│   ├── logging.go    ← Structured logging via slog (stdlib)
│   ├── recovery.go   ← Panic → *mediator.PanicError conversion
│   ├── validation.go ← Calls Validate() if request implements pipeline.Validator
│   └── timeout.go    ← Injects context.WithTimeout into pipeline
│
├── stream/           ← Stream helpers
│   └── sender.go     ← SafeSend, NewStream utilities for handlers
│
└── examples/
    ├── basic/        ← Send + Publish
    ├── pipeline/     ← Logging, Validation, Recovery behaviors
    └── streaming/    ← Channel-based streaming with cancellation
```

---

## Quick Start

### 1. Define a Request and Handler

```go
import (
    "context"
    "github.com/gono-mediator-lib/contracts"
)

// UserDTO is the response type.
type UserDTO struct { ID, Name string }

// GetUserQuery is a CQRS Query.
// Embed contracts.RequestMarker[UserDTO] to declare the response type.
type GetUserQuery struct {
    contracts.RequestMarker[UserDTO]
    UserID string
}

// GetUserHandler implements contracts.RequestHandler[GetUserQuery, UserDTO].
type GetUserHandler struct { db *sql.DB }

func (h *GetUserHandler) Handle(ctx context.Context, q GetUserQuery) (UserDTO, error) {
    // ... query DB
    return UserDTO{ID: q.UserID, Name: "Alice"}, nil
}

// Compile-time interface check (industry best practice):
var _ contracts.RequestHandler[GetUserQuery, UserDTO] = (*GetUserHandler)(nil)
```

### 2. Define a Notification and Handlers

```go
// UserCreatedEvent is a domain event.
// Embed contracts.NotificationMarker to declare it as a notification.
type UserCreatedEvent struct {
    contracts.NotificationMarker
    UserID string
    Email  string
}

// Multiple handlers can listen to the same notification.
type SendWelcomeEmailHandler struct { mailer Mailer }
func (h *SendWelcomeEmailHandler) Handle(ctx context.Context, evt UserCreatedEvent) error {
    return h.mailer.Send(ctx, evt.Email, "Welcome!")
}

type AuditLogHandler struct{}
func (h *AuditLogHandler) Handle(ctx context.Context, evt UserCreatedEvent) error {
    log.Printf("user created: %s", evt.UserID)
    return nil
}
```

### 3. Wire Up and Use

```go
import (
    "context"
    "github.com/gono-mediator-lib/mediator"
    "github.com/gono-mediator-lib/registry"
)

// Create registry and register handlers.
reg := registry.New()
registry.RegisterRequestHandler[GetUserQuery, UserDTO](reg, &GetUserHandler{db: db})
registry.RegisterNotificationHandler[UserCreatedEvent](reg, &SendWelcomeEmailHandler{mailer: m})
registry.RegisterNotificationHandler[UserCreatedEvent](reg, &AuditLogHandler{})

// Create the mediator.
m := mediator.New(mediator.WithRegistry(reg))

// Send a query — fully typed, no casting.
user, err := mediator.Send[UserDTO](ctx, m, GetUserQuery{UserID: "u-42"})

// Publish a notification — all handlers called sequentially by default.
err = mediator.Publish[UserCreatedEvent](ctx, m, UserCreatedEvent{UserID: "u-42", Email: "alice@example.com"})
```

---

## Pipeline Behaviors

Behaviors are cross-cutting concerns that wrap every request — like HTTP middleware.

```
[ Recovery [ Logging [ Validation [ Timeout [ Handler ] ] ] ] ]
   ↑ outermost                              ↑ innermost
   (first registered)                    (last registered)
```

### Built-in Behaviors

| Behavior             | What it does                                                          |
| -------------------- | --------------------------------------------------------------------- |
| `RecoveryBehavior`   | Converts panics to `*mediator.PanicError` — always register outermost |
| `LoggingBehavior`    | Logs request name + duration + error via `slog`                       |
| `ValidationBehavior` | Calls `Validate() error` if request implements `pipeline.Validator`   |
| `TimeoutBehavior`    | Wraps inner pipeline with `context.WithTimeout`                       |

### Usage

```go
import (
    "log/slog"
    "github.com/gono-mediator-lib/mediator"
    "github.com/gono-mediator-lib/pipeline"
)

logger := slog.Default()

m := mediator.New(
    mediator.WithRegistry(reg),
    // Order matters — first registered = outermost.
    mediator.WithBehavior[CreateOrderCommand, OrderResult](
        pipeline.NewRecoveryBehavior[CreateOrderCommand, OrderResult](),
    ),
    mediator.WithBehavior[CreateOrderCommand, OrderResult](
        pipeline.NewLoggingBehavior[CreateOrderCommand, OrderResult](logger),
    ),
    mediator.WithBehavior[CreateOrderCommand, OrderResult](
        pipeline.NewValidationBehavior[CreateOrderCommand, OrderResult](),
    ),
    mediator.WithBehavior[CreateOrderCommand, OrderResult](
        pipeline.NewTimeoutBehavior[CreateOrderCommand, OrderResult](500 * time.Millisecond),
    ),
)
```

### Custom Behavior

```go
type AuthorizationBehavior[TReq any, TRes any] struct {
    authService AuthService
}

func (b *AuthorizationBehavior[TReq, TRes]) Handle(
    ctx context.Context,
    req TReq,
    next contracts.Next[TRes],
) (TRes, error) {
    if !b.authService.IsAuthorized(ctx) {
        var zero TRes
        return zero, ErrUnauthorized
    }
    return next(ctx) // call the rest of the pipeline
}
```

---

## Notification Publisher Strategies

```go
import "github.com/gono-mediator-lib/publisher"

// Sequential (default) — one-by-one, stops on first error.
m := mediator.New(mediator.WithPublisher(publisher.NewSequentialPublisher()))

// Parallel — all handlers concurrently, errors.Join aggregation.
m := mediator.New(mediator.WithPublisher(publisher.NewParallelPublisher()))

// Fire-and-forget — goroutine per handler, errors via callback.
m := mediator.New(mediator.WithPublisher(
    publisher.NewFireAndForgetPublisher(func(idx int, err error) {
        slog.Error("notification handler failed", "handlerIndex", idx, "error", err)
    }),
))
```

---

## Streaming

```go
import "github.com/gono-mediator-lib/stream"

// 1. Define a StreamRequest.
type LiveDataQuery struct {
    contracts.StreamMarker[DataPoint]
    Symbol string
}

// 2. Implement StreamRequestHandler.
type LiveDataHandler struct{}
func (h *LiveDataHandler) Handle(ctx context.Context, req LiveDataQuery) (<-chan DataPoint, error) {
    // stream.NewStream manages goroutine lifecycle and close.
    return stream.NewStream[DataPoint](ctx, 10, func(ctx context.Context, out chan<- DataPoint) error {
        for {
            point := fetchNextPoint(req.Symbol)
            if !stream.SafeSend(ctx, out, point) {
                return nil // context cancelled — stop producing
            }
        }
    })
}

// 3. Register and use.
registry.RegisterStreamHandler[LiveDataQuery, DataPoint](reg, &LiveDataHandler{})
m := mediator.New(mediator.WithRegistry(reg))

ch, err := mediator.CreateStream[DataPoint](ctx, m, LiveDataQuery{Symbol: "AAPL"})
for point := range ch {
    fmt.Println(point)
}
```

---

## Error Handling

```go
import "errors"

_, err := mediator.Send[UserDTO](ctx, m, GetUserQuery{UserID: ""})

// Check for no handler:
if errors.Is(err, mediator.ErrNoHandler) {
    // Request type not registered — programmer error
}

// Inspect a recovered panic:
var pe *mediator.PanicError
if errors.As(err, &pe) {
    log.Printf("panic value: %v\nstack: %s", pe.Value, pe.Stack)
}
```

---

## Validation

Requests opt-in to validation by implementing `pipeline.Validator`:

```go
type CreateUserCommand struct {
    contracts.RequestMarker[UserID]
    Email string
    Age   int
}

func (c CreateUserCommand) Validate() error {
    if c.Email == "" {
        return errors.New("email is required")
    }
    if c.Age < 18 {
        return errors.New("must be 18 or older")
    }
    return nil
}
```

With `pipeline.NewValidationBehavior[CreateUserCommand, UserID]()` registered, the handler is never called if `Validate()` returns an error.

---

## Testing

Because `Mediator` is an explicit struct (no global state), testing is trivial:

```go
func TestMyHandler(t *testing.T) {
    reg := registry.New()
    registry.RegisterRequestHandler[MyQuery, MyResult](reg, &MyHandler{})
    m := mediator.New(mediator.WithRegistry(reg))

    result, err := mediator.Send[MyResult](context.Background(), m, MyQuery{ID: "test"})
    // assert...
}
```

---

## Industry Approach: How Teams Actually Use CQRS/Mediator

1. **One handler file per feature** — `handlers/get_user.go`, `handlers/create_order.go`
2. **Registry wired in `main.go` or DI root** — all registrations in one place
3. **Recovery + Logging always on** — register them globally, they apply to every dispatch
4. **Validation opt-in per command** — queries usually don't validate input
5. **Parallel publisher for analytics/audit events** — commands don't wait for metrics handlers
6. **Fire-and-forget only for truly non-critical side-effects** — prefer parallel for reliability

---

## Running Examples

```bash
go run ./examples/basic/...      # Send + Publish
go run ./examples/pipeline/...   # Pipeline behaviors (logging, validation, recovery)
go run ./examples/streaming/...  # Channel streaming + cancellation
```

## Running Tests

```bash
go test ./... -count=1 -v
go vet ./...
```
