# bikeeper-go-sdk

[![Go Reference](https://pkg.go.dev/badge/github.com/MhasbiM/bikeeper-go-sdk.svg)](https://pkg.go.dev/github.com/MhasbiM/bikeeper-go-sdk)
[![Go Report Card](https://goreportcard.com/badge/github.com/MhasbiM/bikeeper-go-sdk)](https://goreportcard.com/report/github.com/MhasbiM/bikeeper-go-sdk)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
![Version](https://img.shields.io/badge/version-v1.0.0-blue)

Official Go SDK for [Bikeeper](https://github.com/MhasbiM/bikeeper) — a self-hosted error monitoring and performance tracking platform.

## Packages

| Package | Import path | Description |
|---|---|---|
| Core | `github.com/MhasbiM/bikeeper-go-sdk` | Client, Hub, Logger, Span, tracing |
| Fiber | `github.com/MhasbiM/bikeeper-go-sdk/fiber` | Middleware for [Fiber v3](https://github.com/gofiber/fiber) |
| Echo | `github.com/MhasbiM/bikeeper-go-sdk/echo` | Middleware for [Echo v4](https://github.com/labstack/echo) |
| Zap | `github.com/MhasbiM/bikeeper-go-sdk/zap` | [go.uber.org/zap](https://github.com/uber-go/zap) core integration |

## Requirements

- Go 1.21+

## Installation

```bash
go get github.com/MhasbiM/bikeeper-go-sdk@v1.0.0
```

With framework middleware:

```bash
# Fiber
go get github.com/MhasbiM/bikeeper-go-sdk/fiber@v1.0.0

# Echo
go get github.com/MhasbiM/bikeeper-go-sdk/echo@v1.0.0

# Zap integration
go get github.com/MhasbiM/bikeeper-go-sdk/zap@v1.0.0
```

## Quick Start

### Core client

```go
import bikeeper "github.com/MhasbiM/bikeeper-go-sdk"

client := bikeeper.New(bikeeper.Options{
    ClientID:     "your-client-id",
    ClientSecret: "your-client-secret",
    ProjectID:    "your-project-uuid",
    Endpoint:     "https://your-bikeeper-instance.com",
    Environment:  "production",
    Release:      "v1.0.0",
})
defer client.Flush()

// Capture an error
client.CaptureException(ctx, err)

// Capture a message
client.CaptureMessage(ctx, "payment processed", bikeeper.LevelInfo)
```

### Fiber middleware

```go
import (
    bikeeper        "github.com/MhasbiM/bikeeper-go-sdk"
    bikeeperfiber   "github.com/MhasbiM/bikeeper-go-sdk/fiber"
    "github.com/gofiber/fiber/v3"
    "github.com/gofiber/fiber/v3/middleware/recover"
)

client := bikeeper.New(bikeeper.Options{
    ClientID:     "your-client-id",
    ClientSecret: "your-client-secret",
    ProjectID:    "your-project-uuid",
    Endpoint:     "https://your-bikeeper-instance.com",
})

app := fiber.New()
app.Use(recover.New())
app.Use(bikeeperfiber.New(client, bikeeperfiber.Options{
    Repanic: true,
}))

app.Get("/", func(c fiber.Ctx) error {
    // Manual capture inside a handler
    if err := doSomething(); err != nil {
        bikeeperfiber.GetClientFromContext(c).CaptureException(c.Context(), err)
        return fiber.ErrInternalServerError
    }
    return c.SendString("ok")
})
```

### Echo middleware

```go
import (
    bikeeper       "github.com/MhasbiM/bikeeper-go-sdk"
    bikeeperecho   "github.com/MhasbiM/bikeeper-go-sdk/echo"
    "github.com/labstack/echo/v4"
    "github.com/labstack/echo/v4/middleware"
)

client := bikeeper.New(bikeeper.Options{
    ClientID:     "your-client-id",
    ClientSecret: "your-client-secret",
    ProjectID:    "your-project-uuid",
    Endpoint:     "https://your-bikeeper-instance.com",
})

e := echo.New()
e.Use(middleware.Recover())
e.Use(bikeeperecho.New(client, bikeeperecho.Options{
    Repanic: true,
}))

e.GET("/", func(c echo.Context) error {
    if err := doSomething(); err != nil {
        bikeeperecho.GetClientFromContext(c).CaptureException(c.Request().Context(), err)
        return echo.ErrInternalServerError
    }
    return c.String(200, "ok")
})
```

### Logger (structured logging)

Send structured log entries as Bikeeper events using the built-in `Logger` API.
Each log-level method returns a chainable `LogEntry` builder.

```go
import bikeeper "github.com/MhasbiM/bikeeper-go-sdk"

logger := client.NewLogger(ctx)

// Simple emit — args formatted with fmt.Sprint
logger.Info().Emit("server started")

// Formatted emit — fmt.Printf style
logger.Error().Emitf("payment failed: %v", err)

// Per-entry tag (does not modify the logger)
logger.Warn().
    WithTag("gateway", "stripe").
    WithTag("attempt", "3").
    Emit("gateway slow response")

// Logger-level tags — inherited by every entry from the derived logger
serviceLogger := logger.
    WithTag("service", "checkout").
    WithTag("env", "production")
serviceLogger.Debug().Emit("cart validated")
serviceLogger.Error().Emitf("stock check failed: %v", err)
```

### Zap integration

Forward [go.uber.org/zap](https://github.com/uber-go/zap) log entries to Bikeeper automatically.
Structured `zap.Field` values become Bikeeper tags — no changes required at call sites.

```go
import (
    bikeeper        "github.com/MhasbiM/bikeeper-go-sdk"
    bikeeperzap     "github.com/MhasbiM/bikeeper-go-sdk/zap"
    "go.uber.org/zap"
    "go.uber.org/zap/zapcore"
)

// Tee an existing logger: warn+ entries go to Bikeeper, all entries go to stdout.
zapLogger, _ := zap.NewProduction()
zapLogger = bikeeperzap.AttachTo(zapLogger, client, ctx, zapcore.WarnLevel)

// These two lines go to stdout only (below WarnLevel).
zapLogger.Debug("order lookup started", zap.String("order_id", "ORD-001"))
zapLogger.Info("order found", zap.String("order_id", "ORD-001"))

// These go to stdout AND to Bikeeper as captured events.
// zap.Field values are mapped to Bikeeper tags automatically.
zapLogger.Warn("payment retry #2",
    zap.String("gateway", "stripe"),
    zap.Int("attempt", 2),
)
zapLogger.Error("stripe gateway timeout",
    zap.String("order_id", "ORD-001"),
    zap.Error(fmt.Errorf("context deadline exceeded")),
)
```

Build from scratch using `zapcore.NewTee`:

```go
core := zapcore.NewTee(
    zapcore.NewCore(enc, sink, lvl),                          // stdout / file
    bikeeperzap.NewCore(client, ctx, zapcore.WarnLevel),      // Bikeeper
)
logger := zap.New(core, zap.AddCaller())
```

## Configuration

| Option | Type | Default | Description |
|---|---|---|---|
| `ClientID` | `string` | — | Project client ID (**required**) |
| `ClientSecret` | `string` | — | Project client secret (**required**) |
| `ProjectID` | `string` | — | Project UUID from dashboard (**required**) |
| `Endpoint` | `string` | `http://localhost:8080` | Bikeeper server base URL |
| `Environment` | `string` | — | Environment label (e.g. `"production"`) |
| `Release` | `string` | — | Release version tag (e.g. `"v1.0.0"`) |
| `Timeout` | `time.Duration` | `5s` | Per-event HTTP request timeout |
| `FlushTimeout` | `time.Duration` | `2s` | Max wait time on `Flush()` |
| `OnError` | `func(error)` | `nil` | Called on async send failures |

## Tracing

```go
// Start a transaction (top-level span). StartTransaction/StartSpan return a
// single *Span — use span.Context() to get the context for child calls.
tx := bikeeper.StartTransaction(ctx, "order.process")
defer tx.Finish()
ctx = tx.Context()

// Start a child span from that context — it inherits the parent's TraceID.
span := bikeeper.StartSpan(ctx, "db.query", bikeeper.WithDescription("SELECT * FROM orders WHERE id = ?"))
defer span.Finish()
ctx = span.Context()

// Tag the span/transaction directly...
tx.SetTag("http.method", "GET")

// ...or attach tags via the hub's scope so they apply to every event captured
// through it (SetTag lives on Scope, not Hub).
hub := bikeeper.GetHubFromContext(ctx)
hub.Scope().SetTag("user_id", "123")
hub.Scope().SetTag("order_id", "abc")
```

## License

MIT — see [LICENSE](LICENSE).
