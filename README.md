# bikeeper-go-sdk

[![Go Reference](https://pkg.go.dev/badge/github.com/MhasbiM/bikeeper-go-sdk.svg)](https://pkg.go.dev/github.com/MhasbiM/bikeeper-go-sdk)
[![Go Report Card](https://goreportcard.com/badge/github.com/MhasbiM/bikeeper-go-sdk)](https://goreportcard.com/report/github.com/MhasbiM/bikeeper-go-sdk)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
![Version](https://img.shields.io/badge/version-v1.0.0-blue)

Official Go SDK for [Bikeeper](https://github.com/MhasbiM/bikeeper) — a self-hosted error monitoring and performance tracking platform.

## Packages

| Package | Import path | Description |
|---|---|---|
| Core | `github.com/MhasbiM/bikeeper-go-sdk` | Client, Hub, Span, tracing |
| Fiber | `github.com/MhasbiM/bikeeper-go-sdk/fiber` | Middleware for [Fiber v3](https://github.com/gofiber/fiber) |
| Echo | `github.com/MhasbiM/bikeeper-go-sdk/echo` | Middleware for [Echo v4](https://github.com/labstack/echo) |

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
// Start a transaction (top-level span)
ctx, tx := bikeeper.StartTransaction(ctx, "order.process")
defer tx.Finish()

// Start a child span
ctx, span := bikeeper.StartSpan(ctx, "db.query")
defer span.Finish()

// Attach tags to the current hub's scope
hub := bikeeper.GetHubFromContext(ctx)
hub.SetTag("user_id", "123")
hub.SetTag("order_id", "abc")
```

## License

MIT — see [LICENSE](LICENSE).
