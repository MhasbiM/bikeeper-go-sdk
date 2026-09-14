package bikeeper

import "time"

// Defaults applied by New (and by the send queue when a hand-built Client
// leaves them unset).
const (
	defaultTimeout         = 5 * time.Second
	defaultFlushTimeout    = 2 * time.Second
	defaultMaxQueueSize    = 1000
	defaultSendConcurrency = 4
)

// Options configures the Bikeeper client.
type Options struct {
	// ClientID is the project's client ID (required).
	ClientID string

	// ClientSecret is the project's client secret (required).
	ClientSecret string

	// Endpoint is the base URL of the Bikeeper server.
	// Defaults to "http://localhost:8080".
	Endpoint string

	// Environment tags events with an environment name (e.g. "production").
	Environment string

	// Release tags events with a release version (e.g. "v1.2.3").
	Release string

	// Timeout is the HTTP request timeout per event.
	// Defaults to 5s.
	Timeout time.Duration

	// FlushTimeout is the maximum time to wait when flushing buffered events.
	// Defaults to 2s.
	FlushTimeout time.Duration

	// Framework identifies which SDK middleware integration is in use (e.g.
	// "fiber", "echo"). This is set automatically by the framework middleware
	// package (bikeeperfiber.New / bikeeperecho.New) — do NOT set this manually.
	// A process with no framework middleware (a queue consumer, a cron job, a
	// CLI) reports as "go" and sends normally.
	Framework string

	// ProjectID is the project's internal UUID shown on the Bikeeper dashboard.
	// Required — the backend validates that the credentials belong to exactly
	// this project, preventing cross-project credential reuse.
	ProjectID string

	// OnError is called when an async event send fails (e.g. network error,
	// auth failure, server rejection). If nil, failures are silently discarded.
	OnError func(err error)

	// EnableLogging separates Logger entries from events.
	//
	// When false (default), [Logger] / [LogEntry].Emit calls fall through to
	// [Client.CaptureMessage], so log entries appear in the Events view exactly
	// as before.
	//
	// When true, [Logger] / [LogEntry].Emit sends a lightweight [LogRecord] to
	// the dedicated POST /api/v1/logs endpoint and the entry is stored in the
	// logs table — separate from the events table. This mirrors Sentry's
	// structured-logging feature where logs are a first-class resource alongside
	// errors.
	EnableLogging bool

	// MaxQueueSize bounds how many payloads (events, log records, transactions)
	// may wait for delivery at once. Captures made while the queue is full are
	// dropped and counted by [Client.Dropped] rather than queued indefinitely,
	// so a slow or unreachable endpoint costs telemetry instead of memory.
	// Defaults to 1000.
	MaxQueueSize int

	// SendConcurrency is the number of goroutines delivering queued payloads.
	// Defaults to 4.
	SendConcurrency int

	// BeforeSend is called with every enriched event just before it is queued
	// for delivery. Return the event (modified in place or replaced) to send
	// it, or nil to drop it.
	//
	// This is the place to scrub data that must not leave the process — query
	// arguments, tokens, anything carrying personal data — and to silence
	// known-noisy events:
	//
	//	BeforeSend: func(ev *bikeeper.Event) *bikeeper.Event {
	//	    ev.Tags = slices.DeleteFunc(ev.Tags, func(t bikeeper.Tag) bool {
	//	        return t.Key == "args"
	//	    })
	//	    return ev
	//	}
	//
	// It runs on a sender goroutine, so it must not block; a panic inside it
	// drops the event instead of taking the process down.
	BeforeSend func(event *Event) *Event

	// TracesSampleRate is the fraction of traces sampled for APM/performance
	// data (0.0–1.0), rolled once per trace at the root Span's creation
	// (head-based — every descendant inherits the decision, so a trace is
	// never partially sampled).
	//
	// Defaults to 0 (disabled). Framework middleware (bikeeperfiber,
	// bikeeperecho) auto-starts a transaction for every HTTP request, so an
	// opt-in default means upgrading the SDK version alone never silently
	// starts sending performance data — the app owner turns it on
	// deliberately, e.g. TracesSampleRate: 1.0 to capture every trace, or a
	// lower value on high-traffic services. This matches Sentry's own SDKs,
	// which default TracesSampleRate to 0 for the same reason.
	//
	// Span/Tag/Data/TraceID propagation into captured Events is unaffected
	// regardless of this value — only APM sending is gated.
	TracesSampleRate float64
}
