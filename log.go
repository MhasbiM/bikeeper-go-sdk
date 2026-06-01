package bikeeper

import "time"

// LogRecord is the lightweight payload sent to /api/v1/logs when
// [Options.EnableLogging] is true.
//
// Unlike an [Event], a LogRecord has no exception, no stacktrace, and no
// grouping fingerprint — it is a structured log line, not an error occurrence.
// Think of it as the Go SDK equivalent of Sentry's LogEntry API.
//
// LogRecords are created automatically by [Logger] / [LogEntry] when
// EnableLogging is enabled. You do not need to create them manually.
type LogRecord struct {
	// ID is a client-generated UUID used for idempotency.
	ID string `json:"id"`
	// Level is the severity of the log entry.
	Level string `json:"level"`
	// Message is the human-readable log message.
	Message string `json:"message"`
	// Tags are key-value annotations (e.g. service, order_id).
	Tags []Tag `json:"tags"`
	// Timestamp is the moment the log entry was emitted (UTC).
	Timestamp time.Time `json:"timestamp"`
	// SDK identifies the SDK that produced this record.
	SDK *SDKInfo `json:"sdk,omitempty"`
	// Environment is the value of Options.Environment (e.g. "production").
	Environment string `json:"environment,omitempty"`
	// Release is the value of Options.Release (e.g. "v1.2.3").
	Release string `json:"release,omitempty"`
	// ServerName is the hostname of the machine that emitted the record.
	ServerName string `json:"server_name,omitempty"`
}
