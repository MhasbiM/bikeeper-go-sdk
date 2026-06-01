package bikeeper

import "time"

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
	// An event send will fail with an error if no framework middleware has been
	// registered by the time the first event is captured.
	Framework string

	// ProjectID is the project's internal UUID shown on the Bikeeper dashboard.
	// Required — the backend validates that the credentials belong to exactly
	// this project, preventing cross-project credential reuse.
	ProjectID string

	// OnError is called when an async event send fails (e.g. network error,
	// auth failure, server rejection). If nil, failures are silently discarded.
	OnError func(err error)
}
