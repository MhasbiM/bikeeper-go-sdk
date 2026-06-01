package bikeeper

import (
	"time"

	"github.com/google/uuid"
)

// Tag is a key-value pair attached to an event.
type Tag struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// User identifies the end-user who was active when the event was captured.
// Set via Scope.SetUser or Hub.SetUser; fields are merged into the event
// automatically so the Bikeeper dashboard can show per-user error stats.
type User struct {
	ID        string `json:"id,omitempty"`
	Email     string `json:"email,omitempty"`
	Name      string `json:"name,omitempty"`
	IPAddress string `json:"ip_address,omitempty"`
}

// ─── Rich context types ───────────────────────────────────────────────────────

// HTTPRequest captures the incoming HTTP request at the time of the event.
// Sensitive headers (Authorization, Cookie, Set-Cookie, X-Bikeeper-Client-Secret)
// are stripped by the framework middleware before transmission.
type HTTPRequest struct {
	Method      string            `json:"method,omitempty"`
	URL         string            `json:"url,omitempty"`
	QueryString string            `json:"query_string,omitempty"`
	Data        string            `json:"data,omitempty"`
	Headers     map[string]string `json:"headers,omitempty"`
	Env         map[string]string `json:"env,omitempty"` // e.g. REMOTE_ADDR, SERVER_NAME
}

// BrowserInfo holds browser name and version (client-reported via User-Agent).
type BrowserInfo struct {
	Name    string `json:"name,omitempty"`
	Version string `json:"version,omitempty"`
}

// RuntimeInfo holds server-side runtime name and version (e.g. "go", "1.22.3").
// Populated automatically by the SDK from runtime.Version().
type RuntimeInfo struct {
	Name    string `json:"name,omitempty"`
	Version string `json:"version,omitempty"`
}

// OSInfo holds operating system details.
type OSInfo struct {
	Name          string `json:"name,omitempty"`
	Version       string `json:"version,omitempty"`
	Build         string `json:"build,omitempty"`
	KernelVersion string `json:"kernel_version,omitempty"`
}

// DeviceInfo holds hardware / platform info.
type DeviceInfo struct {
	Name   string `json:"name,omitempty"`
	Family string `json:"family,omitempty"`
	Model  string `json:"model,omitempty"`
	Arch   string `json:"arch,omitempty"`
}

// GeoInfo holds geographic info inferred from the client IP address.
// Populated by the server after ingest (not sent by the SDK).
type GeoInfo struct {
	City        string `json:"city,omitempty"`
	CountryCode string `json:"country_code,omitempty"`
	Region      string `json:"region,omitempty"`
}

// TraceInfo holds distributed trace IDs for correlation with APM tools.
type TraceInfo struct {
	TraceID      string `json:"trace_id,omitempty"`
	SpanID       string `json:"span_id,omitempty"`
	ParentSpanID string `json:"parent_span_id,omitempty"`
	Op           string `json:"op,omitempty"`
	Description  string `json:"description,omitempty"`
}

// Contexts bundles all rich context dimensions attached to an event.
type Contexts struct {
	Browser  *BrowserInfo `json:"browser,omitempty"`
	Runtime  *RuntimeInfo `json:"runtime,omitempty"`
	OS       *OSInfo      `json:"os,omitempty"`
	ClientOS *OSInfo      `json:"client_os,omitempty"`
	Device   *DeviceInfo  `json:"device,omitempty"`
	Geo      *GeoInfo     `json:"geo,omitempty"`
	Trace    *TraceInfo   `json:"trace,omitempty"`
}

// Package describes a dependency installed in the SDK host application.
type Package struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

// SDKInfo identifies the SDK that captured the event.
type SDKInfo struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

// ─── Breadcrumb ──────────────────────────────────────────────────────────────

// Breadcrumb records a single step in the execution trail, from request entry
// to the final error. Breadcrumbs are collected via Hub.AddBreadcrumb and
// attached to the event automatically when CaptureException / CaptureMessage
// is called.
type Breadcrumb struct {
	Timestamp time.Time      `json:"timestamp"`
	Type      string         `json:"type,omitempty"`     // "default", "http", "query", "error"
	Category  string         `json:"category,omitempty"` // e.g. "utils", "usecase", "repo"
	Message   string         `json:"message"`
	Level     Level          `json:"level"`
	Data      map[string]any `json:"data,omitempty"`
}

// ─── Event ───────────────────────────────────────────────────────────────────

// Event is the payload sent to the Bikeeper server.
type Event struct {
	ID        string    `json:"id"`
	Level     Level     `json:"level"`
	Message   string    `json:"message"`
	Tags      []Tag     `json:"tags,omitempty"`
	Timestamp time.Time `json:"timestamp"`
	// Enriched fields
	URL         string       `json:"url,omitempty"`
	TraceID     string       `json:"trace_id,omitempty"`
	HTTPRequest *HTTPRequest `json:"http_request,omitempty"`
	Contexts    *Contexts    `json:"contexts,omitempty"`
	Packages    []Package    `json:"packages,omitempty"`
	SDK         *SDKInfo     `json:"sdk,omitempty"`
	// Exception holds the captured error value, its type, and the full stack trace.
	// Populated automatically by CaptureException and Hub.CaptureException.
	Exception *ExceptionValue `json:"exception,omitempty"`
	// Fingerprint is the grouping key(s) for this event.
	Fingerprint []string `json:"fingerprint,omitempty"`
	// Breadcrumbs is the ordered execution trail collected via Hub.AddBreadcrumb.
	// Items are in chronological order (oldest first).
	Breadcrumbs []Breadcrumb `json:"breadcrumbs,omitempty"`
	// User identifies the authenticated user at the time of the event.
	// Populated from Scope.SetUser via hub.SetUser / scope.SetUser.
	User *User `json:"user,omitempty"` // Extra holds named free-form context blocks set via Scope.SetContext / Hub.SetExtra.
	// Each key maps to an arbitrary key-value map (e.g. "device" → {"model": "iPhone 15"}).
	Extra map[string]ExtraContext `json:"extra,omitempty"`
}

// NewEvent constructs a new Event with a generated ID, auto-generated trace ID,
// and current timestamp.
func NewEvent(level Level, message string, tags ...Tag) *Event {
	return &Event{
		ID:        uuid.New().String(),
		TraceID:   uuid.New().String(),
		Level:     level,
		Message:   message,
		Tags:      tags,
		Timestamp: time.Now().UTC(),
	}
}
