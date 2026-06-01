package bikeeper

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// ─── Test helpers ─────────────────────────────────────────────────────────────

// captureTransport is a fake [Transport] that records every event it receives.
// It is safe for concurrent use.
type captureTransport struct {
	mu     sync.Mutex
	events []*Event
}

func (t *captureTransport) Send(_ context.Context, event *Event) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	// Deep-copy the tags slice so tests that mutate the entry after Emit don't
	// race with the transport's snapshot.
	evCopy := *event
	if len(event.Tags) > 0 {
		evCopy.Tags = make([]Tag, len(event.Tags))
		copy(evCopy.Tags, event.Tags)
	}
	t.events = append(t.events, &evCopy)
	return nil
}

func (t *captureTransport) Flush(_ context.Context) {}

// captured returns a snapshot of all events recorded so far.
func (t *captureTransport) captured() []*Event {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]*Event, len(t.events))
	copy(out, t.events)
	return out
}

func (t *captureTransport) reset() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.events = t.events[:0]
}

// newTestClient returns a [Client] wired to a [captureTransport].
// Events are still delivered by background goroutines (the real captureAsync
// path) — call client.Flush() before asserting on tr.captured() so that all
// in-flight goroutines have completed.
func newTestClient() (*Client, *captureTransport) {
	tr := &captureTransport{}
	c := &Client{
		opts: Options{
			ClientID:     "test-client-id",
			ClientSecret: "test-client-secret",
			ProjectID:    "test-project-id",
			Framework:    "test",
			Timeout:      5 * time.Second,
			FlushTimeout: 2 * time.Second,
		},
		transport: tr,
	}
	return c, tr
}

// flush waits for all in-flight background goroutines to finish and returns
// a snapshot of every event the transport received. Use this instead of
// tr.captured() so assertions never race with captureAsync goroutines.
func flush(c *Client, tr *captureTransport) []*Event {
	c.Flush()
	return tr.captured()
}

// findTag looks up a tag value by key from the event, returning "" if absent.
func findTag(event *Event, key string) string {
	for _, t := range event.Tags {
		if t.Key == key {
			return t.Value
		}
	}
	return ""
}

// ─── Logger tests ─────────────────────────────────────────────────────────────

func TestLogger_NewLogger(t *testing.T) {
	t.Parallel()
	c, _ := newTestClient()
	logger := c.NewLogger(context.Background())
	if logger == nil {
		t.Fatal("NewLogger returned nil")
	}
	if logger.client != c {
		t.Error("NewLogger did not bind the client")
	}
}

func TestLogger_LevelMethods(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		call func(l *Logger) *LogEntry
		want Level
	}{
		{"debug", (*Logger).Debug, LevelDebug},
		{"info", (*Logger).Info, LevelInfo},
		{"warn", (*Logger).Warn, LevelWarning},
		{"error", (*Logger).Error, LevelError},
		{"fatal", (*Logger).Fatal, LevelFatal},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			c, tr := newTestClient()
			logger := c.NewLogger(context.Background())
			tt.call(logger).Emit("hello level test")

			events := flush(c, tr)
			if len(events) != 1 {
				t.Fatalf("want 1 event, got %d", len(events))
			}
			if got := events[0].Level; got != tt.want {
				t.Errorf("Level = %q, want %q", got, tt.want)
			}
			if events[0].Message != "hello level test" {
				t.Errorf("Message = %q, want %q", events[0].Message, "hello level test")
			}
		})
	}
}

func TestLogger_WithTag_DoesNotMutateParent(t *testing.T) {
	t.Parallel()
	c, tr := newTestClient()
	base := c.NewLogger(context.Background())

	// Derive a child logger with one extra tag.
	child := base.WithTag("service", "payment")

	// Emit from base and assert no service tag.
	base.Info().Emit("from base")
	baseEvents := flush(c, tr)
	if len(baseEvents) != 1 {
		t.Fatalf("want 1 event from base, got %d", len(baseEvents))
	}
	if findTag(baseEvents[0], "service") != "" {
		t.Error("base logger should NOT have the service tag")
	}

	// Emit from child and assert service tag present.
	tr.reset()
	child.Info().Emit("from child")
	childEvents := flush(c, tr)
	if len(childEvents) != 1 {
		t.Fatalf("want 1 event from child, got %d", len(childEvents))
	}
	if findTag(childEvents[0], "service") != "payment" {
		t.Errorf("child logger service tag = %q, want %q", findTag(childEvents[0], "service"), "payment")
	}
}

func TestLogger_WithTag_Chaining(t *testing.T) {
	t.Parallel()
	c, tr := newTestClient()
	logger := c.NewLogger(context.Background()).
		WithTag("team", "backend").
		WithTag("service", "auth")

	logger.Info().Emit("chained tags")

	events := flush(c, tr)
	if len(events) != 1 {
		t.Fatalf("want 1 event, got %d", len(events))
	}
	if findTag(events[0], "team") != "backend" {
		t.Errorf("tag team = %q, want %q", findTag(events[0], "team"), "backend")
	}
	if findTag(events[0], "service") != "auth" {
		t.Errorf("tag service = %q, want %q", findTag(events[0], "service"), "auth")
	}
}

// ─── LogEntry tests ───────────────────────────────────────────────────────────

func TestLogEntry_Emit_Sprint(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		args []any
		want string
	}{
		{"single string", []any{"hello"}, "hello"},
		{"two strings", []any{"hello ", "world"}, "hello world"},
		{"int", []any{42}, "42"},
		{"mixed", []any{"count:", 5}, "count:5"},
		{"empty", []any{}, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			c, tr := newTestClient()
			c.NewLogger(context.Background()).Info().Emit(tt.args...)
			events := flush(c, tr)
			if len(events) != 1 {
				t.Fatalf("want 1 event, got %d", len(events))
			}
			if events[0].Message != tt.want {
				t.Errorf("Emit(%v) message = %q, want %q", tt.args, events[0].Message, tt.want)
			}
		})
	}
}

func TestLogEntry_Emitf(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		format string
		args   []any
		want   string
	}{
		{"no args", "static message", nil, "static message"},
		{"one arg", "user %q logged in", []any{"alice"}, `user "alice" logged in`},
		{"multiple args", "order %d cost %.2f", []any{7, 19.99}, "order 7 cost 19.99"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			c, tr := newTestClient()
			c.NewLogger(context.Background()).Info().Emitf(tt.format, tt.args...)
			events := flush(c, tr)
			if len(events) != 1 {
				t.Fatalf("want 1 event, got %d", len(events))
			}
			if events[0].Message != tt.want {
				t.Errorf("Emitf(%q, ...) message = %q, want %q", tt.format, events[0].Message, tt.want)
			}
		})
	}
}

func TestLogEntry_WithTag(t *testing.T) {
	t.Parallel()
	c, tr := newTestClient()
	logger := c.NewLogger(context.Background())

	logger.Error().
		WithTag("order_id", "ORD-42").
		WithTag("attempt", "3").
		Emit("payment failed")

	events := flush(c, tr)
	if len(events) != 1 {
		t.Fatalf("want 1 event, got %d", len(events))
	}
	if findTag(events[0], "order_id") != "ORD-42" {
		t.Errorf("tag order_id = %q, want %q", findTag(events[0], "order_id"), "ORD-42")
	}
	if findTag(events[0], "attempt") != "3" {
		t.Errorf("tag attempt = %q, want %q", findTag(events[0], "attempt"), "3")
	}
}

func TestLogEntry_WithTag_DoesNotLeakToLogger(t *testing.T) {
	t.Parallel()
	c, tr := newTestClient()
	logger := c.NewLogger(context.Background())

	// Add a tag to one entry only.
	logger.Info().WithTag("leak", "yes").Emit("first")
	flush(c, tr) // drain the first goroutine

	// Second emit should NOT carry the tag from the previous entry.
	tr.reset()
	logger.Info().Emit("second")
	secondEvents := flush(c, tr)
	if len(secondEvents) != 1 {
		t.Fatalf("want 1 event for second emit, got %d", len(secondEvents))
	}
	if findTag(secondEvents[0], "leak") != "" {
		t.Error("tag 'leak' leaked into the second entry")
	}
}

func TestLogEntry_WithCtx(t *testing.T) {
	t.Parallel()
	type ctxKey struct{}
	base := context.Background()
	child := context.WithValue(base, ctxKey{}, "payload")

	c, tr := newTestClient()
	logger := c.NewLogger(base)

	// Override context on this entry only.
	logger.Info().WithCtx(child).Emit("ctx override")

	// The logger's own context must be unchanged.
	logger.Info().Emit("no override")

	events := flush(c, tr)
	if len(events) != 2 {
		t.Fatalf("want 2 events, got %d", len(events))
	}
	// We can't inspect the context stored inside the event directly, but we can
	// assert that both emissions were delivered — Flush() drains all background
	// goroutines launched by captureAsync before we read tr.captured().
	_ = events
}

// ─── Logger + tag inheritance integration test ────────────────────────────────

func TestLogger_TagInheritance_Integration(t *testing.T) {
	t.Parallel()
	c, tr := newTestClient()

	// Base logger with shared tags.
	base := c.NewLogger(context.Background()).
		WithTag("env", "test").
		WithTag("service", "checkout")

	assertSharedTags := func(ev *Event, label string) {
		if findTag(ev, "env") != "test" {
			t.Errorf("%s: env tag = %q, want %q", label, findTag(ev, "env"), "test")
		}
		if findTag(ev, "service") != "checkout" {
			t.Errorf("%s: service tag = %q, want %q", label, findTag(ev, "service"), "checkout")
		}
	}

	// Emit and assert each event individually to avoid goroutine ordering issues.
	base.Info().Emit("started")
	evs := flush(c, tr)
	if len(evs) != 1 {
		t.Fatalf("want 1 event after 'started', got %d", len(evs))
	}
	assertSharedTags(evs[0], "started")
	if findTag(evs[0], "cart_id") != "" {
		t.Error("'started' event should not have cart_id")
	}

	tr.reset()
	base.Warn().WithTag("cart_id", "C1").Emit("near limit")
	evs = flush(c, tr)
	if len(evs) != 1 {
		t.Fatalf("want 1 event after 'near limit', got %d", len(evs))
	}
	assertSharedTags(evs[0], "near limit")
	if findTag(evs[0], "cart_id") != "C1" {
		t.Errorf("'near limit' cart_id = %q, want %q", findTag(evs[0], "cart_id"), "C1")
	}

	tr.reset()
	base.Error().Emitf("failed after %d attempts", 3)
	evs = flush(c, tr)
	if len(evs) != 1 {
		t.Fatalf("want 1 event after 'failed...', got %d", len(evs))
	}
	assertSharedTags(evs[0], "failed")
	if findTag(evs[0], "cart_id") != "" {
		t.Error("'failed' event should not have cart_id")
	}
}

func TestLogger_Levels_MessageFmt(t *testing.T) {
	t.Parallel()
	c, tr := newTestClient()
	logger := c.NewLogger(context.Background())

	cases := []struct {
		level Level
		msg   string
	}{
		{LevelDebug, "debug event"},
		{LevelInfo, "info event"},
		{LevelWarning, "warning event"},
		{LevelError, "error event"},
		{LevelFatal, "fatal event"},
	}

	for _, tc := range cases {
		tr.reset()
		switch tc.level {
		case LevelDebug:
			logger.Debug().Emitf("%s", tc.msg)
		case LevelInfo:
			logger.Info().Emitf("%s", tc.msg)
		case LevelWarning:
			logger.Warn().Emitf("%s", tc.msg)
		case LevelError:
			logger.Error().Emitf("%s", tc.msg)
		case LevelFatal:
			logger.Fatal().Emitf("%s", tc.msg)
		}
		events := flush(c, tr)
		if len(events) != 1 {
			t.Fatalf("[%s] want 1 event, got %d", tc.level, len(events))
		}
		if events[0].Level != tc.level {
			t.Errorf("[%s] Level = %q, want %q", tc.level, events[0].Level, tc.level)
		}
		if events[0].Message != tc.msg {
			t.Errorf("[%s] Message = %q, want %q", tc.level, events[0].Message, tc.msg)
		}
	}
}

func ExampleClient_NewLogger() {
	client := &Client{
		opts:      Options{ClientID: "id", ClientSecret: "secret", ProjectID: "proj"},
		transport: &captureTransport{},
	}
	logger := client.NewLogger(context.Background())

	logger.Info().Emit("server started")
	logger.Error().WithTag("order_id", "ORD-001").Emitf("payment failed: %v", fmt.Errorf("timeout"))
	// Output:
}

// ─── EnableLogging tests ──────────────────────────────────────────────────────

// logCaptureTransport records both events (via Send) and log records (via SendLog).
// It satisfies the Transport interface and the internal logSender interface so
// that captureLogAsync can dispatch to SendLog when EnableLogging is true.
type logCaptureTransport struct {
	mu     sync.Mutex
	events []*Event
	logs   []*LogRecord
}

func (t *logCaptureTransport) Send(_ context.Context, event *Event) error {
	ev := *event
	t.mu.Lock()
	defer t.mu.Unlock()
	t.events = append(t.events, &ev)
	return nil
}

func (t *logCaptureTransport) Flush(_ context.Context) {}

func (t *logCaptureTransport) SendLog(_ context.Context, rec *LogRecord) error {
	r := *rec
	if len(rec.Tags) > 0 {
		r.Tags = make([]Tag, len(rec.Tags))
		copy(r.Tags, rec.Tags)
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.logs = append(t.logs, &r)
	return nil
}

func (t *logCaptureTransport) capturedLogs() []*LogRecord {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]*LogRecord, len(t.logs))
	copy(out, t.logs)
	return out
}

func (t *logCaptureTransport) capturedEvents() []*Event {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]*Event, len(t.events))
	copy(out, t.events)
	return out
}

// newLogTestClient returns a Client with EnableLogging: true and a logCaptureTransport.
func newLogTestClient() (*Client, *logCaptureTransport) {
	tr := &logCaptureTransport{}
	c := &Client{
		opts: Options{
			ClientID:      "test-client-id",
			ClientSecret:  "test-client-secret",
			ProjectID:     "test-project-id",
			Framework:     "test",
			Timeout:       5 * time.Second,
			FlushTimeout:  2 * time.Second,
			EnableLogging: true,
		},
		transport: tr,
	}
	return c, tr
}

func TestEnableLogging_EmitRoutesToLogRecord(t *testing.T) {
	t.Parallel()
	c, tr := newLogTestClient()
	c.NewLogger(context.Background()).Info().Emit("hello log")
	c.Flush()

	logs := tr.capturedLogs()
	if len(logs) != 1 {
		t.Fatalf("want 1 log record, got %d", len(logs))
	}
	if logs[0].Level != string(LevelInfo) {
		t.Errorf("Level = %q, want %q", logs[0].Level, string(LevelInfo))
	}
	if logs[0].Message != "hello log" {
		t.Errorf("Message = %q, want %q", logs[0].Message, "hello log")
	}
	// Nothing should go to the event endpoint.
	if events := tr.capturedEvents(); len(events) != 0 {
		t.Errorf("want 0 events, got %d — log record must not route to events", len(events))
	}
}

func TestEnableLogging_EmitfRoutesToLogRecord(t *testing.T) {
	t.Parallel()
	c, tr := newLogTestClient()
	c.NewLogger(context.Background()).Error().Emitf("request %d failed", 42)
	c.Flush()

	logs := tr.capturedLogs()
	if len(logs) != 1 {
		t.Fatalf("want 1 log record, got %d", len(logs))
	}
	if logs[0].Message != "request 42 failed" {
		t.Errorf("Message = %q, want %q", logs[0].Message, "request 42 failed")
	}
}

func TestEnableLogging_FalseUsesEventPath(t *testing.T) {
	t.Parallel()
	// Use a logCaptureTransport but with EnableLogging: false — Emit should
	// route to CaptureMessage → Send (event path), not SendLog.
	tr := &logCaptureTransport{}
	c := &Client{
		opts: Options{
			ClientID:      "test-client-id",
			ClientSecret:  "test-client-secret",
			ProjectID:     "test-project-id",
			Framework:     "test",
			Timeout:       5 * time.Second,
			FlushTimeout:  2 * time.Second,
			EnableLogging: false, // explicit: old path
		},
		transport: tr,
	}
	c.NewLogger(context.Background()).Info().Emit("event path")
	c.Flush()

	if logs := tr.capturedLogs(); len(logs) != 0 {
		t.Errorf("want 0 log records when EnableLogging=false, got %d", len(logs))
	}
	if events := tr.capturedEvents(); len(events) != 1 {
		t.Errorf("want 1 event when EnableLogging=false, got %d", len(events))
	}
}

func TestEnableLogging_LogRecord_HasTags(t *testing.T) {
	t.Parallel()
	c, tr := newLogTestClient()
	c.NewLogger(context.Background()).
		Warn().
		WithTag("order_id", "ORD-001").
		WithTag("service", "checkout").
		Emit("near capacity")
	c.Flush()

	logs := tr.capturedLogs()
	if len(logs) != 1 {
		t.Fatalf("want 1 log record, got %d", len(logs))
	}
	rec := logs[0]
	findLogTag := func(key string) string {
		for _, tag := range rec.Tags {
			if tag.Key == key {
				return tag.Value
			}
		}
		return ""
	}
	if findLogTag("order_id") != "ORD-001" {
		t.Errorf("order_id tag = %q, want %q", findLogTag("order_id"), "ORD-001")
	}
	if findLogTag("service") != "checkout" {
		t.Errorf("service tag = %q, want %q", findLogTag("service"), "checkout")
	}
}

func TestEnableLogging_AllLevels(t *testing.T) {
	t.Parallel()
	levels := []struct {
		call func(l *Logger) *LogEntry
		want Level
	}{
		{(*Logger).Debug, LevelDebug},
		{(*Logger).Info, LevelInfo},
		{(*Logger).Warn, LevelWarning},
		{(*Logger).Error, LevelError},
		{(*Logger).Fatal, LevelFatal},
	}
	for _, tt := range levels {
		tt := tt
		t.Run(string(tt.want), func(t *testing.T) {
			t.Parallel()
			c, tr := newLogTestClient()
			tt.call(c.NewLogger(context.Background())).Emit("level test")
			c.Flush()

			logs := tr.capturedLogs()
			if len(logs) != 1 {
				t.Fatalf("[%s] want 1 log record, got %d", tt.want, len(logs))
			}
			if logs[0].Level != string(tt.want) {
				t.Errorf("[%s] Level = %q, want %q", tt.want, logs[0].Level, string(tt.want))
			}
		})
	}
}

func TestEnableLogging_LogRecord_HasSDKInfo(t *testing.T) {
	t.Parallel()
	c, tr := newLogTestClient()
	c.NewLogger(context.Background()).Info().Emit("sdk test")
	c.Flush()

	logs := tr.capturedLogs()
	if len(logs) != 1 {
		t.Fatalf("want 1 log record, got %d", len(logs))
	}
	if logs[0].SDK == nil {
		t.Fatal("LogRecord.SDK must not be nil")
	}
	if logs[0].SDK.Name == "" {
		t.Error("LogRecord.SDK.Name must not be empty")
	}
	if logs[0].ID == "" {
		t.Error("LogRecord.ID must not be empty")
	}
	if logs[0].Timestamp.IsZero() {
		t.Error("LogRecord.Timestamp must not be zero")
	}
}
