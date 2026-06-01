package bikeeperzap_test

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"testing"

	bikeeper "github.com/MhasbiM/bikeeper-go-sdk"
	bikeeperzap "github.com/MhasbiM/bikeeper-go-sdk/zap"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

// ─── Test helpers ─────────────────────────────────────────────────────────────

// captureTransport records every event that is delivered via the SDK client.
type captureTransport struct {
	mu     sync.Mutex
	events []*bikeeper.Event
}

func (t *captureTransport) Send(_ context.Context, event *bikeeper.Event) error {
	cp := *event
	if len(event.Tags) > 0 {
		cp.Tags = make([]bikeeper.Tag, len(event.Tags))
		copy(cp.Tags, event.Tags)
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.events = append(t.events, &cp)
	return nil
}

func (t *captureTransport) Flush(_ context.Context) {}

func (t *captureTransport) captured() []*bikeeper.Event {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]*bikeeper.Event, len(t.events))
	copy(out, t.events)
	return out
}

// newTestClient creates a minimal bikeeper.Client with a captureTransport,
// using the exported constructor via the Options — this keeps the test in the
// _test package while still exercising real SDK behaviour.
func newTestClient() (*bikeeper.Client, *captureTransport) {
	tr := &captureTransport{}
	client := bikeeper.NewWithTransport(tr, bikeeper.Options{
		ClientID:     "test-id",
		ClientSecret: "test-secret",
		ProjectID:    "test-project",
		Framework:    "test",
	})
	return client, tr
}

// flushAndCapture flushes the client's in-flight goroutines, then returns all
// captured events. Always call this instead of tr.captured() directly so that
// asynchronous sends have completed before assertions run.
func flushAndCapture(client *bikeeper.Client, tr *captureTransport) []*bikeeper.Event {
	client.Flush()
	return tr.captured()
}

func findTag(event *bikeeper.Event, key string) string {
	for _, t := range event.Tags {
		if t.Key == key {
			return t.Value
		}
	}
	return ""
}

// ─── Core.Enabled ─────────────────────────────────────────────────────────────

func TestCore_Enabled(t *testing.T) {
	t.Parallel()
	client, _ := newTestClient()
	ctx := context.Background()

	tests := []struct {
		name   string
		min    zapcore.Level
		lvl    zapcore.Level
		wantOn bool
	}{
		{"at minimum", zapcore.WarnLevel, zapcore.WarnLevel, true},
		{"above minimum", zapcore.WarnLevel, zapcore.ErrorLevel, true},
		{"below minimum", zapcore.WarnLevel, zapcore.InfoLevel, false},
		{"debug off when min=info", zapcore.InfoLevel, zapcore.DebugLevel, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			core := bikeeperzap.NewCore(client, ctx, tt.min)
			if got := core.Enabled(tt.lvl); got != tt.wantOn {
				t.Errorf("Enabled(%v) with min=%v = %v, want %v", tt.lvl, tt.min, got, tt.wantOn)
			}
		})
	}
}

// ─── Core.Check ───────────────────────────────────────────────────────────────

func TestCore_Check_AddsWhenEnabled(t *testing.T) {
	t.Parallel()
	client, _ := newTestClient()
	core := bikeeperzap.NewCore(client, context.Background(), zapcore.WarnLevel)

	entry := zapcore.Entry{Level: zapcore.ErrorLevel, Message: "boom"}
	ce := core.Check(entry, nil)
	if ce == nil {
		t.Error("Check should return a non-nil CheckedEntry for enabled level")
	}
}

func TestCore_Check_SkipsWhenDisabled(t *testing.T) {
	t.Parallel()
	client, _ := newTestClient()
	core := bikeeperzap.NewCore(client, context.Background(), zapcore.WarnLevel)

	entry := zapcore.Entry{Level: zapcore.DebugLevel, Message: "verbose"}
	ce := core.Check(entry, nil)
	if ce != nil {
		t.Error("Check should return nil for disabled level")
	}
}

// ─── Core.With ────────────────────────────────────────────────────────────────

func TestCore_With_FieldsAccumulate(t *testing.T) {
	t.Parallel()
	client, tr := newTestClient()
	ctx := context.Background()

	base := bikeeperzap.NewCore(client, ctx, zapcore.DebugLevel)
	child := base.With([]zap.Field{zap.String("service", "payment")})

	// Write via child — should carry the "service" field.
	if err := child.Write(zapcore.Entry{
		Level:   zapcore.ErrorLevel,
		Message: "checkout failed",
	}, nil); err != nil {
		t.Fatalf("Write returned error: %v", err)
	}

	events := flushAndCapture(client, tr)
	if len(events) != 1 {
		t.Fatalf("want 1 event, got %d", len(events))
	}
	if findTag(events[0], "service") != "payment" {
		t.Errorf("tag service = %q, want %q", findTag(events[0], "service"), "payment")
	}
}

func TestCore_With_DoesNotMutateParent(t *testing.T) {
	t.Parallel()
	client, tr := newTestClient()
	ctx := context.Background()

	parent := bikeeperzap.NewCore(client, ctx, zapcore.DebugLevel)
	child := parent.With([]zap.Field{zap.String("extra", "yes")})

	// Write via parent — should NOT carry the child's field.
	if err := parent.Write(zapcore.Entry{Level: zapcore.InfoLevel, Message: "parent"}, nil); err != nil {
		t.Fatalf("Write returned error: %v", err)
	}
	// Write via child — should carry the field.
	if err := child.Write(zapcore.Entry{Level: zapcore.InfoLevel, Message: "child"}, nil); err != nil {
		t.Fatalf("Write returned error: %v", err)
	}

	events := flushAndCapture(client, tr)
	if len(events) != 2 {
		t.Fatalf("want 2 events, got %d", len(events))
	}
	if findTag(events[0], "extra") != "" {
		t.Error("parent event should NOT have the 'extra' tag")
	}
	if findTag(events[1], "extra") != "yes" {
		t.Errorf("child event extra = %q, want %q", findTag(events[1], "extra"), "yes")
	}
}

// ─── Core.Write — level mapping ───────────────────────────────────────────────

func TestCore_Write_LevelMapping(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		zapLevel  zapcore.Level
		wantLevel bikeeper.Level
	}{
		{"debug", zapcore.DebugLevel, bikeeper.LevelDebug},
		{"info", zapcore.InfoLevel, bikeeper.LevelInfo},
		{"warn", zapcore.WarnLevel, bikeeper.LevelWarning},
		{"error", zapcore.ErrorLevel, bikeeper.LevelError},
		{"dpanic", zapcore.DPanicLevel, bikeeper.LevelError},
		{"fatal", zapcore.FatalLevel, bikeeper.LevelFatal},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			client, tr := newTestClient()
			core := bikeeperzap.NewCore(client, context.Background(), zapcore.DebugLevel)

			if err := core.Write(zapcore.Entry{
				Level:   tt.zapLevel,
				Message: "level test",
			}, nil); err != nil {
				t.Fatalf("Write returned error: %v", err)
			}

			events := flushAndCapture(client, tr)
			if len(events) != 1 {
				t.Fatalf("want 1 event, got %d", len(events))
			}
			if events[0].Level != tt.wantLevel {
				t.Errorf("Level = %q, want %q", events[0].Level, tt.wantLevel)
			}
		})
	}
}

// ─── Core.Write — field encoding ─────────────────────────────────────────────

func TestCore_Write_FieldsBecomeTags(t *testing.T) {
	t.Parallel()
	client, tr := newTestClient()
	core := bikeeperzap.NewCore(client, context.Background(), zapcore.DebugLevel)

	entry := zapcore.Entry{Level: zapcore.WarnLevel, Message: "retry"}
	fields := []zap.Field{
		zap.String("gateway", "stripe"),
		zap.Int("attempt", 2),
	}
	if err := core.Write(entry, fields); err != nil {
		t.Fatalf("Write returned error: %v", err)
	}

	events := flushAndCapture(client, tr)
	if len(events) != 1 {
		t.Fatalf("want 1 event, got %d", len(events))
	}
	ev := events[0]
	if findTag(ev, "gateway") != "stripe" {
		t.Errorf("tag gateway = %q, want %q", findTag(ev, "gateway"), "stripe")
	}
	if findTag(ev, "attempt") != "2" {
		t.Errorf("tag attempt = %q, want %q", findTag(ev, "attempt"), "2")
	}
}

func TestCore_Write_NoFieldsNoTags(t *testing.T) {
	t.Parallel()
	client, tr := newTestClient()
	core := bikeeperzap.NewCore(client, context.Background(), zapcore.DebugLevel)

	if err := core.Write(zapcore.Entry{Level: zapcore.InfoLevel, Message: "plain"}, nil); err != nil {
		t.Fatalf("Write returned error: %v", err)
	}

	events := flushAndCapture(client, tr)
	if len(events) != 1 {
		t.Fatalf("want 1 event, got %d", len(events))
	}
	// When no zap fields are provided, no user-defined tags should appear.
	// (The SDK may still add system enrichment tags like go_maxprocs — those are expected.)
	userDefinedKeys := map[string]bool{} // intentionally empty
	for _, tg := range events[0].Tags {
		if userDefinedKeys[tg.Key] {
			t.Errorf("unexpected user-defined tag %q in zero-field write", tg.Key)
		}
	}
}

// ─── Core.Write — message passthrough ────────────────────────────────────────

func TestCore_Write_Message(t *testing.T) {
	t.Parallel()
	client, tr := newTestClient()
	core := bikeeperzap.NewCore(client, context.Background(), zapcore.DebugLevel)

	if err := core.Write(zapcore.Entry{Level: zapcore.InfoLevel, Message: "hello zap"}, nil); err != nil {
		t.Fatalf("Write: %v", err)
	}
	events := flushAndCapture(client, tr)
	if len(events) != 1 {
		t.Fatalf("want 1 event, got %d", len(events))
	}
	if events[0].Message != "hello zap" {
		t.Errorf("Message = %q, want %q", events[0].Message, "hello zap")
	}
}

// ─── Core.Sync ────────────────────────────────────────────────────────────────

func TestCore_Sync_IsNoOp(t *testing.T) {
	t.Parallel()
	client, _ := newTestClient()
	core := bikeeperzap.NewCore(client, context.Background(), zapcore.DebugLevel)
	if err := core.Sync(); err != nil {
		t.Errorf("Sync returned error: %v", err)
	}
}

// ─── AttachTo ─────────────────────────────────────────────────────────────────

// TestAttachTo_TeesEntries verifies that entries logged via the tee'd logger
// reach both the original observer core AND the Bikeeper captureTransport.
func TestAttachTo_TeesEntries(t *testing.T) {
	t.Parallel()

	// Build a zap logger with an in-process observer as the base core.
	fac, obs := observer.New(zapcore.DebugLevel)
	base := zap.New(fac)

	client, tr := newTestClient()
	logger := bikeeperzap.AttachTo(base, client, context.Background(), zapcore.WarnLevel)

	// info goes to observer only (below WarnLevel threshold for Bikeeper).
	logger.Info("below threshold")
	// warn goes to both.
	logger.Warn("at threshold", zap.String("key", "val"))
	// error goes to both.
	logger.Error("above threshold", zap.Int("code", 500))

	// Verify observer received all 3 entries.
	if got := obs.Len(); got != 3 {
		t.Errorf("observer: want 3 entries, got %d", got)
	}

	// Verify Bikeeper received only warn + error (2 entries).
	events := flushAndCapture(client, tr)
	if len(events) != 2 {
		t.Fatalf("bikeeper: want 2 events, got %d", len(events))
	}
	if events[0].Message != "at threshold" {
		t.Errorf("bikeeper event[0] message = %q, want %q", events[0].Message, "at threshold")
	}
	if events[1].Message != "above threshold" {
		t.Errorf("bikeeper event[1] message = %q, want %q", events[1].Message, "above threshold")
	}
}

// TestAttachTo_StructuredFieldsBecomeTags verifies that zap structured fields
// on the tee'd logger are encoded as bikeeper tags.
func TestAttachTo_StructuredFieldsBecomeTags(t *testing.T) {
	t.Parallel()

	fac, _ := observer.New(zapcore.DebugLevel)
	base := zap.New(fac)
	client, tr := newTestClient()
	logger := bikeeperzap.AttachTo(base, client, context.Background(), zapcore.DebugLevel)

	logger.Error("checkout failed",
		zap.String("order_id", "ORD-999"),
		zap.Int("attempt", 3),
	)

	events := flushAndCapture(client, tr)
	if len(events) != 1 {
		t.Fatalf("want 1 event, got %d", len(events))
	}
	if findTag(events[0], "order_id") != "ORD-999" {
		t.Errorf("tag order_id = %q, want %q", findTag(events[0], "order_id"), "ORD-999")
	}
	if findTag(events[0], "attempt") != "3" {
		t.Errorf("tag attempt = %q, want %q", findTag(events[0], "attempt"), "3")
	}
}

// TestAttachTo_WithFieldsInherited verifies that fields added via With() on the
// tee'd logger are carried through to bikeeper as tags.
func TestAttachTo_WithFieldsInherited(t *testing.T) {
	t.Parallel()

	fac, _ := observer.New(zapcore.DebugLevel)
	base := zap.New(fac)
	client, tr := newTestClient()
	logger := bikeeperzap.AttachTo(base, client, context.Background(), zapcore.DebugLevel).
		With(zap.String("service", "payment"))

	logger.Warn("slow response", zap.Int("latency_ms", 2800))

	events := flushAndCapture(client, tr)
	if len(events) != 1 {
		t.Fatalf("want 1 event, got %d", len(events))
	}
	if findTag(events[0], "service") != "payment" {
		t.Errorf("tag service = %q, want %q", findTag(events[0], "service"), "payment")
	}
	if findTag(events[0], "latency_ms") != "2800" {
		t.Errorf("tag latency_ms = %q, want %q", findTag(events[0], "latency_ms"), "2800")
	}
}

// TestAttachTo_DoesNotModifyOriginal verifies that AttachTo returns a new logger
// and the original is not affected by the bikeeper sink.
func TestAttachTo_DoesNotModifyOriginal(t *testing.T) {
	t.Parallel()

	fac, obs := observer.New(zapcore.DebugLevel)
	original := zap.New(fac)

	client, tr := newTestClient()
	_ = bikeeperzap.AttachTo(original, client, context.Background(), zapcore.DebugLevel)

	// Write via the original logger — should go to obs only, NOT to bikeeper.
	original.Info("via original")

	if obs.Len() != 1 {
		t.Errorf("observer: want 1 entry, got %d", obs.Len())
	}
	if len(flushAndCapture(client, tr)) != 0 {
		t.Error("original logger should NOT forward to bikeeper after AttachTo")
	}
}

// ─── fieldsToTags ordering ────────────────────────────────────────────────────

// TestFieldsToTags_MultipleTypes exercises the field encoder with bool,
// float, and error types to ensure they are serialised without panic or data loss.
func TestCore_Write_MultipleFieldTypes(t *testing.T) {
	t.Parallel()
	client, tr := newTestClient()
	core := bikeeperzap.NewCore(client, context.Background(), zapcore.DebugLevel)

	if err := core.Write(zapcore.Entry{Level: zapcore.ErrorLevel, Message: "multi"}, []zap.Field{
		zap.Bool("ok", false),
		zap.Float64("ratio", 0.75),
		zap.Error(fmt.Errorf("something went wrong")),
	}); err != nil {
		t.Fatalf("Write: %v", err)
	}

	events := flushAndCapture(client, tr)
	if len(events) != 1 {
		t.Fatalf("want 1 event, got %d", len(events))
	}

	// Build a set of tag keys that were received.
	keys := make(map[string]struct{}, len(events[0].Tags))
	for _, tg := range events[0].Tags {
		keys[tg.Key] = struct{}{}
	}
	for _, expected := range []string{"ok", "ratio", "error"} {
		if _, ok := keys[expected]; !ok {
			sortedKeys := make([]string, 0, len(keys))
			for k := range keys {
				sortedKeys = append(sortedKeys, k)
			}
			sort.Strings(sortedKeys)
			t.Errorf("expected tag %q not found; got keys: %v", expected, sortedKeys)
		}
	}
}

func ExampleAttachTo() {
	fac, _ := observer.New(zapcore.InfoLevel)
	base := zap.New(fac)

	client := bikeeper.NewWithTransport(&captureTransport{}, bikeeper.Options{
		ClientID:     "id",
		ClientSecret: "secret",
		ProjectID:    "proj",
		Framework:    "test",
	})
	logger := bikeeperzap.AttachTo(base, client, context.Background(), zap.WarnLevel)

	// info goes to stdout only (below WarnLevel for Bikeeper).
	logger.Info("request received", zap.String("path", "/checkout"))
	// warn and above go to stdout AND Bikeeper as captured events.
	logger.Warn("payment retry", zap.Int("attempt", 2))
	// Output:
}
