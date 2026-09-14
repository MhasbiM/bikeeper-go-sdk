package bikeeper_test

import (
	"context"
	"errors"
	"runtime"
	"slices"
	"sync"
	"testing"
	"time"

	bikeeper "github.com/MhasbiM/bikeeper-go-sdk"
)

// TestClientClose_NoGoroutineLeak verifies that Client.Close() cleans up all
// background goroutines started by the SDK, leaving the goroutine count at or
// below the baseline.
//
// We allow a small delta (+3) to tolerate goroutines spawned by the Go testing
// runtime itself (e.g. finaliser goroutine, signal handling goroutine).
func TestClientClose_NoGoroutineLeak(t *testing.T) {
	t.Parallel()

	// Let the runtime settle before sampling the baseline.
	time.Sleep(50 * time.Millisecond)
	before := runtime.NumGoroutine()

	client := bikeeper.New(bikeeper.Options{
		ProjectID:    "test-project",
		ClientID:     "test-client",
		ClientSecret: "test-secret",
		Endpoint:     "http://127.0.0.1:1", // unreachable — ensures no sends succeed
	})

	// Close should flush and release any background goroutines.
	client.Close()

	// Give any lingering goroutines a moment to exit.
	time.Sleep(100 * time.Millisecond)
	after := runtime.NumGoroutine()

	// Allow a delta of 3 to absorb testing-runtime variance.
	const delta = 3
	if after > before+delta {
		t.Errorf("possible goroutine leak: goroutines before=%d after=%d (delta allowed=%d)",
			before, after, delta)
	}
}

// TestClientClose_FlushesInFlightEvents verifies that Close() waits for
// in-flight CaptureEventAsync goroutines to complete (or timeout) before returning.
// This is a behaviour test — we just ensure Close() does not panic or deadlock.
func TestClientClose_FlushesInFlightEvents(t *testing.T) {
	t.Parallel()

	client := bikeeper.New(bikeeper.Options{
		ProjectID:    "test-project",
		ClientID:     "test-client",
		ClientSecret: "test-secret",
		Endpoint:     "http://127.0.0.1:1",  // unreachable — send will timeout
		Timeout:      10 * time.Millisecond, // short timeout so test stays fast
		FlushTimeout: 200 * time.Millisecond,
	})

	hub := bikeeper.NewHub(client)

	// Fire several events asynchronously then wait for Close to flush them.
	for range 5 {
		hub.CaptureMessage("test message", bikeeper.LevelInfo)
	}
	_ = hub // ensure hub is not optimised away before the goroutines land

	// Close must return within a reasonable time (FlushTimeout + margin).
	done := make(chan struct{})
	go func() {
		client.Close()
		close(done)
	}()

	select {
	case <-done:
		// pass
	case <-time.After(2 * time.Second):
		t.Error("Client.Close() did not return within 2s — possible deadlock or hang")
	}
}

// ─── BeforeSend ──────────────────────────────────────────────────────────────

// fakeTransport records events instead of sending them, and optionally blocks
// in Send so a test can fill the send queue.
type fakeTransport struct {
	mu     sync.Mutex
	events []*bikeeper.Event
	block  chan struct{} // when non-nil, Send waits on it before returning
}

func (t *fakeTransport) Send(_ context.Context, event *bikeeper.Event) error {
	if t.block != nil {
		<-t.block
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.events = append(t.events, event)
	return nil
}

func (t *fakeTransport) Flush(_ context.Context) {}

func (t *fakeTransport) captured() []*bikeeper.Event {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]*bikeeper.Event, len(t.events))
	copy(out, t.events)
	return out
}

func newFakeClient(t *testing.T, tr *fakeTransport, opts bikeeper.Options) *bikeeper.Client {
	t.Helper()
	opts.ProjectID = "test-project"
	opts.ClientID = "test-client"
	opts.ClientSecret = "test-secret"
	return bikeeper.NewWithTransport(tr, opts)
}

func TestBeforeSend_CanScrubAndDrop(t *testing.T) {
	t.Parallel()
	tr := &fakeTransport{}
	client := newFakeClient(t, tr, bikeeper.Options{
		BeforeSend: func(ev *bikeeper.Event) *bikeeper.Event {
			if ev.Level == bikeeper.LevelInfo {
				return nil // drop routine noise entirely
			}
			ev.Tags = slices.DeleteFunc(ev.Tags, func(tag bikeeper.Tag) bool {
				return tag.Key == "args"
			})
			return ev
		},
	})

	client.CaptureMessage(context.Background(), "routine", bikeeper.LevelInfo)
	client.CaptureMessage(context.Background(), "db query failed", bikeeper.LevelError,
		bikeeper.Tag{Key: "args", Value: `["0812xxxxxxx"]`},
		bikeeper.Tag{Key: "sql", Value: "SELECT 1"},
	)
	client.Flush()

	events := tr.captured()
	if len(events) != 1 {
		t.Fatalf("want 1 event delivered (the info one dropped), got %d", len(events))
	}
	for _, tag := range events[0].Tags {
		if tag.Key == "args" {
			t.Errorf("args tag should have been scrubbed, got %q", tag.Value)
		}
	}
}

func TestBeforeSend_PanicDropsEventWithoutCrashing(t *testing.T) {
	t.Parallel()
	tr := &fakeTransport{}
	var reported error
	client := newFakeClient(t, tr, bikeeper.Options{
		BeforeSend: func(*bikeeper.Event) *bikeeper.Event { panic("boom") },
		OnError:    func(err error) { reported = err },
	})

	client.CaptureMessage(context.Background(), "anything", bikeeper.LevelError)
	client.Flush()

	if got := len(tr.captured()); got != 0 {
		t.Errorf("want 0 events delivered, got %d", got)
	}
	if reported == nil {
		t.Error("a panicking BeforeSend should be reported through OnError")
	}
}

// ─── Send queue ──────────────────────────────────────────────────────────────

func TestSendQueue_DropsWhenFull(t *testing.T) {
	t.Parallel()
	block := make(chan struct{})
	tr := &fakeTransport{block: block}
	client := newFakeClient(t, tr, bikeeper.Options{
		MaxQueueSize:    2,
		SendConcurrency: 1,
		Timeout:         time.Second,
		FlushTimeout:    2 * time.Second,
	})

	// One send is stuck in the transport, two fit the queue, the rest are lost.
	for range 20 {
		client.CaptureMessage(context.Background(), "flood", bikeeper.LevelError)
	}
	if client.Dropped() == 0 {
		t.Fatal("want events dropped once the queue filled up, got none")
	}

	close(block)
	client.Flush()

	if delivered := len(tr.captured()); delivered == 0 || delivered > 20 {
		t.Errorf("want some but not all events delivered, got %d", delivered)
	}
}

// ─── Context-aware capture ───────────────────────────────────────────────────

func TestCaptureException_AttachesHubScopeAndTrace(t *testing.T) {
	t.Parallel()
	tr := &fakeTransport{}
	client := newFakeClient(t, tr, bikeeper.Options{})

	hub := bikeeper.NewHub(client)
	hub.SetUser(bikeeper.User{ID: "usr-7"})
	ctx := bikeeper.SetHubOnContext(context.Background(), hub)
	span := bikeeper.StartTransaction(ctx, "mq.consume.void_item")
	defer span.Finish()

	client.CaptureException(span.Context(), errors.New("handler failed"))
	client.Flush()

	events := tr.captured()
	if len(events) != 1 {
		t.Fatalf("want 1 event, got %d", len(events))
	}
	ev := events[0]
	if ev.TraceID != span.TraceID {
		t.Errorf("TraceID = %q, want %q", ev.TraceID, span.TraceID)
	}
	if ev.User == nil || ev.User.ID != "usr-7" {
		t.Errorf("User = %+v, want ID usr-7", ev.User)
	}
	if ev.Message != "handler failed" {
		t.Errorf("Message = %q, want %q", ev.Message, "handler failed")
	}
	if !hub.HasCaptured() {
		t.Error("hub should be marked as having captured an event")
	}
}

func TestCaptureMessage_WithoutHubStillAttachesTrace(t *testing.T) {
	t.Parallel()
	tr := &fakeTransport{}
	client := newFakeClient(t, tr, bikeeper.Options{})

	span := bikeeper.StartTransaction(context.Background(), "worker.tick")
	defer span.Finish()

	client.CaptureMessage(span.Context(), "tick failed", bikeeper.LevelError,
		bikeeper.Tag{Key: "worker", Value: "outbox"})
	client.Flush()

	events := tr.captured()
	if len(events) != 1 {
		t.Fatalf("want 1 event, got %d", len(events))
	}
	if events[0].TraceID != span.TraceID {
		t.Errorf("TraceID = %q, want %q", events[0].TraceID, span.TraceID)
	}
	var found bool
	for _, tag := range events[0].Tags {
		if tag.Key == "worker" && tag.Value == "outbox" {
			found = true
		}
	}
	if !found {
		t.Errorf("per-call tags should survive, got %+v", events[0].Tags)
	}
}
