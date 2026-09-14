package bikeeper_test

import (
	"context"
	"errors"
	"testing"

	bikeeper "github.com/MhasbiM/bikeeper-go-sdk"
)

func TestStartJob_SendsTransactionAndTracesWork(t *testing.T) {
	t.Parallel()
	tr := &fakeTransport{}
	client := newFakeClient(t, tr, bikeeper.Options{TracesSampleRate: 1.0})

	ctx, job := bikeeper.StartJob(context.Background(), client, "mq.consume.void_item")

	// Work below the job attaches to it, and an error logged there reaches the
	// job's trace — which is the whole point of installing a hub.
	child := bikeeper.StartSpan(ctx, "db.query")
	child.Finish()
	client.CaptureException(ctx, errors.New("handler failed"))

	bikeeper.FinishJob(job, errors.New("handler failed"))
	client.Flush()

	events := tr.captured()
	if len(events) != 1 {
		t.Fatalf("want 1 event, got %d", len(events))
	}
	if events[0].TraceID != job.TraceID {
		t.Errorf("event TraceID = %q, want the job's %q", events[0].TraceID, job.TraceID)
	}
	if child.TraceID != job.TraceID {
		t.Errorf("child span TraceID = %q, want the job's %q", child.TraceID, job.TraceID)
	}
}

func TestStartJob_NilClientIsNoOp(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	gotCtx, job := bikeeper.StartJob(ctx, nil, "worker.tick")

	if gotCtx != ctx {
		t.Error("context should come back unchanged when monitoring is disabled")
	}
	if job != nil {
		t.Errorf("job = %v, want nil", job)
	}
	bikeeper.FinishJob(job, errors.New("boom")) // must not panic
}

func TestScrubSensitive(t *testing.T) {
	t.Parallel()
	event := &bikeeper.Event{
		Tags: []bikeeper.Tag{
			{Key: "sql", Value: "SELECT 1"},
			{Key: "args", Value: `["Budi","081234567890"]`},
			{Key: "Authorization", Value: "Bearer abc"},
		},
		HTTPRequest: &bikeeper.HTTPRequest{
			Headers: map[string]string{"Cookie": "session=abc", "User-Agent": "curl/8.7.1"},
		},
	}

	got := bikeeper.ScrubSensitive(event)

	want := map[string]string{
		"sql":           "SELECT 1", // harmless, kept as-is
		"args":          bikeeper.Redacted,
		"Authorization": bikeeper.Redacted,
	}
	for _, tag := range got.Tags {
		if want[tag.Key] != tag.Value {
			t.Errorf("tag %q = %q, want %q", tag.Key, tag.Value, want[tag.Key])
		}
	}
	if got := got.HTTPRequest.Headers["Cookie"]; got != bikeeper.Redacted {
		t.Errorf("Cookie header = %q, want %q", got, bikeeper.Redacted)
	}
	if got := got.HTTPRequest.Headers["User-Agent"]; got != "curl/8.7.1" {
		t.Errorf("User-Agent header = %q, want it untouched", got)
	}
}

func TestNewScrubber_ExtraKeysAndNilEvent(t *testing.T) {
	t.Parallel()
	scrub := bikeeper.NewScrubber("visitor_phone")

	event := scrub(&bikeeper.Event{Tags: []bikeeper.Tag{
		{Key: "Visitor_Phone", Value: "0812"},
		{Key: "order_id", Value: "42"},
	}})

	if event.Tags[0].Value != bikeeper.Redacted {
		t.Errorf("extra key should be redacted case-insensitively, got %q", event.Tags[0].Value)
	}
	if event.Tags[1].Value != "42" {
		t.Errorf("unrelated tag = %q, want it untouched", event.Tags[1].Value)
	}
	if got := scrub(nil); got != nil {
		t.Errorf("scrub(nil) = %v, want nil", got)
	}
}
