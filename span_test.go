package bikeeper

import (
	"context"
	"sync"
	"testing"
	"time"
)

// transactionCaptureTransport records transactions sent via SendTransaction.
// It satisfies the Transport interface and the internal transactionSender
// interface, mirroring logCaptureTransport's role for SendLog.
type transactionCaptureTransport struct {
	mu           sync.Mutex
	transactions []*TransactionPayload
}

func (t *transactionCaptureTransport) Send(_ context.Context, _ *Event) error { return nil }
func (t *transactionCaptureTransport) Flush(_ context.Context)                {}

func (t *transactionCaptureTransport) SendTransaction(_ context.Context, payload *TransactionPayload) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.transactions = append(t.transactions, payload)
	return nil
}

func (t *transactionCaptureTransport) captured() []*TransactionPayload {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]*TransactionPayload, len(t.transactions))
	copy(out, t.transactions)
	return out
}

func newTracingTestClient(t *testing.T, sampleRate float64) (*Client, *transactionCaptureTransport, *Hub) {
	t.Helper()
	transport := &transactionCaptureTransport{}
	client := NewWithTransport(transport, Options{
		ClientID:         "test-client",
		ClientSecret:     "test-secret",
		ProjectID:        "test-project",
		TracesSampleRate: sampleRate,
	})
	return client, transport, NewHub(client)
}

func TestSpan_SamplingDefaultsToDisabled(t *testing.T) {
	t.Parallel()
	client, transport, hub := newTracingTestClient(t, 0) // TracesSampleRate left unset
	ctx := SetHubOnContext(context.Background(), hub)

	txn := StartTransaction(ctx, "GET /orders/:id")
	txn.Finish()
	client.Flush()

	if got := transport.captured(); len(got) != 0 {
		t.Fatalf("expected 0 transactions sent with TracesSampleRate=0, got %d", len(got))
	}
}

func TestSpan_SampledTransactionIsSent(t *testing.T) {
	t.Parallel()
	client, transport, hub := newTracingTestClient(t, 1.0)
	ctx := SetHubOnContext(context.Background(), hub)

	txn := StartTransaction(ctx, "GET /orders/:id")
	txn.Finish()
	client.Flush()

	got := transport.captured()
	if len(got) != 1 {
		t.Fatalf("expected exactly 1 transaction sent, got %d", len(got))
	}
	if got[0].Op != "GET /orders/:id" {
		t.Errorf("Op = %q, want %q", got[0].Op, "GET /orders/:id")
	}
	if got[0].Duration <= 0 {
		t.Errorf("Duration = %v, want > 0", got[0].Duration)
	}
	// The root's own SpanID must be sent — without it, a direct child's
	// ParentSpanID (== txn.SpanID) is an unresolvable dangling reference on
	// the backend/frontend, since the root itself is never listed in Spans.
	if got[0].SpanID != txn.SpanID {
		t.Errorf("SpanID = %q, want %q (txn.SpanID)", got[0].SpanID, txn.SpanID)
	}
}

// TestSpan_SetOpRenamesBeforeFinish exercises the exact sequencing framework
// middleware needs: start a transaction with a provisional name, then rename
// it (SetOp) once the real name is known, before Finish() sends it.
func TestSpan_SetOpRenamesBeforeFinish(t *testing.T) {
	t.Parallel()
	client, transport, hub := newTracingTestClient(t, 1.0)
	ctx := SetHubOnContext(context.Background(), hub)

	txn := StartTransaction(ctx, "GET /") // provisional, matches the observed bug's placeholder
	txn.SetOp("GET /api/v1/orders/:id")   // resolved after routing completes
	txn.Finish()
	client.Flush()

	got := transport.captured()
	if len(got) != 1 {
		t.Fatalf("expected exactly 1 transaction sent, got %d", len(got))
	}
	if got[0].Op != "GET /api/v1/orders/:id" {
		t.Errorf("Op = %q, want the renamed value %q", got[0].Op, "GET /api/v1/orders/:id")
	}

	// TraceInfo() must also reflect the rename (it's what gets attached to
	// error events captured within this span).
	if info := txn.TraceInfo(); info.Op != "GET /api/v1/orders/:id" {
		t.Errorf("TraceInfo().Op = %q, want the renamed value %q", info.Op, "GET /api/v1/orders/:id")
	}
}

func TestSpan_BundlesFinishedChildSpans(t *testing.T) {
	t.Parallel()
	client, transport, hub := newTracingTestClient(t, 1.0)
	ctx := SetHubOnContext(context.Background(), hub)

	txn := StartTransaction(ctx, "GET /orders/:id")
	child1 := StartSpan(txn.Context(), "db.query", WithDescription("SELECT 1"))
	child2 := StartSpan(txn.Context(), "http.request")
	time.Sleep(time.Millisecond) // ensure measurable, distinct durations
	child1.Finish()
	child2.Finish()
	txn.Finish()
	client.Flush()

	got := transport.captured()
	if len(got) != 1 {
		t.Fatalf("expected exactly 1 transaction sent, got %d", len(got))
	}
	if len(got[0].Spans) != 2 {
		t.Fatalf("expected 2 bundled spans, got %d", len(got[0].Spans))
	}
	byOp := map[string]SpanPayload{}
	for _, s := range got[0].Spans {
		byOp[s.Op] = s
	}
	dbSpan, ok := byOp["db.query"]
	if !ok {
		t.Fatal("missing db.query span in payload")
	}
	if dbSpan.ParentSpanID != txn.SpanID {
		t.Errorf("db.query ParentSpanID = %q, want %q", dbSpan.ParentSpanID, txn.SpanID)
	}
	if dbSpan.Description != "SELECT 1" {
		t.Errorf("db.query Description = %q, want %q", dbSpan.Description, "SELECT 1")
	}
	if _, ok := byOp["http.request"]; !ok {
		t.Fatal("missing http.request span in payload")
	}
}

func TestSpan_LateChildFinishIsDroppedSilently(t *testing.T) {
	t.Parallel()
	client, transport, hub := newTracingTestClient(t, 1.0)
	ctx := SetHubOnContext(context.Background(), hub)

	txn := StartTransaction(ctx, "GET /orders/:id")
	child := StartSpan(txn.Context(), "db.query")

	// Finish the transaction BEFORE the child — the child is still "in
	// flight" from the transaction's point of view.
	txn.Finish()
	client.Flush()

	got := transport.captured()
	if len(got) != 1 {
		t.Fatalf("expected exactly 1 transaction sent, got %d", len(got))
	}
	if len(got[0].Spans) != 0 {
		t.Fatalf("expected 0 bundled spans (child not yet finished), got %d", len(got[0].Spans))
	}

	// Finishing the child afterward must not panic or send anything further.
	child.Finish()
	client.Flush()
	if got := transport.captured(); len(got) != 1 {
		t.Fatalf("expected still exactly 1 transaction sent after late child.Finish(), got %d", len(got))
	}
}

func TestSpan_FinishIsIdempotent(t *testing.T) {
	t.Parallel()
	client, transport, hub := newTracingTestClient(t, 1.0)
	ctx := SetHubOnContext(context.Background(), hub)

	txn := StartTransaction(ctx, "GET /orders/:id")
	txn.Finish()
	txn.Finish() // second call must be a no-op, not a second send
	client.Flush()

	if got := transport.captured(); len(got) != 1 {
		t.Fatalf("expected exactly 1 transaction sent despite calling Finish() twice, got %d", len(got))
	}
}

// TestSpan_ConcurrentChildSpans exercises the documented cross-goroutine
// usage pattern (concurrent StartSpan + Finish against a shared parent) under
// the race detector. Run with `go test -race`.
func TestSpan_ConcurrentChildSpans(t *testing.T) {
	t.Parallel()
	client, transport, hub := newTracingTestClient(t, 1.0)
	ctx := SetHubOnContext(context.Background(), hub)

	txn := StartTransaction(ctx, "batch.process")
	txnCtx := txn.Context()

	const n = 20
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			child := StartSpan(txnCtx, "worker")
			child.SetTag("worker", "concurrent")
			child.SetHTTPStatus(200)
			child.Finish()
		}()
	}
	wg.Wait()
	txn.Finish()
	client.Flush()

	got := transport.captured()
	if len(got) != 1 {
		t.Fatalf("expected exactly 1 transaction sent, got %d", len(got))
	}
	if len(got[0].Spans) != n {
		t.Fatalf("expected %d bundled spans, got %d", n, len(got[0].Spans))
	}
}
