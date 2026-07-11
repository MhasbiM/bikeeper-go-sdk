package bikeeper

import (
	"sync"
	"time"
)

// transactionState is shared by a root transaction Span and every descendant
// Span in the same trace. mu is ONE lock for the whole trace — it serializes
// registering new children, finalizing any single span's mutable fields
// (Status/Tags/Data/endTime), and the root's one-time read of every
// descendant when building the outgoing payload. This matches the package's
// existing coarse-grained-mutex style (Scope, Hub); traces are not expected
// to be wide/hot enough for single-mutex contention to matter.
type transactionState struct {
	mu sync.Mutex
	// root identifies which Span in the trace IS the transaction:
	// root.txn.root == root.
	root *Span
	// children are descendants registered so far, in creation order.
	children []*Span
	// sampled is the head-based sampling decision, rolled once when the
	// root is created. Every descendant inherits it unchanged.
	sampled bool
	// client is resolved from ctx's Hub at root-creation time. Nil disables
	// sending entirely (Span still functions for trace-ID propagation).
	client *Client
}

// TransactionPayload is sent to POST /api/v1/transactions when a root Span
// (a transaction) finishes and its trace was sampled. Field/json-tag
// conventions mirror Event and LogRecord.
type TransactionPayload struct {
	// SpanID is the root span's own identifier — child spans' ParentSpanID
	// references this value when they are direct children of the
	// transaction itself, so the backend/frontend can reconstruct the tree
	// (without this, a direct child's ParentSpanID would be an unresolvable
	// dangling reference, since the root is never listed in Spans).
	SpanID      string        `json:"span_id"`
	TraceID     string        `json:"trace_id"`
	Op          string        `json:"op"`
	Description string        `json:"description,omitempty"`
	Status      SpanStatus    `json:"status,omitempty"`
	StartTime   time.Time     `json:"start_time"`
	Duration    time.Duration `json:"duration"`
	Tags        []Tag         `json:"tags,omitempty"`
	SDK         *SDKInfo      `json:"sdk,omitempty"`
	// Spans holds every descendant span that had finished by the time the
	// transaction sent — see buildTransactionPayload.
	Spans []SpanPayload `json:"spans,omitempty"`
}

// SpanPayload is one child span within a TransactionPayload's waterfall.
type SpanPayload struct {
	SpanID       string `json:"span_id"`
	ParentSpanID string `json:"parent_span_id,omitempty"`
	Op           string `json:"op"`
	Description  string `json:"description,omitempty"`
	Status       SpanStatus `json:"status,omitempty"`
	// StartOffset is relative to the transaction's StartTime.
	StartOffset time.Duration  `json:"start_offset"`
	Duration    time.Duration  `json:"duration"`
	Tags        []Tag          `json:"tags,omitempty"`
	Data        map[string]any `json:"data,omitempty"`
}

// buildTransactionPayload copies every mutable field of root and its
// finished children into an independent payload. The caller (Span.Finish)
// must hold txn.mu for the duration of this call so the copy is consistent
// and the returned payload never aliases live Span state that another
// goroutine could still mutate after the lock is released.
func buildTransactionPayload(root *Span, children []*Span, client *Client) *TransactionPayload {
	// Prepend environment/release/global tags, mirroring Client.enrichEvent's
	// behavior for Events — without this, transactions would never carry the
	// tags the Performance page's environment/release filters depend on.
	tags := tagsFromMap(root.Tags)
	var extra []Tag
	if client.opts.Environment != "" {
		extra = append(extra, Tag{Key: "environment", Value: client.opts.Environment})
	}
	if client.opts.Release != "" {
		extra = append(extra, Tag{Key: "release", Value: client.opts.Release})
	}
	extra = append(extra, client.Tags()...)
	if len(extra) > 0 {
		tags = append(extra, tags...)
	}

	p := &TransactionPayload{
		SpanID:      root.SpanID,
		TraceID:     root.TraceID,
		Op:          root.Op,
		Description: root.Description,
		Status:      root.Status,
		StartTime:   root.startTime.UTC(),
		Duration:    root.endTime.Sub(root.startTime),
		Tags:        tags,
		SDK:         &SDKInfo{Name: sdkName, Version: sdkVersion},
	}
	for _, c := range children {
		if c.endTime.IsZero() {
			// Not finished by the time the transaction sent. Dropped, not
			// included — covers both a child finishing after its
			// transaction already sent, and a child whose append lost the
			// lock race against the transaction's own Finish().
			continue
		}
		p.Spans = append(p.Spans, SpanPayload{
			SpanID:       c.SpanID,
			ParentSpanID: c.ParentSpanID,
			Op:           c.Op,
			Description:  c.Description,
			Status:       c.Status,
			StartOffset:  c.startTime.Sub(root.startTime),
			Duration:     c.endTime.Sub(c.startTime),
			Tags:         tagsFromMap(c.Tags),
			Data:         copyDataMap(c.Data),
		})
	}
	return p
}

// tagsFromMap converts a Span's map[string]string tags into the []Tag shape
// used on the wire everywhere else (Event.Tags, LogRecord.Tags) — keeping
// transactions filterable by the backend's existing tag-containment queries
// (e.g. environment/release) without a parallel query shape just for them.
func tagsFromMap(m map[string]string) []Tag {
	if len(m) == 0 {
		return nil
	}
	out := make([]Tag, 0, len(m))
	for k, v := range m {
		out = append(out, Tag{Key: k, Value: v})
	}
	return out
}

// copyDataMap returns a shallow copy of a span's Data map, safe to hand to
// another goroutine after the source's lock is released.
func copyDataMap(m map[string]any) map[string]any {
	if len(m) == 0 {
		return nil
	}
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}
