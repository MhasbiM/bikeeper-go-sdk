package bikeeper

import "context"

// StartJob opens a transaction covering one unit of background work — a
// consumed queue message, one tick of a scheduled worker, one CLI command —
// and returns a context carrying it together with the transaction itself.
//
// Background work has to ask for this explicitly. An HTTP request gets a Hub
// and a root transaction from the framework middleware (bikeeperfiber,
// bikeeperecho); a queue consumer gets neither, and a Span started without a
// Hub on its context has no client to send through — it is built, tagged, and
// silently dropped on Finish. Every span the code below a consumer creates is
// wasted work until a Hub is installed here.
//
// Pass the returned context down so spans, queries and log calls attach to
// this transaction:
//
//	func (c *consumer) handle(ctx context.Context, msg Message) {
//	    ctx, job := bikeeper.StartJob(ctx, c.client, "mq.consume.order")
//	    err := c.process(ctx, msg)
//	    bikeeper.FinishJob(job, err)
//	}
//
// A nil client (monitoring disabled, or misconfigured credentials) is a no-op:
// ctx comes back unchanged and the returned span is nil, which [FinishJob]
// tolerates.
func StartJob(ctx context.Context, client *Client, op string, opts ...SpanOption) (context.Context, *Span) {
	if client == nil {
		return ctx, nil
	}
	ctx = SetHubOnContext(ctx, NewHub(client))
	opts = append([]SpanOption{WithTransactionSource(SourceTask)}, opts...)
	span := StartTransaction(ctx, op, opts...)
	return span.Context(), span
}

// FinishJob closes a transaction opened by [StartJob], marking it failed when
// err is non-nil so the trace is filed as an error rather than a success. A
// nil span is ignored.
func FinishJob(span *Span, err error) {
	if span == nil {
		return
	}
	if err != nil {
		span.SetStatus(SpanStatusInternalError)
		span.SetData("error", err.Error())
	}
	span.Finish()
}
