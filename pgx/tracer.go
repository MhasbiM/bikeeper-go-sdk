// Package bikeeperpgx traces pgx database work with the Bikeeper SDK.
//
// It provides three pieces, each usable on its own:
//
//   - [Tracer] wraps every query and batch in an APM span, attaching to
//     whatever Bikeeper transaction is active on the caller's context.
//   - [Chain] composes several pgx tracers into the single one
//     pgxpool.Config.ConnConfig.Tracer accepts, so query logging and tracing
//     can coexist.
//   - [NewLogger] rewrites pgx's bare failure messages ("Query", "Connect")
//     into ones that name the query and the error, which is what an error
//     monitor turns into an issue title.
//
// # Usage
//
//	config, _ := pgxpool.ParseConfig(dsn)
//	config.ConnConfig.Tracer = bikeeperpgx.Chain(
//	    &tracelog.TraceLog{
//	        LogLevel: tracelog.LogLevelWarn,
//	        Logger:   bikeeperpgx.NewLogger(pgxzap.NewLogger(log)),
//	    },
//	    bikeeperpgx.NewTracer(),
//	)
//
// Spans are a complete no-op when no Bikeeper transaction is active on the
// context (a background job with no hub, or monitoring disabled), so this is
// always safe to install.
package bikeeperpgx

import (
	"context"
	"fmt"

	bikeeper "github.com/MhasbiM/bikeeper-go-sdk"
	"github.com/jackc/pgx/v5"
)

// spanContextKey is the key a query's span is stored under between its
// Trace*Start and the matching Trace*End — pgx keeps whatever context Start
// returns and hands that same context back to End.
type spanContextKey struct{}

// defaultMaxSQLLength bounds the statement text recorded as a span's
// description. Generated queries can run long, and a trace carries one of
// these per statement.
const defaultMaxSQLLength = 4096

// Tracer wraps every pgx query and batch in a "db.query" / "db.batch" span,
// attaching to whatever Bikeeper transaction is active on the caller's context
// (started by framework middleware for an HTTP request, or by
// bikeeper.StartJob for background work).
//
// It implements [pgx.QueryTracer] and [pgx.BatchTracer]. Bind parameters are
// deliberately never recorded — they are where personal data lives.
type Tracer struct {
	maxSQLLength int
}

// Option configures a [Tracer].
type Option func(*Tracer)

// WithMaxSQLLength caps how much statement text is recorded as a span's
// description. Defaults to 4096 characters; a value <= 0 restores that default.
func WithMaxSQLLength(n int) Option {
	return func(t *Tracer) {
		if n <= 0 {
			n = defaultMaxSQLLength
		}
		t.maxSQLLength = n
	}
}

// NewTracer returns a Tracer ready to install on
// pgxpool.Config.ConnConfig.Tracer — on its own, or combined with an existing
// tracer via [Chain].
func NewTracer(opts ...Option) *Tracer {
	t := &Tracer{maxSQLLength: defaultMaxSQLLength}
	for _, opt := range opts {
		opt(t)
	}
	return t
}

// Compile-time proof that Tracer covers both traced operations.
var (
	_ pgx.QueryTracer = (*Tracer)(nil)
	_ pgx.BatchTracer = (*Tracer)(nil)
)

func (t *Tracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	span := bikeeper.StartSpan(ctx, "db.query", bikeeper.WithDescription(truncate(data.SQL, t.maxSQLLength)))
	return context.WithValue(ctx, spanContextKey{}, span)
}

func (t *Tracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	finish(ctx, data.Err)
}

func (t *Tracer) TraceBatchStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceBatchStartData) context.Context {
	span := bikeeper.StartSpan(ctx, "db.batch", bikeeper.WithDescription(fmt.Sprintf("%d statements", data.Batch.Len())))
	return context.WithValue(ctx, spanContextKey{}, span)
}

// TraceBatchQuery is a no-op: per-statement detail within a batch is not
// captured as individual spans (that would mean correlating N calls back to
// one parent). The enclosing db.batch span already carries the batch's
// duration, which is the useful signal in a waterfall.
func (t *Tracer) TraceBatchQuery(_ context.Context, _ *pgx.Conn, _ pgx.TraceBatchQueryData) {}

func (t *Tracer) TraceBatchEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceBatchEndData) {
	finish(ctx, data.Err)
}

// finish closes the span stored on ctx, marking it failed when the operation
// errored.
func finish(ctx context.Context, err error) {
	span, ok := ctx.Value(spanContextKey{}).(*bikeeper.Span)
	if !ok {
		return
	}
	if err != nil {
		span.SetStatus(bikeeper.SpanStatusInternalError)
		span.SetData("db.error", err.Error())
	}
	span.Finish()
}

func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max] + "…"
}
