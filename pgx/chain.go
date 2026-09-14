package bikeeperpgx

import (
	"context"

	"github.com/jackc/pgx/v5"
)

// Chain composes several pgx tracers into one, because
// pgxpool.Config.ConnConfig.Tracer holds a single value — so installing
// Bikeeper tracing would otherwise mean giving up query logging.
//
//	config.ConnConfig.Tracer = bikeeperpgx.Chain(existingTraceLog, bikeeperpgx.NewTracer())
//
// Each tracer keeps its own state under its own context key, so threading the
// context returned by one into the next preserves all of them. Tracers are
// started in the order given and ended in reverse, the way nested middleware
// behaves.
//
// The returned value implements every optional pgx tracer interface
// ([pgx.BatchTracer], [pgx.CopyFromTracer], [pgx.PrepareTracer],
// [pgx.ConnectTracer]) and forwards each call only to the members that
// implement it — pgx type-asserts for these, and a tracer that swallowed them
// would silently disable a member's logging.
func Chain(tracers ...pgx.QueryTracer) pgx.QueryTracer {
	return &chain{tracers: tracers}
}

type chain struct {
	tracers []pgx.QueryTracer
}

var (
	_ pgx.QueryTracer    = (*chain)(nil)
	_ pgx.BatchTracer    = (*chain)(nil)
	_ pgx.CopyFromTracer = (*chain)(nil)
	_ pgx.PrepareTracer  = (*chain)(nil)
	_ pgx.ConnectTracer  = (*chain)(nil)
)

func (c *chain) TraceQueryStart(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	for _, t := range c.tracers {
		ctx = t.TraceQueryStart(ctx, conn, data)
	}
	return ctx
}

func (c *chain) TraceQueryEnd(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryEndData) {
	for i := len(c.tracers) - 1; i >= 0; i-- {
		c.tracers[i].TraceQueryEnd(ctx, conn, data)
	}
}

func (c *chain) TraceBatchStart(ctx context.Context, conn *pgx.Conn, data pgx.TraceBatchStartData) context.Context {
	for _, t := range c.tracers {
		if bt, ok := t.(pgx.BatchTracer); ok {
			ctx = bt.TraceBatchStart(ctx, conn, data)
		}
	}
	return ctx
}

func (c *chain) TraceBatchQuery(ctx context.Context, conn *pgx.Conn, data pgx.TraceBatchQueryData) {
	for _, t := range c.tracers {
		if bt, ok := t.(pgx.BatchTracer); ok {
			bt.TraceBatchQuery(ctx, conn, data)
		}
	}
}

func (c *chain) TraceBatchEnd(ctx context.Context, conn *pgx.Conn, data pgx.TraceBatchEndData) {
	for i := len(c.tracers) - 1; i >= 0; i-- {
		if bt, ok := c.tracers[i].(pgx.BatchTracer); ok {
			bt.TraceBatchEnd(ctx, conn, data)
		}
	}
}

func (c *chain) TraceCopyFromStart(ctx context.Context, conn *pgx.Conn, data pgx.TraceCopyFromStartData) context.Context {
	for _, t := range c.tracers {
		if ct, ok := t.(pgx.CopyFromTracer); ok {
			ctx = ct.TraceCopyFromStart(ctx, conn, data)
		}
	}
	return ctx
}

func (c *chain) TraceCopyFromEnd(ctx context.Context, conn *pgx.Conn, data pgx.TraceCopyFromEndData) {
	for i := len(c.tracers) - 1; i >= 0; i-- {
		if ct, ok := c.tracers[i].(pgx.CopyFromTracer); ok {
			ct.TraceCopyFromEnd(ctx, conn, data)
		}
	}
}

func (c *chain) TracePrepareStart(ctx context.Context, conn *pgx.Conn, data pgx.TracePrepareStartData) context.Context {
	for _, t := range c.tracers {
		if pt, ok := t.(pgx.PrepareTracer); ok {
			ctx = pt.TracePrepareStart(ctx, conn, data)
		}
	}
	return ctx
}

func (c *chain) TracePrepareEnd(ctx context.Context, conn *pgx.Conn, data pgx.TracePrepareEndData) {
	for i := len(c.tracers) - 1; i >= 0; i-- {
		if pt, ok := c.tracers[i].(pgx.PrepareTracer); ok {
			pt.TracePrepareEnd(ctx, conn, data)
		}
	}
}

func (c *chain) TraceConnectStart(ctx context.Context, data pgx.TraceConnectStartData) context.Context {
	for _, t := range c.tracers {
		if ct, ok := t.(pgx.ConnectTracer); ok {
			ctx = ct.TraceConnectStart(ctx, data)
		}
	}
	return ctx
}

func (c *chain) TraceConnectEnd(ctx context.Context, data pgx.TraceConnectEndData) {
	for i := len(c.tracers) - 1; i >= 0; i-- {
		if ct, ok := c.tracers[i].(pgx.ConnectTracer); ok {
			ct.TraceConnectEnd(ctx, data)
		}
	}
}
