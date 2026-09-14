package bikeeperpgx_test

import (
	"context"
	"errors"
	"testing"

	bikeeper "github.com/MhasbiM/bikeeper-go-sdk"

	bikeeperpgx "github.com/MhasbiM/bikeeper-go-sdk/pgx"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/tracelog"
)

// recordingLogger captures what the wrapped logger was handed.
type recordingLogger struct {
	level tracelog.LogLevel
	msg   string
	data  map[string]any
}

func (l *recordingLogger) Log(_ context.Context, level tracelog.LogLevel, msg string, data map[string]any) {
	l.level, l.msg, l.data = level, msg, data
}

func TestLogger_NamesTheQueryAndTheError(t *testing.T) {
	t.Parallel()
	queryErr := errors.New(`ERROR: column "use_self_order" does not exist (SQLSTATE 42703)`)

	tests := []struct {
		name  string
		level tracelog.LogLevel
		msg   string
		data  map[string]any
		want  string
	}{
		{
			name:  "sqlc query name is used",
			level: tracelog.LogLevelError,
			msg:   "Query",
			data: map[string]any{
				"sql": "-- name: CheckSelfOrderEnabled :one\nSELECT use_self_order FROM x",
				"err": queryErr,
			},
			want: `db query failed: CheckSelfOrderEnabled: ERROR: column "use_self_order" does not exist (SQLSTATE 42703)`,
		},
		{
			name:  "operations without sql still describe the error",
			level: tracelog.LogLevelError,
			msg:   "Connect",
			data:  map[string]any{"err": errors.New("dial tcp: connection refused")},
			want:  "db connect failed: dial tcp: connection refused",
		},
		{
			name:  "ad-hoc sql has no name to report",
			level: tracelog.LogLevelError,
			msg:   "Query",
			data:  map[string]any{"sql": "SELECT 1", "err": errors.New("boom")},
			want:  "db query failed: boom",
		},
		{
			name:  "an error entry without an error is left alone",
			level: tracelog.LogLevelError,
			msg:   "Query",
			data:  map[string]any{"sql": "SELECT 1"},
			want:  "Query",
		},
		{
			name:  "non-error levels are left alone",
			level: tracelog.LogLevelInfo,
			msg:   "Query",
			data:  map[string]any{"sql": "SELECT 1", "err": queryErr},
			want:  "Query",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rec := &recordingLogger{}
			bikeeperpgx.NewLogger(rec).Log(context.Background(), tt.level, tt.msg, tt.data)

			if rec.msg != tt.want {
				t.Errorf("message = %q, want %q", rec.msg, tt.want)
			}
			if len(rec.data) != len(tt.data) {
				t.Errorf("data should be passed through untouched, got %v want %v", rec.data, tt.data)
			}
		})
	}
}

// queryOnlyTracer implements pgx.QueryTracer and nothing else — Chain must not
// assume every member handles batches.
type queryOnlyTracer struct{ calls *[]string }

func (t queryOnlyTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryStartData) context.Context {
	*t.calls = append(*t.calls, "query-only start")
	return ctx
}

func (t queryOnlyTracer) TraceQueryEnd(_ context.Context, _ *pgx.Conn, _ pgx.TraceQueryEndData) {
	*t.calls = append(*t.calls, "query-only end")
}

func TestChain_StartsInOrderAndEndsInReverse(t *testing.T) {
	t.Parallel()
	var calls []string
	chained := bikeeperpgx.Chain(queryOnlyTracer{&calls}, bikeeperpgx.NewTracer())

	ctx := chained.TraceQueryStart(context.Background(), nil, pgx.TraceQueryStartData{SQL: "SELECT 1"})
	chained.TraceQueryEnd(ctx, nil, pgx.TraceQueryEndData{Err: errors.New("boom")})

	want := []string{"query-only start", "query-only end"}
	if len(calls) != len(want) || calls[0] != want[0] || calls[1] != want[1] {
		t.Errorf("calls = %v, want %v", calls, want)
	}
}

// A member that does not implement BatchTracer must simply be skipped rather
// than breaking the batch path for the members that do.
func TestChain_SkipsMembersWithoutTheOptionalInterface(t *testing.T) {
	t.Parallel()
	var calls []string
	chained := bikeeperpgx.Chain(queryOnlyTracer{&calls}, bikeeperpgx.NewTracer())

	batch, ok := chained.(pgx.BatchTracer)
	if !ok {
		t.Fatal("Chain should implement pgx.BatchTracer")
	}
	ctx := batch.TraceBatchStart(context.Background(), nil, pgx.TraceBatchStartData{Batch: &pgx.Batch{}})
	batch.TraceBatchEnd(ctx, nil, pgx.TraceBatchEndData{})

	if len(calls) != 0 {
		t.Errorf("query-only tracer should not see batch calls, got %v", calls)
	}
}

// Outside any transaction — a background job with no hub, or monitoring
// switched off — the tracer must do nothing at all rather than build spans
// that can never be sent.
func TestTracer_WithoutActiveTransactionDoesNothing(t *testing.T) {
	// Not parallel: AllocsPerRun below needs the process to itself.
	tracer := bikeeperpgx.NewTracer()
	ctx := context.Background()

	if got := tracer.TraceQueryStart(ctx, nil, pgx.TraceQueryStartData{SQL: "SELECT 1"}); got != ctx {
		t.Error("the context should come back untouched when nothing is listening")
	}
	// An End that never saw its Start must also be harmless.
	tracer.TraceQueryEnd(ctx, nil, pgx.TraceQueryEndData{Err: errors.New("boom")})
	tracer.TraceBatchEnd(ctx, nil, pgx.TraceBatchEndData{})

	if allocs := testing.AllocsPerRun(100, func() {
		qctx := tracer.TraceQueryStart(ctx, nil, pgx.TraceQueryStartData{SQL: "SELECT 1"})
		tracer.TraceQueryEnd(qctx, nil, pgx.TraceQueryEndData{})
	}); allocs != 0 {
		t.Errorf("allocations per query = %v, want 0 when tracing is off", allocs)
	}
}

// With a transaction in reach, the span is carried on the returned context so
// TraceQueryEnd can finish it.
func TestTracer_WithActiveTransaction(t *testing.T) {
	t.Parallel()
	client := bikeeper.NewWithTransport(nopTransport{}, bikeeper.Options{
		ClientID: "id", ClientSecret: "secret", ProjectID: "project",
	})
	tracer := bikeeperpgx.NewTracer(bikeeperpgx.WithMaxSQLLength(10))
	jobCtx, job := bikeeper.StartJob(context.Background(), client, "worker.tick")

	qctx := tracer.TraceQueryStart(jobCtx, nil, pgx.TraceQueryStartData{SQL: "SELECT a_very_long_statement"})
	if qctx == jobCtx {
		t.Error("the span should be carried on the returned context")
	}
	tracer.TraceQueryEnd(qctx, nil, pgx.TraceQueryEndData{Err: errors.New("boom")})
	bikeeper.FinishJob(job, nil)
}
