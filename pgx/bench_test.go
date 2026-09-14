package bikeeperpgx_test

import (
	"context"
	"testing"

	bikeeper "github.com/MhasbiM/bikeeper-go-sdk"

	bikeeperpgx "github.com/MhasbiM/bikeeper-go-sdk/pgx"
	"github.com/jackc/pgx/v5"
)

// The cost a query pays when nothing is listening — monitoring disabled, or a
// code path no transaction covers.
func BenchmarkTracerWithoutTransaction(b *testing.B) {
	tracer := bikeeperpgx.NewTracer()
	start := pgx.TraceQueryStartData{SQL: "-- name: GetSetting :one\nSELECT 1"}
	ctx := context.Background()

	b.ReportAllocs()
	for b.Loop() {
		qctx := tracer.TraceQueryStart(ctx, nil, start)
		tracer.TraceQueryEnd(qctx, nil, pgx.TraceQueryEndData{})
	}
}

// The cost when a transaction IS active, for comparison.
func BenchmarkTracerWithTransaction(b *testing.B) {
	tracer := bikeeperpgx.NewTracer()
	start := pgx.TraceQueryStartData{SQL: "-- name: GetSetting :one\nSELECT 1"}
	ctx, _ := bikeeper.StartJob(context.Background(), newBenchClient(), "worker.tick")

	b.ReportAllocs()
	for b.Loop() {
		qctx := tracer.TraceQueryStart(ctx, nil, start)
		tracer.TraceQueryEnd(qctx, nil, pgx.TraceQueryEndData{})
	}
}

func newBenchClient() *bikeeper.Client {
	return bikeeper.NewWithTransport(nopTransport{}, bikeeper.Options{
		ClientID: "id", ClientSecret: "secret", ProjectID: "project",
	})
}

type nopTransport struct{}

func (nopTransport) Send(context.Context, *bikeeper.Event) error { return nil }
func (nopTransport) Flush(context.Context)                       {}
