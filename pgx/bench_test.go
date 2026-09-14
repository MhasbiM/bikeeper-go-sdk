package bikeeperpgx_test

import (
	"context"
	"testing"

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
