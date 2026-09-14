// Package bikeeperzap integrates [go.uber.org/zap] with the Bikeeper SDK.
//
// Attach the [Core] alongside an existing *zap.Logger so that every log entry
// at or above the configured minimum level is automatically forwarded to the
// Bikeeper dashboard as a captured event — no changes required at call sites.
//
// # Quick start — tee into an existing logger
//
//	import (
//	    bikeeperzap "github.com/MhasbiM/bikeeper-go-sdk/zap"
//	    "go.uber.org/zap"
//	)
//
//	// Forward warnings and above to Bikeeper.
//	logger = bikeeperzap.AttachTo(logger, client, ctx, zap.WarnLevel)
//
//	// Structured fields become Bikeeper tags automatically.
//	logger.Error("checkout failed",
//	    zap.String("order_id", id),
//	    zap.Int("attempt", 3),
//	)
//
// # Carrying request context
//
// A log line knows nothing about the request it happened in, so by default a
// forwarded event arrives with a stack trace and little else. Hand the core the
// context and the event picks up the active trace, the request URL, the user,
// and the breadcrumb trail that Bikeeper already tracks for it:
//
//	log := bikeeperzap.WithContext(u.log, ctx)
//	log.Error("checkout failed", zap.Error(err))
//
//	// or per call:
//	u.log.Error("checkout failed", zap.Error(err), bikeeperzap.Ctx(ctx))
//
// [Ctx] is invisible to every other core: it encodes to nothing, so console and
// file output are unchanged.
//
// # Build from scratch (tee two cores)
//
//	core := zapcore.NewTee(
//	    zapcore.NewCore(enc, sink, lvl),                       // stdout / file
//	    bikeeperzap.NewCore(client, ctx, zap.WarnLevel),       // Bikeeper
//	)
//	logger := zap.New(core, zap.AddCaller())
package bikeeperzap

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	bikeeper "github.com/MhasbiM/bikeeper-go-sdk"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Compile-time proof that Core implements zapcore.Core.
var _ zapcore.Core = (*Core)(nil)

// ctxFieldKey names the pseudo-field [Ctx] uses to hand a context.Context to
// the Bikeeper core. It is deliberately not a plausible user field name.
const ctxFieldKey = "__bikeeper_ctx"

// ctxCarrier smuggles a context.Context through zap's field list.
type ctxCarrier struct{ ctx context.Context }

// Ctx returns a zap field that hands ctx to the Bikeeper core, so events
// forwarded from this log call are enriched with whatever ctx carries: the
// active span's trace, and — on a served request — the hub's scope (URL, HTTP
// request, user, breadcrumbs, tags).
//
//	u.log.Error("checkout failed", zap.Error(err), bikeeperzap.Ctx(ctx))
//
// The field is of zap's skip type, so no other core (console, file, JSON) ever
// renders it — it exists only for the Bikeeper core to pick up.
func Ctx(ctx context.Context) zap.Field {
	return zap.Field{Key: ctxFieldKey, Type: zapcore.SkipType, Interface: ctxCarrier{ctx: ctx}}
}

// WithContext returns a logger whose Bikeeper core captures with ctx. It is
// shorthand for logger.With(Ctx(ctx)) and is the convenient form when a
// function already derives a scoped logger:
//
//	log := bikeeperzap.WithContext(u.log, ctx).With(zap.String("method", "GetSetting"))
func WithContext(logger *zap.Logger, ctx context.Context) *zap.Logger {
	if logger == nil {
		return nil
	}
	return logger.With(Ctx(ctx))
}

// extractContext splits a context handed over by [Ctx] out of fields. It
// returns a nil context and the original slice when there is none, which is
// the common case and allocates nothing.
func extractContext(fields []zap.Field) (context.Context, []zap.Field) {
	found := -1
	var ctx context.Context
	for i, f := range fields {
		if f.Key != ctxFieldKey {
			continue
		}
		if carrier, ok := f.Interface.(ctxCarrier); ok && carrier.ctx != nil {
			ctx = carrier.ctx
			found = i
		}
	}
	if found < 0 {
		return nil, fields
	}
	rest := make([]zap.Field, 0, len(fields)-1)
	for i, f := range fields {
		if i == found || f.Key == ctxFieldKey {
			continue
		}
		rest = append(rest, f)
	}
	return ctx, rest
}

// Core is a [zapcore.Core] that forwards log entries to the Bikeeper client.
// Use [NewCore] to construct it and [zapcore.NewTee] to combine with your
// existing core, or use [AttachTo] as a single-call shortcut.
type Core struct {
	client *bikeeper.Client
	ctx    context.Context
	min    zapcore.Level
	fields []zap.Field // accumulated structured fields from With()
}

// NewCore returns a [zapcore.Core] that sends log entries at or above min to
// Bikeeper. Combine it with an existing core via [zapcore.NewTee].
//
//	core := zapcore.NewTee(
//	    existingCore,
//	    bikeeperzap.NewCore(client, ctx, zap.ErrorLevel),
//	)
func NewCore(client *bikeeper.Client, ctx context.Context, min zapcore.Level) *Core {
	return &Core{client: client, ctx: ctx, min: min}
}

// AttachTo wraps an existing [*zap.Logger] so every entry at or above min is
// also forwarded to Bikeeper as a captured event. The original logger is not
// modified; a new logger is returned.
//
//	logger = bikeeperzap.AttachTo(logger, client, ctx, zap.WarnLevel)
func AttachTo(logger *zap.Logger, client *bikeeper.Client, ctx context.Context, min zapcore.Level) *zap.Logger {
	return logger.WithOptions(zap.WrapCore(func(existing zapcore.Core) zapcore.Core {
		return zapcore.NewTee(existing, NewCore(client, ctx, min))
	}))
}

// Enabled reports whether the entry level meets the configured minimum.
func (c *Core) Enabled(lvl zapcore.Level) bool { return lvl >= c.min }

// With returns a shallow copy of the Core with the given fields accumulated.
// Accumulated fields are merged into every subsequent [Core.Write] call.
func (c *Core) With(fields []zap.Field) zapcore.Core {
	cp := *c
	if ctx, rest := extractContext(fields); ctx != nil {
		cp.ctx = ctx
		fields = rest
	}
	cp.fields = make([]zap.Field, len(c.fields)+len(fields))
	copy(cp.fields, c.fields)
	copy(cp.fields[len(c.fields):], fields)
	return &cp
}

// Check adds this Core to ce when the entry level is enabled.
func (c *Core) Check(entry zapcore.Entry, ce *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if c.Enabled(entry.Level) {
		return ce.AddCore(entry, c)
	}
	return ce
}

// Write converts the zap log entry and its accumulated fields into a Bikeeper
// event. Structured zap fields are encoded into Bikeeper [bikeeper.Tag] values
// so they appear in the dashboard's tag panel, and a logged error is folded
// into the event's message — see [messageWithError].
func (c *Core) Write(entry zapcore.Entry, fields []zap.Field) error {
	if c.client == nil {
		return nil
	}
	ctx, fields := extractContext(fields)
	if ctx == nil {
		ctx = c.ctx
	}
	if ctx == nil {
		ctx = context.Background()
	}
	level := zapLevelToBikeeper(entry.Level)
	allFields := append(c.fields, fields...) //nolint:gocritic // intentional append-to-slice
	tags := fieldsToTags(allFields)
	c.client.CaptureMessage(ctx, messageWithError(entry.Message, tags), level, tags...)
	return nil
}

// Sync is a no-op — Bikeeper delivers events asynchronously.
func (c *Core) Sync() error { return nil }

// ─── Helpers ─────────────────────────────────────────────────────────────────

// zapLevelToBikeeper maps a [zapcore.Level] to the corresponding [bikeeper.Level].
//
//	DebugLevel  → LevelDebug
//	InfoLevel   → LevelInfo
//	WarnLevel   → LevelWarning
//	ErrorLevel  → LevelError
//	DPanicLevel → LevelError
//	PanicLevel  → LevelFatal
//	FatalLevel  → LevelFatal
func zapLevelToBikeeper(lvl zapcore.Level) bikeeper.Level {
	switch lvl {
	case zapcore.DebugLevel:
		return bikeeper.LevelDebug
	case zapcore.InfoLevel:
		return bikeeper.LevelInfo
	case zapcore.WarnLevel:
		return bikeeper.LevelWarning
	case zapcore.ErrorLevel, zapcore.DPanicLevel:
		return bikeeper.LevelError
	case zapcore.PanicLevel, zapcore.FatalLevel:
		return bikeeper.LevelFatal
	default:
		return bikeeper.LevelInfo
	}
}

// errorFieldKeys are the zap field names that conventionally carry the error
// being logged, in the order they are preferred: "error" is the key
// [zap.Error] uses, "err" is what several libraries pick instead (pgx's
// tracelog among them).
var errorFieldKeys = []string{"error", "err"}

// messageWithError returns message with the logged error appended, so the
// event describes the failure and not merely the operation that hit it.
//
// A log message is written for a viewer who can see the entry's fields next to
// it, so libraries routinely log a bare verb ("Query") and leave the detail to
// zap.Error. An event has no such adjacency: its message becomes the issue
// title in the dashboard and the task title in downstream integrations, where
// a hundred unrelated failures all reading "Query" are indistinguishable.
// Appending the error keeps those titles apart without touching grouping,
// which is computed from the exception type and stack frames rather than the
// message.
//
// The message is returned unchanged when no error field is present, or when it
// already contains the error text (a call site that formatted the error into
// its own message should not have it repeated).
func messageWithError(message string, tags []bikeeper.Tag) string {
	errText := ""
	for _, key := range errorFieldKeys {
		for _, tag := range tags {
			if tag.Key == key && tag.Value != "" && tag.Value != "<nil>" {
				errText = tag.Value
				break
			}
		}
		if errText != "" {
			break
		}
	}

	switch {
	case errText == "" || strings.Contains(message, errText):
		return message
	case message == "":
		return errText
	default:
		return message + ": " + errText
	}
}

// fieldsToTags encodes zap fields into [bikeeper.Tag] values.
// It uses [zapcore.NewMapObjectEncoder] to extract field values via the
// standard zap encoding path, then formats each value with formatFieldValue.
func fieldsToTags(fields []zap.Field) []bikeeper.Tag {
	if len(fields) == 0 {
		return nil
	}
	enc := zapcore.NewMapObjectEncoder()
	for _, f := range fields {
		f.AddTo(enc)
	}
	tags := make([]bikeeper.Tag, 0, len(enc.Fields))
	for k, v := range enc.Fields {
		tags = append(tags, bikeeper.Tag{
			Key:   k,
			Value: formatFieldValue(v),
		})
	}
	return tags
}

// formatFieldValue renders a single zap field value as a Tag value string.
// Scalars, errors, and fmt.Stringers keep their natural %v form (so
// zap.Error(err) still reads as err.Error(), not a struct dump). Maps,
// slices, arrays, and structs are JSON-encoded instead — %v on those produces
// Go's debug syntax (e.g. "map[Content-Type: User-Agent:...]"), which isn't
// valid JSON and reads poorly in the dashboard; encoding preserves the
// structure as parseable JSON.
func formatFieldValue(v any) string {
	if v == nil {
		return "<nil>"
	}
	if _, ok := v.(error); ok {
		return fmt.Sprintf("%v", v)
	}
	if _, ok := v.(fmt.Stringer); ok {
		return fmt.Sprintf("%v", v)
	}
	switch reflect.ValueOf(v).Kind() {
	case reflect.Map, reflect.Slice, reflect.Array, reflect.Struct:
		if b, err := json.Marshal(v); err == nil {
			return string(b)
		}
	}
	return fmt.Sprintf("%v", v)
}
