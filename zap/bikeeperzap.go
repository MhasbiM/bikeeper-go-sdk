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
	"fmt"

	bikeeper "github.com/MhasbiM/bikeeper-go-sdk"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Compile-time proof that Core implements zapcore.Core.
var _ zapcore.Core = (*Core)(nil)

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
// so they appear in the dashboard's tag panel.
func (c *Core) Write(entry zapcore.Entry, fields []zap.Field) error {
	level := zapLevelToBikeeper(entry.Level)
	allFields := append(c.fields, fields...) //nolint:gocritic // intentional append-to-slice
	tags := fieldsToTags(allFields)
	c.client.CaptureMessage(c.ctx, entry.Message, level, tags...)
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

// fieldsToTags encodes zap fields into [bikeeper.Tag] values.
// It uses [zapcore.NewMapObjectEncoder] to extract field values via the
// standard zap encoding path — complex types (errors, arrays, objects)
// are serialised with fmt.Sprintf so no data is silently dropped.
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
			Value: fmt.Sprintf("%v", v),
		})
	}
	return tags
}
