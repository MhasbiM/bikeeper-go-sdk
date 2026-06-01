package bikeeper

import (
	"context"
	"fmt"
)

// Logger captures structured log entries as Bikeeper events.
// Each log-level method returns a [LogEntry] builder — chain [LogEntry.WithCtx]
// and [LogEntry.WithTag], then call [LogEntry.Emit] or [LogEntry.Emitf] to send.
//
// Create a Logger from a [Client]:
//
//	logger := client.NewLogger(ctx)
//
//	// Simple emit (like fmt.Print)
//	logger.Info().Emit("server started")
//
//	// Formatted emit (like fmt.Printf)
//	logger.Error().Emitf("payment failed: %v", err)
//
//	// Override context inline
//	logger.Warn().WithCtx(reqCtx).WithTag("order_id", id).Emit("retrying")
type Logger struct {
	client *Client
	ctx    context.Context
	tags   []Tag
}

// NewLogger creates a Logger bound to the given client and context.
// The context propagates active span / hub information to every emitted event
// so logs are automatically correlated with the current request trace.
func (c *Client) NewLogger(ctx context.Context) *Logger {
	return &Logger{client: c, ctx: ctx} // nil client is handled safely in LogEntry.Emit/Emitf
}

// WithTag returns a new Logger that prepends key/value to every log entry
// emitted from it. Tags are additive — each call appends without replacing.
// The receiver is not modified.
func (l *Logger) WithTag(key, value string) *Logger {
	tags := make([]Tag, len(l.tags)+1)
	copy(tags, l.tags)
	tags[len(l.tags)] = Tag{Key: key, Value: value}
	return &Logger{client: l.client, ctx: l.ctx, tags: tags}
}

// Debug returns a [LogEntry] at [LevelDebug].
func (l *Logger) Debug() *LogEntry { return l.entry(LevelDebug) }

// Info returns a [LogEntry] at [LevelInfo].
func (l *Logger) Info() *LogEntry { return l.entry(LevelInfo) }

// Warn returns a [LogEntry] at [LevelWarning].
func (l *Logger) Warn() *LogEntry { return l.entry(LevelWarning) }

// Error returns a [LogEntry] at [LevelError].
func (l *Logger) Error() *LogEntry { return l.entry(LevelError) }

// Fatal returns a [LogEntry] at [LevelFatal].
func (l *Logger) Fatal() *LogEntry { return l.entry(LevelFatal) }

func (l *Logger) entry(level Level) *LogEntry {
	return &LogEntry{
		client: l.client,
		ctx:    l.ctx,
		level:  level,
		tags:   append([]Tag{}, l.tags...),
	}
}

// LogEntry is a single log record builder returned by the level methods on
// [Logger]. Methods are chainable; call [LogEntry.Emit] or [LogEntry.Emitf]
// to finalize and send the event.
type LogEntry struct {
	client *Client
	ctx    context.Context
	level  Level
	tags   []Tag
}

// WithCtx attaches ctx to this entry only, overriding the parent [Logger]'s
// context. The parent Logger is not modified.
//
//	logger.Info().WithCtx(requestCtx).Emit("context passed")
func (e *LogEntry) WithCtx(ctx context.Context) *LogEntry {
	e.ctx = ctx
	return e
}

// WithTag attaches a key/value tag to this entry only.
//
//	logger.Error().WithTag("order_id", id).Emit("checkout failed")
func (e *LogEntry) WithTag(key, value string) *LogEntry {
	e.tags = append(e.tags, Tag{Key: key, Value: value})
	return e
}

// Emit sends the log entry. args are formatted with [fmt.Sprint] —
// adjacent non-string operands are space-separated, same as [fmt.Print].
//
//	logger.Info().Emit("Hello ", "world!")
//
// When [Options.EnableLogging] is true the entry is stored as a [LogRecord]
// via POST /api/v1/logs (separate from the events table).
// When false, it falls through to [Client.CaptureMessage] for backward
// compatibility.
func (e *LogEntry) Emit(args ...any) {
	if e.client == nil {
		return
	}
	msg := fmt.Sprint(args...)
	if e.client.opts.EnableLogging {
		e.client.captureLogAsync(e.client.newLogRecord(e.level, msg, e.tags))
		return
	}
	e.client.CaptureMessage(e.ctx, msg, e.level, e.tags...)
}

// Emitf sends the log entry with a [fmt.Sprintf]-formatted message.
//
//	logger.Info().Emitf("Hello %v!", "world")
//
// When [Options.EnableLogging] is true the entry is stored as a [LogRecord]
// via POST /api/v1/logs (separate from the events table).
// When false, it falls through to [Client.CaptureMessage] for backward
// compatibility.
func (e *LogEntry) Emitf(format string, args ...any) {
	if e.client == nil {
		return
	}
	msg := fmt.Sprintf(format, args...)
	if e.client.opts.EnableLogging {
		e.client.captureLogAsync(e.client.newLogRecord(e.level, msg, e.tags))
		return
	}
	e.client.CaptureMessage(e.ctx, msg, e.level, e.tags...)
}
