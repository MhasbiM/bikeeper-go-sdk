// Package bikeeperredis traces go-redis commands with the Bikeeper SDK.
//
// Install the hook once on the client and every command — from any call site,
// through any layer — is wrapped in an APM span attached to whatever Bikeeper
// transaction is active on the caller's context:
//
//	rdb := redis.NewClient(opts)
//	rdb.AddHook(bikeeperredis.NewHook())
//
// Spans are a complete no-op when no transaction is active, so this is always
// safe to install.
package bikeeperredis

import (
	"context"
	"fmt"
	"strings"

	bikeeper "github.com/MhasbiM/bikeeper-go-sdk"
	"github.com/redis/go-redis/v9"
)

// defaultMaxKeyLength bounds how much of a key is recorded in span data.
const defaultMaxKeyLength = 160

// Hook is a [redis.Hook] that wraps every command and pipeline in a Bikeeper
// span. Use [NewHook] to build one.
//
// Command values and replies are deliberately never recorded — a cache holds
// whatever the application put in it, which is usually the most personal data
// the system has.
type Hook struct {
	maxKeyLength int
}

// Option configures a [Hook].
type Option func(*Hook)

// WithMaxKeyLength caps how much of a command's key is recorded. Defaults to
// 160 characters; a value <= 0 restores that default.
func WithMaxKeyLength(n int) Option {
	return func(h *Hook) {
		if n <= 0 {
			n = defaultMaxKeyLength
		}
		h.maxKeyLength = n
	}
}

// NewHook returns a hook ready for redis.Client.AddHook.
func NewHook(opts ...Option) *Hook {
	h := &Hook{maxKeyLength: defaultMaxKeyLength}
	for _, opt := range opts {
		opt(h)
	}
	return h
}

var _ redis.Hook = (*Hook)(nil)

// DialHook is a pass-through: connection setup happens outside any request's
// span and says nothing useful about application latency.
func (h *Hook) DialHook(next redis.DialHook) redis.DialHook { return next }

// ProcessHook wraps a single command (GET, SET, SETNX, DEL, TTL, …).
//
// redis.Nil (key not found) is a routine cache-miss outcome, not a failure, so
// it does not mark the span as errored.
func (h *Hook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if !tracing(ctx) {
			return next(ctx, cmd)
		}
		span := bikeeper.StartSpan(ctx, "redis."+cmd.Name())
		err := next(ctx, cmd)
		span.SetData("redis.command", h.describe(cmd))
		finish(span, err)
		return err
	}
}

// ProcessPipelineHook wraps a batched call as one span covering every command
// it contains, rather than one span per command — a pipeline's whole point is
// a single round trip, so that is the meaningful unit here.
func (h *Hook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		if !tracing(ctx) {
			return next(ctx, cmds)
		}
		span := bikeeper.StartSpan(ctx, "redis.pipeline")
		span.SetData("redis.command_count", len(cmds))
		err := next(ctx, cmds)
		finish(span, err)
		return err
	}
}

// tracing reports whether a span started on ctx could ever be sent: either it
// nests inside one already being recorded, or ctx carries a hub that owns a
// transaction.
//
// Without this check every command on an uninstrumented path — a background
// job with no transaction, or a process with monitoring switched off entirely
// — would allocate a span, an ID pair and two maps that Finish then drops.
// Cache calls are among the hottest paths an application has.
func tracing(ctx context.Context) bool {
	return bikeeper.SpanFromContext(ctx) != nil || bikeeper.HasHub(ctx)
}

func finish(span *bikeeper.Span, err error) {
	if err != nil && err != redis.Nil {
		span.SetStatus(bikeeper.SpanStatusInternalError)
		span.SetData("redis.error", err.Error())
	}
	span.Finish()
}

// describe renders a command as its name and key — never its value, never its
// reply.
//
// Cmder.String() is the obvious choice and the wrong one: it appends the
// command's result, so a cache GET ships back whatever the application stored
// — sessions, user profiles, rendered pages — to the monitoring backend, on
// every single call. Write commands are no better, since the value being
// stored sits in the arguments. Only the first argument, the key, is kept; the
// rest are reduced to a count, which is all a waterfall view needs.
func (h *Hook) describe(cmd redis.Cmder) string {
	args := cmd.Args()
	if len(args) < 2 {
		return strings.ToUpper(cmd.Name())
	}

	key := fmt.Sprint(args[1])
	if len(key) > h.maxKeyLength {
		key = key[:h.maxKeyLength] + "…"
	}

	desc := strings.ToUpper(cmd.Name()) + " " + key
	if extra := len(args) - 2; extra > 0 {
		desc += fmt.Sprintf(" (+%d args)", extra)
	}
	return desc
}
