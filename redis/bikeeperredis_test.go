package bikeeperredis_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	bikeeperredis "github.com/MhasbiM/bikeeper-go-sdk/redis"
	"github.com/redis/go-redis/v9"
)

// The span description must never carry a command's value or its reply —
// those hold whatever the application cached.
func TestHook_DescriptionKeepsKeyDropsValue(t *testing.T) {
	t.Parallel()
	hook := bikeeperredis.NewHook()

	tests := []struct {
		name string
		cmd  redis.Cmder
		want string
	}{
		{
			name: "read keeps the key",
			cmd:  redis.NewStringCmd(context.Background(), "get", "session:got-1"),
			want: "GET session:got-1",
		},
		{
			name: "write counts the value instead of showing it",
			cmd:  redis.NewStatusCmd(context.Background(), "set", "session:got-1", `{"name":"Budi","phone":"08123"}`, "EX", "300"),
			want: "SET session:got-1 (+3 args)",
		},
		{
			name: "no arguments",
			cmd:  redis.NewStringCmd(context.Background(), "ping"),
			want: "PING",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := bikeeperredis.Describe(hook, tt.cmd); got != tt.want {
				t.Errorf("description = %q, want %q", got, tt.want)
			}
			// Running the command through the hook proves the span path
			// itself is harmless without an active transaction.
			process := hook.ProcessHook(func(context.Context, redis.Cmder) error { return nil })
			if err := process(context.Background(), tt.cmd); err != nil {
				t.Fatalf("ProcessHook: %v", err)
			}
		})
	}
}

func TestHook_TruncatesLongKeys(t *testing.T) {
	t.Parallel()
	hook := bikeeperredis.NewHook(bikeeperredis.WithMaxKeyLength(10))
	got := bikeeperredis.Describe(hook, redis.NewStringCmd(context.Background(), "get", strings.Repeat("k", 50)))

	if !strings.HasSuffix(got, "…") {
		t.Errorf("a long key should be truncated, got %q", got)
	}
	if len(got) > len("GET ")+10+len("…") {
		t.Errorf("description = %q, longer than the configured cap", got)
	}
}

// A cache miss is an outcome, not a failure: the error still reaches the
// caller, and the hook must not swallow real errors either.
func TestHook_PassesErrorsThrough(t *testing.T) {
	t.Parallel()
	hook := bikeeperredis.NewHook()
	want := errors.New("connection refused")

	process := hook.ProcessHook(func(context.Context, redis.Cmder) error { return want })
	if got := process(context.Background(), redis.NewStringCmd(context.Background(), "get", "k")); !errors.Is(got, want) {
		t.Errorf("error = %v, want %v", got, want)
	}

	pipeline := hook.ProcessPipelineHook(func(context.Context, []redis.Cmder) error { return redis.Nil })
	if got := pipeline(context.Background(), nil); !errors.Is(got, redis.Nil) {
		t.Errorf("error = %v, want redis.Nil", got)
	}
}

// Outside any transaction — a background job with no hub, or monitoring
// switched off — the hook must run the command and nothing else.
func TestHook_WithoutActiveTransactionDoesNothing(t *testing.T) {
	// Not parallel: AllocsPerRun needs the process to itself.
	hook := bikeeperredis.NewHook()
	ctx := context.Background()
	process := hook.ProcessHook(func(context.Context, redis.Cmder) error { return nil })
	cmd := redis.NewStringCmd(ctx, "get", "session:got-1")

	if allocs := testing.AllocsPerRun(100, func() {
		_ = process(ctx, cmd)
	}); allocs != 0 {
		t.Errorf("allocations per command = %v, want 0 when tracing is off", allocs)
	}
}
