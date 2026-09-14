package bikeeperpgx

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/jackc/pgx/v5/tracelog"
)

// sqlcQueryName matches the "-- name: CheckSelfOrderEnabled :one" header sqlc
// writes above every generated query, so a failure can be named instead of
// being identified by its full SQL text.
var sqlcQueryName = regexp.MustCompile(`--\s*name:\s*(\w+)`)

// NewLogger wraps a [tracelog.Logger] and rewrites the bare message pgx emits
// for a failed operation ("Query", "Prepare", "Connect", …) into one that says
// what actually broke:
//
//	db query failed: CheckSelfOrderEnabled: ERROR: column "use_self_order" does not exist (SQLSTATE 42703)
//
// pgx puts all the useful detail in the log's structured data (sql, args, err)
// and leaves the message as a bare verb. That reads fine in a log viewer where
// the fields sit next to it — but an error monitor derives an event's title
// from the message alone, so every failing query in the system arrives as an
// indistinguishable "Query", with the cause buried in a tag panel.
//
// The data map is passed through untouched, so the fields a log sink (or a tag
// panel) shows are unchanged.
//
//	tracelog.TraceLog{
//	    LogLevel: tracelog.LogLevelWarn,
//	    Logger:   bikeeperpgx.NewLogger(pgxzap.NewLogger(log)),
//	}
func NewLogger(next tracelog.Logger) tracelog.Logger {
	return &logger{next: next}
}

type logger struct {
	next tracelog.Logger
}

func (l *logger) Log(ctx context.Context, level tracelog.LogLevel, msg string, data map[string]any) {
	if level == tracelog.LogLevelError {
		msg = describeFailure(msg, data)
	}
	l.next.Log(ctx, level, msg, data)
}

// describeFailure builds "db <operation> failed[: <query name>]: <error>",
// falling back to the original message for entries that carry no error.
func describeFailure(msg string, data map[string]any) string {
	err, ok := data["err"].(error)
	if !ok || err == nil {
		return msg
	}

	parts := []string{fmt.Sprintf("db %s failed", strings.ToLower(msg))}
	if name := queryName(data); name != "" {
		parts = append(parts, name)
	}
	parts = append(parts, err.Error())
	return strings.Join(parts, ": ")
}

// queryName returns the sqlc name of the statement in data, or "" for SQL that
// did not come from sqlc (migrations, ad-hoc queries) and for operations that
// carry no SQL at all (Connect, Acquire).
func queryName(data map[string]any) string {
	sql, _ := data["sql"].(string)
	if match := sqlcQueryName.FindStringSubmatch(sql); match != nil {
		return match[1]
	}
	return ""
}
