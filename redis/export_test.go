package bikeeperredis

import "github.com/redis/go-redis/v9"

// Describe exposes the span description for tests in the external test
// package, where the recorded value is otherwise only observable through a
// live transaction.
func Describe(h *Hook, cmd redis.Cmder) string { return h.describe(cmd) }
