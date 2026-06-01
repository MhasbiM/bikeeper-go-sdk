package bikeeper_test

import (
	"runtime"
	"testing"
	"time"

	bikeeper "github.com/MhasbiM/bikeeper-go-sdk"
)

// TestClientClose_NoGoroutineLeak verifies that Client.Close() cleans up all
// background goroutines started by the SDK, leaving the goroutine count at or
// below the baseline.
//
// We allow a small delta (+3) to tolerate goroutines spawned by the Go testing
// runtime itself (e.g. finaliser goroutine, signal handling goroutine).
func TestClientClose_NoGoroutineLeak(t *testing.T) {
	t.Parallel()

	// Let the runtime settle before sampling the baseline.
	time.Sleep(50 * time.Millisecond)
	before := runtime.NumGoroutine()

	client := bikeeper.New(bikeeper.Options{
		ProjectID:    "test-project",
		ClientID:     "test-client",
		ClientSecret: "test-secret",
		Endpoint:     "http://127.0.0.1:1", // unreachable — ensures no sends succeed
	})

	// Close should flush and release any background goroutines.
	client.Close()

	// Give any lingering goroutines a moment to exit.
	time.Sleep(100 * time.Millisecond)
	after := runtime.NumGoroutine()

	// Allow a delta of 3 to absorb testing-runtime variance.
	const delta = 3
	if after > before+delta {
		t.Errorf("possible goroutine leak: goroutines before=%d after=%d (delta allowed=%d)",
			before, after, delta)
	}
}

// TestClientClose_FlushesInFlightEvents verifies that Close() waits for
// in-flight CaptureEventAsync goroutines to complete (or timeout) before returning.
// This is a behaviour test — we just ensure Close() does not panic or deadlock.
func TestClientClose_FlushesInFlightEvents(t *testing.T) {
	t.Parallel()

	client := bikeeper.New(bikeeper.Options{
		ProjectID:    "test-project",
		ClientID:     "test-client",
		ClientSecret: "test-secret",
		Endpoint:     "http://127.0.0.1:1",  // unreachable — send will timeout
		Timeout:      10 * time.Millisecond, // short timeout so test stays fast
		FlushTimeout: 200 * time.Millisecond,
	})

	hub := bikeeper.NewHub(client)

	// Fire several events asynchronously then wait for Close to flush them.
	for range 5 {
		hub.CaptureMessage("test message", bikeeper.LevelInfo)
	}
	_ = hub // ensure hub is not optimised away before the goroutines land

	// Close must return within a reasonable time (FlushTimeout + margin).
	done := make(chan struct{})
	go func() {
		client.Close()
		close(done)
	}()

	select {
	case <-done:
		// pass
	case <-time.After(2 * time.Second):
		t.Error("Client.Close() did not return within 2s — possible deadlock or hang")
	}
}
