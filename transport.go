package bikeeper

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"

	"github.com/bytedance/sonic"
)

// Transport is the interface for sending events to the Bikeeper server.
type Transport interface {
	Send(ctx context.Context, event *Event) error
	Flush(ctx context.Context)
}

type httpTransport struct {
	client   *http.Client
	endpoint string
	opts     *Options // pointer — mutations via SetFramework are visible at send time
}

func newHTTPTransport(opts *Options) Transport {
	return &httpTransport{
		client:   &http.Client{Timeout: opts.Timeout},
		endpoint: opts.Endpoint,
		opts:     opts,
	}
}

func (t *httpTransport) Send(ctx context.Context, event *Event) error {
	body, err := sonic.Marshal(event)
	if err != nil {
		return fmt.Errorf("bikeeper: marshaling event: %w", err)
	}

	url := t.endpoint + "/api/v1/ingest"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("bikeeper: creating request: %w", err)
	}

	if t.opts.Framework == "" {
		return fmt.Errorf("bikeeper: Framework not set — register bikeeperfiber.New or bikeeperecho.New middleware before sending events")
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Bikeeper-Client-ID", t.opts.ClientID)
	req.Header.Set("X-Bikeeper-Client-Secret", t.opts.ClientSecret)
	req.Header.Set("X-Bikeeper-SDK-Framework", t.opts.Framework)
	req.Header.Set("X-Bikeeper-Project-ID", t.opts.ProjectID)

	resp, err := t.client.Do(req)
	if err != nil {
		return fmt.Errorf("bikeeper: sending event: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		errBody, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("bikeeper: server returned %d: %s", resp.StatusCode, bytes.TrimSpace(errBody))
	}

	return nil
}

// Flush is a no-op for the HTTP transport (events are sent immediately).
func (t *httpTransport) Flush(_ context.Context) {}
