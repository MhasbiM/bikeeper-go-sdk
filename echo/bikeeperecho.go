// Package bikeeperecho provides a Bikeeper error-monitoring middleware for
// the Echo web framework (labstack/echo v4).
//
// Usage:
//
//	client := bikeeper.New(bikeeper.Options{
//	    ClientID:     "your-client-id",
//	    ClientSecret: "your-client-secret",
//	    Endpoint:     "http://your-bikeeper-host:8080",
//	})
//
//	e := echo.New()
//	e.Use(middleware.Recover())                               // Echo's own recover middleware
//	e.Use(bikeeperecho.New(client, bikeeperecho.Options{     // Bikeeper middleware
//	    Repanic: true,                                        // let Echo handle the 500 response
//	}))
//
// The middleware automatically:
//   - Stores the client on every echo.Context (retrieve with GetClientFromContext)
//   - Recovers from handler panics and captures them as Fatal events
//   - Captures HTTP 5xx errors returned by handlers (unless disabled)
//
// Capture additional errors manually inside any handler:
//
//	func myHandler(c echo.Context) error {
//	    if err := doSomething(); err != nil {
//	        bikeeperecho.GetClientFromContext(c).CaptureException(c.Request().Context(), err)
//	        return echo.ErrInternalServerError
//	    }
//	    return nil
//	}
package bikeeperecho

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	bikeeper "github.com/MhasbiM/bikeeper-go-sdk"
	"github.com/labstack/echo/v4"
)

// clientKey is the key used to store the Bikeeper client on an echo.Context.
const clientKey = "bikeeper_client"

// Options configures the Bikeeper Echo middleware.
type Options struct {
	// Repanic controls whether the middleware re-panics after capturing a panic.
	//
	// Recommended: set to true when using Echo's own middleware.Recover() so that
	// Echo can still write the 500 HTTP response while Bikeeper captures the event.
	// Set to false if Bikeeper is the only recovery middleware.
	Repanic bool

	// DisableInternalErrorCapture prevents the middleware from automatically
	// capturing HTTP 5xx errors returned by handlers. Defaults to false
	// (i.e. internal errors ARE captured by default).
	DisableInternalErrorCapture bool
}

// New returns an Echo middleware that automatically captures panics and 5xx
// errors and sends them to Bikeeper asynchronously.
//
// Placement: register this middleware AFTER Echo's own middleware.Recover()
// (i.e. add Bikeeper's middleware last with Use()) so the panic propagates
// inward to Bikeeper first, then outward to Echo's recovery.
func New(client *bikeeper.Client, opts Options) echo.MiddlewareFunc {
	if client == nil {
		panic("bikeeperecho: client must not be nil")
	}
	if f := client.Framework(); f != "" && f != "echo" {
		panic(fmt.Sprintf("bikeeperecho: client is configured for framework %q — use the fiber middleware package instead", f))
	}
	// Auto-detect: stamp the client with "echo" so the transport sends the
	// correct X-Bikeeper-SDK-Framework header without the caller having to set
	// Options.Framework manually.
	client.SetFramework("echo")
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) (retErr error) {
			// Make the client available to downstream handlers.
			c.Set(clientKey, client)

			req := c.Request()

			// Panic recovery — must run even if c.Next() panics.
			defer func() {
				if r := recover(); r != nil {
					panicErr := toError(r)
					captureHTTPEvent(client, c, bikeeper.LevelFatal, panicErr, http.StatusInternalServerError)
					if opts.Repanic {
						panic(r)
					}
					// Respond with 500 when not repanicin; Echo won't have a chance
					// to do it on its own since we swallowed the panic.
					retErr = echo.ErrInternalServerError
				}
			}()

			// Call downstream handlers.
			err := next(c)

			// Optionally capture 5xx errors returned by handlers.
			if !opts.DisableInternalErrorCapture && err != nil {
				status := errorStatus(err)
				if status >= http.StatusInternalServerError {
					captureHTTPEvent(client, c, bikeeper.LevelError, err, status)
				}
			}

			// Suppress unused variable warning — req is no longer used directly
			// after refactoring to captureHTTPEvent, but keep for clarity.
			_ = req
			return err
		}
	}
}

// GetClientFromContext retrieves the Bikeeper client stored by the middleware.
// Returns nil if the middleware is not registered on this route.
func GetClientFromContext(c echo.Context) *bikeeper.Client {
	v := c.Get(clientKey)
	if v == nil {
		return nil
	}
	if cl, ok := v.(*bikeeper.Client); ok {
		return cl
	}
	return nil
}

// errorStatus returns the HTTP status code for a handler error.
// Echo's *echo.HTTPError carries the code; all other errors default to 500.
func errorStatus(err error) int {
	// Go 1.26+: errors.AsType[T] returns the typed value directly without
	// requiring a pre-declared target variable.
	if he, ok := errors.AsType[*echo.HTTPError](err); ok {
		return he.Code
	}
	return http.StatusInternalServerError
}

// toError converts a recovered panic value to an error.
func toError(r any) error {
	if err, ok := r.(error); ok {
		return err
	}
	return fmt.Errorf("panic: %v", r)
}

// buildTags returns Bikeeper tags populated with HTTP request metadata.
func buildTags(method, path, ip, userAgent string, status int) []bikeeper.Tag {
	tags := []bikeeper.Tag{
		{Key: "http.method", Value: method},
		{Key: "http.path", Value: path},
		{Key: "http.status_code", Value: strconv.Itoa(status)},
	}
	if ip != "" {
		tags = append(tags, bikeeper.Tag{Key: "http.ip", Value: ip})
	}
	if userAgent != "" {
		tags = append(tags, bikeeper.Tag{Key: "http.user_agent", Value: userAgent})
	}
	return tags
}

// sensitiveHeaders lists lowercase header keys that must never be sent to
// the Bikeeper ingest endpoint to prevent credential / session leakage.
var sensitiveHeaders = map[string]bool{
	"authorization":            true,
	"cookie":                   true,
	"set-cookie":               true,
	"x-bikeeper-client-secret": true,
}

// buildHTTPRequest snapshots the Echo request into a bikeeper.HTTPRequest.
// Sensitive headers are stripped before the snapshot is attached to the event.
func buildHTTPRequest(c echo.Context) *bikeeper.HTTPRequest {
	req := c.Request()
	headers := make(map[string]string)
	for k, vals := range req.Header {
		if !sensitiveHeaders[strings.ToLower(k)] {
			headers[k] = strings.Join(vals, ", ")
		}
	}
	qs := req.URL.RawQuery
	return &bikeeper.HTTPRequest{
		Method:      req.Method,
		URL:         req.URL.RequestURI(),
		QueryString: qs,
		Headers:     headers,
		Env: map[string]string{
			"REMOTE_ADDR": c.RealIP(),
			"SERVER_NAME": req.Host,
		},
	}
}

// captureHTTPEvent builds a full bikeeper.Event with HTTP request context and
// dispatches it asynchronously via CaptureEventAsync.
func captureHTTPEvent(client *bikeeper.Client, c echo.Context, level bikeeper.Level, err error, status int) {
	req := c.Request()
	tags := buildTags(req.Method, req.URL.Path, c.RealIP(), req.UserAgent(), status)
	evt := bikeeper.NewEvent(level, err.Error(), tags...)
	// Build absolute URL so callers see the full endpoint, not just the path.
	scheme := "http"
	if req.TLS != nil {
		scheme = "https"
	}
	evt.URL = scheme + "://" + req.Host + req.URL.RequestURI()
	evt.HTTPRequest = buildHTTPRequest(c)
	client.CaptureEventAsync(evt)
}

// CaptureMessage captures a message with full HTTP context from the current
// Echo request. URL and HTTP request snapshot are attached automatically.
// Use this instead of client.CaptureMessage inside Echo handlers.
func CaptureMessage(c echo.Context, level bikeeper.Level, message string, tags ...bikeeper.Tag) {
	client := GetClientFromContext(c)
	if client == nil {
		return
	}
	req := c.Request()
	evt := bikeeper.NewEvent(level, message, tags...)
	scheme := "http"
	if req.TLS != nil {
		scheme = "https"
	}
	evt.URL = scheme + "://" + req.Host + req.URL.RequestURI()
	evt.HTTPRequest = buildHTTPRequest(c)
	client.CaptureEventAsync(evt)
}

// CaptureException captures an error with full HTTP context from the current
// Echo request. URL and HTTP request snapshot are attached automatically.
// Use this instead of client.CaptureException inside Echo handlers.
func CaptureException(c echo.Context, err error, tags ...bikeeper.Tag) {
	if err == nil {
		return
	}
	client := GetClientFromContext(c)
	if client == nil {
		return
	}
	req := c.Request()
	evt := bikeeper.NewEvent(bikeeper.LevelError, err.Error(), tags...)
	scheme := "http"
	if req.TLS != nil {
		scheme = "https"
	}
	evt.URL = scheme + "://" + req.Host + req.URL.RequestURI()
	evt.HTTPRequest = buildHTTPRequest(c)
	client.CaptureEventAsync(evt)
}
