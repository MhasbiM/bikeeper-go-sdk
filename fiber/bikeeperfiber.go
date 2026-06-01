// Package bikeeperfiber provides a Bikeeper error-monitoring middleware for
// the Fiber web framework (gofiber/fiber v3).
//
// Usage:
//
//	client := bikeeper.New(bikeeper.Options{
//	    ClientID:     "your-client-id",
//	    ClientSecret: "your-client-secret",
//	    Endpoint:     "http://your-bikeeper-host:8080",
//	})
//
//	app := fiber.New()
//	app.Use(recover.New())                                     // Fiber's own recover middleware
//	app.Use(bikeeperfiber.New(client, bikeeperfiber.Options{   // Bikeeper middleware
//	    Repanic: true,                                          // let Fiber's recover handle the 500
//	}))
//
// The middleware automatically:
//   - Stores the client on every fiber.Ctx via Locals (retrieve with GetClientFromContext)
//   - Recovers from handler panics and captures them as Fatal events
//   - Captures HTTP 5xx errors returned by handlers (unless disabled)
//   - Captures committed 5xx responses written directly (e.g. c.Status(500).SendString("..."))
//
// Capture additional errors manually inside any handler:
//
//	func myHandler(c fiber.Ctx) error {
//	    if err := doSomething(); err != nil {
//	        bikeeperfiber.GetClientFromContext(c).CaptureException(c.Context(), err)
//	        return fiber.ErrInternalServerError
//	    }
//	    return nil
//	}
package bikeeperfiber

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	bikeeper "github.com/MhasbiM/bikeeper-go-sdk"
	"github.com/gofiber/fiber/v3"
)

// clientKey is the key used to store the Bikeeper client via fiber.Ctx.Locals.
const clientKey = "bikeeper_client"

// hubKey is the key used to store the per-request Hub via fiber.Ctx.Locals.
const hubKey = "bikeeper_hub"

// capturedKey is the key used to signal that the handler has already captured
// this request's error manually. When set to true, the middleware skips its
// automatic 5xx response capture so no duplicate events are sent.
const capturedKey = "bikeeper_captured"

// Options configures the Bikeeper Fiber middleware.
type Options struct {
	// Repanic controls whether the middleware re-panics after capturing a panic.
	//
	// Recommended: set to true when using Fiber's own recover middleware so that
	// Fiber can write the 500 HTTP response while Bikeeper captures the event.
	// Set to false if Bikeeper is the only recovery middleware.
	Repanic bool

	// DisableInternalErrorCapture prevents the middleware from automatically
	// capturing HTTP 5xx errors and responses. Defaults to false
	// (i.e. internal errors ARE captured by default).
	DisableInternalErrorCapture bool
}

// New returns a Fiber middleware handler that automatically captures panics
// and 5xx errors/responses and sends them to Bikeeper asynchronously.
//
// Placement: register this middleware AFTER Fiber's own recover middleware
// (i.e. call app.Use(bikeeperfiber.New(...)) last) so the panic flows inward
// to Bikeeper first, then outward to Fiber's recovery.
func New(client *bikeeper.Client, opts Options) fiber.Handler {
	if client == nil {
		panic("bikeeperfiber: client must not be nil")
	}
	if f := client.Framework(); f != "" && f != "fiber" {
		panic(fmt.Sprintf("bikeeperfiber: client is configured for framework %q — use the echo middleware package instead", f))
	}
	// Auto-detect: stamp the client with "fiber" so the transport sends the
	// correct X-Bikeeper-SDK-Framework header without the caller having to set
	// Options.Framework manually.
	client.SetFramework("fiber")
	return func(c fiber.Ctx) (retErr error) {
		// Create a per-request Hub and make both the hub and client available to
		// downstream handlers. Handlers should prefer GetHubFromContext so they
		// get trace-aware capture; GetClientFromContext is kept for back-compat.
		hub := bikeeper.NewHub(client)
		c.Locals(clientKey, client)
		c.Locals(hubKey, hub)

		// Snapshot the HTTP request and parsed UA into the hub's scope immediately
		// so that any hub.CaptureException / hub.CaptureMessage call from a handler
		// or service layer automatically includes URL, http_request, browser, and
		// OS context — even when calling hub methods directly.
		{
			ua := c.Get(fiber.HeaderUserAgent)
			secCHUA := c.Get("Sec-CH-UA")
			bName, bVersion, osName := parseUserAgent(ua, secCHUA)
			switch bName {
			case "Google Chrome":
				bName = "Chrome"
			case "Microsoft Edge":
				bName = "Edge"
			}
			hub.Scope().SetHTTPContext(absoluteURL(c), buildHTTPRequest(c), bName, bVersion, osName)
		}

		// Panic recovery — covers panics that propagate through c.Next().
		defer func() {
			if r := recover(); r != nil {
				panicErr := toError(r)
				capturePanicEvent(client, c, panicErr)
				if opts.Repanic {
					panic(r)
				}
				// Write 500 and surface as an error so Fiber's error handler runs.
				_ = c.Status(http.StatusInternalServerError).SendString(http.StatusText(http.StatusInternalServerError))
				retErr = fiber.ErrInternalServerError
			}
		}()

		// Call downstream handlers.
		err := c.Next()

		// Capture 5xx errors returned by handlers (e.g. fiber.ErrInternalServerError).
		// Skip when the handler already captured the event manually via MarkCaptured.
		alreadyCaptured, _ := c.Locals(capturedKey).(bool)
		if !opts.DisableInternalErrorCapture && !alreadyCaptured {
			if err != nil {
				status := fiberErrorStatus(err)
				if status >= http.StatusInternalServerError {
					captureHTTPEvent(client, c, bikeeper.LevelError, err, status)
				}
			} else {
				// Handler returned nil but may have committed a 5xx response directly.
				status := c.Response().StatusCode()
				if status >= http.StatusInternalServerError {
					captureErr := fmt.Errorf("HTTP %d: %s", status, http.StatusText(status))
					captureHTTPEvent(client, c, bikeeper.LevelError, captureErr, status)
				}
			}
		}

		return err
	}
}

// MarkCaptured signals to the middleware that this request's error has already
// been captured manually by the handler. When called, the middleware skips its
// automatic 5xx response capture so no duplicate events are sent to Bikeeper.
//
// Call this immediately after a manual hub.CaptureException / hub.CaptureMessage
// and before returning the HTTP response:
//
//	hub.CaptureException(err)
//	bikeeperfiber.MarkCaptured(c)
//	return c.Status(fiber.StatusInternalServerError).JSON(...)
func MarkCaptured(c fiber.Ctx) {
	c.Locals(capturedKey, true)
}

// GetHubFromContext retrieves the per-request Hub stored by the middleware.
// Use the Hub for trace-aware event capture:
//
//	hub := bikeeperfiber.GetHubFromContext(c)
//	hub.WithScope(func(s *bikeeper.Scope) { s.SetTag("user.id", uid) })
//
// Returns nil if the middleware is not registered on this route.
func GetHubFromContext(c fiber.Ctx) *bikeeper.Hub {
	v := c.Locals(hubKey)
	if v == nil {
		return nil
	}
	if h, ok := v.(*bikeeper.Hub); ok {
		return h
	}
	return nil
}

// GetClientFromContext retrieves the Bikeeper client stored by the middleware.
// Returns nil if the middleware is not registered on this route.
func GetClientFromContext(c fiber.Ctx) *bikeeper.Client {
	v := c.Locals(clientKey)
	if v == nil {
		return nil
	}
	if cl, ok := v.(*bikeeper.Client); ok {
		return cl
	}
	return nil
}

// fiberErrorStatus returns the HTTP status code for a Fiber handler error.
// *fiber.Error carries the code; all other errors default to 500.
func fiberErrorStatus(err error) int {
	if fe, ok := errors.AsType[*fiber.Error](err); ok {
		return fe.Code
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

// buildHTTPRequest snapshots the Fiber request into a bikeeper.HTTPRequest.
// Sensitive headers are stripped before the snapshot is attached to the event.
func buildHTTPRequest(c fiber.Ctx) *bikeeper.HTTPRequest {
	headers := make(map[string]string)
	c.Request().Header.VisitAll(func(key, val []byte) {
		k := strings.ToLower(string(key))
		if !sensitiveHeaders[k] {
			headers[string(key)] = string(val)
		}
	})
	return &bikeeper.HTTPRequest{
		Method:      c.Method(),
		URL:         c.OriginalURL(),
		QueryString: string(c.Request().URI().QueryString()),
		Headers:     headers,
		Env: map[string]string{
			"REMOTE_ADDR": c.IP(),
			"SERVER_NAME": c.Hostname(),
		},
	}
}

// requestScheme returns "https" when the connection is TLS, "http" otherwise.
// c.Protocol() returns the HTTP version string (e.g. "HTTP/1.1") in fasthttp,
// not the scheme, so we use c.Secure() which Fiber provides for TLS detection.
func requestScheme(c fiber.Ctx) string {
	if c.Secure() {
		return "https"
	}
	return "http"
}

// absoluteURL builds "scheme://host:port/path?query" for the current request.
// c.Host() returns the Host header value which includes the port (e.g. localhost:3000).
func absoluteURL(c fiber.Ctx) string {
	return requestScheme(c) + "://" + c.Host() + c.OriginalURL()
}

// captureHTTPEvent builds a full bikeeper.Event with HTTP request context and
// dispatches it asynchronously via CaptureEventAsync.
func captureHTTPEvent(client *bikeeper.Client, c fiber.Ctx, level bikeeper.Level, err error, status int) {
	tags := buildTags(c.Method(), c.Path(), c.IP(), c.Get(fiber.HeaderUserAgent), status)
	evt := bikeeper.NewEvent(level, err.Error(), tags...)
	enrichWithHTTPContext(evt, c)
	client.CaptureEventAsync(evt)
}

// capturePanicEvent builds an event with a panic stacktrace and dispatches it.
func capturePanicEvent(client *bikeeper.Client, c fiber.Ctx, err error) {
	tags := buildTags(c.Method(), c.Path(), c.IP(), c.Get(fiber.HeaderUserAgent), http.StatusInternalServerError)
	evt := bikeeper.NewEvent(bikeeper.LevelFatal, err.Error(), tags...)
	enrichWithHTTPContext(evt, c)
	// +3: capturePanicEvent → captureHTTPEvent skipped, but we're not in that path;
	// skip: capturePanicEvent + buildPanicExceptionValue + captureStacktrace
	ex := bikeeper.BuildPanicExceptionValue(err, 3)
	evt.Exception = ex
	if ex.Stacktrace != nil {
		evt.Fingerprint = []string{
			bikeeper.ComputeAllFramesGroupingHash(ex.Type, ex.Stacktrace.Frames),
			bikeeper.ComputeGroupingHash(ex.Type, ex.Stacktrace.Frames),
		}
	}
	client.CaptureEventAsync(evt)
}

// CaptureMessage captures a message with full HTTP context from the current
// Fiber request. URL, HTTP request snapshot, and browser/OS context are attached
// automatically. A stacktrace is captured at the call site and attached as
// exception data so the call-site frame appears in the Bikeeper dashboard.
// Use this instead of client.CaptureMessage inside Fiber handlers.
func CaptureMessage(c fiber.Ctx, level bikeeper.Level, message string, tags ...bikeeper.Tag) {
	client := GetClientFromContext(c)
	if client == nil {
		return
	}
	evt := bikeeper.NewEvent(level, message, tags...)
	enrichWithHTTPContext(evt, c)
	// skip=1: bikeeperfiber.CaptureMessage itself is omitted; handler is first visible frame.
	ex := bikeeper.BuildMessageExceptionValue(message, 1)
	evt.Exception = ex
	if ex.Stacktrace != nil {
		evt.Fingerprint = []string{
			bikeeper.ComputeAllFramesGroupingHash(ex.Type, ex.Stacktrace.Frames),
			bikeeper.ComputeGroupingHash(ex.Type, ex.Stacktrace.Frames),
		}
	}
	client.CaptureEventAsync(evt)
}

// CaptureException captures an error with full HTTP context from the current
// Fiber request. URL, HTTP request snapshot, browser/OS context, and a full Go
// stack trace are attached automatically. Use this instead of client.CaptureException
// inside Fiber handlers.
func CaptureException(c fiber.Ctx, err error, tags ...bikeeper.Tag) {
	if err == nil {
		return
	}
	client := GetClientFromContext(c)
	if client == nil {
		return
	}
	evt := bikeeper.NewEvent(bikeeper.LevelError, err.Error(), tags...)
	enrichWithHTTPContext(evt, c)
	ex := bikeeper.BuildExceptionValue(err, 1) // skip: bikeeperfiber.CaptureException itself; user handler is the first captured frame
	evt.Exception = ex
	if ex.Stacktrace != nil {
		evt.Fingerprint = []string{
			bikeeper.ComputeAllFramesGroupingHash(ex.Type, ex.Stacktrace.Frames),
			bikeeper.ComputeGroupingHash(ex.Type, ex.Stacktrace.Frames),
		}
	}
	client.CaptureEventAsync(evt)
}

// enrichWithHTTPContext populates evt.URL, evt.HTTPRequest, and derives
// browser / client-OS context from the User-Agent and Sec-CH-UA headers.
// It is shared by captureHTTPEvent, CaptureMessage, and CaptureException so
// that all capture paths produce a consistent, richly-annotated event.
func enrichWithHTTPContext(evt *bikeeper.Event, c fiber.Ctx) {
	evt.URL = absoluteURL(c)
	evt.HTTPRequest = buildHTTPRequest(c)

	ua := c.Get(fiber.HeaderUserAgent)
	secCHUA := c.Get("Sec-CH-UA")

	browserName, browserVersion, clientOSName := parseUserAgent(ua, secCHUA)

	// Normalise verbose Sec-CH-UA brand names to short display names.
	switch browserName {
	case "Google Chrome":
		browserName = "Chrome"
	case "Microsoft Edge":
		browserName = "Edge"
	}

	if browserName != "" {
		combined := browserName
		if browserVersion != "" {
			combined = browserName + " " + browserVersion
		}
		evt.Tags = append(evt.Tags,
			bikeeper.Tag{Key: "browser", Value: combined},
			bikeeper.Tag{Key: "browser.name", Value: browserName},
		)
		if browserVersion != "" {
			evt.Tags = append(evt.Tags, bikeeper.Tag{Key: "browser.version", Value: browserVersion})
		}
		if evt.Contexts == nil {
			evt.Contexts = &bikeeper.Contexts{}
		}
		evt.Contexts.Browser = &bikeeper.BrowserInfo{Name: browserName, Version: browserVersion}
	}

	if clientOSName != "" {
		evt.Tags = append(evt.Tags,
			bikeeper.Tag{Key: "client_os", Value: clientOSName},
			bikeeper.Tag{Key: "client_os.name", Value: clientOSName},
		)
		if evt.Contexts == nil {
			evt.Contexts = &bikeeper.Contexts{}
		}
		evt.Contexts.ClientOS = &bikeeper.OSInfo{Name: clientOSName}
	}
}

// parseUserAgent parses the User-Agent string and optional Sec-CH-UA client-hint
// header to extract the browser name, major version, and client OS name.
// Handles Chromium-family browsers (Chrome, Brave, Edge, Opera), Firefox, and
// Safari, plus the most common desktop and mobile operating systems.
// No external dependencies — uses string operations only.
func parseUserAgent(userAgent, secCHUA string) (browserName, browserVersion, clientOSName string) {
	// ── Browser: prefer Sec-CH-UA brands (Chromium-family, more accurate) ──
	// Brave, Edge, and Opera all include "Google Chrome" in their brand list,
	// so we must scan ALL brands and prioritise the most-specific one.
	// Priority: Brave > Opera > Edge > Google Chrome > (any other non-noise brand)
	if secCHUA != "" {
		type brandEntry struct{ name, version string }
		var brands []brandEntry
		for _, part := range strings.Split(secCHUA, ",") {
			part = strings.TrimSpace(part)
			bStart := strings.Index(part, `"`)
			if bStart < 0 {
				continue
			}
			bEnd := strings.Index(part[bStart+1:], `"`)
			if bEnd < 0 {
				continue
			}
			brand := part[bStart+1 : bStart+1+bEnd]
			// Skip generic noise brands
			if strings.Contains(brand, "Not") || brand == "Chromium" {
				continue
			}
			vIdx := strings.Index(part, `v="`)
			if vIdx >= 0 {
				vRest := part[vIdx+3:]
				if vEnd := strings.Index(vRest, `"`); vEnd >= 0 {
					brands = append(brands, brandEntry{brand, vRest[:vEnd]})
				}
			}
		}
		// Prioritise specific browsers over the generic "Google Chrome" entry.
		priority := func(name string) int {
			switch name {
			case "Brave":
				return 4
			case "Opera", "OPR":
				return 3
			case "Microsoft Edge":
				return 2
			case "Google Chrome":
				return 1
			default:
				return 0
			}
		}
		best := -1
		for i, b := range brands {
			if best < 0 || priority(b.name) > priority(brands[best].name) {
				best = i
			}
		}
		if best >= 0 {
			browserName = brands[best].name
			browserVersion = brands[best].version
		}
	}

	// ── Browser: fallback to classic UA-string detection ────────────────────
	if browserName == "" {
		switch {
		case strings.Contains(userAgent, "Firefox/"):
			browserName = "Firefox"
			browserVersion = uaMajorVersion(userAgent, "Firefox/")
		case strings.Contains(userAgent, "Edg/"):
			browserName = "Edge"
			browserVersion = uaMajorVersion(userAgent, "Edg/")
		case strings.Contains(userAgent, "OPR/"):
			browserName = "Opera"
			browserVersion = uaMajorVersion(userAgent, "OPR/")
		case strings.Contains(userAgent, "Chrome/"):
			browserName = "Chrome"
			browserVersion = uaMajorVersion(userAgent, "Chrome/")
		case strings.Contains(userAgent, "Version/") && strings.Contains(userAgent, "Safari/"):
			browserName = "Safari"
			browserVersion = uaMajorVersion(userAgent, "Version/")
		}
	}

	// ── Client OS: derived from the platform section of the UA string ───────
	switch {
	case strings.Contains(userAgent, "iPhone") || strings.Contains(userAgent, "iPad"):
		clientOSName = "iOS"
	case strings.Contains(userAgent, "Android"):
		clientOSName = "Android"
	case strings.Contains(userAgent, "Macintosh") || strings.Contains(userAgent, "Mac OS X"):
		clientOSName = "macOS"
	case strings.Contains(userAgent, "Windows NT"):
		clientOSName = "Windows"
	case strings.Contains(userAgent, "CrOS"):
		clientOSName = "Chrome OS"
	case strings.Contains(userAgent, "Linux"):
		clientOSName = "Linux"
	}
	return
}

// uaMajorVersion extracts the major version number after prefix in the UA string.
// Example: uaMajorVersion("Chrome/147.0.0.0 Safari/537.36", "Chrome/") → "147"
func uaMajorVersion(ua, prefix string) string {
	idx := strings.Index(ua, prefix)
	if idx < 0 {
		return ""
	}
	rest := ua[idx+len(prefix):]
	for i, ch := range rest {
		if ch == '.' || ch == ' ' || ch == ';' {
			return rest[:i]
		}
	}
	return rest
}
