// Package bikeeper provides a Go SDK for the Bikeeper error-monitoring platform.
//
// Usage:
//
//	client := bikeeper.New(bikeeper.Options{
//	    ClientID:     "your-client-id",
//	    ClientSecret: "your-client-secret",
//	    Endpoint:     "https://your-bikeeper-instance.com",
//	})
//
//	client.CaptureException(ctx, err)
//	client.CaptureMessage(ctx, "something happened", bikeeper.LevelInfo)
package bikeeper

import (
	"context"
	"net"
	"os"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

// Level represents the severity of an event.
type Level string

const (
	LevelDebug   Level = "debug"
	LevelInfo    Level = "info"
	LevelWarning Level = "warning"
	LevelError   Level = "error"
	LevelFatal   Level = "fatal"
)

// sdkName and sdkVersion identify this SDK in the sdk field of every event.
const sdkName = "bikeeper-go"
const sdkVersion = "1.0.0"

// Client is the Bikeeper SDK client.
type Client struct {
	opts      Options
	transport Transport
	wg        sync.WaitGroup // tracks in-flight captureAsync goroutines
	packages  []Package      // cached from runtime/debug.ReadBuildInfo at New() time
	serverIPs []string       // non-loopback IPs collected once at startup

	mu         sync.RWMutex // protects globalTags
	globalTags []Tag        // set via SetTag; prepended to every event
}

// New creates a new Bikeeper client.
// It panics if ClientID, ClientSecret, or ProjectID are empty.
func New(opts Options) *Client {
	if opts.ClientID == "" {
		panic("bikeeper: ClientID must not be empty")
	}
	if opts.ClientSecret == "" {
		panic("bikeeper: ClientSecret must not be empty")
	}
	if opts.ProjectID == "" {
		panic("bikeeper: ProjectID must not be empty — copy it from the Bikeeper dashboard")
	}

	if opts.Endpoint == "" {
		opts.Endpoint = "http://localhost:8080"
	}
	if opts.Timeout == 0 {
		opts.Timeout = 5 * time.Second
	}
	if opts.FlushTimeout == 0 {
		opts.FlushTimeout = 2 * time.Second
	}

	c := &Client{opts: opts, packages: collectPackages(), serverIPs: collectServerIPs()}
	c.transport = newHTTPTransport(&c.opts)
	return c
}

// NewWithTransport creates a Client using the provided [Transport] instead of
// the default HTTP transport. Intended for testing — callers can pass a fake
// transport to capture events in-process without a running server.
//
// The same credential validation as [New] applies.
func NewWithTransport(t Transport, opts Options) *Client {
	if opts.ClientID == "" {
		panic("bikeeper: ClientID must not be empty")
	}
	if opts.ClientSecret == "" {
		panic("bikeeper: ClientSecret must not be empty")
	}
	if opts.ProjectID == "" {
		panic("bikeeper: ProjectID must not be empty — copy it from the Bikeeper dashboard")
	}
	if opts.Timeout == 0 {
		opts.Timeout = 5 * time.Second
	}
	if opts.FlushTimeout == 0 {
		opts.FlushTimeout = 2 * time.Second
	}
	c := &Client{opts: opts, packages: collectPackages(), serverIPs: collectServerIPs()}
	c.transport = t
	return c
}

// collectServerIPs returns all non-loopback IPv4 and IPv6 addresses assigned
// to the host's network interfaces. Called once at startup and cached.
func collectServerIPs() []string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return nil
	}
	var ips []string
	for _, addr := range addrs {
		var ip net.IP
		switch v := addr.(type) {
		case *net.IPNet:
			ip = v.IP
		case *net.IPAddr:
			ip = v.IP
		}
		if ip == nil || ip.IsLoopback() || ip.IsLinkLocalUnicast() {
			continue
		}
		ips = append(ips, ip.String())
	}
	return ips
}

// collectPackages reads the Go module dependency list that was embedded into the
// binary at build time via runtime/debug.ReadBuildInfo.
// Returns nil when build info is unavailable (e.g. go run with no module).
func collectPackages() []Package {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return nil
	}
	pkgs := make([]Package, 0, len(info.Deps)+1)
	// Include the main module itself.
	if info.Main.Path != "" {
		v := info.Main.Version
		if v == "" || v == "(devel)" {
			v = "dev"
		}
		pkgs = append(pkgs, Package{Name: info.Main.Path, Version: v})
	}
	for _, dep := range info.Deps {
		if dep == nil {
			continue
		}
		v := dep.Version
		// If the dep is replaced, use the replacement's version.
		if dep.Replace != nil {
			v = dep.Replace.Version
		}
		if v == "" || v == "(devel)" {
			v = "dev"
		}
		pkgs = append(pkgs, Package{Name: dep.Path, Version: v})
	}
	return pkgs
}

// newLogRecord builds a [LogRecord] enriched with Options metadata.
// Global tags, Environment, Release, and ServerName are merged in.
func (c *Client) newLogRecord(level Level, message string, tags []Tag) *LogRecord {
	c.mu.RLock()
	global := make([]Tag, len(c.globalTags))
	copy(global, c.globalTags)
	c.mu.RUnlock()

	all := append(global, tags...)

	var serverName string
	if h, err := os.Hostname(); err == nil {
		serverName = h
	}

	return &LogRecord{
		ID:          uuid.New().String(),
		Level:       string(level),
		Message:     message,
		Tags:        all,
		Timestamp:   time.Now().UTC(),
		SDK:         &SDKInfo{Name: sdkName, Version: sdkVersion},
		Environment: c.opts.Environment,
		Release:     c.opts.Release,
		ServerName:  serverName,
	}
}

// captureLogAsync sends a [LogRecord] to the /api/v1/logs endpoint
// asynchronously via a background goroutine tracked by wg.
//
// If the underlying transport does not implement [logSender] (e.g. a test fake
// that only captures events), the record is dropped silently — no error is
// reported and no goroutine is launched.
func (c *Client) captureLogAsync(record *LogRecord) {
	ls, ok := c.transport.(logSender)
	if !ok {
		return
	}
	c.wg.Go(func() {
		sendCtx, cancel := context.WithTimeout(context.Background(), c.opts.Timeout)
		defer cancel()
		if err := ls.SendLog(sendCtx, record); err != nil {
			if c.opts.OnError != nil {
				c.opts.OnError(err)
			}
		}
	})
}

// CaptureException captures an error event and sends it to Bikeeper asynchronously.
// A full Go stack trace is captured automatically at the call site and attached
// to the event as structured exception data (type, message, frames with source
// context). The grouping fingerprint is computed from the error type + in-app
// frames so that the same root cause is grouped as one issue in the dashboard.
func (c *Client) CaptureException(ctx context.Context, err error, tags ...Tag) {
	if c == nil || err == nil {
		return
	}
	event := NewEvent(LevelError, err.Error(), tags...)
	ex := buildExceptionValue(err, 1) // skip: CaptureException itself; direct caller is the first captured frame
	event.Exception = ex
	if ex.Stacktrace != nil {
		// fingerprint[0] = all-frames hash, fingerprint[1] = in-app-only hash
		event.Fingerprint = []string{
			computeAllFramesGroupingHash(ex.Type, ex.Stacktrace.Frames),
			computeGroupingHash(ex.Type, ex.Stacktrace.Frames),
		}
	}
	event = c.enrichEvent(event)
	c.captureAsync(event)
}

// CaptureMessage captures a message event and sends it asynchronously.
// A stacktrace is captured at the call site and attached as exception data so
// the call-site frame appears in the Bikeeper dashboard alongside the message.
func (c *Client) CaptureMessage(ctx context.Context, message string, level Level, tags ...Tag) {
	if c == nil {
		return
	}
	event := NewEvent(level, message, tags...)
	// skip=1: CaptureMessage itself is omitted; user code is the first visible frame.
	st := captureStacktrace(1)
	exType := callerFunctionName(st)
	event.Exception = &ExceptionValue{
		Type:       exType,
		Value:      message,
		Mechanism:  &ExceptionMechanism{Type: "generic", Handled: true},
		Stacktrace: st,
	}
	if st != nil {
		event.Fingerprint = []string{
			computeAllFramesGroupingHash(exType, st.Frames),
			computeGroupingHash(exType, st.Frames),
		}
	}
	event = c.enrichEvent(event)
	c.captureAsync(event)
}

// Capture sends an event synchronously and returns any transport error.
func (c *Client) Capture(ctx context.Context, event *Event) error {
	if c == nil || event == nil {
		return nil
	}
	return c.transport.Send(ctx, c.enrichEvent(event))
}

// CaptureEventAsync sends a pre-built event asynchronously.
// Unlike CaptureException and CaptureMessage, the caller is responsible for
// setting Level and Message; enrichment (SDK info, runtime context, env tags)
// is still applied automatically.
// This is the method framework middlewares use when they build a full event
// with HTTP request context before sending.
func (c *Client) CaptureEventAsync(event *Event) {
	if c == nil || event == nil {
		return
	}
	c.captureAsync(c.enrichEvent(event))
}

// Flush blocks until all in-flight async events are delivered or FlushTimeout expires.
func (c *Client) Flush() {
	if c == nil {
		return
	}
	done := make(chan struct{})
	go func() {
		c.wg.Wait()
		close(done)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), c.opts.FlushTimeout)
	defer cancel()
	select {
	case <-done:
	case <-ctx.Done():
	}
}

// Close flushes remaining events and releases resources.
func (c *Client) Close() {
	if c == nil {
		return
	}
	c.Flush()
}

// captureAsync sends an event in a background goroutine.
// The goroutine is tracked by wg so Flush can wait for it.
// If the send fails and OnError is set, the error is forwarded to the caller.
func (c *Client) captureAsync(event *Event) {
	c.wg.Go(func() {
		sendCtx, cancel := context.WithTimeout(context.Background(), c.opts.Timeout)
		defer cancel()
		if err := c.transport.Send(sendCtx, event); err != nil {
			if c.opts.OnError != nil {
				c.opts.OnError(err)
			}
		}
	})
}

// SetTag sets a global tag that is automatically attached to every event sent
// by this client. If a tag with the same key already exists it is overwritten.
// Tags set here are merged before per-event tags, so per-event tags take precedence.
func (c *Client) SetTag(key, value string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for i, t := range c.globalTags {
		if t.Key == key {
			c.globalTags[i].Value = value
			return
		}
	}
	c.globalTags = append(c.globalTags, Tag{Key: key, Value: value})
}

// RemoveTag removes a global tag by key. No-op if the key does not exist.
func (c *Client) RemoveTag(key string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for i, t := range c.globalTags {
		if t.Key == key {
			c.globalTags = append(c.globalTags[:i], c.globalTags[i+1:]...)
			return
		}
	}
}

// Tags returns a snapshot of the current global tags.
func (c *Client) Tags() []Tag {
	if c == nil {
		return nil
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	snap := make([]Tag, len(c.globalTags))
	copy(snap, c.globalTags)
	return snap
}

// Framework returns the framework identifier configured for this client (e.g. "fiber", "echo").
func (c *Client) Framework() string {
	if c == nil {
		return ""
	}
	return c.opts.Framework
}

// SetFramework sets the framework identifier on the client.
// This is called automatically by framework middleware packages (bikeeperfiber,
// bikeeperecho) at startup — there is no need to set Options.Framework manually.
func (c *Client) SetFramework(f string) {
	if c == nil {
		return
	}
	c.opts.Framework = f
}

// enrichEvent prepends Environment and Release from Options as tags, and
// auto-populates SDK info, server runtime, OS, and device arch so every event
// carries full context without requiring the caller to set them manually.
func (c *Client) enrichEvent(event *Event) *Event { //nolint:cyclop
	c.mu.RLock()
	global := make([]Tag, len(c.globalTags))
	copy(global, c.globalTags)
	c.mu.RUnlock()

	var extra []Tag
	if c.opts.Environment != "" {
		extra = append(extra, Tag{Key: "environment", Value: c.opts.Environment})
	}
	if c.opts.Release != "" {
		extra = append(extra, Tag{Key: "release", Value: c.opts.Release})
	}
	extra = append(extra, global...)
	if len(extra) > 0 {
		event.Tags = append(extra, event.Tags...)
	}

	if event.SDK == nil {
		event.SDK = &SDKInfo{Name: sdkName, Version: sdkVersion}
	}
	if len(event.Packages) == 0 && len(c.packages) > 0 {
		event.Packages = c.packages
	}

	enrichContexts(event)
	c.appendServerMetaTags(event)

	return event
}

// enrichContexts fills nil Contexts fields with server-side runtime information.
func enrichContexts(event *Event) {
	if event.Contexts == nil {
		event.Contexts = &Contexts{}
	}
	if event.Contexts.Runtime == nil {
		ver := strings.TrimPrefix(runtime.Version(), "go")
		event.Contexts.Runtime = &RuntimeInfo{Name: "go", Version: ver}
	}
	if event.Contexts.OS == nil {
		event.Contexts.OS = &OSInfo{Name: runtime.GOOS}
	}
	if event.Contexts.Device == nil {
		hostname, _ := os.Hostname()
		event.Contexts.Device = &DeviceInfo{
			Name: hostname,
			Arch: runtime.GOARCH,
		}
	}
}

// appendServerMetaTags appends runtime, host, and memory metadata tags to event,
// skipping any keys already present.
func (c *Client) appendServerMetaTags(event *Event) {
	rtVer := event.Contexts.Runtime.Version

	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	serverMeta := []Tag{
		{Key: "num_cpu", Value: strconv.Itoa(runtime.NumCPU())},
		{Key: "go_maxprocs", Value: strconv.Itoa(runtime.GOMAXPROCS(0))},
		{Key: "go_numroutines", Value: strconv.Itoa(runtime.NumGoroutine())},
		{Key: "os", Value: runtime.GOOS},
		{Key: "os.name", Value: runtime.GOOS},
		{Key: "runtime", Value: "go " + rtVer},
		{Key: "runtime.name", Value: "go"},
		// Memory stats
		{Key: "mem_alloc_kb", Value: strconv.FormatUint(ms.Alloc/1024, 10)},
		{Key: "mem_sys_kb", Value: strconv.FormatUint(ms.Sys/1024, 10)},
		{Key: "mem_heap_inuse_kb", Value: strconv.FormatUint(ms.HeapInuse/1024, 10)},
		{Key: "mem_heap_objects", Value: strconv.FormatUint(ms.HeapObjects, 10)},
	}

	var gcStats debug.GCStats
	debug.ReadGCStats(&gcStats)
	serverMeta = append(serverMeta, Tag{Key: "go_numgcalls", Value: strconv.FormatInt(gcStats.NumGC, 10)})

	if hostname, err := os.Hostname(); err == nil && hostname != "" {
		serverMeta = append(serverMeta, Tag{Key: "server_name", Value: hostname})
	}

	// Server IP addresses — cached at startup, joined as comma-separated string.
	if len(c.serverIPs) > 0 {
		serverMeta = append(serverMeta,
			Tag{Key: "server_ip", Value: c.serverIPs[0]},
			Tag{Key: "server_ips", Value: strings.Join(c.serverIPs, ",")},
		)
	}

	existing := make(map[string]struct{}, len(event.Tags))
	for _, t := range event.Tags {
		existing[t.Key] = struct{}{}
	}
	for _, t := range serverMeta {
		if _, dup := existing[t.Key]; !dup {
			event.Tags = append(event.Tags, t)
		}
	}
}
