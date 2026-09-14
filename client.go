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
	"fmt"
	"net"
	"os"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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
const sdkVersion = "1.2.0"

// Client is the Bikeeper SDK client.
type Client struct {
	opts      Options
	transport Transport
	packages  []Package // cached from runtime/debug.ReadBuildInfo at New() time
	serverIPs []string  // non-loopback IPs collected once at startup

	// Send queue. Every capture hands a task to a fixed pool of sender
	// goroutines rather than starting one of its own — see enqueue.
	startOnce sync.Once
	tasks     chan sendTask
	pending   sync.WaitGroup // queued + in-flight tasks; Flush waits on this
	workers   sync.WaitGroup // sender goroutines; Close waits on this
	closeMu   sync.RWMutex   // guards closed against a concurrent enqueue
	closed    bool
	dropped   atomic.Uint64 // events discarded because the queue was full

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
		opts.Timeout = defaultTimeout
	}
	if opts.FlushTimeout == 0 {
		opts.FlushTimeout = defaultFlushTimeout
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
		opts.Timeout = defaultTimeout
	}
	if opts.FlushTimeout == 0 {
		opts.FlushTimeout = defaultFlushTimeout
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
	c.enqueue(func(ctx context.Context) error { return ls.SendLog(ctx, record) })
}

// captureTransactionAsync sends a [TransactionPayload] to the
// /api/v1/transactions endpoint asynchronously via a background goroutine
// tracked by wg — mirrors captureLogAsync exactly.
//
// If the underlying transport does not implement [transactionSender] (e.g. a
// test fake that only captures events), the payload is dropped silently — no
// error is reported and no goroutine is launched.
func (c *Client) captureTransactionAsync(payload *TransactionPayload) {
	ts, ok := c.transport.(transactionSender)
	if !ok {
		return
	}
	c.enqueue(func(ctx context.Context) error { return ts.SendTransaction(ctx, payload) })
}

// CaptureException captures an error event and sends it to Bikeeper asynchronously.
// A full Go stack trace is captured automatically at the call site and attached
// to the event as structured exception data (type, message, frames with source
// context). The grouping fingerprint is computed from the error type + in-app
// frames so that the same root cause is grouped as one issue in the dashboard.
//
// ctx is not merely carried along: whatever it holds is attached to the event —
// see [Client.captureWithContext].
func (c *Client) CaptureException(ctx context.Context, err error, tags ...Tag) {
	if c == nil || err == nil {
		return
	}
	// skip=1: CaptureException itself is omitted; its caller is the first frame.
	c.captureWithContext(ctx, buildExceptionEvent(err, 1), tags)
}

// CaptureMessage captures a message event and sends it asynchronously.
// A stacktrace is captured at the call site and attached as exception data so
// the call-site frame appears in the Bikeeper dashboard alongside the message.
//
// ctx is not merely carried along: whatever it holds is attached to the event —
// see [Client.captureWithContext].
func (c *Client) CaptureMessage(ctx context.Context, message string, level Level, tags ...Tag) {
	if c == nil {
		return
	}
	// skip=1: CaptureMessage itself is omitted; its caller is the first frame.
	c.captureWithContext(ctx, buildMessageEvent(message, level, 1), tags)
}

// captureWithContext attaches everything ctx knows about the work in progress
// before queueing ev.
//
// An event without context is a stack trace and nothing else: which request
// produced it, which user hit it, and which trace it belongs to all have to be
// guessed from timestamps. Those answers are already in ctx — the active span
// (from StartSpan / framework middleware) and, on a served request, the Hub
// holding that request's scope — so this attaches them: trace identifiers from
// the span, and URL, HTTP request, breadcrumbs, user and scope tags from the
// hub. This is what makes an error logged deep inside a usecase land on the
// dashboard already linked to the request and trace that caused it.
//
// A hub also learns that this request has now reported something, so framework
// middleware can skip its own automatic 5xx capture instead of filing the same
// failure twice.
func (c *Client) captureWithContext(ctx context.Context, ev *Event, tags []Tag) {
	hub := GetHubFromContext(ctx)
	if hub == nil {
		attachSpanContext(ctx, ev)
		ev.Tags = append(ev.Tags, tags...)
		c.captureAsync(c.enrichEvent(ev))
		return
	}

	scope := hub.scopeSnapshot()
	if fp := scope.fingerprint(); fp != nil {
		ev.Fingerprint = fp
	}
	attachSpanContext(ctx, ev)
	applyHTTPContext(ev, scope)
	applyScopeData(ev, scope, tags)
	scope.markCaptured()

	c.captureAsync(c.enrichEvent(ev))
}

// buildExceptionEvent builds the event for a captured error. skip is how many
// frames above this call to leave out of the stack trace.
func buildExceptionEvent(err error, skip int) *Event {
	ev := NewEvent(LevelError, err.Error())
	ex := buildExceptionValue(err, skip+1)
	ev.Exception = ex
	if ex.Stacktrace != nil {
		// fingerprint[0] = all-frames hash, fingerprint[1] = in-app-only hash
		ev.Fingerprint = []string{
			computeAllFramesGroupingHash(ex.Type, ex.Stacktrace.Frames),
			computeGroupingHash(ex.Type, ex.Stacktrace.Frames),
		}
	}
	return ev
}

// buildMessageEvent builds the event for a captured message, attaching the
// call site as exception data so the dashboard has a frame to show. skip is
// how many frames above this call to leave out of the stack trace.
func buildMessageEvent(message string, level Level, skip int) *Event {
	ev := NewEvent(level, message)
	st := captureStacktrace(skip + 1)
	exType := callerFunctionName(st)
	ev.Exception = &ExceptionValue{
		Type:       exType,
		Value:      message,
		Mechanism:  &ExceptionMechanism{Type: "generic", Handled: true},
		Stacktrace: st,
	}
	if st != nil {
		ev.Fingerprint = []string{
			computeAllFramesGroupingHash(exType, st.Frames),
			computeGroupingHash(exType, st.Frames),
		}
	}
	return ev
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
		c.pending.Wait()
		close(done)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), c.opts.FlushTimeout)
	defer cancel()
	select {
	case <-done:
	case <-ctx.Done():
	}
}

// Close flushes remaining events and stops the sender pool. Captures made
// after Close are dropped (and counted by Dropped) rather than panicking on a
// closed queue.
func (c *Client) Close() {
	if c == nil {
		return
	}
	c.Flush()

	c.closeMu.Lock()
	alreadyClosed := c.closed
	c.closed = true
	if !alreadyClosed && c.tasks != nil {
		close(c.tasks)
	}
	c.closeMu.Unlock()

	if !alreadyClosed {
		c.workers.Wait()
	}
}

// captureAsync runs the event through BeforeSend and queues it for delivery.
// A nil return from BeforeSend drops the event.
func (c *Client) captureAsync(event *Event) {
	event = c.applyBeforeSend(event)
	if event == nil {
		return
	}
	c.enqueue(func(ctx context.Context) error { return c.transport.Send(ctx, event) })
}

// applyBeforeSend gives the application its last look at an enriched event.
// A panicking BeforeSend drops the event rather than taking the process down
// with it — this runs on a sender goroutine, where an unrecovered panic would
// be fatal and entirely unrelated to whatever the app was actually doing.
func (c *Client) applyBeforeSend(event *Event) (out *Event) {
	if c.opts.BeforeSend == nil {
		return event
	}
	defer func() {
		if r := recover(); r != nil {
			out = nil
			if c.opts.OnError != nil {
				c.opts.OnError(fmt.Errorf("bikeeper: BeforeSend panicked (%v) — event dropped", r))
			}
		}
	}()
	return c.opts.BeforeSend(event)
}

// ─── Send queue ──────────────────────────────────────────────────────────────

// sendTask is one delivery attempt — an event, a log record, or a transaction
// payload already bound to its destination method.
type sendTask func(ctx context.Context) error

// enqueue hands task to the sender pool, discarding it when the queue is full.
//
// Dropping is deliberate: monitoring must never become the thing that takes
// the application down. Before this, every capture started its own goroutine
// and a failing dependency (a slow Bikeeper endpoint, an error storm, or both
// at once — they arrive together) could pile up thousands of them, each
// holding an event and a connection for up to Timeout. A bounded queue turns
// that unbounded memory and socket growth into a counted, reported loss of
// telemetry, which is the cheaper failure by far. Dropped() reports the count.
func (c *Client) enqueue(task sendTask) {
	// The closed check comes first so a capture after Close cannot start a
	// worker pool that would then never be shut down.
	c.closeMu.RLock()
	defer c.closeMu.RUnlock()
	if c.closed {
		c.drop("client closed")
		return
	}
	c.ensureWorkers()

	c.pending.Add(1)
	select {
	case c.tasks <- task:
	default:
		c.pending.Done()
		c.drop("send queue full")
	}
}

// drop counts a discarded payload and reports it through OnError.
func (c *Client) drop(reason string) {
	n := c.dropped.Add(1)
	if c.opts.OnError != nil {
		c.opts.OnError(fmt.Errorf("bikeeper: %s — payload dropped (%d dropped so far)", reason, n))
	}
}

// Dropped returns how many payloads have been discarded because the send
// queue was full (or the client was already closed). A non-zero, growing
// count means the endpoint cannot keep up with what the app is capturing:
// raise MaxQueueSize / SendConcurrency, or capture less.
func (c *Client) Dropped() uint64 {
	if c == nil {
		return 0
	}
	return c.dropped.Load()
}

// ensureWorkers starts the sender pool on first use. It is done lazily rather
// than in New so that a Client built as a struct literal (as some tests do)
// still delivers.
func (c *Client) ensureWorkers() {
	c.startOnce.Do(func() {
		size := c.opts.MaxQueueSize
		if size <= 0 {
			size = defaultMaxQueueSize
		}
		concurrency := c.opts.SendConcurrency
		if concurrency <= 0 {
			concurrency = defaultSendConcurrency
		}
		timeout := c.opts.Timeout
		if timeout <= 0 {
			timeout = defaultTimeout
		}

		c.tasks = make(chan sendTask, size)
		for range concurrency {
			c.workers.Go(func() {
				for task := range c.tasks {
					c.runTask(task, timeout)
				}
			})
		}
	})
}

// runTask executes one queued send, always marking it done so a panicking
// transport cannot wedge Flush forever.
func (c *Client) runTask(task sendTask, timeout time.Duration) {
	defer c.pending.Done()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := task(ctx); err != nil && c.opts.OnError != nil {
		c.opts.OnError(err)
	}
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
