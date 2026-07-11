package bikeeper

import (
	"context"
	cryptorand "crypto/rand"
	"encoding/hex"
	mathrand "math/rand"
	"sync"
	"time"
)

// contextKey is an unexported type for context value keys that prevents
// collisions with other packages that also store values in context.
type contextKey int

const (
	hubContextKey  contextKey = iota // Hub stored by SetHubOnContext
	spanContextKey contextKey = iota // active Span stored by StartSpan / StartTransaction
)

// ─── Hub ─────────────────────────────────────────────────────────────────────

// Hub carries a Client and a per-request Scope, mirroring Sentry's Hub concept.
// Create one Hub per request via bikeeperfiber.New (automatic) or NewHub (manual),
// then propagate it with SetHubOnContext / GetHubFromContext.
type Hub struct {
	client *Client
	scope  *Scope
	// mu protects concurrent reads/writes of the scope field (e.g. WithScope).
	mu sync.Mutex
	// ctx is the context from which this hub was retrieved.
	// When non-nil, CaptureException / CaptureMessage automatically extract the
	// active Span and attach its trace context to the captured event.
	ctx context.Context
}

// NewHub creates a Hub wrapping client with an empty Scope.
func NewHub(client *Client) *Hub {
	return &Hub{client: client, scope: &Scope{}}
}

// SetHubOnContext attaches hub to ctx and returns the updated context.
// Use this to carry the Hub across layer boundaries without depending on
// framework-specific context types.
//
//	hub := bikeeperfiber.GetHubFromContext(c)
//	ctx := bikeeper.SetHubOnContext(context.Background(), hub)
//	transaction := bikeeper.StartTransaction(ctx, "order.process")
func SetHubOnContext(ctx context.Context, hub *Hub) context.Context {
	return context.WithValue(ctx, hubContextKey, hub)
}

// GetHubFromContext retrieves the Hub stored in ctx.
// The returned hub is scoped to ctx: if ctx carries an active Span
// (set by StartSpan or StartTransaction), CaptureException and CaptureMessage
// automatically attach the span's trace context to every captured event.
// Returns nil when no hub is stored in ctx.
func GetHubFromContext(ctx context.Context) *Hub {
	if v := ctx.Value(hubContextKey); v != nil {
		if h, ok := v.(*Hub); ok {
			// Return a shallow copy scoped to this context so CaptureException
			// can extract the active span without modifying the stored hub.
			return &Hub{client: h.client, scope: h.scope, ctx: ctx}
		}
	}
	return nil
}

// scopeSnapshot returns the hub's current scope under the hub mutex.
// CaptureException and CaptureMessage call this so they always see a consistent
// scope even when WithScope is running concurrently on another goroutine.
func (h *Hub) scopeSnapshot() *Scope {
	h.mu.Lock()
	s := h.scope
	h.mu.Unlock()
	return s
}

// CaptureException captures err through the hub's client.
// If the hub's ctx carries an active Span, the event is automatically enriched
// with the span's trace context (TraceID, SpanID, ParentSpanID, Op, Description).
// A full Go stacktrace is captured at the call site.
func (h *Hub) CaptureException(err error, tags ...Tag) {
	if h == nil || h.client == nil || err == nil {
		return
	}
	ctx := h.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	scope := h.scopeSnapshot()

	ev := NewEvent(LevelError, err.Error())
	ex := buildExceptionValue(err, 1)
	ev.Exception = ex
	if ex.Type != "" &&
		ex.Type != "*errors.errorString" &&
		ex.Type != "*fmt.wrapError" &&
		ex.Type != "error" {
		ev.Message = ex.Type
	}
	if ex.Stacktrace != nil {
		ev.Fingerprint = []string{
			computeAllFramesGroupingHash(ex.Type, ex.Stacktrace.Frames),
			computeGroupingHash(ex.Type, ex.Stacktrace.Frames),
		}
	}
	if fp := scope.fingerprint(); fp != nil {
		ev.Fingerprint = fp
	}

	attachSpanContext(ctx, ev)
	applyHTTPContext(ev, scope)
	applyScopeData(ev, scope, tags)

	h.client.CaptureEventAsync(ev)
}

// CaptureMessage captures a message through the hub's client.
// If the hub's ctx carries an active Span, the span's trace context is attached.
func (h *Hub) CaptureMessage(message string, level Level, tags ...Tag) {
	if h == nil || h.client == nil {
		return
	}
	ctx := h.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	scope := h.scopeSnapshot()

	ev := NewEvent(level, message)

	st := captureStacktrace(1)
	exType := callerFunctionName(st)
	ev.Exception = &ExceptionValue{
		Type:       exType,
		Value:      message,
		Mechanism:  &ExceptionMechanism{Type: "generic", Handled: true},
		Stacktrace: st,
	}

	if fp := scope.fingerprint(); fp != nil {
		ev.Fingerprint = fp
	} else if st != nil {
		ev.Fingerprint = []string{
			computeAllFramesGroupingHash(exType, st.Frames),
			computeGroupingHash(exType, st.Frames),
		}
	}

	attachSpanContext(ctx, ev)
	applyHTTPContext(ev, scope)
	applyScopeData(ev, scope, tags)

	h.client.CaptureEventAsync(ev)
}

// attachSpanContext enriches ev with trace information from the active span in ctx.
func attachSpanContext(ctx context.Context, ev *Event) {
	span := SpanFromContext(ctx)
	if span == nil {
		return
	}
	ev.TraceID = span.TraceID
	if ev.Contexts == nil {
		ev.Contexts = &Contexts{}
	}
	ev.Contexts.Trace = span.TraceInfo()
	// Tags is mutated by SetTag/SetHTTPStatus (possibly from another
	// goroutine — spans are documented as safe to pass across goroutine
	// boundaries), so it must be read under the same per-trace lock rather
	// than ranged over directly.
	var tags []Tag
	span.withLock(func() { tags = tagsFromMap(span.Tags) })
	ev.Tags = append(ev.Tags, tags...)
}

// applyHTTPContext enriches ev with HTTP request context from scope
// (URL, request, browser info, and client OS tags).
func applyHTTPContext(ev *Event, scope *Scope) {
	scopeURL, scopeReq, bName, bVersion, osName := scope.httpContext()
	if scopeURL == "" {
		return
	}
	if ev.URL == "" {
		ev.URL = scopeURL
	}
	if ev.HTTPRequest == nil {
		ev.HTTPRequest = scopeReq
	}
	applyBrowserContext(ev, bName, bVersion)
	applyClientOSContext(ev, osName)
}

// applyBrowserContext adds browser info and tags to ev when bName is non-empty.
func applyBrowserContext(ev *Event, bName, bVersion string) {
	if bName == "" {
		return
	}
	if ev.Contexts == nil {
		ev.Contexts = &Contexts{}
	}
	if ev.Contexts.Browser == nil {
		ev.Contexts.Browser = &BrowserInfo{Name: bName, Version: bVersion}
	}
	display := bName
	if bVersion != "" {
		display = bName + " " + bVersion
	}
	ev.Tags = append(ev.Tags,
		Tag{Key: "browser", Value: display},
		Tag{Key: "browser.name", Value: bName},
	)
	if bVersion != "" {
		ev.Tags = append(ev.Tags, Tag{Key: "browser.version", Value: bVersion})
	}
}

// applyClientOSContext adds client OS info and tags to ev when osName is non-empty.
func applyClientOSContext(ev *Event, osName string) {
	if osName == "" {
		return
	}
	if ev.Contexts == nil {
		ev.Contexts = &Contexts{}
	}
	if ev.Contexts.ClientOS == nil {
		ev.Contexts.ClientOS = &OSInfo{Name: osName}
	}
	ev.Tags = append(ev.Tags,
		Tag{Key: "client_os", Value: osName},
		Tag{Key: "client_os.name", Value: osName},
	)
}

// applyScopeData merges scope tags, per-call tags, breadcrumbs, user, and extras into ev.
func applyScopeData(ev *Event, scope *Scope, tags []Tag) {
	ev.Tags = append(scope.Tags(), ev.Tags...)
	ev.Tags = append(ev.Tags, tags...)
	ev.Breadcrumbs = scope.breadcrumbSnapshot()
	ev.User = scope.getUser()
	ev.Extra = scope.extrasSnapshot()
}

// Scope returns the hub's mutable Scope.
func (h *Hub) Scope() *Scope {
	if h == nil {
		return &Scope{}
	}
	return h.scope
}

// Clone creates an independent copy of the hub suitable for use in a goroutine.
// The clone shares the same client but gets a fresh Scope pre-populated with
// the parent's tags and HTTP context. Breadcrumbs and custom fingerprints are
// NOT inherited — the goroutine builds its own execution trail.
//
// Always use a cloned hub when passing Bikeeper context across goroutine
// boundaries to prevent concurrent scope mutations.
//
//	go func(ctx context.Context) {
//	    clonedHub := hub.Clone()
//	    ctx = bikeeper.SetHubOnContext(ctx, clonedHub)
//	    // ... span work ...
//	}(ctx)
func (h *Hub) Clone() *Hub {
	if h == nil {
		return &Hub{}
	}
	newScope := &Scope{}
	// Copy tags so the goroutine inherits global context (service, region, etc.)
	// but can add its own without racing on the parent scope.
	existing := h.scope.Tags()
	newScope.tags = append([]Tag(nil), existing...)
	// Copy HTTP context (URL, browser, OS) — these are read-only after middleware
	// sets them, so sharing the pointer is safe.
	url, req, bName, bVer, osName := h.scope.httpContext()
	newScope.requestURL = url
	newScope.httpRequest = req
	newScope.browserName = bName
	newScope.browserVersion = bVer
	newScope.clientOSName = osName
	// Copy user identity so the goroutine inherits who triggered the action.
	if u := h.scope.getUser(); u != nil {
		newScope.user = u
	}
	return &Hub{client: h.client, scope: newScope}
}

// CloneWithBreadcrumbs creates an independent hub copy like Clone, but also
// inherits the parent scope's breadcrumb trail. Use this when a goroutine
// should continue the execution narrative that started on the parent request.
//
//	go func() {
//	    clonedHub := hub.CloneWithBreadcrumbs()
//	    clonedHub.AddBreadcrumb(bikeeper.Breadcrumb{Message: "goroutine started"})
//	    clonedHub.CaptureException(err)
//	}()
func (h *Hub) CloneWithBreadcrumbs() *Hub {
	clone := h.Clone()
	if h != nil {
		clone.scope.breadcrumbs = h.scope.breadcrumbSnapshot()
	}
	return clone
}

// SetUser sets the authenticated user on the hub's scope. The user is merged
// into every event captured through this hub.
//
//	hub.SetUser(bikeeper.User{ID: "usr-42", Email: "alice@example.com"})
func (h *Hub) SetUser(u User) {
	if h == nil {
		return
	}
	h.scope.SetUser(u)
}

// Flush waits for all queued events to be sent to Bikeeper, up to the client's
// configured FlushTimeout. Call this before process exit to prevent event loss.
//
//	defer hub.Flush()
func (h *Hub) Flush() {
	if h == nil || h.client == nil {
		return
	}
	h.client.Flush()
}

// SetExtra attaches an arbitrary key/value context bag to every event captured
// through this hub. Use it for domain-specific metadata that does not fit a
// simple Tag (e.g. large request payloads, structured service-call results).
//
//	hub.SetExtra("payment", bikeeper.ExtraContext{"provider": "stripe", "amount": 1500})
func (h *Hub) SetExtra(key string, ctx ExtraContext) {
	if h == nil {
		return
	}
	h.scope.SetContext(key, ctx)
}

// scopeMu guards concurrent WithScope calls on the same Hub.
// Without this, two goroutines calling WithScope simultaneously could race on
// reading and writing h.scope — one goroutine's restore could overwrite the
// other's clone. The mutex is per-Hub (not per-Scope) because the race is on
// the Hub pointer field, not on the Scope contents.

// WithScope executes fn with an isolated copy of the hub's scope.
// Mutations (SetTag, SetFingerprint, SetUser, etc.) made inside fn are scoped
// to that callback only and do NOT permanently alter the hub's real scope.
// Any hub.CaptureException / hub.CaptureMessage calls made inside fn will use
// the modified scope, giving you one-off event enrichment without side effects.
//
// Thread-safe: the scope swap is protected by the hub's internal mutex, so
// multiple goroutines can call WithScope concurrently without data races.
//
//	hub.WithScope(func(s *bikeeper.Scope) {
//	    s.SetFingerprint("payment-error", provider)
//	    s.SetTag("payment.provider", provider)
//	    hub.CaptureException(err)  // uses the cloned, enriched scope
//	})
func (h *Hub) WithScope(fn func(*Scope)) {
	if h == nil {
		return
	}
	h.mu.Lock()
	clone := h.scope.clone()
	original := h.scope
	h.scope = clone
	h.mu.Unlock()

	defer func() {
		h.mu.Lock()
		h.scope = original
		h.mu.Unlock()
	}()
	fn(clone)
}

// AddBreadcrumb appends a breadcrumb to the hub's scope.
// Breadcrumbs are automatically attached to the next captured event so that
// the Bikeeper dashboard can show the full execution trail.
//
//	hub.AddBreadcrumb(bikeeper.Breadcrumb{
//	    Category: "usecase",
//	    Message:  "processOrder: started",
//	    Level:    bikeeper.LevelInfo,
//	    Data:     map[string]any{"order_id": orderID},
//	})
func (h *Hub) AddBreadcrumb(b Breadcrumb) {
	if h == nil {
		return
	}
	h.scope.AddBreadcrumb(b)
}

// ─── Scope ───────────────────────────────────────────────────────────────────

// maxBreadcrumbs caps the breadcrumb buffer per scope to prevent unbounded
// memory growth on long request chains.
const maxBreadcrumbs = 100

// Scope holds per-request annotations that are merged into all events captured
// through the parent Hub.
type Scope struct {
	mu   sync.Mutex
	tags []Tag
	// Custom fingerprint: when non-nil, overrides the auto-computed stack-trace
	// hash and forces all events captured through this scope into a single group.
	customFingerprint []string
	// HTTP context snapshotted from the framework request at hub creation time.
	// Automatically applied to every event captured through this scope's hub.
	requestURL  string
	httpRequest *HTTPRequest
	// Parsed browser / OS context (derived from UA + Sec-CH-UA at request start).
	browserName    string
	browserVersion string
	clientOSName   string
	// breadcrumbs is the ordered execution trail, capped at maxBreadcrumbs.
	breadcrumbs []Breadcrumb
	// user identifies the authenticated end-user at the time of the event.
	user *User
	// extras holds named free-form context blocks set via SetContext.
	// Keys are arbitrary names (e.g. "device", "runtime"); values are ExtraContext maps.
	extras map[string]ExtraContext
}

// SetFingerprint sets a custom grouping fingerprint on the scope. When set, it
// overrides the auto-computed stack-trace hash for every event captured through
// this scope, forcing all those events into a single issue group in Bikeeper.
// Pass the parts that uniquely identify the logical error group, e.g.:
//
//	s.SetFingerprint("GET /order/:id", "order_error")
func (s *Scope) SetFingerprint(parts ...string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.customFingerprint = append([]string(nil), parts...)
}

// fingerprint returns a copy of the custom fingerprint, or nil if none is set.
func (s *Scope) fingerprint() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.customFingerprint) == 0 {
		return nil
	}
	out := make([]string, len(s.customFingerprint))
	copy(out, s.customFingerprint)
	return out
}

// SetHTTPContext stores the HTTP request snapshot and parsed UA details in the
// scope so that every event captured via Hub.CaptureException / Hub.CaptureMessage
// is enriched with URL, http_request, browser, and OS context automatically.
func (s *Scope) SetHTTPContext(url string, req *HTTPRequest, browserName, browserVersion, clientOSName string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.requestURL = url
	s.httpRequest = req
	s.browserName = browserName
	s.browserVersion = browserVersion
	s.clientOSName = clientOSName
}

// httpContext returns a copy of the stored HTTP context fields.
func (s *Scope) httpContext() (url string, req *HTTPRequest, browserName, browserVersion, clientOSName string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.requestURL, s.httpRequest, s.browserName, s.browserVersion, s.clientOSName
}

// SetTag adds or updates a key-value tag on the scope.
func (s *Scope) SetTag(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, t := range s.tags {
		if t.Key == key {
			s.tags[i].Value = value
			return
		}
	}
	s.tags = append(s.tags, Tag{Key: key, Value: value})
}

// Tags returns a snapshot of the scope's current tags.
func (s *Scope) Tags() []Tag {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Tag, len(s.tags))
	copy(out, s.tags)
	return out
}

// SetUser sets the authenticated user on this scope. The user is attached to
// every event captured through the parent hub.
//
//	s.SetUser(bikeeper.User{ID: "usr-42", Email: "alice@example.com"})
func (s *Scope) SetUser(u User) {
	s.mu.Lock()
	defer s.mu.Unlock()
	copy := u
	s.user = &copy
}

// user returns the scope's current user, or nil if none is set.
func (s *Scope) getUser() *User {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.user == nil {
		return nil
	}
	copy := *s.user
	return &copy
}

// clone creates a deep copy of the scope. Used by WithScope to provide
// isolation so that mutations inside the callback cannot affect the hub's
// real scope after the callback returns.
func (s *Scope) clone() *Scope {
	s.mu.Lock()
	defer s.mu.Unlock()
	c := &Scope{
		requestURL:     s.requestURL,
		httpRequest:    s.httpRequest,
		browserName:    s.browserName,
		browserVersion: s.browserVersion,
		clientOSName:   s.clientOSName,
	}
	c.tags = append([]Tag(nil), s.tags...)
	c.breadcrumbs = append([]Breadcrumb(nil), s.breadcrumbs...)
	c.customFingerprint = append([]string(nil), s.customFingerprint...)
	if s.user != nil {
		u := *s.user
		c.user = &u
	}
	if len(s.extras) > 0 {
		c.extras = make(map[string]ExtraContext, len(s.extras))
		for k, v := range s.extras {
			c.extras[k] = v
		}
	}
	return c
}

// SetContext stores a named free-form context block on the scope. It matches
// Sentry's Scope.SetContext API for easy migration from Sentry to Bikeeper.
// The stored contexts are attached to every event captured through this scope
// under the event's Extra field, keyed by name.
func (s *Scope) SetContext(key string, ctx ExtraContext) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.extras == nil {
		s.extras = make(map[string]ExtraContext)
	}
	s.extras[key] = ctx
}

// extrasSnapshot returns a shallow copy of the extras map, safe to read without
// holding the scope lock.
func (s *Scope) extrasSnapshot() map[string]ExtraContext {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.extras) == 0 {
		return nil
	}
	out := make(map[string]ExtraContext, len(s.extras))
	for k, v := range s.extras {
		out[k] = v
	}
	return out
}

// AddBreadcrumb appends a breadcrumb to the scope's execution trail.
// When the buffer reaches maxBreadcrumbs the oldest entry is dropped so
// memory stays bounded.
func (s *Scope) AddBreadcrumb(b Breadcrumb) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if b.Timestamp.IsZero() {
		b.Timestamp = time.Now().UTC()
	}
	if len(s.breadcrumbs) >= maxBreadcrumbs {
		// Drop the oldest entry (FIFO ring).
		s.breadcrumbs = s.breadcrumbs[1:]
	}
	s.breadcrumbs = append(s.breadcrumbs, b)
}

// breadcrumbSnapshot returns a copy of the scope's breadcrumbs slice.
func (s *Scope) breadcrumbSnapshot() []Breadcrumb {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.breadcrumbs) == 0 {
		return nil
	}
	out := make([]Breadcrumb, len(s.breadcrumbs))
	copy(out, s.breadcrumbs)
	return out
}

// ExtraContext is a free-form map of additional structured data, equivalent to
// Sentry's sentry.Context type.
type ExtraContext map[string]any

// ─── Span ─────────────────────────────────────────────────────────────────────

// SpanStatus represents the final status of a span, mirroring OpenTelemetry codes.
type SpanStatus string

const (
	SpanStatusOK            SpanStatus = "ok"
	SpanStatusError         SpanStatus = "error"
	SpanStatusInternalError SpanStatus = "internal_error"
	SpanStatusNotFound      SpanStatus = "not_found"
	SpanStatusUnknown       SpanStatus = "unknown"
)

// Transaction source constants for WithTransactionSource.
// These mirror Sentry's TransactionSource values for easy migration.
const (
	SourceURL    = "url"
	SourceRoute  = "route"
	SourceView   = "view"
	SourceTask   = "task"
	SourceCustom = "custom"
)

// SpanOption is a functional option applied to a Span at creation time.
type SpanOption func(*Span)

// WithDescription sets the human-readable description of a span.
// For database spans this is typically the SQL query; for function spans it is
// the fully-qualified function name (e.g. "usecase.processOrder").
//
//	span := bikeeper.StartSpan(ctx, "db.query",
//	    bikeeper.WithDescription("SELECT * FROM orders WHERE id = ?"),
//	)
func WithDescription(desc string) SpanOption {
	return func(s *Span) { s.Description = desc }
}

// WithTransactionSource annotates a root transaction with its source type.
// Accepted values: SourceURL, SourceRoute, SourceView, SourceTask, SourceCustom.
func WithTransactionSource(src string) SpanOption {
	return func(s *Span) { s.transactionSource = src }
}

// Span represents a single unit of work within a distributed trace.
// A Span without a parent is called a Transaction and acts as the top-level
// boundary of a request or background job.
//
// Create root spans with StartTransaction and child spans with StartSpan.
// Always defer Finish() to mark the span as complete:
//
//	span := bikeeper.StartSpan(ctx, "db.query", bikeeper.WithDescription("SELECT ..."))
//	defer span.Finish()
type Span struct {
	// TraceID is the 128-bit hex identifier shared across all spans in the trace.
	TraceID string
	// SpanID is the 64-bit hex identifier unique to this span.
	SpanID string
	// ParentSpanID is the SpanID of the parent span, empty for root spans.
	ParentSpanID string
	// Op is the short operation category (e.g. "db.query", "http.request", "function").
	Op string
	// Description is the human-readable detail (SQL text, function name, URL, etc.).
	Description string
	// Status is the final result of the span operation.
	Status SpanStatus
	// Tags are string key-value annotations on this span.
	Tags map[string]string
	// Data holds arbitrary structured data attached to the span.
	Data map[string]any

	startTime time.Time
	// endTime is zero until Finish() runs; guarded by txn.mu.
	endTime           time.Time
	transactionSource string
	ctx               context.Context // context carrying this span (+ propagated hub)
	// txn is the state shared by every Span in this trace — always non-nil
	// once created via StartTransaction/StartSpan. See transaction.go.
	txn *transactionState
}

// newSpanID generates a cryptographically random 64-bit (16 hex char) span ID.
func newSpanID() string {
	b := make([]byte, 8)
	_, _ = cryptorand.Read(b)
	return hex.EncodeToString(b)
}

// newTraceID generates a cryptographically random 128-bit (32 hex char) trace ID.
func newTraceID() string {
	b := make([]byte, 16)
	_, _ = cryptorand.Read(b)
	return hex.EncodeToString(b)
}

// StartTransaction creates a root Span (transaction) and stores it in the returned
// context. Any Hub stored in ctx is preserved so that child spans and captured
// events can find it via GetHubFromContext.
//
// Usage:
//
//	hub := bikeeperfiber.GetHubFromContext(c)
//	ctx := bikeeper.SetHubOnContext(context.Background(), hub)
//
//	transaction := bikeeper.StartTransaction(ctx, "order.process",
//	    bikeeper.WithTransactionSource(bikeeper.SourceRoute),
//	)
//	transaction.SetTag("http.method", c.Method())
//	defer transaction.Finish()
//
//	// Pass transaction.Context() to child functions.
func StartTransaction(ctx context.Context, name string, opts ...SpanOption) *Span {
	s := &Span{
		TraceID:   newTraceID(),
		SpanID:    newSpanID(),
		Op:        name,
		Tags:      make(map[string]string),
		Data:      make(map[string]any),
		startTime: time.Now(),
	}
	for _, opt := range opts {
		opt(s)
	}
	s.txn = newRootTransactionState(ctx, s)
	s.ctx = context.WithValue(ctx, spanContextKey, s)
	return s
}

// newRootTransactionState resolves a *Client via any Hub already on ctx and
// rolls the head-based sampling decision exactly once. If ctx carries no Hub
// (or the Hub has no Client), the returned state still lets the Span
// function normally for trace-ID propagation — Finish() simply never builds
// or sends a payload, mirroring how Hub.CaptureException already no-ops
// silently when its client is nil.
func newRootTransactionState(ctx context.Context, root *Span) *transactionState {
	txn := &transactionState{root: root}
	if hub := GetHubFromContext(ctx); hub != nil && hub.client != nil {
		txn.client = hub.client
		txn.sampled = mathrand.Float64() < hub.client.opts.TracesSampleRate
	}
	return txn
}

// StartSpan creates a child Span within the current trace.
// The new span inherits the parent's TraceID and records the parent's SpanID.
// If no active span is found in ctx, a new root trace is started instead.
//
// Usage:
//
//	span := bikeeper.StartSpan(ctx, "function",
//	    bikeeper.WithDescription("usecase.processOrder"),
//	)
//	span.SetTag("layer", "usecase")
//	defer span.Finish()
//
//	// Pass span.Context() to child functions.
func StartSpan(ctx context.Context, op string, opts ...SpanOption) *Span {
	s := &Span{
		SpanID:    newSpanID(),
		Op:        op,
		Tags:      make(map[string]string),
		Data:      make(map[string]any),
		startTime: time.Now(),
	}
	parent := SpanFromContext(ctx)
	if parent != nil {
		s.TraceID = parent.TraceID
		s.ParentSpanID = parent.SpanID
		s.txn = parent.txn
	} else {
		s.TraceID = newTraceID()
		s.txn = newRootTransactionState(ctx, s)
	}
	for _, opt := range opts {
		opt(s)
	}
	s.ctx = context.WithValue(ctx, spanContextKey, s)

	if parent != nil {
		// Register onto the shared trace state only now that every
		// SpanOption has run and every field is final, so nothing ever
		// observes a half-populated span: the mutex Unlock/Lock pair below
		// is the synchronization edge that publishes these writes to a
		// concurrently-running root Finish().
		s.txn.mu.Lock()
		s.txn.children = append(s.txn.children, s)
		s.txn.mu.Unlock()
	}
	return s
}

// SpanFromContext returns the active Span stored in ctx, or nil if none.
func SpanFromContext(ctx context.Context) *Span {
	if v := ctx.Value(spanContextKey); v != nil {
		if s, ok := v.(*Span); ok {
			return s
		}
	}
	return nil
}

// Context returns the context that carries this span and any propagated Hub.
// Pass this to child functions so they can start child spans with StartSpan and
// capture events enriched with the correct trace context.
func (s *Span) Context() context.Context {
	return s.ctx
}

// withLock runs fn while holding this span's shared per-trace mutex. Every
// Span created via StartTransaction/StartSpan has txn set; the nil check
// only guards a zero-value Span{} built directly (e.g. in tests).
func (s *Span) withLock(fn func()) {
	if s.txn == nil {
		fn()
		return
	}
	s.txn.mu.Lock()
	defer s.txn.mu.Unlock()
	fn()
}

// SetTag adds or updates a string key-value annotation on the span.
func (s *Span) SetTag(key, value string) {
	s.withLock(func() { s.Tags[key] = value })
}

// SetData adds or updates structured data on the span.
func (s *Span) SetData(key string, value any) {
	s.withLock(func() { s.Data[key] = value })
}

// SetOp updates the span's operation name after creation. Framework
// middleware uses this to correct an HTTP transaction's name once route
// matching resolves to the actual endpoint pattern — e.g. Fiber's
// c.Route().Path is only accurate after c.Next() returns, but the
// transaction must already be started (and sampling decided) before
// c.Next() so that child spans created inside the handler have something to
// attach to. Since Op is now mutable post-creation, TraceInfo() reads it
// under the same lock rather than treating it as immutable-after-publish.
func (s *Span) SetOp(op string) {
	s.withLock(func() { s.Op = op })
}

// SetHTTPStatus maps an HTTP response status code to the span's Status field.
// 5xx → SpanStatusInternalError, 4xx → SpanStatusError, 3xx/2xx/1xx → SpanStatusOK.
// The raw code is also stored in span data as "http.status_code" so the frontend
// can display it in trace views.
//
//	transaction.SetHTTPStatus(c.Response().StatusCode())
func (s *Span) SetHTTPStatus(code int) {
	s.withLock(func() {
		switch {
		case code >= 500:
			s.Status = SpanStatusInternalError
		case code >= 400:
			s.Status = SpanStatusError
		default:
			s.Status = SpanStatusOK
		}
		// Inlined rather than calling SetData (sync.Mutex isn't reentrant).
		s.Data["http.status_code"] = code
	})
}

// Finish marks the span as complete. For a root transaction whose trace was
// sampled (see Options.TracesSampleRate), it builds and asynchronously sends
// a TransactionPayload containing the transaction's own timing plus every
// descendant span that had already finished. Child (non-root) spans just
// finalize their own duration/status in place — only the root ever sends.
//
// Safe to call more than once (idempotent) and safe to call from any
// goroutine. A child span whose Finish() runs after its root transaction has
// already sent is silently dropped from that payload — child spans must
// complete before their transaction does; this is a standard APM
// constraint, not something this package tries to work around.
//
//	span := bikeeper.StartSpan(ctx, "db.query", bikeeper.WithDescription("SELECT ..."))
//	defer span.Finish()
func (s *Span) Finish() {
	if s == nil || s.txn == nil {
		return
	}
	txn := s.txn

	txn.mu.Lock()
	if !s.endTime.IsZero() {
		txn.mu.Unlock()
		return // already finished
	}
	s.endTime = time.Now()

	if txn.root != s {
		txn.mu.Unlock()
		return // child span: local state finalized; the root owns sending
	}

	var payload *TransactionPayload
	if txn.sampled && txn.client != nil {
		payload = buildTransactionPayload(s, txn.children, txn.client)
	}
	txn.mu.Unlock()

	if payload != nil {
		txn.client.captureTransactionAsync(payload)
	}
}

// TraceInfo returns a TraceInfo struct populated from this span's identifiers.
// It is attached to Events captured within the span so the frontend can render
// the Trace Details card with description, op, parent span ID, etc.
//
// Op is read under the span's shared lock since SetOp can mutate it after
// creation (unlike TraceID/SpanID/ParentSpanID/Description, which are set
// once before the span is ever published and stay safely lock-free).
func (s *Span) TraceInfo() *TraceInfo {
	var op string
	s.withLock(func() { op = s.Op })
	return &TraceInfo{
		TraceID:      s.TraceID,
		SpanID:       s.SpanID,
		ParentSpanID: s.ParentSpanID,
		Op:           op,
		Description:  s.Description,
	}
}
