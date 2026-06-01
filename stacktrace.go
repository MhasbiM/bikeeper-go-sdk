package bikeeper

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
)

// ─── Stack frame types ────────────────────────────────────────────────────────

// ContextLine is a single line of source code surrounding the error location.
// Lines are read from the source file at capture time.
type ContextLine struct {
	// Line is the 1-based line number.
	Line int `json:"line"`
	// Code is the raw source text for this line (tabs preserved).
	Code string `json:"code"`
	// IsCurrent marks the line where the error or capture call occurred.
	IsCurrent bool `json:"is_current"`
}

// StackFrame represents a single frame in a captured Go call stack.
type StackFrame struct {
	// Function is the short function / method name within the module.
	// Example: "processOrderUsecase", "(*Server).ServeHTTP"
	Function string `json:"function,omitempty"`
	// Module is the fully-qualified package path up to (but not including) the
	// function name. Example: "main", "github.com/gofiber/fiber/v3"
	Module string `json:"module,omitempty"`
	// Filename is the absolute path to the source file as embedded in the binary
	// by the Go compiler.
	Filename string `json:"filename,omitempty"`
	// Line is the 1-based source line number.
	Line int `json:"line,omitempty"`
	// InApp is true for frames that belong to the application's own code (not
	// the Go runtime, standard library, or third-party dependencies).
	InApp bool `json:"in_app"`
	// Context holds lines of source code surrounding Filename:Line, read at
	// capture time. Empty when the source file is not accessible.
	Context []ContextLine `json:"context,omitempty"`
}

// Stacktrace holds the ordered list of captured stack frames.
// Frames are stored innermost-first (closest to the error site first).
type Stacktrace struct {
	Frames []StackFrame `json:"frames,omitempty"`
}

// ExceptionMechanism describes how the exception was captured.
type ExceptionMechanism struct {
	// Type is one of "generic" (CaptureException), "panic" (panic recovery), or
	// "signal" (OS signal handler).
	Type string `json:"type,omitempty"`
	// Handled is false when the exception terminated or nearly terminated the process
	// (e.g. an unrecovered panic) and true for handled errors.
	Handled bool `json:"handled"`
}

// ExceptionValue holds a single captured exception together with its type, message,
// mechanism, and full stack trace.
type ExceptionValue struct {
	// Type is the Go type name of the error value (e.g. "*errors.errorString",
	// "*fmt.wrapError", "*MyCustomError").
	Type string `json:"type,omitempty"`
	// Value is the error message (err.Error()).
	Value string `json:"value,omitempty"`
	// Stacktrace is the captured call stack at the point of capture.
	Stacktrace *Stacktrace `json:"stacktrace,omitempty"`
	// Mechanism describes how the exception was captured.
	Mechanism *ExceptionMechanism `json:"mechanism,omitempty"`
}

// ─── Capture helpers ─────────────────────────────────────────────────────────

// BuildExceptionValue constructs an ExceptionValue for err, capturing the
// current goroutine's call stack. skip controls how many call frames to omit
// from the top of the stack (the caller should pass 1; this function adds 1
// more for its own frame). This exported version is intended for use by
// framework middleware sub-packages (bikeeperfiber, bikeeperecho).
func BuildExceptionValue(err error, skip int) *ExceptionValue {
	return buildExceptionValue(err, skip+1)
}

// BuildPanicExceptionValue constructs an ExceptionValue for a recovered panic,
// marking the mechanism as "panic" and Handled: false. Exported for framework
// middleware sub-packages.
func BuildPanicExceptionValue(panicErr error, skip int) *ExceptionValue {
	return buildPanicExceptionValue(panicErr, skip+1)
}

// BuildMessageExceptionValue captures the current call stack and returns a
// synthetic ExceptionValue for use in CaptureMessage events. The mechanism type
// is "generic" with Handled: true, distinguishing message-level events from
// exception captures in the Bikeeper dashboard.
//
// The Type field is set to the caller's function name (from the first in-app
// frame) rather than a fixed "CaptureMessage" string so the Bikeeper issues
// list shows a meaningful origin instead of a generic label.
//
// skip is the number of frames above BuildMessageExceptionValue to omit.
// Pass 1 from a direct CaptureMessage wrapper so that wrapper frame is hidden
// and user code appears as the first visible frame.
func BuildMessageExceptionValue(message string, skip int) *ExceptionValue {
	st := captureStacktrace(skip + 1) // +1 for BuildMessageExceptionValue itself
	exType := callerFunctionName(st)
	return &ExceptionValue{
		Type:       exType,
		Value:      message,
		Mechanism:  &ExceptionMechanism{Type: "generic", Handled: true},
		Stacktrace: st,
	}
}

// callerFunctionName returns the short function name of the first in-app frame
// in st, falling back to "CaptureMessage" when the stacktrace is nil or has no
// in-app frames.
func callerFunctionName(st *Stacktrace) string {
	if st == nil {
		return "CaptureMessage"
	}
	// Frames are innermost-first; the first in-app frame is the call site.
	for _, f := range st.Frames {
		if f.InApp && f.Function != "" {
			return f.Function
		}
	}
	return "CaptureMessage"
}

// ComputeAllFramesGroupingHash returns a 32-character hex fingerprint computed
// from the error type and ALL stack frames (both in-app and framework frames).
// Stored as fingerprint[0]; use ComputeGroupingHash for the in-app-only variant.
func ComputeAllFramesGroupingHash(errType string, frames []StackFrame) string {
	return computeAllFramesGroupingHash(errType, frames)
}

// ComputeGroupingHash returns the 32-character hex fingerprint used to group
// events with the same root cause as a single issue in the dashboard. Exported
// for use by framework middleware sub-packages.
func ComputeGroupingHash(errType string, frames []StackFrame) string {
	return computeGroupingHash(errType, frames)
}

// buildExceptionValue constructs an ExceptionValue for err, capturing the
// current goroutine's call stack. skip controls how many call frames to omit
// from the top of the stack (the caller should pass 1; this function adds 1
// more for its own frame).
func buildExceptionValue(err error, skip int) *ExceptionValue {
	ex := &ExceptionValue{
		Type:      exceptionType(err),
		Value:     err.Error(),
		Mechanism: &ExceptionMechanism{Type: "generic", Handled: true},
	}
	st := captureStacktrace(skip + 1)
	if st != nil && len(st.Frames) > 0 {
		ex.Stacktrace = st
	}
	return ex
}

// buildPanicExceptionValue constructs an ExceptionValue for a recovered panic.
// It marks the mechanism as "panic" and Handled: false.
func buildPanicExceptionValue(panicErr error, skip int) *ExceptionValue {
	ex := buildExceptionValue(panicErr, skip+1)
	ex.Mechanism = &ExceptionMechanism{Type: "panic", Handled: false}
	return ex
}

// captureStacktrace captures the calling goroutine's stack starting skip+1
// frames above captureStacktrace itself, so that internal bikeeper frames are
// excluded from the top. Non-application frames are retained but marked
// InApp: false so the frontend can fold them.
func captureStacktrace(skip int) *Stacktrace {
	pcs := make([]uintptr, 64)
	// +2: runtime.Callers itself + captureStacktrace
	n := runtime.Callers(skip+2, pcs)
	if n == 0 {
		return nil
	}
	cf := runtime.CallersFrames(pcs[:n])
	var frames []StackFrame
	for {
		f, more := cf.Next()
		// Skip bare runtime internals that only add noise.
		if isBareRuntimeFrame(f.Function) {
			if !more {
				break
			}
			continue
		}
		module, function := splitFunctionName(f.Function)
		fr := StackFrame{
			Function: function,
			Module:   module,
			Filename: f.File,
			Line:     f.Line,
			InApp:    isInApp(f.Function, f.File),
		}
		if fr.InApp && fr.Filename != "" && fr.Line > 0 {
			fr.Context = readSourceContext(fr.Filename, fr.Line, 4)
		}
		frames = append(frames, fr)
		if !more {
			break
		}
	}
	if len(frames) == 0 {
		return nil
	}
	return &Stacktrace{Frames: frames}
}

// readSourceContext reads up to n lines before and after targetLine from
// filename and returns them as ContextLine values. Returns nil on any I/O error
// (source file not present in production deployments without source).
func readSourceContext(filename string, targetLine, n int) []ContextLine {
	f, err := os.Open(filename)
	if err != nil {
		return nil
	}
	defer f.Close()

	var all []string
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		all = append(all, sc.Text())
	}
	if sc.Err() != nil || len(all) == 0 {
		return nil
	}

	// Convert to 0-based index.
	idx := targetLine - 1
	if idx < 0 || idx >= len(all) {
		return nil
	}
	start := max(0, idx-n)
	end := min(len(all)-1, idx+n)

	out := make([]ContextLine, 0, end-start+1)
	for i := start; i <= end; i++ {
		out = append(out, ContextLine{
			Line:      i + 1,
			Code:      all[i],
			IsCurrent: i == idx,
		})
	}
	return out
}

// max returns the larger of a and b.
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// min returns the smaller of a and b.
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// ─── Frame classification ─────────────────────────────────────────────────────

// isBareRuntimeFrame returns true for low-level runtime frames that provide no
// diagnostic value (runtime.gopanic, runtime.goexit, etc.).
func isBareRuntimeFrame(fn string) bool {
	switch fn {
	case "runtime.gopanic", "runtime.goexit", "runtime.sigpanic",
		"runtime.morestack", "runtime.newstack":
		return true
	}
	return false
}

// isInApp returns true when the frame belongs to the application's own code.
// Standard library, Go runtime, third-party dependencies (go/pkg/mod), and
// the Bikeeper SDK itself are excluded.
func isInApp(fn, file string) bool {
	if strings.HasPrefix(fn, "runtime.") ||
		strings.HasPrefix(fn, "reflect.") ||
		strings.HasPrefix(fn, "testing.") {
		return false
	}
	if strings.Contains(file, "/vendor/") ||
		strings.Contains(file, "go/pkg/mod/") ||
		strings.Contains(file, filepath.Join("pkg", "mod")) {
		return false
	}
	// Exclude the Bikeeper SDK's own frames.
	if strings.Contains(fn, "github.com/MhasbiM/bikeeper-go-sdk") {
		return false
	}
	return true
}

// splitFunctionName splits a fully-qualified Go function name
// (e.g. "github.com/example/pkg.(*Type).Method") into a (module, function) pair
// where module is the package path and function is the short name.
func splitFunctionName(fn string) (module, function string) {
	// Find the last slash to isolate the package.function part.
	slashIdx := strings.LastIndex(fn, "/")
	if slashIdx < 0 {
		// No slash: simple "pkg.Function" pattern.
		dot := strings.IndexByte(fn, '.')
		if dot < 0 {
			return "", fn
		}
		return fn[:dot], fn[dot+1:]
	}
	after := fn[slashIdx+1:] // "pkg.Function" or "pkg.Type.Method"
	dot := strings.IndexByte(after, '.')
	if dot < 0 {
		return fn, ""
	}
	return fn[:slashIdx+1+dot], after[dot+1:]
}

// ─── Exception type introspection ────────────────────────────────────────────

// exceptionType returns the Go type name of err suitable for grouping
// (e.g. "*errors.errorString", "*fmt.wrapError", "*MyPkg.NotFoundError").
func exceptionType(err error) string {
	if err == nil {
		return ""
	}
	t := reflect.TypeOf(err)
	if t == nil {
		return "error"
	}
	return t.String()
}

// ─── Grouping hash ───────────────────────────────────────────────────────────

// computeGroupingHash returns a 32-character hex fingerprint that identifies
// an exception uniquely by its type and the ordered list of in-app stack frames.
// Events sharing the same hash are grouped as the "same issue" in the dashboard.
func computeGroupingHash(errType string, frames []StackFrame) string {
	h := sha256.New()
	h.Write([]byte(errType))
	for _, f := range frames {
		if f.InApp {
			h.Write([]byte("\x00"))
			h.Write([]byte(f.Module))
			h.Write([]byte("\x00"))
			h.Write([]byte(f.Function))
		}
	}
	return hex.EncodeToString(h.Sum(nil))[:32]
}

// computeAllFramesGroupingHash returns a 32-character hex fingerprint using ALL
// frames (in-app and framework), giving a broader uniqueness signal.
func computeAllFramesGroupingHash(errType string, frames []StackFrame) string {
	h := sha256.New()
	h.Write([]byte("all\x00"))
	h.Write([]byte(errType))
	for _, f := range frames {
		h.Write([]byte("\x00"))
		h.Write([]byte(f.Module))
		h.Write([]byte("\x00"))
		h.Write([]byte(f.Function))
	}
	return hex.EncodeToString(h.Sum(nil))[:32]
}
