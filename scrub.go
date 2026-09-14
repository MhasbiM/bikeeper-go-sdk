package bikeeper

import "strings"

// Redacted replaces a withheld value. A placeholder rather than a deletion, so
// it stays visible that the field existed and was deliberately not sent.
const Redacted = "[redacted]"

// DefaultSensitiveTagKeys are the tag keys [ScrubSensitive] redacts.
//
// "args" earns its place at the top: database drivers log a failed statement
// with its bind parameters, which is exactly where customer names, phone
// numbers and identifiers live. The rest are the usual credential carriers
// that a log call site picks up without anyone noticing.
var DefaultSensitiveTagKeys = []string{
	"args",
	"api_key",
	"apikey",
	"authorization",
	"credential",
	"credentials",
	"password",
	"passwd",
	"secret",
	"token",
}

// DefaultSensitiveHeaders are the request headers [ScrubSensitive] redacts
// from captured HTTP context.
var DefaultSensitiveHeaders = []string{
	"authorization",
	"cookie",
	"proxy-authorization",
	"set-cookie",
	"x-api-key",
	"x-auth-token",
}

// ScrubSensitive redacts personal data and credentials from an event on its
// way out. Install it as [Options.BeforeSend]:
//
//	client := bikeeper.New(bikeeper.Options{
//	    // ...
//	    BeforeSend: bikeeper.ScrubSensitive,
//	})
//
// Error monitoring quietly widens the blast radius of a leak: whatever a log
// call happens to carry ends up in a dashboard, in the task tracker, and in an
// AI prompt. Deciding what may leave here — once, on the way out — rather than
// at every log call site is the only version of this that stays true as the
// code changes.
//
// Use [NewScrubber] to extend the key list with what a particular application
// treats as sensitive.
func ScrubSensitive(event *Event) *Event {
	return defaultScrubber(event)
}

var defaultScrubber = NewScrubber()

// NewScrubber returns a [Options.BeforeSend] function that redacts
// [DefaultSensitiveTagKeys] plus any extra tag keys given here. Matching is
// case-insensitive.
//
//	BeforeSend: bikeeper.NewScrubber("visitor_phone", "table_code"),
func NewScrubber(extraTagKeys ...string) func(*Event) *Event {
	tagKeys := make(map[string]bool, len(DefaultSensitiveTagKeys)+len(extraTagKeys))
	for _, key := range DefaultSensitiveTagKeys {
		tagKeys[strings.ToLower(key)] = true
	}
	for _, key := range extraTagKeys {
		tagKeys[strings.ToLower(key)] = true
	}

	headers := make(map[string]bool, len(DefaultSensitiveHeaders))
	for _, name := range DefaultSensitiveHeaders {
		headers[strings.ToLower(name)] = true
	}

	return func(event *Event) *Event {
		if event == nil {
			return nil
		}
		for i, tag := range event.Tags {
			if tagKeys[strings.ToLower(tag.Key)] {
				event.Tags[i].Value = Redacted
			}
		}
		if event.HTTPRequest != nil {
			for name := range event.HTTPRequest.Headers {
				if headers[strings.ToLower(name)] {
					event.HTTPRequest.Headers[name] = Redacted
				}
			}
		}
		return event
	}
}
