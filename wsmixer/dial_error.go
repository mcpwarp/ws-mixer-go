package wsmixer

import (
	"net/http"
	"strconv"
	"time"
)

// DialError carries the detail behind a failed Dial that plain error-string
// matching can't recover: it is what Client's reconnect state machine
// (client_reconnect.go) inspects, via errors.As, to classify a dial failure
// per WIRE.md section 2.9's reconnect table -- HTTP status, a parsed
// Retry-After, and whether the failure is fatal. Wrapping an error in a
// *DialError never changes Dial's existing error-string format: Error()
// returns the wrapped error's message verbatim, so `fmt.Errorf("wsmixer:
// dial: %w", dialErr)` prints exactly what it always has.
type DialError struct {
	// Err is the underlying error: the transport/library error from
	// websocket.Dial, or a locally constructed one (e.g. the subprotocol
	// mismatch case in Dial).
	Err error
	// HTTPStatus is the HTTP status code of the failed upgrade response
	// (401, 403, 404, 429, a 5xx, ...), or 0 if the failure never reached an
	// HTTP response at all (DNS failure, TCP-level refusal, the 10s connect
	// timeout).
	HTTPStatus int
	// RetryAfter/HasRetryAfter carry a 429 response's parsed Retry-After
	// header (WIRE.md section 2.9: "429: honour Retry-After"), whether the
	// server sent delta-seconds or an HTTP-date (RFC 9110 section 10.2.3).
	RetryAfter    time.Duration
	HasRetryAfter bool
	// Fatal marks a dial failure WIRE.md section 2.9's reconnect table says
	// must never be retried: HTTP 403/404, or a missing/mismatched
	// subprotocol echo. HTTP 401 is deliberately NOT marked fatal here --
	// the reconnect layer owns the "call the token provider once more and
	// retry immediately" rule and decides 401's fatality itself, only after
	// that one retry has already failed.
	Fatal bool
	// Mismatch is true specifically for a missing/mismatched subprotocol
	// echo, so a caller can tell the two fatal, non-auth dial failures
	// (Mismatch vs. plain HTTPStatus 403/404) apart without parsing Error().
	Mismatch bool
}

func (e *DialError) Error() string { return e.Err.Error() }
func (e *DialError) Unwrap() error { return e.Err }

// newDialError classifies a websocket.Dial failure using the *http.Response
// it returned alongside the error (coder/websocket's Dial hands one back
// whenever the failure happened at the HTTP layer, e.g. a non-101 status;
// resp is nil for a failure below that, like a DNS error, TCP refusal, or
// the connect timeout).
func newDialError(err error, resp *http.Response) *DialError {
	de := &DialError{Err: err}
	if resp == nil {
		return de
	}
	de.HTTPStatus = resp.StatusCode
	switch resp.StatusCode {
	case http.StatusForbidden, http.StatusNotFound:
		de.Fatal = true
	case http.StatusTooManyRequests:
		if d, ok := parseRetryAfter(resp.Header.Get("Retry-After")); ok {
			de.RetryAfter = d
			de.HasRetryAfter = true
		}
	}
	// HTTP 401 is deliberately left Fatal=false: see the Fatal field doc.
	return de
}

// parseRetryAfter parses an HTTP Retry-After header value: either
// delta-seconds or an HTTP-date (RFC 9110 section 10.2.3).
func parseRetryAfter(v string) (time.Duration, bool) {
	if v == "" {
		return 0, false
	}
	if secs, err := strconv.Atoi(v); err == nil {
		if secs < 0 {
			secs = 0
		}
		return time.Duration(secs) * time.Second, true
	}
	if t, err := http.ParseTime(v); err == nil {
		d := time.Until(t)
		if d < 0 {
			d = 0
		}
		return d, true
	}
	return 0, false
}
