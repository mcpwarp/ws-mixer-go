package wsmixer

// Tests for ErrTokenUnavailable (client_reconnect.go): a TokenProvider MAY
// mark a failure to obtain a token as temporary, and Client then treats the
// attempt like an ordinary failed dial instead of going fatal -- CLIENT-SDK.md's
// "Provider failure" row, WIRE.md section 2.9's Reconnect table, and
// docs/DECISIONS.md's entry implementing spec D-2026-09-20-08.

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

// TestClientMarkedProviderFailureNonFatalReconnects is the core positive
// case: a marked provider failure on a reconnect gets exactly one non-fatal
// report {Phase: dial, Fatal: false, Cause wrapping ErrTokenUnavailable, and
// Cause the exact object the provider returned}, backs off with the normal
// fullJitter ladder (no immediate retry), and the next attempt calls the
// provider again and connects. This also pins the "first Connect" behavior:
// the very first attempt fails marked, and Connect() does not return an
// error for it -- it keeps waiting for the later success, exactly like any
// other recoverable first-attempt failure (e.g. connection refused).
func TestClientMarkedProviderFailureNonFatalReconnects(t *testing.T) {
	_, url := startTestServer(t, testAcceptHandler{})
	rec := newDisconnectRecorder()
	fa := &fakeAfter{}
	rc := testReconnectOptions(fa, 0.5)

	markedErr := fmt.Errorf("auth server unreachable: %w", ErrTokenUnavailable)
	var calls atomic.Int32
	token := TokenProvider(func(context.Context) (string, error) {
		if calls.Add(1) == 1 {
			return "", markedErr
		}
		return "tok", nil
	})
	cl := NewClient(url, token, ClientConfig{
		Reconnect:    rc,
		OnDisconnect: rec.onDisconnect,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// Connect() blocks past the first, marked-failed attempt and only
	// returns once the retry actually welcomes -- it must NOT surface the
	// marked failure as Connect's own error.
	if err := cl.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v (a marked provider failure on the first attempt must not fail Connect)", err)
	}
	defer cl.Close(context.Background())

	r := rec.waitNext(t)
	if r.Phase != PhaseDial {
		t.Errorf("Phase = %v, want dial", r.Phase)
	}
	if r.Fatal {
		t.Error("Fatal = true, want false")
	}
	if !errors.Is(r.Cause, ErrTokenUnavailable) {
		t.Errorf("Cause = %v, does not wrap ErrTokenUnavailable", r.Cause)
	}
	if r.Cause != markedErr {
		t.Errorf("Cause = %v (%p), want the exact provider error object %v (%p)", r.Cause, r.Cause, markedErr, markedErr)
	}
	if got := calls.Load(); got != 2 {
		t.Errorf("token provider called %d times, want 2 (marked failure + the retry that connects)", got)
	}

	// fullJitter backoff, not an immediate retry: Base=10ms, rand=0.5 ->
	// fullJitter(1) = 0.5 * min(cap, 10ms*2^1) = 10ms exactly.
	delays := waitForBackoffCalls(t, fa, rc, 1)
	if want := 10 * time.Millisecond; delays[0] != want {
		t.Errorf("backoff delay = %v, want exactly %v (fullJitter(1), rand=0.5)", delays[0], want)
	}
}

// TestClientMarkedProviderFailureExhaustsMaxAttempts checks that repeated
// marked failures still count as ordinary failed attempts: the backoff
// ladder climbs exactly like any other recoverable failure, and MaxAttempts
// still exhausts to a fatal disconnect once it's used up -- a marked failure
// is not a free pass around the bounded-retry budget.
func TestClientMarkedProviderFailureExhaustsMaxAttempts(t *testing.T) {
	rec := newDisconnectRecorder()
	fa := &fakeAfter{}
	rc := testReconnectOptions(fa, 0.5)
	rc.MaxAttempts = 3

	markedErr := fmt.Errorf("network down: %w", ErrTokenUnavailable)
	token := TokenProvider(func(context.Context) (string, error) { return "", markedErr })
	// No server is ever actually dialed: the provider fails on every call,
	// before Dial is ever reached.
	cl := NewClient("ws://127.0.0.1:1/tunnel", token, ClientConfig{
		Reconnect:    rc,
		OnDisconnect: rec.onDisconnect,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := cl.Connect(ctx); err == nil {
		t.Fatal("Connect should fail once MaxAttempts is exhausted")
	}

	// Base=10ms, Cap=1s, rand=0.5: fullJitter(1)=10ms, fullJitter(2)=20ms,
	// fullJitter(3)=40ms -- the climbing ladder a real (unmarked) recoverable
	// failure would also produce.
	delays := waitForCalls(t, fa, 3)
	want := []time.Duration{10 * time.Millisecond, 20 * time.Millisecond, 40 * time.Millisecond}
	for i, w := range want {
		if delays[i] != w {
			t.Errorf("delays[%d] = %v, want %v (climbing fullJitter ladder: %v)", i, delays[i], w, delays)
		}
	}

	// Connect() returning an error only means the exhaustion branch ran --
	// cl.report(reason) there just enqueues onto callbackLoop's own queue
	// (report/enqueueCb, client_reconnect.go), asynchronously to
	// finishFirstResult, so the final fatal DisconnectReason can still be
	// in flight when Connect() returns. Wait on the recorder's channel
	// itself, not State()/a snapshot poll, so this doesn't race that queue.
	var last DisconnectReason
	for i := 0; i < 10; i++ {
		last = rec.waitNext(t)
		if last.Fatal {
			break
		}
	}
	if !last.Fatal {
		t.Fatalf("no fatal DisconnectReason ever arrived (last seen: %+v)", last)
	}
	if !errors.Is(last.Cause, ErrTokenUnavailable) {
		t.Errorf("final reason.Cause = %v, does not wrap ErrTokenUnavailable", last.Cause)
	}
	if last.Phase != PhaseDial {
		t.Errorf("final reason.Phase = %v, want dial", last.Phase)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && cl.State() != "closed" {
		time.Sleep(10 * time.Millisecond)
	}
	if got := cl.State(); got != "closed" {
		t.Fatalf("State() = %s, want closed after MaxAttempts exhausted", got)
	}
}

// customTemporaryErr is a TokenProvider error type that marks itself
// temporary via an Is method rather than wrapping ErrTokenUnavailable with
// fmt.Errorf's %w -- CLIENT-SDK.md's rule is "a custom error whose Is
// matches it", not specifically %w-wrapping, and errors.Is consults Is
// methods on its own.
type customTemporaryErr struct{ msg string }

func (e *customTemporaryErr) Error() string { return e.msg }
func (e *customTemporaryErr) Is(target error) bool {
	return target == ErrTokenUnavailable
}

// TestClientCustomIsMethodTreatedAsMarked checks that a provider error
// marking itself temporary via a custom Is method (not %w-wrapping) is
// detected the same way a wrapped sentinel is -- errors.Is is the only
// detection mechanism, and it already supports this per the standard
// library's own contract.
func TestClientCustomIsMethodTreatedAsMarked(t *testing.T) {
	_, url := startTestServer(t, testAcceptHandler{})
	rec := newDisconnectRecorder()
	fa := &fakeAfter{}
	rc := testReconnectOptions(fa, 0)

	markedErr := &customTemporaryErr{msg: "custom temporary failure"}
	var calls atomic.Int32
	token := TokenProvider(func(context.Context) (string, error) {
		if calls.Add(1) == 1 {
			return "", markedErr
		}
		return "tok", nil
	})
	cl := NewClient(url, token, ClientConfig{
		Reconnect:    rc,
		OnDisconnect: rec.onDisconnect,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := cl.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer cl.Close(context.Background())

	r := rec.waitNext(t)
	if r.Fatal {
		t.Error("Fatal = true, want false (custom Is method should mark this temporary)")
	}
	if !errors.Is(r.Cause, ErrTokenUnavailable) {
		t.Errorf("Cause = %v, does not wrap ErrTokenUnavailable via its Is method", r.Cause)
	}
}

// duckTypedRetryableErr has both a Retryable() and a Temporary() bool
// method, the exact shape a naive "infer temporary from the error" check
// would duck-type on -- but it does NOT wrap or match ErrTokenUnavailable.
// The anti-duck-typing pin: this must stay FATAL.
type duckTypedRetryableErr struct{ msg string }

func (e *duckTypedRetryableErr) Error() string   { return e.msg }
func (e *duckTypedRetryableErr) Retryable() bool { return true }
func (e *duckTypedRetryableErr) Temporary() bool { return true }

// TestClientDuckTypedRetryableErrorStaysFatal is CLIENT-SDK.md's explicit
// anti-duck-typing requirement: an error that merely LOOKS retryable (a
// Retryable()/Temporary() bool method, exactly the kind of shape a naive
// implementation might infer "temporary" from) but does not wrap or match
// ErrTokenUnavailable via errors.Is must still be treated as an ordinary,
// fatal, unmarked provider failure -- opt-in only, no inference from shape.
func TestClientDuckTypedRetryableErrorStaysFatal(t *testing.T) {
	url, _ := newFlakyServer(t, nil, testAcceptHandler{})
	boom := &duckTypedRetryableErr{msg: "looks retryable but isn't marked"}
	token := TokenProvider(func(context.Context) (string, error) { return "", boom })
	rec := newDisconnectRecorder()
	fa := &fakeAfter{}
	cl := NewClient(url, token, ClientConfig{
		Reconnect:    testReconnectOptions(fa, 0),
		OnDisconnect: rec.onDisconnect,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := cl.Connect(ctx); err == nil {
		t.Fatal("Connect should fail: an unmarked provider error (even one with Retryable()/Temporary() methods) is fatal")
	}
	r := rec.waitNext(t)
	if !r.Fatal {
		t.Error("Fatal = false, want true (duck-typed shape must not be treated as marked)")
	}
	if errors.Is(r.Cause, ErrTokenUnavailable) {
		t.Error("errors.Is(Cause, ErrTokenUnavailable) = true, want false: this error was never marked")
	}
	if !errors.Is(r.Cause, boom) {
		t.Errorf("Cause = %v, want the exact provider error object %v (verbatim)", r.Cause, boom)
	}
}

// TestClientMarkedRefreshRetryThenLaterFatal covers the refresh-retry
// interaction CLIENT-SDK.md's "Provider failure" row calls out explicitly:
// "If the provider's call on this one refresh-retry fails with the
// marked-temporary signal ... the refresh budget is still spent -- it does
// not get a second attempt -- and the failure takes the ordinary recoverable
// path". Sequence: first dial gets HTTP 401 -> the one-time refresh-retry
// calls the provider again, which returns a MARKED failure -> non-fatal,
// ordinary backoff, budget spent. A later attempt is rejected again (401)
// before the connection ever stabilizes: since the refresh-retry budget is
// already spent, dialAndHandshake goes straight to fatal (markFatal) without
// calling the provider a second time for that attempt.
func TestClientMarkedRefreshRetryThenLaterFatal(t *testing.T) {
	url, _ := newFlakyServer(t, []int{401, 401}, testAcceptHandler{})

	markedErr := fmt.Errorf("auth server unreachable: %w", ErrTokenUnavailable)
	var calls atomic.Int32
	token := TokenProvider(func(context.Context) (string, error) {
		switch calls.Add(1) {
		case 1:
			return "tok-a", nil // the attempt that hits the first 401
		case 2:
			return "", markedErr // the refresh-retry's own call: marked
		case 3:
			return "tok-b", nil // the next attempt, which hits the second 401
		default:
			return "", errors.New("token provider called more times than this test expects")
		}
	})
	rec := newDisconnectRecorder()
	fa := &fakeAfter{}
	cl := NewClient(url, token, ClientConfig{
		Reconnect:    testReconnectOptions(fa, 0),
		OnDisconnect: rec.onDisconnect,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := cl.Connect(ctx); err == nil {
		t.Fatal("Connect should fail: the second (post-refresh-retry-budget) 401 rejection is fatal")
	}

	// First report: the refresh-retry's own call came back marked -- non-fatal.
	first := rec.waitNext(t)
	if first.Fatal {
		t.Errorf("first reason.Fatal = true, want false (a marked refresh-retry failure is not itself fatal)")
	}
	if !errors.Is(first.Cause, ErrTokenUnavailable) {
		t.Errorf("first reason.Cause = %v, does not wrap ErrTokenUnavailable", first.Cause)
	}

	// Second report: the next attempt's own 401, with the refresh-retry
	// budget already spent, goes straight to fatal.
	second := rec.waitNext(t)
	if !second.Fatal {
		t.Errorf("second reason.Fatal = false, want true (budget already spent, no further refresh-retry)")
	}
	if second.HTTPStatus != 401 {
		t.Errorf("second reason.HTTPStatus = %d, want 401", second.HTTPStatus)
	}

	if got := calls.Load(); got != 3 {
		t.Errorf("token provider called %d times, want exactly 3 (no fourth call: the budget was already spent)", got)
	}
	if cl.State() != "closed" {
		t.Errorf("State() = %s, want closed", cl.State())
	}
}
