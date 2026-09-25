package wsmixer

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// --- pure unit tests: jitter math and error classification -----------------

func TestFullJitterBounds(t *testing.T) {
	rc := ReconnectOptions{Base: 1000 * time.Millisecond, Cap: 60 * time.Second}
	rc.setDefaults()
	for attempt := 0; attempt <= 8; attempt++ {
		want := float64(rc.Base) * pow2(attempt)
		if want > float64(rc.Cap) {
			want = float64(rc.Cap)
		}
		for i := 0; i < 20; i++ {
			d := rc.fullJitter(attempt)
			if d < 0 || float64(d) > want {
				t.Fatalf("attempt=%d: fullJitter=%v out of bounds [0,%v]", attempt, d, time.Duration(want))
			}
		}
	}
}

func pow2(n int) float64 {
	r := 1.0
	for i := 0; i < n; i++ {
		r *= 2
	}
	return r
}

func TestJitterBounds(t *testing.T) {
	rc := ReconnectOptions{}
	rc.setDefaults()
	for i := 0; i < 50; i++ {
		d := rc.jitter(2 * time.Second)
		if d < 0 || d > 2*time.Second {
			t.Fatalf("jitter(2s) = %v, out of bounds", d)
		}
	}
	if d := rc.jitter(0); d != 0 {
		t.Errorf("jitter(0) = %v, want 0", d)
	}
}

func TestClassifyFailureErr(t *testing.T) {
	cases := []struct {
		name      string
		err       error
		wantPhase DisconnectPhase
		wantFatal bool
		wantHTTP  int
		wantCode  ErrorCode
		hasCode   bool
	}{
		{
			name:      "http 401 not fatal at this layer",
			err:       fmt.Errorf("wrap: %w", newDialError(errors.New("401"), &http.Response{StatusCode: 401})),
			wantPhase: PhaseDial, wantFatal: false, wantHTTP: 401,
		},
		{
			name:      "http 403 fatal",
			err:       newDialError(errors.New("403"), &http.Response{StatusCode: 403}),
			wantPhase: PhaseDial, wantFatal: true, wantHTTP: 403,
		},
		{
			name:      "http 404 fatal",
			err:       newDialError(errors.New("404"), &http.Response{StatusCode: 404}),
			wantPhase: PhaseDial, wantFatal: true, wantHTTP: 404,
		},
		{
			name:      "plain network error (1006-equivalent)",
			err:       newDialError(errors.New("connection refused"), nil),
			wantPhase: PhaseDial, wantFatal: false,
		},
		{
			name:      "subprotocol mismatch fatal",
			err:       &DialError{Err: errors.New("mismatch"), Fatal: true, Mismatch: true},
			wantPhase: PhaseDial, wantFatal: true, wantCode: UnsupportedCode, hasCode: true,
		},
		{
			name:      "handshake unauthorized not fatal until forced",
			err:       &ConnError{Code: UnauthorizedCode, Message: "nope"},
			wantPhase: PhaseHandshake, wantFatal: false, wantCode: UnauthorizedCode, hasCode: true,
		},
		{
			name:      "forced fatal (second refresh failure)",
			err:       markFatal(&ConnError{Code: UnauthorizedCode, Message: "nope again"}),
			wantPhase: PhaseHandshake, wantFatal: true, wantCode: UnauthorizedCode, hasCode: true,
		},
		{
			name:      "handshake unsupported always fatal",
			err:       &ConnError{Code: UnsupportedCode, Message: "v mismatch"},
			wantPhase: PhaseHandshake, wantFatal: true, wantCode: UnsupportedCode, hasCode: true,
		},
		{
			name:      "provider error always fatal",
			err:       &providerError{cause: errors.New("boom")},
			wantPhase: PhaseDial, wantFatal: true,
		},
		// --- D-2026-09-20-09: bare-close ErrorCode/ErrorName derivation ---
		{
			name:      "bare close in ws-mixer range derives ErrorCode/ErrorName",
			err:       websocket.CloseError{Code: 4014, Reason: "over cap"},
			wantPhase: PhaseHandshake, wantFatal: false, wantCode: ApplicationCloseCode, hasCode: true,
		},
		{
			name:      "bare close with an unknown ws-mixer code derives INTERNAL_ERROR",
			err:       websocket.CloseError{Code: 4777, Reason: "?"},
			wantPhase: PhaseHandshake, wantFatal: false, wantCode: ErrorCode(777), hasCode: true,
		},
		{
			name:      "bare close 1001 (not a ws-mixer code) has no ErrorCode",
			err:       websocket.CloseError{Code: 1001, Reason: "going away"},
			wantPhase: PhaseHandshake, wantFatal: false,
		},
		{
			name:      "error{} preceding a 4009 close keeps the message's code, not the derived one",
			err:       &ConnError{Code: EnhanceYourCalm, Message: "over cap"},
			wantPhase: PhaseHandshake, wantFatal: false, wantCode: EnhanceYourCalm, hasCode: true,
		},
		{
			name:      "http 401 never gets an ErrorCode (HTTPStatus only)",
			err:       newDialError(errors.New("401"), &http.Response{StatusCode: 401}),
			wantPhase: PhaseDial, wantFatal: false, wantHTTP: 401,
		},
		{
			name:      "http 403 never gets an ErrorCode (HTTPStatus only)",
			err:       newDialError(errors.New("403"), &http.Response{StatusCode: 403}),
			wantPhase: PhaseDial, wantFatal: true, wantHTTP: 403,
		},
		{
			name:      "http 503 never gets an ErrorCode (HTTPStatus only)",
			err:       newDialError(errors.New("503"), &http.Response{StatusCode: 503}),
			wantPhase: PhaseDial, wantFatal: false, wantHTTP: 503,
		},
		{
			name:      "abnormal closure (no close frame) has no ErrorCode",
			err:       newDialError(errors.New("connection refused"), nil),
			wantPhase: PhaseDial, wantFatal: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fi := classifyFailureErr(tc.err)
			if fi.phase != tc.wantPhase {
				t.Errorf("phase = %v, want %v", fi.phase, tc.wantPhase)
			}
			if fi.fatal != tc.wantFatal {
				t.Errorf("fatal = %v, want %v", fi.fatal, tc.wantFatal)
			}
			if fi.httpStatus != tc.wantHTTP {
				t.Errorf("httpStatus = %d, want %d", fi.httpStatus, tc.wantHTTP)
			}
			// hasErrorCode is checked unconditionally (not just when
			// tc.hasCode is true): D-2026-09-20-09 requires ErrorCode be
			// left UNSET for anything that isn't a ws-mixer code, and this
			// table exists specifically to pin both directions.
			if fi.hasErrorCode != tc.hasCode {
				t.Errorf("hasErrorCode = %v, want %v", fi.hasErrorCode, tc.hasCode)
			}
			if tc.hasCode && fi.errCode != tc.wantCode {
				t.Errorf("errCode = %v, want %v", fi.errCode, tc.wantCode)
			}
		})
	}
}

// TestBuildConnectedDisconnectReasonBareCloseDerivation unit-tests the
// connected-phase half of D-2026-09-20-09's derivation (classifyFailureErr's
// table above covers the handshake-phase half): buildConnectedDisconnectReason
// derives ErrorCode/ErrorName from a bare close in ws-mixer's own range, and
// leaves them unset for an abnormal closure or an ordinary non-ws-mixer close
// code -- built directly against a *Conn (newUnrunConn, close_before_run_test.go),
// no network needed.
func TestBuildConnectedDisconnectReasonBareCloseDerivation(t *testing.T) {
	cl := &Client{}

	t.Run("bare 4014 in range derives ErrorCode/ErrorName", func(t *testing.T) {
		ws := newFakeWS()
		c := newUnrunConn(ws)
		c.mu.Lock()
		c.observedCloseCode = 4014
		c.err = errors.New("ws-mixer: peer closed with code 4014")
		c.mu.Unlock()
		r := cl.buildConnectedDisconnectReason(c)
		if r.WSCode != 4014 {
			t.Errorf("WSCode = %d, want 4014", r.WSCode)
		}
		if !r.HasErrorCode || r.ErrorCode != ApplicationCloseCode || r.ErrorName != "APPLICATION_CLOSE" {
			t.Errorf("HasErrorCode/ErrorCode/ErrorName = %v/%v/%q, want true/ApplicationCloseCode/APPLICATION_CLOSE", r.HasErrorCode, r.ErrorCode, r.ErrorName)
		}
	})

	t.Run("bare 1001 (not a ws-mixer code) has no ErrorCode", func(t *testing.T) {
		ws := newFakeWS()
		c := newUnrunConn(ws)
		c.mu.Lock()
		c.observedCloseCode = 1001
		c.err = errors.New("ws-mixer: peer closed with code 1001")
		c.mu.Unlock()
		r := cl.buildConnectedDisconnectReason(c)
		if r.WSCode != 1001 {
			t.Errorf("WSCode = %d, want 1001", r.WSCode)
		}
		if r.HasErrorCode {
			t.Errorf("HasErrorCode = true, want false: 1001 is not a ws-mixer close code")
		}
	})

	t.Run("abnormal closure (no close frame observed) has no ErrorCode", func(t *testing.T) {
		ws := newFakeWS()
		c := newUnrunConn(ws)
		// observedCloseCode stays at its newConn-assigned -1 (none observed).
		c.mu.Lock()
		c.err = errors.New("ws-mixer: write failed: connection reset")
		c.mu.Unlock()
		r := cl.buildConnectedDisconnectReason(c)
		if r.WSCode != 0 {
			t.Errorf("WSCode = %d, want 0 (never fabricated)", r.WSCode)
		}
		if r.HasErrorCode {
			t.Errorf("HasErrorCode = true, want false: no close frame was ever observed")
		}
	})
}

func TestIsUnauthorized(t *testing.T) {
	if !isUnauthorized(newDialError(errors.New("x"), &http.Response{StatusCode: 401})) {
		t.Error("HTTP 401 should be unauthorized")
	}
	if !isUnauthorized(&ConnError{Code: UnauthorizedCode}) {
		t.Error("ConnError{UnauthorizedCode} should be unauthorized")
	}
	if isUnauthorized(&ConnError{Code: UnsupportedCode}) {
		t.Error("ConnError{UnsupportedCode} should not be unauthorized")
	}
	if isUnauthorized(newDialError(errors.New("x"), &http.Response{StatusCode: 403})) {
		t.Error("HTTP 403 should not be unauthorized")
	}
}

// --- network-backed reconnect state machine tests ---------------------------

// fakeAfter is a deterministic stand-in for time.After: it fires immediately
// (so tests never actually sleep), while recording every requested delay so
// a test can assert on WIRE.md section 2.9's exact backoff/jitter values.
type fakeAfter struct {
	mu    sync.Mutex
	calls []time.Duration
}

func (f *fakeAfter) after(d time.Duration) <-chan time.Time {
	f.mu.Lock()
	f.calls = append(f.calls, d)
	f.mu.Unlock()
	ch := make(chan time.Time, 1)
	ch <- time.Now()
	return ch
}

func (f *fakeAfter) snapshot() []time.Duration {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]time.Duration(nil), f.calls...)
}

// waitForCalls polls fa until it has recorded at least n delays, or fails
// the test: reportAndSchedule reports a DisconnectReason synchronously but
// calls rc.after() from a separately spawned goroutine (attemptLoop), so a
// test must not assume the delay is already recorded the instant
// OnDisconnect fires.
func waitForCalls(t *testing.T, fa *fakeAfter, n int) []time.Duration {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		calls := fa.snapshot()
		if len(calls) >= n {
			return calls
		}
		if time.Now().After(deadline) {
			t.Fatalf("only %d delays recorded after 2s, want >= %d", len(calls), n)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// nonStabilityCalls filters raw fa.snapshot() output down to backoff/jitter
// delays only, dropping every armStability arm (client_reconnect.go: every
// successful welcome now also calls rc.after(rc.StableAfter), sharing the
// same seam the backoff/jitter delays themselves use, per StableAfter's own
// doc comment on why it must). Distinguishable purely by value, not
// position -- armStability's call can land anywhere in the slice relative
// to a test's own backoff/jitter calls, racing goroutine scheduling, not
// something a test should ever depend on ordering-wise. testReconnectOptions
// never sets StableAfter, so it defaults to 10s -- always far larger than
// any Base=10ms/Cap=1s backoff value or any jitter(0,2s) call these tests
// configure.
func nonStabilityCalls(calls []time.Duration, stableAfter time.Duration) []time.Duration {
	out := make([]time.Duration, 0, len(calls))
	for _, d := range calls {
		if d == stableAfter {
			continue
		}
		out = append(out, d)
	}
	return out
}

// waitForBackoffCalls is waitForCalls, but counting (and returning) only
// the non-stability delays (nonStabilityCalls) -- for a test asserting a
// specific backoff/jitter value, since raw waitForCalls's count now also
// includes however many armStability arms have fired by the time it polls.
// stableAfterOf returns rc.StableAfter, defaulted the same way
// ReconnectOptions.setDefaults does -- testReconnectOptions builds a
// ReconnectOptions directly, without ever calling setDefaults itself (only
// NewClient's own internal copy gets defaulted), so a test's local rc
// variable still reads StableAfter's zero value.
func stableAfterOf(rc ReconnectOptions) time.Duration {
	if rc.StableAfter <= 0 {
		return 10 * time.Second
	}
	return rc.StableAfter
}

func waitForBackoffCalls(t *testing.T, fa *fakeAfter, rc ReconnectOptions, n int) []time.Duration {
	t.Helper()
	stableAfter := stableAfterOf(rc)
	deadline := time.Now().Add(2 * time.Second)
	for {
		calls := nonStabilityCalls(fa.snapshot(), stableAfter)
		if len(calls) >= n {
			return calls
		}
		if time.Now().After(deadline) {
			t.Fatalf("only %d non-stability delays recorded after 2s, want >= %d (raw: %v)", len(calls), n, fa.snapshot())
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func fixedRand(v float64) func() float64 { return func() float64 { return v } }

func testReconnectOptions(fa *fakeAfter, rnd float64) ReconnectOptions {
	rc := ReconnectOptions{
		Base: 10 * time.Millisecond, Cap: time.Second, ConnectTimeout: 2 * time.Second,
	}
	rc.after = fa.after
	rc.randFloat64 = fixedRand(rnd)
	return rc
}

// disconnectRecorder collects every DisconnectReason a Client reports, in
// order, safe for concurrent use.
type disconnectRecorder struct {
	mu      sync.Mutex
	reasons []DisconnectReason
	ch      chan DisconnectReason
}

func newDisconnectRecorder() *disconnectRecorder {
	return &disconnectRecorder{ch: make(chan DisconnectReason, 64)}
}

func (r *disconnectRecorder) onDisconnect(reason DisconnectReason) {
	r.mu.Lock()
	r.reasons = append(r.reasons, reason)
	r.mu.Unlock()
	select {
	case r.ch <- reason:
	default:
	}
}

// snapshotAll returns every DisconnectReason recorded so far, in order.
func (r *disconnectRecorder) snapshotAll() []DisconnectReason {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]DisconnectReason(nil), r.reasons...)
}

func (r *disconnectRecorder) waitNext(t *testing.T) DisconnectReason {
	t.Helper()
	select {
	case reason := <-r.ch:
		return reason
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for a DisconnectReason")
		return DisconnectReason{}
	}
}

// flakyPreUpgrade returns statuses[i] for the i-th HTTP request (before any
// WebSocket upgrade is attempted) and falls through to inner from then on --
// used to simulate an HTTP 401 that only fails the first dial attempt.
type flakyPreUpgrade struct {
	n        atomic.Int32
	statuses []int
	inner    http.Handler
}

func (f *flakyPreUpgrade) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	i := int(f.n.Add(1)) - 1
	if i < len(f.statuses) {
		http.Error(w, "flaky", f.statuses[i])
		return
	}
	f.inner.ServeHTTP(w, r)
}

func newFlakyServer(t *testing.T, statuses []int, ln testAcceptHandler) (string, *flakyPreUpgrade) {
	t.Helper()
	if ln.Logger == nil {
		ln.Logger = discardLogger()
	}
	inner := newTestListener(ln)
	flaky := &flakyPreUpgrade{statuses: statuses, inner: inner}
	srv := httptest.NewServer(flaky)
	t.Cleanup(srv.Close)
	return "ws" + srv.URL[len("http"):] + "/tunnel", flaky
}

func TestClient401RefreshRetrySuccess(t *testing.T) {
	url, _ := newFlakyServer(t, []int{401}, testAcceptHandler{})
	var calls atomic.Int32
	token := TokenProvider(func(context.Context) (string, error) {
		calls.Add(1)
		return "tok", nil
	})
	rec := newDisconnectRecorder()
	fa := &fakeAfter{}
	cl := NewClient(url, token, ClientConfig{
		Reconnect:    testReconnectOptions(fa, 0),
		OnDisconnect: rec.onDisconnect,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := cl.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer cl.Close(context.Background())
	if got := calls.Load(); got != 2 {
		t.Errorf("token provider called %d times, want 2 (initial + one refresh retry)", got)
	}
}

func TestClientSecond401Fatal(t *testing.T) {
	url, _ := newFlakyServer(t, []int{401, 401}, testAcceptHandler{})
	rec := newDisconnectRecorder()
	fa := &fakeAfter{}
	// A real TokenProvider, not StaticToken: nit 2 gates the one-time
	// immediate refresh-retry on isRealProvider, since retrying a static
	// token can never turn a 401 into anything else. This test exercises
	// the retry-then-fatal path, so it needs a provider the retry could
	// plausibly help.
	token := TokenProvider(func(context.Context) (string, error) { return "tok", nil })
	cl := NewClient(url, token, ClientConfig{
		Reconnect:    testReconnectOptions(fa, 0),
		OnDisconnect: rec.onDisconnect,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := cl.Connect(ctx)
	if err == nil {
		t.Fatal("Connect should have failed after a second 401")
	}
	reason := rec.waitNext(t)
	if !reason.Fatal {
		t.Errorf("reason.Fatal = false, want true")
	}
	if reason.Phase != PhaseDial {
		t.Errorf("reason.Phase = %v, want dial", reason.Phase)
	}
	if reason.HTTPStatus != 401 {
		t.Errorf("reason.HTTPStatus = %d, want 401", reason.HTTPStatus)
	}
	if cl.State() != "closed" {
		t.Errorf("State() = %s, want closed", cl.State())
	}
}

// TestClientMissingSubprotocolEchoReportsUnsupported: a server that upgrades
// but doesn't echo the ws-mixer subprotocol (websocket.Accept called with no
// Subprotocols of its own) makes Dial (client.go) fail before clientHandshake
// is ever reached -- a *DialError{Mismatch: true}, never a close frame or an
// HTTP status. This is CLIENT-SDK.md's third errorCode source: a ws-mixer
// error the SDK raises locally. classifyFailureErr's *DialError branch now
// reports it as UnsupportedCode/UNSUPPORTED even though nothing was ever on
// the wire (D-2026-09-20-09's amendment).
func TestClientMissingSubprotocolEchoReportsUnsupported(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// No Subprotocols: the upgrade succeeds, but the client's requested
		// ws-mixer subprotocol is never echoed back.
		ws, err := websocket.Accept(w, r, &websocket.AcceptOptions{CompressionMode: websocket.CompressionDisabled})
		if err != nil {
			return
		}
		defer ws.CloseNow()
		<-r.Context().Done()
	}))
	t.Cleanup(srv.Close)
	url := "ws" + srv.URL[len("http"):] + "/tunnel"

	rec := newDisconnectRecorder()
	fa := &fakeAfter{}
	rc := testReconnectOptions(fa, 0)
	cl := NewClient(url, StaticToken("tok"), ClientConfig{
		Reconnect:    rc,
		OnDisconnect: rec.onDisconnect,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := cl.Connect(ctx); err == nil {
		t.Fatal("Connect should have failed fatally on a missing subprotocol echo")
	}

	r := rec.waitNext(t)
	if r.Phase != PhaseDial {
		t.Errorf("Phase = %v, want dial", r.Phase)
	}
	if !r.Fatal {
		t.Error("Fatal = false, want true")
	}
	if !r.HasErrorCode || r.ErrorCode != UnsupportedCode || r.ErrorName != "UNSUPPORTED" {
		t.Errorf("HasErrorCode/ErrorCode/ErrorName = %v/%v/%q, want true/UnsupportedCode/UNSUPPORTED", r.HasErrorCode, r.ErrorCode, r.ErrorName)
	}
	if r.WSCode != 0 {
		t.Errorf("WSCode = %d, want 0 (no close frame was involved)", r.WSCode)
	}
	if r.HTTPStatus != 0 {
		t.Errorf("HTTPStatus = %d, want 0 (the upgrade itself succeeded)", r.HTTPStatus)
	}
	if cl.State() != "closed" {
		t.Errorf("State() = %s, want closed", cl.State())
	}
}

// TestClientHandshakePhaseBareClose4010Fatal: a peer that closes 4010
// (UNSUPPORTED) before ever sending welcome, with no preceding ws-mixer
// error{} frame -- an SDK bug on the peer's part, but WIRE.md section 2.9's
// fatal set is keyed on the close code, not on whether error{} happened to
// precede it. No retry mechanism applies to 4010 (unlike 4011's one-time
// refresh-retry below): the very first attempt is fatal.
func TestClientHandshakePhaseBareClose4010Fatal(t *testing.T) {
	_, url := startTestServer(t, testAcceptHandler{OnRawConn: func(ws *websocket.Conn) {
		_ = ws.Close(4010, "unsupported protocol version")
	}})

	rec := newDisconnectRecorder()
	fa := &fakeAfter{}
	cl := NewClient(url, StaticToken("tok"), ClientConfig{
		Reconnect:    testReconnectOptions(fa, 0),
		OnDisconnect: rec.onDisconnect,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := cl.Connect(ctx); err == nil {
		t.Fatal("Connect should have failed on a fatal 4010 close")
	}

	r := rec.waitNext(t)
	if r.Phase != PhaseHandshake {
		t.Errorf("Phase = %v, want handshake", r.Phase)
	}
	if r.WSCode != 4010 {
		t.Errorf("WSCode = %d, want 4010", r.WSCode)
	}
	if !r.HasErrorCode || r.ErrorCode != UnsupportedCode || r.ErrorName != "UNSUPPORTED" {
		t.Errorf("HasErrorCode/ErrorCode/ErrorName = %v/%v/%q, want true/UnsupportedCode/UNSUPPORTED (derived from the bare 4010 close, D-2026-09-20-09)", r.HasErrorCode, r.ErrorCode, r.ErrorName)
	}
	if !r.Fatal {
		t.Error("Fatal = false, want true")
	}
	if cl.State() != "closed" {
		t.Errorf("State() = %s, want closed", cl.State())
	}
}

// TestClientHandshakePhaseBareClose4011OneRetryThenFatal: a peer that closes
// 4011 (UNAUTHORIZED) before ever sending welcome joins the same one-time
// immediate refresh-retry as an HTTP 401 or a *ConnError{UnauthorizedCode}
// (isUnauthorized) -- the first failure is silent (no report, no backoff,
// straight to a second attempt with a freshly re-fetched token), and only a
// second 4011 is fatal. Mirrors TestClientSecond401Fatal exactly, one layer
// lower on the wire.
func TestClientHandshakePhaseBareClose4011OneRetryThenFatal(t *testing.T) {
	var accepts atomic.Int32
	_, url := startTestServer(t, testAcceptHandler{OnRawConn: func(ws *websocket.Conn) {
		accepts.Add(1)
		_ = ws.Close(4011, "reauth required")
	}})

	rec := newDisconnectRecorder()
	fa := &fakeAfter{}
	var tokenCalls atomic.Int32
	// A real TokenProvider, not StaticToken: nit 2 gates the one-time
	// immediate refresh-retry on isRealProvider, exactly like
	// TestClientSecond401Fatal.
	token := TokenProvider(func(context.Context) (string, error) {
		tokenCalls.Add(1)
		return "tok", nil
	})
	cl := NewClient(url, token, ClientConfig{
		Reconnect:    testReconnectOptions(fa, 0),
		OnDisconnect: rec.onDisconnect,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := cl.Connect(ctx); err == nil {
		t.Fatal("Connect should have failed after the one-time 4011 refresh-retry also failed")
	}

	if got := accepts.Load(); got != 2 {
		t.Errorf("server accepted %d connections, want 2 (initial + one refresh retry)", got)
	}
	if got := tokenCalls.Load(); got != 2 {
		t.Errorf("token provider called %d times, want 2", got)
	}

	r := rec.waitNext(t)
	if r.Phase != PhaseHandshake {
		t.Errorf("Phase = %v, want handshake", r.Phase)
	}
	if r.WSCode != 4011 {
		t.Errorf("WSCode = %d, want 4011", r.WSCode)
	}
	if !r.HasErrorCode || r.ErrorCode != UnauthorizedCode || r.ErrorName != "UNAUTHORIZED" {
		t.Errorf("HasErrorCode/ErrorCode/ErrorName = %v/%v/%q, want true/UnauthorizedCode/UNAUTHORIZED (derived from the bare 4011 close, D-2026-09-20-09)", r.HasErrorCode, r.ErrorCode, r.ErrorName)
	}
	if !r.Fatal {
		t.Error("Fatal = false, want true")
	}
}

func TestClientProviderErrorFatal(t *testing.T) {
	url, _ := newFlakyServer(t, nil, testAcceptHandler{})
	boom := errors.New("provider exploded")
	token := TokenProvider(func(context.Context) (string, error) { return "", boom })
	rec := newDisconnectRecorder()
	fa := &fakeAfter{}
	cl := NewClient(url, token, ClientConfig{
		Reconnect:    testReconnectOptions(fa, 0),
		OnDisconnect: rec.onDisconnect,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := cl.Connect(ctx)
	if err == nil {
		t.Fatal("Connect should fail when the token provider errors")
	}
	reason := rec.waitNext(t)
	if !reason.Fatal {
		t.Error("reason.Fatal = false, want true")
	}
	if !errors.Is(reason.Cause, boom) {
		t.Errorf("reason.Cause = %v, want %v (verbatim)", reason.Cause, boom)
	}
	if reason.Message != boom.Error() {
		t.Errorf("reason.Message = %q, want %q (verbatim)", reason.Message, boom.Error())
	}
}

func TestClientDrainTriggersParallelReconnect(t *testing.T) {
	connCh := make(chan *Conn, 4)
	_, url := startTestServer(t, testAcceptHandler{OnConn: func(c *Conn) { connCh <- c }})

	rec := newDisconnectRecorder()
	fa := &fakeAfter{}
	rc := testReconnectOptions(fa, 0.5)
	var onConnectCount atomic.Int32
	cl := NewClient(url, StaticToken("tok"), ClientConfig{
		Reconnect:    rc,
		OnDisconnect: rec.onDisconnect,
		OnConnect:    func(*Conn, *WelcomeMsg) { onConnectCount.Add(1) },
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := cl.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer cl.Close(context.Background())

	var serverConn *Conn
	select {
	case serverConn = <-connCh:
	case <-time.After(2 * time.Second):
		t.Fatal("server never observed the first connection")
	}

	go func() {
		_ = serverConn.Drain(context.Background(), "rollout", DrainOptions{Deadline: 200 * time.Millisecond})
	}()

	// The parallel reconnect lands as a second server-side connection and a
	// second OnConnect callback.
	select {
	case <-connCh:
	case <-time.After(3 * time.Second):
		t.Fatal("server never observed the drain-triggered reconnect")
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if onConnectCount.Load() >= 2 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := onConnectCount.Load(); got < 2 {
		t.Fatalf("OnConnect fired %d times, want >= 2 (initial + reconnect)", got)
	}

	// The initial connect's own welcome also arms a stability timer
	// (armStability, sharing this same rc.after seam) -- waitForBackoffCalls
	// filters it out rather than assuming index 0 is the drain-triggered
	// delay.
	calls := waitForBackoffCalls(t, fa, rc, 1)
	if d := calls[0]; d < 0 || d > 2*time.Second {
		t.Errorf("drain reconnect delay = %v, want in [0,2s]", d)
	}
}

// TestClientKeepaliveTimeoutImmediateRetry covers WIRE.md section 2.9's
// close-4013 row: "one immediate attempt, then normal backoff". The
// "immediate attempt" half is directly testable end to end: a keepalive
// timeout's very next reconnect attempt costs no backoff delay at all. The
// "then normal backoff" half only applies to a keepalive timeout that
// recurs without an intervening STABLE connection (Client's
// keepaliveRetryUsed flag -- like the JS reference's
// keepaliveImmediateRetryUsed -- is cleared once a connection has stayed up
// ReconnectOptions.StableAfter past its own welcome, not at welcome itself
// -- D-2026-09-20, armStability -- for this flag too); that narrower case
// is exercised at the unit level in TestClassifyFailureErr's siblings
// rather than choreographed over real network here. TestClientKeepalive4013NotReArmedByWelcomeThenClose
// pins the StableAfter-specific half of this directly.
func TestClientKeepaliveTimeoutImmediateRetry(t *testing.T) {
	var closed atomic.Bool
	var onConnectCount atomic.Int32
	_, url := startTestServer(t, testAcceptHandler{OnConn: func(c *Conn) {
		if closed.CompareAndSwap(false, true) {
			go func() { _ = c.Close(uint32(KeepaliveTimeout), "test keepalive") }()
		}
	}})

	rec := newDisconnectRecorder()
	fa := &fakeAfter{}
	rc := testReconnectOptions(fa, 1)
	cl := NewClient(url, StaticToken("tok"), ClientConfig{
		Reconnect:    rc,
		OnDisconnect: rec.onDisconnect,
		OnConnect:    func(*Conn, *WelcomeMsg) { onConnectCount.Add(1) },
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := cl.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer cl.Close(context.Background())

	r := rec.waitNext(t)
	if r.WSCode != 4013 || r.Fatal {
		t.Fatalf("reason = %+v, want WSCode=4013 fatal=false", r)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && onConnectCount.Load() < 2 {
		time.Sleep(10 * time.Millisecond)
	}
	if got := onConnectCount.Load(); got < 2 {
		t.Fatalf("OnConnect fired %d times, want 2 (initial + immediate retry)", got)
	}
	if calls := nonStabilityCalls(fa.snapshot(), stableAfterOf(rc)); len(calls) != 0 {
		t.Errorf("backoff delays recorded = %v, want none (the retry was immediate)", calls)
	}
}

func TestClientEnhanceYourCalmStartsAtCap(t *testing.T) {
	_, url := startTestServer(t, testAcceptHandler{OnConn: func(c *Conn) {
		go func() { _ = c.Close(uint32(EnhanceYourCalm), "test enhance your calm") }()
	}})
	rec := newDisconnectRecorder()
	fa := &fakeAfter{}
	rc := testReconnectOptions(fa, 0.5)
	cl := NewClient(url, StaticToken("tok"), ClientConfig{
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
	if r.WSCode != 4009 {
		t.Fatalf("reason.WSCode = %d, want 4009", r.WSCode)
	}
	calls := waitForBackoffCalls(t, fa, rc, 1)
	want := time.Duration(0.5 * float64(time.Second)) // rand=0.5, cap=1s
	if calls[0] != want {
		t.Errorf("4009 delay = %v, want exactly %v (jitter(cap), no retry_after_ms present)", calls[0], want)
	}
}

// TestClientApplicationCloseStartsAtCap: WIRE.md section 2.9 gives 4014
// (APPLICATION_CLOSE) the same "start at cap" treatment as 4009 -- both are
// deliberate post-welcome refusals (the app accepted, then refused, e.g. a
// per-account connection cap) made right after onAttemptSucceeded reset
// attempt to 0, so ordinary fullJitter(attempt) would never climb past its
// lowest rung and a refused client would redial about once a second
// forever. This variant is the error{14}+close shape (Conn.Close).
func TestClientApplicationCloseStartsAtCap(t *testing.T) {
	_, url := startTestServer(t, testAcceptHandler{OnConn: func(c *Conn) {
		go func() { _ = c.Close(uint32(ApplicationCloseCode), "CONNECTION_LIMIT: test") }()
	}})
	rec := newDisconnectRecorder()
	fa := &fakeAfter{}
	rc := testReconnectOptions(fa, 0.5)
	rc.MaxAttempts = 1 // one failure is all this test needs; non-fatal codes would otherwise reconnect forever
	cl := NewClient(url, StaticToken("tok"), ClientConfig{
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
	if r.WSCode != 4014 {
		t.Fatalf("reason.WSCode = %d, want 4014", r.WSCode)
	}
	if r.Fatal {
		t.Errorf("reason.Fatal = true, want false")
	}
	calls := waitForBackoffCalls(t, fa, rc, 1)
	want := time.Duration(0.5 * float64(time.Second)) // rand=0.5, cap=1s
	if calls[0] != want {
		t.Errorf("4014 delay = %v, want exactly %v (jitter(cap), not fullJitter)", calls[0], want)
	}
}

// TestClientApplicationCloseBareStartsAtCap: the bare-close variant of the
// same case -- a peer that sends a raw WS close 4014 with no preceding
// error{} (effectiveWSCode falls back to conn.PeerCloseCode() and reaches
// the same switch case) must schedule the same jitter(cap) delay, not
// fullJitter. ErrorCode/ErrorName are still derived from the bare 4014 close
// code itself (deriveBareCloseErrorCode, D-2026-09-20-09), even though no
// error{} message was ever seen.
func TestClientApplicationCloseBareStartsAtCap(t *testing.T) {
	var raw atomic.Pointer[websocket.Conn]
	_, url := startTestServer(t, testAcceptHandler{OnRawConn: func(ws *websocket.Conn) { raw.Store(ws) }})

	rec := newDisconnectRecorder()
	fa := &fakeAfter{}
	rc := testReconnectOptions(fa, 0.5)
	rc.MaxAttempts = 1
	cl := NewClient(url, StaticToken("tok"), ClientConfig{
		Reconnect:    rc,
		OnDisconnect: rec.onDisconnect,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := cl.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer cl.Close(context.Background())

	ws := raw.Load()
	if ws == nil {
		t.Fatal("server never captured the raw *websocket.Conn")
	}
	go func() { _ = ws.Close(4014, "CONNECTION_LIMIT: test") }()

	r := rec.waitNext(t)
	if r.WSCode != 4014 {
		t.Fatalf("reason.WSCode = %d, want 4014", r.WSCode)
	}
	if !r.HasErrorCode || r.ErrorCode != ApplicationCloseCode || r.ErrorName != "APPLICATION_CLOSE" {
		t.Errorf("reason.HasErrorCode/ErrorCode/ErrorName = %v/%v/%q, want true/ApplicationCloseCode/APPLICATION_CLOSE (derived from the bare 4014 close)", r.HasErrorCode, r.ErrorCode, r.ErrorName)
	}
	if r.Fatal {
		t.Errorf("reason.Fatal = true, want false")
	}
	calls := waitForBackoffCalls(t, fa, rc, 1)
	want := time.Duration(0.5 * float64(time.Second)) // rand=0.5, cap=1s
	if calls[0] != want {
		t.Errorf("bare 4014 delay = %v, want exactly %v (jitter(cap), not fullJitter)", calls[0], want)
	}
}

func TestClientFatalCloseCodesPostConnect(t *testing.T) {
	for _, code := range []ErrorCode{UnsupportedCode, UnauthorizedCode} {
		t.Run(code.String(), func(t *testing.T) {
			_, url := startTestServer(t, testAcceptHandler{OnConn: func(c *Conn) {
				go func() { _ = c.Close(uint32(code), "test fatal post-connect") }()
			}})
			rec := newDisconnectRecorder()
			fa := &fakeAfter{}
			cl := NewClient(url, StaticToken("tok"), ClientConfig{
				Reconnect:    testReconnectOptions(fa, 0),
				OnDisconnect: rec.onDisconnect,
			})
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := cl.Connect(ctx); err != nil {
				t.Fatalf("Connect: %v", err)
			}
			r := rec.waitNext(t)
			if !r.Fatal {
				t.Errorf("reason.Fatal = false, want true for %s", code)
			}
			if r.Phase != PhaseConnected {
				t.Errorf("reason.Phase = %v, want connected", r.Phase)
			}
			deadline := time.Now().Add(2 * time.Second)
			for time.Now().Before(deadline) && cl.State() != "closed" {
				time.Sleep(10 * time.Millisecond)
			}
			if cl.State() != "closed" {
				t.Errorf("State() = %s, want closed (no further reconnect attempts)", cl.State())
			}
		})
	}
}

// TestClientAttemptResetOnWelcome is superseded by
// client_reconnect_stability_test.go's TestClientAttemptResetOnlyAfterStability
// and TestClientAttemptResetAfterStabilityElapses (D-2026-09-20,
// StableAfter): the attempt counter no longer resets at welcome alone, so
// this test's premise no longer holds. Kept as a named pointer rather than
// silently deleted, since it is exactly the test D-2026-09-20's
// revert-proof exercises (see TestClientAttemptResetProofRevertsWithoutStabilityGate's
// doc comment).

func TestClientCloseSendsClientRequestedDrainAndWaits(t *testing.T) {
	var serverDrain atomic.Pointer[DrainMsg]
	connCh := make(chan *Conn, 1)
	_, url := startTestServer(t, testAcceptHandler{OnConn: func(c *Conn) {
		c.OnDrain(func(d *DrainMsg) { serverDrain.Store(d) })
		connCh <- c
	}})

	cl := NewClient(url, StaticToken("tok"), ClientConfig{})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := cl.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	var serverConn *Conn
	select {
	case serverConn = <-connCh:
	case <-time.After(2 * time.Second):
		t.Fatal("server never observed the connection")
	}

	closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer closeCancel()
	if err := cl.Close(closeCtx); err != nil {
		t.Fatalf("Close: %v", err)
	}

	select {
	case <-serverConn.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("server-side connection never closed")
	}

	d := serverDrain.Load()
	if d == nil {
		t.Fatal("server never received the client's drain message")
	}
	if d.Reason != "client_requested" {
		t.Errorf("drain.Reason = %q, want client_requested", d.Reason)
	}
	if serverConn.CloseCode() != 1000 {
		t.Errorf("server-observed close code = %d, want 1000 (NO_ERROR, per WIRE.md section 2.10 rule 14)", serverConn.CloseCode())
	}
}

// TestClientCloseTreatsAbnormalDrainEndAsSuccess: the peer drops the raw
// transport (no WS close frame at all) while this side is waiting out its
// own graceful drain{client_requested} -- Conn.Drain's `case <-c.closed:`
// branch now returns connClosedErr's io.ErrUnexpectedEOF (conn.go) for
// exactly this abnormal-closure case, but that must not surface as an error
// from Client.Close: a peer that simply drops the connection mid-drain is
// still an ordinary, successful shutdown outcome from the client's own
// point of view (Close's contract is "the client ends up closed", not "the
// peer completed the handshake").
func TestClientCloseTreatsAbnormalDrainEndAsSuccess(t *testing.T) {
	var raw atomic.Pointer[websocket.Conn]
	_, url := startTestServer(t, testAcceptHandler{
		OnRawConn: func(ws *websocket.Conn) { raw.Store(ws) },
		OnConn: func(c *Conn) {
			// Drain's initial select (drain.go) resolves immediately via
			// waitForDrainedOrEmpty() when the stream table is already
			// empty -- this test needs the <-c.closed branch instead, so it
			// keeps one stream open (never closed) to make Drain actually
			// wait rather than complete in the same instant it starts.
			go func() { _, _ = c.OpenStream(context.Background()) }()
			c.OnDrain(func(*DrainMsg) {
				// Drop the transport outright instead of letting the normal
				// server-side drain sequence run: no WS close frame at all,
				// exactly a TCP reset/half-open death mid-drain.
				go func() {
					if ws := raw.Load(); ws != nil {
						_ = ws.CloseNow()
					}
				}()
			})
		},
	})

	rec := newDisconnectRecorder()
	cl := NewClient(url, StaticToken("tok"), ClientConfig{OnDisconnect: rec.onDisconnect})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := cl.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer closeCancel()
	if err := cl.Close(closeCtx); err != nil {
		t.Errorf("Close() = %v, want nil (an abnormal transport drop mid-drain is still a successful shutdown)", err)
	}
	if got := cl.State(); got != "closed" {
		t.Errorf("State() = %s, want closed", got)
	}

	r := rec.waitNext(t)
	if r.Fatal {
		t.Errorf("reason.Fatal = true, want false")
	}
}
