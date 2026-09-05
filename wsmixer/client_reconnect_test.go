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
			wantPhase: PhaseDial, wantFatal: true,
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
			if tc.hasCode && (!fi.hasErrorCode || fi.errCode != tc.wantCode) {
				t.Errorf("errCode = %v (has=%v), want %v", fi.errCode, fi.hasErrorCode, tc.wantCode)
			}
		})
	}
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
	var onConnectCount atomic.Int32
	cl := NewClient(url, StaticToken("tok"), ClientConfig{
		Reconnect:    testReconnectOptions(fa, 0.5),
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

	calls := fa.snapshot()
	if len(calls) == 0 {
		t.Fatal("no backoff delay recorded for the drain-triggered reconnect")
	}
	if d := calls[0]; d < 0 || d > 2*time.Second {
		t.Errorf("drain reconnect delay = %v, want in [0,2s]", d)
	}
}

// TestClientKeepaliveTimeoutImmediateRetry covers WIRE.md section 2.9's
// close-4013 row: "one immediate attempt, then normal backoff". The
// "immediate attempt" half is directly testable end to end: a keepalive
// timeout's very next reconnect attempt costs no backoff delay at all. The
// "then normal backoff" half only applies to a keepalive timeout that
// recurs without an intervening successful welcome (Client's
// keepaliveRetryUsed flag -- like the JS reference's
// keepaliveImmediateRetryUsed -- is cleared on every welcome, matching
// WIRE.md's "attempt resets ONLY on welcome" for this flag too); that
// narrower case is exercised at the unit level in TestClassifyFailureErr's
// siblings rather than choreographed over real network here.
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
	cl := NewClient(url, StaticToken("tok"), ClientConfig{
		Reconnect:    testReconnectOptions(fa, 1),
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
	if calls := fa.snapshot(); len(calls) != 0 {
		t.Errorf("backoff delays recorded = %v, want none (the retry was immediate)", calls)
	}
}

func TestClientEnhanceYourCalmStartsAtCap(t *testing.T) {
	_, url := startTestServer(t, testAcceptHandler{OnConn: func(c *Conn) {
		go func() { _ = c.Close(uint32(EnhanceYourCalm), "test enhance your calm") }()
	}})
	rec := newDisconnectRecorder()
	fa := &fakeAfter{}
	cl := NewClient(url, StaticToken("tok"), ClientConfig{
		Reconnect:    testReconnectOptions(fa, 0.5),
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
	calls := waitForCalls(t, fa, 1)
	want := time.Duration(0.5 * float64(time.Second)) // rand=0.5, cap=1s
	if calls[0] != want {
		t.Errorf("4009 delay = %v, want exactly %v (jitter(cap), no retry_after_ms present)", calls[0], want)
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

func TestClientAttemptResetOnWelcome(t *testing.T) {
	var attempt atomic.Int32
	_, url := startTestServer(t, testAcceptHandler{OnConn: func(c *Conn) {
		n := attempt.Add(1)
		if n == 2 {
			// The second connection (the retry after the first plain
			// connected-phase failure below) succeeds and is left alone.
			return
		}
		go func() { _ = c.Close(uint32(ProtocolErrorCode), "test forcing a plain backoff") }()
	}})

	rec := newDisconnectRecorder()
	fa := &fakeAfter{}
	cl := NewClient(url, StaticToken("tok"), ClientConfig{
		Reconnect:    testReconnectOptions(fa, 1), // rand=1: delay lands exactly on fullJitter's upper bound
		OnDisconnect: rec.onDisconnect,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := cl.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	// First failure: attempt goes 0->1, delay = fullJitter(1).
	rec.waitNext(t)
	// Wait for the reconnect to actually land (welcome resets attempt to 0).
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && cl.Conn() == nil {
		time.Sleep(10 * time.Millisecond)
	}
	if cl.Conn() == nil {
		t.Fatal("client never reconnected")
	}

	// Force one more plain failure by closing the now-active connection
	// directly, and check the resulting delay used attempt=1 again (i.e. was
	// reset by the intervening welcome), not attempt=2.
	_ = cl.Conn().Close(uint32(ProtocolErrorCode), "test forcing a second plain backoff")
	rec.waitNext(t)
	defer cl.Close(context.Background())

	rc := testReconnectOptions(fa, 1)
	want := rc.fullJitter(1)
	calls := waitForCalls(t, fa, 2)
	if calls[0] != want || calls[1] != want {
		t.Errorf("delays = %v, want both == %v (attempt counter reset on welcome, not accumulating)", calls, want)
	}
}

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
