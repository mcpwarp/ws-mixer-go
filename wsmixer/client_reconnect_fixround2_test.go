package wsmixer

// Regression tests for the v0.4.0 client review's second fix round: F1 (a
// permanent callbackLoop leak after any fatal termination that never goes
// through Close()), F2 (a duplicate server drain frame orphaning a live
// conn and wedging Close()), nit 3 (a drain race's winner never reporting
// the superseded conn's disconnect), and nit 6's two test gaps (StaticToken
// vs. a real provider on 401, and a raw WS 1001 close's classification).

import (
	"context"
	"encoding/json"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// settledGoroutines polls runtime.NumGoroutine and returns the lowest count
// observed over the window, giving already-finished goroutines (test
// harness cleanup, GC workers, etc.) time to actually exit before the count
// is read -- a stray goroutine leak from the code under test shows up as a
// floor that never drops, not a momentary spike that would.
func settledGoroutines(t *testing.T) int {
	t.Helper()
	min := runtime.NumGoroutine()
	for i := 0; i < 100; i++ {
		time.Sleep(5 * time.Millisecond)
		runtime.GC()
		if n := runtime.NumGoroutine(); n < min {
			min = n
		}
	}
	return min
}

// TestClientFatalTerminationDoesNotLeakCallbackLoop pins F1: a fatal
// termination that the caller never explicitly Close()s until afterward --
// goFatal (a fatal post-connect close code, exercised here) is one of three
// paths that used to set cl.closing directly, which made a later Close()
// take its early-return branch without ever reaching wg.Wait()/closing the
// callback queue, leaking callbackLoop forever. Repeated across N clients,
// pre-fix this leaks N goroutines that never go away; post-fix, the
// goroutine count returns to its baseline.
func TestClientFatalTerminationDoesNotLeakCallbackLoop(t *testing.T) {
	const n = 20
	_, url := startTestServer(t, testAcceptHandler{OnConn: func(c *Conn) {
		go func() { _ = c.Close(uint32(UnauthorizedCode), "fatal test") }()
	}})

	before := settledGoroutines(t)

	for i := 0; i < n; i++ {
		fa := &fakeAfter{}
		cl := NewClient(url, StaticToken("tok"), ClientConfig{Reconnect: testReconnectOptions(fa, 0)})
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		// The server accepts the first connection normally (so Connect
		// itself succeeds) and only then closes it with a post-connect
		// fatal code -- goFatal runs from watchConn, asynchronously, so
		// wait for State() to reach "closed" (goFatal already ran and set
		// cl.closing directly, without a Close() call) before calling
		// Close(), the exact precondition F1 pins.
		if err := cl.Connect(ctx); err != nil {
			cancel()
			t.Fatalf("client #%d: Connect: %v", i, err)
		}
		cancel()
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) && cl.State() != "closed" {
			time.Sleep(2 * time.Millisecond)
		}
		if got := cl.State(); got != "closed" {
			t.Fatalf("client #%d: State() = %s, want closed before Close()", i, got)
		}
		if err := cl.Close(context.Background()); err != nil {
			t.Fatalf("client #%d: Close: %v", i, err)
		}
	}

	after := settledGoroutines(t)
	// n callbackLoop goroutines is the exact leak this pins; a handful of
	// slack absorbs anything unrelated to this test (finalizers, http
	// keep-alive teardown, ...) without hiding a real regression.
	if after > before+5 {
		t.Errorf("goroutine count %d -> %d after %d fatal clients + Close (callbackLoop leak?)", before, after, n)
	}
}

// TestClientDuplicateDrainFramesOneReplacement pins F2: a second drain frame
// for a conn already being replaced must not start a second parallel
// reconnect -- doing so orphans whichever replacement loses that race (its
// onAttemptSucceeded finds cl.retiringConn already cleared by the winner,
// so it never closes what it just dialed), and that orphan's watchConn used
// to have no way to ever end, wedging Close() until a real peer's keepalive
// timeout. A real server never sends drain twice, but the client must not
// assume it can't happen.
func TestClientDuplicateDrainFramesOneReplacement(t *testing.T) {
	connCh := make(chan *Conn, 8)
	_, url := startTestServer(t, testAcceptHandler{OnConn: func(c *Conn) { connCh <- c }})

	fa := &fakeAfter{}
	var onConnectCount atomic.Int32
	cl := NewClient(url, StaticToken("tok"), ClientConfig{
		Reconnect: testReconnectOptions(fa, 0),
		OnConnect: func(*Conn, *WelcomeMsg) { onConnectCount.Add(1) },
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := cl.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	c1 := <-connCh

	// Two raw drain frames back to back, bypassing Conn.Drain's own
	// already-draining no-op guard (drain.go: "if c.draining { return nil
	// }") -- sendControl has no such guard, so this is the one way to make
	// the client actually observe two drain frames on the same conn.
	deadlineMS := int64(10_000)
	drainMsg := &DrainMsg{T: "drain", Reason: "rollout", DeadlineMS: &deadlineMS}
	if err := c1.sendControl(drainMsg); err != nil {
		t.Fatalf("sendControl (1st drain): %v", err)
	}
	if err := c1.sendControl(drainMsg); err != nil {
		t.Fatalf("sendControl (2nd drain): %v", err)
	}

	select {
	case <-connCh:
		// the one replacement
	case <-time.After(3 * time.Second):
		t.Fatal("no reconnect after the duplicate drain frames")
	}
	select {
	case extra := <-connCh:
		t.Fatalf("a second drain frame triggered a second replacement: %v", extra)
	case <-time.After(500 * time.Millisecond):
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && onConnectCount.Load() < 2 {
		time.Sleep(10 * time.Millisecond)
	}
	if got := onConnectCount.Load(); got != 2 {
		t.Fatalf("OnConnect fired %d times, want exactly 2 (initial + the one replacement)", got)
	}

	start := time.Now()
	if err := cl.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("Close() took %v, want <2s (an orphaned conn must not wedge it)", d)
	}
}

// TestClientDrainRaceWinnerReportsSupersededDisconnect pins nit 3:
// CLIENT-SDK.md's disconnect reason shape is unconditional -- every
// disconnect gets exactly one report -- but a drain race's winning
// replacement (landing before the old conn actually closes) used to leave
// the superseded old conn silently unreported, unlike the losing case
// (watchConn's drainSched branch, already covered by
// TestClientDrainFlagStuck) which always got one.
func TestClientDrainRaceWinnerReportsSupersededDisconnect(t *testing.T) {
	connCh := make(chan *Conn, 8)
	_, url := startTestServer(t, testAcceptHandler{OnConn: func(c *Conn) { connCh <- c }})

	fa := &fakeAfter{}
	rec := newDisconnectRecorder()
	var onConnectCount atomic.Int32
	cl := NewClient(url, StaticToken("tok"), ClientConfig{
		Reconnect:    testReconnectOptions(fa, 0),
		OnDisconnect: rec.onDisconnect,
		OnConnect:    func(*Conn, *WelcomeMsg) { onConnectCount.Add(1) },
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := cl.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer cl.Close(context.Background())

	c1 := <-connCh
	// Keep one stream live so c1 does not close the instant it drains --
	// otherwise it might lose the race instead of winning it, which is a
	// different (already-covered) code path.
	if _, err := c1.OpenStream(context.Background()); err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	go func() { _ = c1.Drain(context.Background(), "rollout", DrainOptions{Deadline: 10 * time.Second}) }()

	<-connCh // the parallel reconnect (the replacement)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && onConnectCount.Load() < 2 {
		time.Sleep(10 * time.Millisecond)
	}
	if onConnectCount.Load() < 2 {
		t.Fatal("reconnect never landed")
	}

	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		found := false
		for _, r := range rec.snapshotAll() {
			if r.Message == supersededMessage {
				found = true
				if r.Fatal {
					t.Errorf("superseded-conn reason.Fatal = true, want false: %+v", r)
				}
				break
			}
		}
		if found {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("no OnDisconnect reported for the superseded conn (reasons so far: %+v)", rec.snapshotAll())
}

// TestClientStaticToken401Fatal pins the reversed nit-2 decision
// (docs/DECISIONS.md 2026-09-20): a StaticToken client has no way for a
// redial to fix an HTTP 401 (it would hand back the exact same,
// already-rejected token), so a rejected static token is now fatal at once
// -- retrying it forever with backoff can never turn into success, only
// hammer the auth service and hide the problem. Exactly one dial happens;
// no refresh-retry (that needs a real TokenProvider, see
// TestClientSecond401Fatal) and no backoff (fatal skips reportAndSchedule's
// scheduling entirely).
func TestClientStaticToken401Fatal(t *testing.T) {
	// newFlakyServer with statuses covering every attempt (more than the one
	// dial this test expects) is what proves no redial ever happens: a
	// second dial would fall through to the inner listener instead.
	url, flaky := newFlakyServer(t, []int{401, 401, 401}, testAcceptHandler{})

	rec := newDisconnectRecorder()
	fa := &fakeAfter{}
	cl := NewClient(url, StaticToken("tok"), ClientConfig{
		Reconnect:    testReconnectOptions(fa, 1),
		OnDisconnect: rec.onDisconnect,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := cl.Connect(ctx); err == nil {
		t.Fatal("Connect should have failed fatally on a rejected static token")
	}
	defer cl.Close(context.Background())

	r := rec.waitNext(t)
	if !r.Fatal {
		t.Errorf("reason.Fatal = false, want true (a rejected static token is fatal at once)")
	}
	if r.HTTPStatus != 401 {
		t.Errorf("reason.HTTPStatus = %d, want 401", r.HTTPStatus)
	}
	if r.Phase != PhaseDial {
		t.Errorf("reason.Phase = %v, want dial", r.Phase)
	}
	if got := flaky.n.Load(); got != 1 {
		t.Errorf("dial attempts = %d, want exactly 1 (fatal at once, no refresh-retry, no backoff redial)", got)
	}
	if got := cl.State(); got != "closed" {
		t.Errorf("State() = %s, want closed", got)
	}
}

// TestClientStaticTokenBareClose4011HandshakeFatal pins the same
// reversed nit-2 decision one layer lower on the wire: a bare pre-welcome WS
// close 4011 (no ws-mixer error{} frame), with a StaticToken client, is
// fatal at once -- exactly one dial, no refresh-retry (isRealProvider gates
// that off for a static token, same as ever), no backoff redial.
func TestClientStaticTokenBareClose4011HandshakeFatal(t *testing.T) {
	var accepts atomic.Int32
	_, url := startTestServer(t, testAcceptHandler{OnRawConn: func(ws *websocket.Conn) {
		accepts.Add(1)
		_ = ws.Close(4011, "reauth required")
	}})

	rec := newDisconnectRecorder()
	fa := &fakeAfter{}
	cl := NewClient(url, StaticToken("tok"), ClientConfig{
		Reconnect:    testReconnectOptions(fa, 1),
		OnDisconnect: rec.onDisconnect,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := cl.Connect(ctx); err == nil {
		t.Fatal("Connect should have failed fatally on a rejected static token")
	}
	defer cl.Close(context.Background())

	r := rec.waitNext(t)
	if !r.Fatal {
		t.Errorf("reason.Fatal = false, want true (a rejected static token is fatal at once)")
	}
	if r.Phase != PhaseHandshake {
		t.Errorf("reason.Phase = %v, want handshake", r.Phase)
	}
	if r.WSCode != 4011 {
		t.Errorf("reason.WSCode = %d, want 4011", r.WSCode)
	}
	if got := accepts.Load(); got != 1 {
		t.Errorf("server accepted %d connections, want exactly 1", got)
	}
}

// TestClientStaticTokenConnErrorUnauthorizedFatal is the same again, one
// layer higher: a real error{11} stream-0 frame before welcome (a
// *ConnError{UnauthorizedCode} on the client side, not just a bare close)
// with a StaticToken client is fatal at once. Written directly on the raw
// WebSocket (OnRawConn), the same way TestClientHandshakePhaseBareCloseReason
// pokes the wire below AcceptConn's own handshake.
func TestClientStaticTokenConnErrorUnauthorizedFatal(t *testing.T) {
	var accepts atomic.Int32
	_, url := startTestServer(t, testAcceptHandler{OnRawConn: func(ws *websocket.Conn) {
		accepts.Add(1)
		errMsg := &ErrorMsg{T: "error", Code: uint32(UnauthorizedCode), Message: "nope"}
		b, err := json.Marshal(errMsg)
		if err != nil {
			t.Fatalf("marshal error{}: %v", err)
		}
		if err := ws.Write(context.Background(), websocket.MessageBinary, EncodeData(0, b)); err != nil {
			t.Fatalf("write error{}: %v", err)
		}
		_ = ws.Close(4011, "nope")
	}})

	rec := newDisconnectRecorder()
	fa := &fakeAfter{}
	cl := NewClient(url, StaticToken("tok"), ClientConfig{
		Reconnect:    testReconnectOptions(fa, 1),
		OnDisconnect: rec.onDisconnect,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := cl.Connect(ctx); err == nil {
		t.Fatal("Connect should have failed fatally on a rejected static token")
	}
	defer cl.Close(context.Background())

	r := rec.waitNext(t)
	if !r.Fatal {
		t.Errorf("reason.Fatal = false, want true (a rejected static token is fatal at once)")
	}
	if r.Phase != PhaseHandshake {
		t.Errorf("reason.Phase = %v, want handshake", r.Phase)
	}
	if !r.HasErrorCode || r.ErrorCode != UnauthorizedCode {
		t.Errorf("reason.ErrorCode = %v (has=%v), want UnauthorizedCode", r.ErrorCode, r.HasErrorCode)
	}
	if got := accepts.Load(); got != 1 {
		t.Errorf("server accepted %d connections, want exactly 1", got)
	}
}

// TestClientCloseCode1001ClassifiedLikeGoingAway pins nit 6's other test
// gap: a raw WebSocket 1001 close with no preceding ws-mixer error{} frame
// (OVERVIEW.md section 2.8, matching the JS SDK's ABNORMAL_CLOSURE_WS_CODE
// branch) is classified exactly like 4012 -- reported non-fatal, and
// reconnected immediately with jitter(0,2s) -- not run through the plain
// full-jitter-backoff default case.
func TestClientCloseCode1001ClassifiedLikeGoingAway(t *testing.T) {
	var raw atomic.Pointer[websocket.Conn]
	_, url := startTestServer(t, testAcceptHandler{OnRawConn: func(ws *websocket.Conn) { raw.Store(ws) }})

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

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && raw.Load() == nil {
		time.Sleep(5 * time.Millisecond)
	}
	ws := raw.Load()
	if ws == nil {
		t.Fatal("never captured the raw server-side WebSocket conn")
	}
	_ = ws.Close(websocket.StatusGoingAway, "test 1001") // StatusGoingAway == 1001

	r := rec.waitNext(t)
	if r.WSCode != 1001 || r.Fatal {
		t.Fatalf("reason = %+v, want WSCode=1001 fatal=false", r)
	}
	calls := waitForBackoffCalls(t, fa, rc, 1)
	want := time.Duration(0.5 * float64(2*time.Second)) // rand=0.5, jitter(0,2s)
	if calls[0] != want {
		t.Errorf("1001 reconnect delay = %v, want exactly %v (jitter(0,2s), same as 4012)", calls[0], want)
	}

	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && onConnectCount.Load() < 2 {
		time.Sleep(10 * time.Millisecond)
	}
	if got := onConnectCount.Load(); got < 2 {
		t.Fatalf("OnConnect fired %d times, want >= 2 (client reconnected after 1001)", got)
	}
}

// TestClientDrainThenFatalCloseNeverReconnects pins the bug fixed alongside
// this test: handleServerDrain, run from conn.go's post-close
// flushDeliveryQueue for a drain frame that arrived just before a fatal
// error{UNAUTHORIZED}/close 4011, used to check only cl.closing and
// cl.conn == conn -- never whether conn was still alive -- and so still set
// drainReconnectScheduled and started a reconnect for an already-dead conn.
// watchConn's drainSched branch then reported that disconnect as a
// non-fatal, already-being-retried one instead of running the 4010/4011
// switch case, turning a fatal close into a silent reconnect.
//
// This drives the fixed check directly rather than racing for it: no
// Connect() is called, so there is no watchConn goroutine to race against
// in the first place. The Client is built white-box (cl.conn/cl.state set
// by hand, the same pattern TestBuildConnectedDisconnectReasonBareCloseDerivation
// uses for newUnrunConn) with a conn that is already dead -- failed and past
// conn.Done() -- before handleServerDrain ever runs, so its
// "case <-conn.Done():" branch is guaranteed to fire, no ordering to
// arrange. startTestServer is only here to hand NewClient a URL; nothing
// ever dials it, since Connect is never called.
func TestClientDrainThenFatalCloseNeverReconnects(t *testing.T) {
	var dials atomic.Int32
	_, url := startTestServer(t, testAcceptHandler{OnConn: func(*Conn) { dials.Add(1) }})

	fa := &fakeAfter{}
	cl := NewClient(url, StaticToken("tok"), ClientConfig{
		Reconnect: testReconnectOptions(fa, 0.5),
	})

	conn := newUnrunConn(newFakeWS())
	cl.mu.Lock()
	cl.conn = conn
	cl.state = clientConnected
	cl.mu.Unlock()

	conn.fail(newConnErrorf(UnauthorizedCode, "test: unauthorized right after drain"))
	<-conn.Done()

	cl.handleServerDrain(conn, &DrainMsg{T: "drain", Reason: "reauth"})

	cl.mu.Lock()
	scheduled := cl.drainReconnectScheduled
	retiring := cl.retiringConn
	cl.mu.Unlock()
	if scheduled {
		t.Error("drainReconnectScheduled = true, want false: conn was already dead when the drain ran")
	}
	if retiring != nil {
		t.Errorf("retiringConn = %v, want nil", retiring)
	}
	if got := dials.Load(); got != 0 {
		t.Errorf("dial attempts = %d, want 0 (a flushed drain on a dead conn must not trigger a reconnect)", got)
	}
}
