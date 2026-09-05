package wsmixer

// Regression tests for the v0.4.0 client review's second fix round: F1 (a
// permanent callbackLoop leak after any fatal termination that never goes
// through Close()), F2 (a duplicate server drain frame orphaning a live
// conn and wedging Close()), nit 3 (a drain race's winner never reporting
// the superseded conn's disconnect), and nit 6's two test gaps (StaticToken
// vs. a real provider on 401, and a raw WS 1001 close's classification).

import (
	"context"
	"net/http"
	"net/http/httptest"
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

// TestClientStaticTokenNoImmediateRetryOn401 pins nit 6: a StaticToken
// client has no way for a redial to fix an HTTP 401 (it would hand back the
// exact same, already-rejected token), so isRealProvider gates the
// one-time immediate refresh-retry off for it -- the 401 goes through the
// ordinary recoverable-disconnect path (reported, then backed off) instead.
// This asserts the outward, user-visible behavior (a report, not a second
// immediate dial) rather than reflecting on isRealProvider directly.
// TestClient401RefreshRetrySuccess is this test's provider-side
// counterpart: 2 token-provider calls before Connect ever returns, no
// intervening report.
//
// The retry this test must rule out happens inline, inside dialAndHandshake,
// strictly before onAttemptFailed/reportAndSchedule ever runs -- so it can't
// be told apart from the correctly-scheduled backoff retry by racing a
// channel receive against however fast a second dial reaches the network
// (both would look identical, and on this machine the scheduled retry often
// wins that race despite being entirely legitimate). Instead, the backoff
// clock itself is held closed (rc.after never fires) until after the
// assertion: since a real provider's retry never calls rc.after at all (see
// TestClient401RefreshRetrySuccess -- it succeeds inline, no report, no
// backoff), holding it closed can only ever stall the correct, scheduled
// retry, never mask an incorrect inline one -- so "still exactly 1 dial
// while backoff is held closed" is a deterministic proof, not a timing bet.
func TestClientStaticTokenNoImmediateRetryOn401(t *testing.T) {
	var n atomic.Int32
	inner := newTestListener(testAcceptHandler{Options: Options{Logger: discardLogger()}})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if n.Add(1) == 1 {
			http.Error(w, "flaky", http.StatusUnauthorized)
			return
		}
		inner.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	url := "ws" + srv.URL[len("http"):] + "/tunnel"

	afterCh := make(chan time.Time)
	rc := ReconnectOptions{Base: 10 * time.Millisecond, Cap: time.Second, ConnectTimeout: 2 * time.Second}
	rc.after = func(time.Duration) <-chan time.Time { return afterCh }
	// randFloat64 must not be 0: fullJitter(next) = randFloat64()*exp, and a
	// delay of exactly 0 takes waitBackoff's fast path without ever calling
	// rc.after at all -- defeating the block below.
	rc.randFloat64 = fixedRand(1)

	rec := newDisconnectRecorder()
	cl := NewClient(url, StaticToken("tok"), ClientConfig{
		Reconnect:    rc,
		OnDisconnect: rec.onDisconnect,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() { _ = cl.Connect(ctx) }()
	defer cl.Close(context.Background())

	r := rec.waitNext(t)
	if r.HTTPStatus != 401 {
		t.Fatalf("reason.HTTPStatus = %d, want 401", r.HTTPStatus)
	}
	if got := n.Load(); got != 1 {
		t.Errorf("dial attempts while backoff is held closed = %d, want exactly 1 (no immediate refresh-retry for a static token)", got)
	}
	close(afterCh) // let the correctly-scheduled retry (against inner) through
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
	calls := waitForCalls(t, fa, 1)
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
