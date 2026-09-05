package wsmixer

// Regressions for the v0.4.0 client review's third fix round: B1 (a fatal
// disconnect while a live predecessor conn is still up wedges Close, because
// the fatal path never closed it) and B2 (Close() racing a landing dial: a
// conn published to cl.conn after Close captured its own snapshot was never
// closed either). Ported from the reviewer's probe files
// (zz_probe_test.go), deterministic.

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestClientFatalDuringDrainReconnectWedgesClose pins B1: a fatal failure of
// the drain-triggered parallel reconnect while the old conn is still alive.
// Before the fix, the fatal path (onAttemptFailed's fatal branch) set
// closing + closed closeCh, but never closed the still-active cl.conn:
// watchConn's closeCh escape only force-closed conns nothing claimed, and
// this one was still cl.conn -- so it parked on <-conn.Done() forever,
// wedging Close()'s wg.Wait().
func TestClientFatalDuringDrainReconnectWedgesClose(t *testing.T) {
	connCh := make(chan *Conn, 4)
	inner := newTestListener(testAcceptHandler{
		Options: Options{Logger: discardLogger()},
		OnConn:  func(c *Conn) { connCh <- c },
	})
	var n atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if n.Add(1) == 1 {
			inner.ServeHTTP(w, r)
			return
		}
		http.Error(w, "gone for good", http.StatusForbidden) // fatal per WIRE 2.9
	}))
	t.Cleanup(srv.Close)
	url := "ws" + srv.URL[len("http"):] + "/tunnel"

	fa := &fakeAfter{}
	rec := newDisconnectRecorder()
	cl := NewClient(url, StaticToken("tok"), ClientConfig{
		Reconnect:    testReconnectOptions(fa, 0),
		OnDisconnect: rec.onDisconnect,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := cl.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	c1 := <-connCh

	// Raw drain frame: the client starts a parallel reconnect, the old conn
	// stays fully alive (the server never closes it) -- so the fatal 403 on
	// the replacement dial fires while cl.conn is still c1.
	deadlineMS := int64(30_000)
	if err := c1.sendControl(&DrainMsg{T: "drain", Reason: "rollout", DeadlineMS: &deadlineMS}); err != nil {
		t.Fatalf("sendControl(drain): %v", err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && cl.State() != "closed" {
		time.Sleep(5 * time.Millisecond)
	}
	if got := cl.State(); got != "closed" {
		t.Fatalf("State = %s, want closed (403 on the drain reconnect is fatal)", got)
	}

	done := make(chan struct{})
	start := time.Now()
	go func() { _ = cl.Close(context.Background()); close(done) }()
	select {
	case <-done:
		t.Logf("Close returned in %v", time.Since(start))
	case <-time.After(8 * time.Second):
		t.Fatalf("Close() did not return within 8s -- wedged on wg.Wait()")
	}
}

// TestClientDrainReconnectDrainAgain pins F2b (kept green by this round, not
// broken by it): drain, reconnect, then a second drain on the REPLACEMENT
// conn must still produce a second replacement -- the dup-drain guard must
// not swallow a legitimate new drain.
func TestClientDrainReconnectDrainAgain(t *testing.T) {
	connCh := make(chan *Conn, 8)
	_, url := startTestServer(t, testAcceptHandler{OnConn: func(c *Conn) { connCh <- c }})

	fa := &fakeAfter{}
	cl := NewClient(url, StaticToken("tok"), ClientConfig{Reconnect: testReconnectOptions(fa, 0)})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := cl.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer func() {
		cctx, ccancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer ccancel()
		_ = cl.Close(cctx)
	}()

	deadlineMS := int64(30_000)
	drain := &DrainMsg{T: "drain", Reason: "rollout", DeadlineMS: &deadlineMS}

	c1 := <-connCh
	if err := c1.sendControl(drain); err != nil {
		t.Fatalf("drain 1: %v", err)
	}
	var c2 *Conn
	select {
	case c2 = <-connCh:
	case <-time.After(3 * time.Second):
		t.Fatal("no first replacement")
	}
	// Let the old conn actually finish so cl.conn settles on the replacement.
	_ = c1.Close(uint32(GoingAwayCode), "drain deadline")
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && cl.Conn() == nil {
		time.Sleep(5 * time.Millisecond)
	}
	if err := c2.sendControl(drain); err != nil {
		t.Fatalf("drain 2: %v", err)
	}
	select {
	case <-connCh:
		t.Log("second replacement dialed: OK")
	case <-time.After(3 * time.Second):
		t.Fatal("second drain on the replacement conn was swallowed: no reconnect")
	}
	_ = c2.Close(uint32(GoingAwayCode), "drain deadline")
}

// TestClientDrainRaceWinnerThenDrainAgain is TestClientDrainReconnectDrainAgain
// without letting the first old conn close first (the drain race is won by
// the replacement instead), then a drain on the replacement.
func TestClientDrainRaceWinnerThenDrainAgain(t *testing.T) {
	connCh := make(chan *Conn, 8)
	_, url := startTestServer(t, testAcceptHandler{OnConn: func(c *Conn) { connCh <- c }})

	fa := &fakeAfter{}
	cl := NewClient(url, StaticToken("tok"), ClientConfig{Reconnect: testReconnectOptions(fa, 0)})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := cl.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer func() {
		cctx, ccancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer ccancel()
		_ = cl.Close(cctx)
	}()

	deadlineMS := int64(30_000)
	drain := &DrainMsg{T: "drain", Reason: "rollout", DeadlineMS: &deadlineMS}

	c1 := <-connCh
	if err := c1.sendControl(drain); err != nil {
		t.Fatalf("drain 1: %v", err)
	}
	var c2 *Conn
	select {
	case c2 = <-connCh:
	case <-time.After(3 * time.Second):
		t.Fatal("no first replacement")
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && cl.Conn() == nil {
		time.Sleep(5 * time.Millisecond)
	}
	if err := c2.sendControl(drain); err != nil {
		t.Fatalf("drain 2: %v", err)
	}
	select {
	case <-connCh:
		t.Log("second replacement dialed: OK")
	case <-time.After(3 * time.Second):
		t.Fatal("second drain on the replacement conn was swallowed: no reconnect")
	}
}

// TestClientCloseRacesLandingDial pins B2: onAttemptSucceeded used to
// publish cl.conn = conn without re-checking cl.closing under the same
// cl.mu -- a conn that landed after Close() had already captured its own
// snapshot of cl.conn (nil, at that point) was never closed by anyone,
// and watchConn's old orphaned-conn check saw it was "still cl.conn" (it had
// just been published) and left it alone too. probeWidenWindow (declared in
// client_reconnect.go, normally nil) hooks the exact window between
// attemptLoop's own closingNow check and onAttemptSucceeded's closing
// re-check, making the race deterministic instead of a timing bet.
func TestClientCloseRacesLandingDial(t *testing.T) {
	connCh := make(chan *Conn, 8)
	_, url := startTestServer(t, testAcceptHandler{OnConn: func(c *Conn) { connCh <- c }})

	var n atomic.Int32
	landing := make(chan struct{})
	var once sync.Once
	probeWidenWindow = func() {
		if n.Add(1) >= 2 {
			once.Do(func() { close(landing) })
			time.Sleep(500 * time.Millisecond)
		}
	}
	t.Cleanup(func() { probeWidenWindow = nil })

	fa := &fakeAfter{}
	cl := NewClient(url, StaticToken("tok"), ClientConfig{Reconnect: testReconnectOptions(fa, 0)})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := cl.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	c1 := <-connCh
	_ = c1.Close(uint32(GoingAwayCode), "rollout") // force a reconnect

	select {
	case <-landing: // the replacement dial completed, not yet published
	case <-time.After(5 * time.Second):
		t.Fatal("replacement never reached the window")
	}
	done := make(chan struct{})
	start := time.Now()
	go func() { _ = cl.Close(context.Background()); close(done) }()
	select {
	case <-done:
		t.Logf("Close returned in %v", time.Since(start))
	case <-time.After(8 * time.Second):
		t.Fatalf("Close() wedged: a conn published after Close captured cl.conn is never closed")
	}
}

// TestClientCloseFromOnDisconnect pins the documented-supported case of
// calling Close() from inside OnDisconnect.
func TestClientCloseFromOnDisconnect(t *testing.T) {
	connCh := make(chan *Conn, 4)
	_, url := startTestServer(t, testAcceptHandler{OnConn: func(c *Conn) { connCh <- c }})
	fa := &fakeAfter{}
	var cl *Client
	done := make(chan struct{})
	var once sync.Once
	cl = NewClient(url, StaticToken("tok"), ClientConfig{
		Reconnect: testReconnectOptions(fa, 0),
		OnDisconnect: func(DisconnectReason) {
			once.Do(func() { _ = cl.Close(context.Background()); close(done) })
		},
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := cl.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	c1 := <-connCh
	_ = c1.Close(uint32(GoingAwayCode), "bye")
	select {
	case <-done:
	case <-time.After(8 * time.Second):
		t.Fatal("Close() from OnDisconnect wedged")
	}
}

// TestClientCloseFromOnDrain pins the same Close()-from-a-callback support
// for OnDrain, a delivery-loop goroutine rather than callbackLoop's own.
func TestClientCloseFromOnDrain(t *testing.T) {
	connCh := make(chan *Conn, 4)
	_, url := startTestServer(t, testAcceptHandler{OnConn: func(c *Conn) { connCh <- c }})
	fa := &fakeAfter{}
	var cl *Client
	done := make(chan struct{})
	var once sync.Once
	cl = NewClient(url, StaticToken("tok"), ClientConfig{
		Reconnect: testReconnectOptions(fa, 0),
		OnDrain: func(*DrainMsg) {
			once.Do(func() { _ = cl.Close(context.Background()); close(done) })
		},
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := cl.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	c1 := <-connCh
	deadlineMS := int64(30_000)
	if err := c1.sendControl(&DrainMsg{T: "drain", Reason: "rollout", DeadlineMS: &deadlineMS}); err != nil {
		t.Fatalf("sendControl: %v", err)
	}
	select {
	case <-done:
	case <-time.After(8 * time.Second):
		t.Fatal("Close() from OnDrain wedged")
	}
}
