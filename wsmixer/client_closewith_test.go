package wsmixer

// Tests for Change 4 (docs/DECISIONS.md 2026-09-20): Client.CloseWith,
// CLIENT-SDK.md's "Application close" row for a reconnecting client.

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// TestClientCloseWithApplicationCloseCode: CloseWith on a connected client
// closes it at once (no drain, no grace), the server observes
// error{14,"bye"} then WS close 4014, and the client itself reports exactly
// one non-fatal disconnect with ErrorName APPLICATION_CLOSE, then stops
// reconnecting for good (State() closed, no further accepts).
func TestClientCloseWithApplicationCloseCode(t *testing.T) {
	connCh := make(chan *Conn, 4)
	var accepts atomic.Int32
	_, url := startTestServer(t, testAcceptHandler{OnConn: func(c *Conn) {
		accepts.Add(1)
		connCh <- c
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

	var serverConn *Conn
	select {
	case serverConn = <-connCh:
	case <-time.After(2 * time.Second):
		t.Fatal("server never observed the connection")
	}

	closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer closeCancel()
	if err := cl.CloseWith(closeCtx, "bye"); err != nil {
		t.Fatalf("CloseWith: %v", err)
	}

	select {
	case <-serverConn.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("server-side connection never closed")
	}
	if got := serverConn.CloseCode(); got != 4014 {
		t.Errorf("server-observed close code = %d, want 4014", got)
	}
	if got := serverConn.ErrCodeName(); got != "APPLICATION_CLOSE" {
		t.Errorf("server-observed error name = %q, want APPLICATION_CLOSE", got)
	}
	if ce, ok := serverConn.Err().(*ConnError); !ok || ce.Message != "bye" {
		t.Errorf("server-observed error{} = %+v, want message %q", serverConn.Err(), "bye")
	}

	r := rec.waitNext(t)
	if r.Fatal {
		t.Errorf("reason.Fatal = true, want false")
	}
	if r.WSCode != 4014 {
		t.Errorf("reason.WSCode = %d, want 4014", r.WSCode)
	}
	if !r.HasErrorCode || r.ErrorName != "APPLICATION_CLOSE" {
		t.Errorf("reason.ErrorName = %q (has=%v), want APPLICATION_CLOSE", r.ErrorName, r.HasErrorCode)
	}

	select {
	case r := <-rec.ch:
		t.Errorf("unexpected second OnDisconnect: %+v", r)
	case <-time.After(300 * time.Millisecond):
	}
	select {
	case c := <-connCh:
		t.Errorf("client redialed after CloseWith: %v", c)
	case <-time.After(500 * time.Millisecond):
	}
	// State() is now guaranteed exactly "closed" (review item 4 fixed the
	// watchConn/closeStart race that used to leave it transiently
	// "disconnected" -- watchConn's own clientDisconnected write is now
	// guarded on cl.state != clientClosed).
	if got := cl.State(); got != "closed" {
		t.Errorf("State() = %s, want closed", got)
	}
	if got := accepts.Load(); got != 1 {
		t.Errorf("server accepted %d connections, want exactly 1 (no redial after CloseWith)", got)
	}
}

// TestClientCloseStateExactlyClosed pins review item 4 directly on plain
// Close(): a connected client's State() must read exactly "closed" once
// Close() returns, not transiently "disconnected".
func TestClientCloseStateExactlyClosed(t *testing.T) {
	connCh := make(chan *Conn, 4)
	_, url := startTestServer(t, testAcceptHandler{OnConn: func(c *Conn) { connCh <- c }})
	fa := &fakeAfter{}
	cl := NewClient(url, StaticToken("tok"), ClientConfig{Reconnect: testReconnectOptions(fa, 0)})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := cl.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	<-connCh
	if err := cl.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := cl.State(); got != "closed" {
		t.Errorf("State() = %s, want closed", got)
	}
}

// TestClientFatalStateExactlyClosed pins the same for the fatal path
// (goFatal): State() must read "closed" once the client has gone fatal on
// an active connection, never a transient "disconnected" left behind by a
// racing watchConn.
func TestClientFatalStateExactlyClosed(t *testing.T) {
	_, url := startTestServer(t, testAcceptHandler{OnConn: func(c *Conn) {
		go func() { _ = c.Close(uint32(UnauthorizedCode), "fatal test") }()
	}})
	fa := &fakeAfter{}
	cl := NewClient(url, StaticToken("tok"), ClientConfig{Reconnect: testReconnectOptions(fa, 0)})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := cl.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer cl.Close(context.Background())
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && cl.State() != "closed" {
		time.Sleep(5 * time.Millisecond)
	}
	if got := cl.State(); got != "closed" {
		t.Errorf("State() = %s, want closed", got)
	}
}

// TestClientCloseWithDuringDial pins CloseWith called while a dial is in
// flight (or backing off): it must return promptly, not wait out any
// per-attempt timeout, and must not let a subsequent dial land.
func TestClientCloseWithDuringDial(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			_ = c // accept and never answer: the HTTP upgrade hangs
		}
	}()
	rc := ReconnectOptions{Base: time.Millisecond, Cap: time.Millisecond, ConnectTimeout: 8 * time.Second, MaxAttempts: 1}
	cl := NewClient("ws://"+ln.Addr().String()+"/tunnel", StaticToken("tok"), ClientConfig{Reconnect: rc})
	go func() { _ = cl.Connect(context.Background()) }()
	time.Sleep(300 * time.Millisecond)
	start := time.Now()
	if err := cl.CloseWith(context.Background(), "bye"); err != nil {
		t.Fatalf("CloseWith: %v", err)
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("CloseWith() took %v while a dial was in flight (ConnectTimeout=8s)", d)
	}
	if got := cl.State(); got != "closed" {
		t.Errorf("State() = %s, want closed", got)
	}
}

// TestClientCloseWithThenCloseIdempotent and its mirror pin idempotency: no
// panic, no second report, whichever of Close/CloseWith runs first.
func TestClientCloseWithThenCloseIdempotent(t *testing.T) {
	connCh := make(chan *Conn, 4)
	_, url := startTestServer(t, testAcceptHandler{OnConn: func(c *Conn) { connCh <- c }})
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
	<-connCh

	if err := cl.CloseWith(context.Background(), "bye"); err != nil {
		t.Fatalf("CloseWith: %v", err)
	}
	rec.waitNext(t)
	if err := cl.Close(context.Background()); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	select {
	case r := <-rec.ch:
		t.Errorf("unexpected second OnDisconnect after Close following CloseWith: %+v", r)
	case <-time.After(300 * time.Millisecond):
	}
}

func TestClientCloseThenCloseWithIdempotent(t *testing.T) {
	connCh := make(chan *Conn, 4)
	_, url := startTestServer(t, testAcceptHandler{OnConn: func(c *Conn) { connCh <- c }})
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
	<-connCh

	if err := cl.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	rec.waitNext(t)
	if err := cl.CloseWith(context.Background(), "bye"); err != nil {
		t.Fatalf("CloseWith after Close: %v", err)
	}
	select {
	case r := <-rec.ch:
		t.Errorf("unexpected second OnDisconnect after CloseWith following Close: %+v", r)
	case <-time.After(300 * time.Millisecond):
	}
}

// TestClientCloseWithConcurrentIdempotent pins the concurrent case
// explicitly: many goroutines racing Close/CloseWith must never panic, and
// exactly one non-fatal disconnect gets reported.
func TestClientCloseWithConcurrentIdempotent(t *testing.T) {
	connCh := make(chan *Conn, 4)
	_, url := startTestServer(t, testAcceptHandler{OnConn: func(c *Conn) { connCh <- c }})
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
	<-connCh

	done := make(chan struct{}, 4)
	for i := 0; i < 2; i++ {
		go func() { _ = cl.Close(context.Background()); done <- struct{}{} }()
		go func() { _ = cl.CloseWith(context.Background(), "bye"); done <- struct{}{} }()
	}
	for i := 0; i < 4; i++ {
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("a concurrent Close/CloseWith call never returned")
		}
	}

	rec.waitNext(t)
	select {
	case r := <-rec.ch:
		t.Errorf("unexpected second OnDisconnect from concurrent Close/CloseWith: %+v", r)
	case <-time.After(300 * time.Millisecond):
	}
}

// TestClientCloseWithDoesNotLeakGoroutines mirrors
// TestClientFatalTerminationDoesNotLeakCallbackLoop's pattern for
// CloseWith specifically.
func TestClientCloseWithDoesNotLeakGoroutines(t *testing.T) {
	const n = 10
	connCh := make(chan *Conn, n)
	_, url := startTestServer(t, testAcceptHandler{OnConn: func(c *Conn) { connCh <- c }})

	before := settledGoroutines(t)
	for i := 0; i < n; i++ {
		fa := &fakeAfter{}
		cl := NewClient(url, StaticToken("tok"), ClientConfig{Reconnect: testReconnectOptions(fa, 0)})
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := cl.Connect(ctx); err != nil {
			cancel()
			t.Fatalf("client #%d: Connect: %v", i, err)
		}
		cancel()
		<-connCh
		if err := cl.CloseWith(context.Background(), "bye"); err != nil {
			t.Fatalf("client #%d: CloseWith: %v", i, err)
		}
	}
	after := settledGoroutines(t)
	if after > before+5 {
		t.Errorf("goroutine count %d -> %d after %d CloseWith clients (leak?)", before, after, n)
	}
}

// TestClientCloseWithDuringDrainHandoverSendsApplicationCode pins review
// items 5 and 6: during a drain hand-over's window, cl.conn and
// cl.retiringConn point at the very same live conn (the ownership
// invariant above Close) -- there is no reachable window in this file
// where they are simultaneously non-nil and DIFFERENT (onAttemptSucceeded
// always updates cl.conn and nils cl.retiringConn together, atomically
// under cl.mu, so any other goroutine's own cl.mu-guarded snapshot -- e.g.
// closeStart's -- only ever observes them equal or retiringConn nil, never
// diverged). serverConn below plays both roles at once, so its single
// observed close code pins both: (item 5) calling CloseWith right then
// must still send the application code, not lose a race to the bare
// NoError close "the retiring conn" (the very same live conn, from the
// other side) would otherwise send; (item 6, cross-SDK alignment) had that
// retiring-side close actually run, it now also carries
// ApplicationCloseCode and this call's message, not Close's bare NO_ERROR,
// matching the JS SDK's close({message}) closing every conn it still owns
// with that same message -- defense-in-depth for a divergent-retiring-conn
// state this file's current invariants don't appear to make reachable at all, kept
// consistent with closeLiveConns' identical existing guard on the
// fatal paths. The parallel reconnect's own dial is gated (blocked
// mid-upgrade) so this test can land CloseWith deterministically inside
// the (only reachable) same-conn window, rather than racing a reconnect
// that (with every other clock in this suite firing instantly) would
// otherwise land -- and replace cl.conn with a genuinely different *Conn,
// via the unrelated drain-race-winner path (onAttemptSucceeded's own
// wonDrainRace NoError close) -- before CloseWith ever ran, which is a
// different mechanism than the one this test targets.
// Caveat: the item 5 guard's own race is closeOnce-guarded and inherently
// narrow regardless -- the retiring branch's `go func(){...}()` dispatch is
// consistently slower to reach its own Conn.Close call than this method's
// own synchronous one a few lines later, so this test does not reliably
// fail without the item 5 guard (confirmed by temporarily removing it:
// still passed, 3/3). Kept anyway as defense-in-depth, matching
// closeLiveConns' existing guard for the fatal paths, and as documentation
// of the race this guards against.
func TestClientCloseWithDuringDrainHandoverSendsApplicationCode(t *testing.T) {
	connCh := make(chan *Conn, 4)
	inner := newTestListener(testAcceptHandler{
		Options: Options{Logger: discardLogger()},
		OnConn:  func(c *Conn) { connCh <- c },
	})
	var n atomic.Int32
	gate := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if n.Add(1) == 2 {
			<-gate // block the parallel reconnect's own dial mid-upgrade
		}
		inner.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	t.Cleanup(func() { close(gate) })
	url := "ws" + srv.URL[len("http"):] + "/tunnel"

	fa := &fakeAfter{}
	cl := NewClient(url, StaticToken("tok"), ClientConfig{Reconnect: testReconnectOptions(fa, 0)})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := cl.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	serverConn := <-connCh
	// Keep a stream open so serverConn doesn't close the instant it drains,
	// widening the cl.conn == cl.retiringConn window CloseWith below needs
	// to land inside.
	if _, err := serverConn.OpenStream(context.Background()); err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	go func() {
		_ = serverConn.Drain(context.Background(), "rollout", DrainOptions{Deadline: 10 * time.Second})
	}()

	// Wait for the parallel reconnect's dial to actually reach (and block
	// on) the gate: only then is cl.retiringConn guaranteed set to
	// serverConn, with cl.conn still serverConn too (the replacement dial
	// hasn't landed yet -- it can't, it's blocked).
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && n.Load() < 2 {
		time.Sleep(2 * time.Millisecond)
	}
	if n.Load() < 2 {
		t.Fatal("the parallel reconnect's dial never started")
	}
	time.Sleep(20 * time.Millisecond) // let handleServerDrain finish setting cl.retiringConn

	if err := cl.CloseWith(context.Background(), "bye"); err != nil {
		t.Fatalf("CloseWith: %v", err)
	}

	select {
	case <-serverConn.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("server-side connection never closed")
	}
	if got := serverConn.CloseCode(); got != 4014 {
		t.Errorf("server-observed close code = %d, want 4014 (the application code, not NoError/1000 lost to the retiring-conn race)", got)
	}
}

// TestClientCloseWithFromOnDisconnectSync pins the doc comment's blocker-2
// claim directly for CloseWith (Close's own equivalent is
// TestClientCloseFromOnDisconnectSync): calling CloseWith synchronously
// from inside OnDisconnect -- which runs on callbackLoop, never
// cl.wg-tracked -- must not deadlock against CloseWith's own cl.wg.Wait().
func TestClientCloseWithFromOnDisconnectSync(t *testing.T) {
	_, url := startTestServer(t, testAcceptHandler{OnConn: func(c *Conn) {
		go func() { _ = c.Close(uint32(ProtocolErrorCode), "boom") }()
	}})
	fa := &fakeAfter{}
	done := make(chan struct{})
	var cl *Client
	cl = NewClient(url, StaticToken("tok"), ClientConfig{
		Reconnect: testReconnectOptions(fa, 0),
		OnDisconnect: func(DisconnectReason) {
			_ = cl.CloseWith(context.Background(), "bye") // the natural thing a user writes
			close(done)
		},
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := cl.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	select {
	case <-done:
	case <-time.After(4 * time.Second):
		t.Fatal("CloseWith() called synchronously from OnDisconnect never returned (deadlock)")
	}
}
