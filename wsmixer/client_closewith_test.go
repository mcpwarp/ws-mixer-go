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

// TestClientFatalStateExactlyClosed pins the terminal state on the fatal path
// (goFatal, here via a post-welcome 4011): goFatal sets clientClosed before
// it reports, so State() must already read "closed" from inside the fatal
// OnDisconnect itself -- sampled there, not polled for -- and must still read
// "closed" once every wg-tracked client goroutine has exited, so nothing that
// ran after goFatal left a different state behind. watchConn's own
// "disconnected" write just before it calls goFatal is the expected nit-4
// transient, not a failure; watchConn's clientClosed guard is a no-op on this
// path (goFatal nils cl.conn in the same critical section), so that guard is
// pinned by TestClientCloseWithApplicationCloseCode/
// TestClientCloseStateExactlyClosed instead.
func TestClientFatalStateExactlyClosed(t *testing.T) {
	_, url := startTestServer(t, testAcceptHandler{OnConn: func(c *Conn) {
		go func() { _ = c.Close(uint32(UnauthorizedCode), "fatal test") }()
	}})
	fa := &fakeAfter{}
	type report struct {
		reason DisconnectReason
		state  string
	}
	reports := make(chan report, 4)
	var cl *Client
	cl = NewClient(url, StaticToken("tok"), ClientConfig{
		Reconnect: testReconnectOptions(fa, 0),
		OnDisconnect: func(r DisconnectReason) {
			reports <- report{r, cl.State()}
		},
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// Connect can itself return the 4011 reason: onAttemptSucceeded starts
	// watchConn before it resolves Connect's first result, so an immediate
	// server close can reach goFatal's finishFirstResult first. The fatal
	// OnDisconnect below is what this test asserts on either way.
	_ = cl.Connect(ctx)
	defer cl.Close(context.Background())

	var got report
	select {
	case got = <-reports:
	case <-time.After(5 * time.Second):
		t.Fatal("no OnDisconnect after the server closed with 4011")
	}
	if !got.reason.Fatal || got.reason.WSCode != 4011 {
		t.Fatalf("reason = %+v, want the fatal 4011 report", got.reason)
	}
	if got.state != "closed" {
		t.Errorf("State() inside the fatal OnDisconnect = %s, want closed", got.state)
	}

	done := make(chan struct{})
	go func() { cl.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("client goroutines never exited after going fatal")
	}
	if s := cl.State(); s != "closed" {
		t.Errorf("State() after every client goroutine exited = %s, want closed", s)
	}
}

// TestClientCloseWithDuringDial pins CloseWith called while a dial is in
// flight: the server accepts TCP and never answers the upgrade, and the test
// waits for that accept (and State() == "dialing") before calling CloseWith,
// so the call provably lands mid-dial -- not before the dial started, and
// not in backoff (TestClientCloseWithDuringBackoff covers that). It must
// return promptly, not wait out ConnectTimeout, and no further dial may
// reach the server.
func TestClientCloseWithDuringDial(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	var accepts atomic.Int32
	accepted := make(chan struct{}, 1)
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			_ = c // accept and never answer: the HTTP upgrade hangs
			accepts.Add(1)
			select {
			case accepted <- struct{}{}:
			default:
			}
		}
	}()
	rc := ReconnectOptions{Base: time.Millisecond, Cap: time.Millisecond, ConnectTimeout: 8 * time.Second, MaxAttempts: 1}
	cl := NewClient("ws://"+ln.Addr().String()+"/tunnel", StaticToken("tok"), ClientConfig{Reconnect: rc})
	connectErr := make(chan error, 1)
	go func() { connectErr <- cl.Connect(context.Background()) }()
	select {
	case <-accepted:
	case <-time.After(2 * time.Second):
		t.Fatal("the dial never reached the server")
	}
	if got := cl.State(); got != "dialing" {
		t.Fatalf("State() before CloseWith = %s, want dialing", got)
	}

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
	if got := accepts.Load(); got != 1 {
		t.Errorf("server accepted %d dials, want exactly 1 (no dial after CloseWith)", got)
	}
	select {
	case err := <-connectErr:
		if err == nil {
			t.Error("Connect() = nil after CloseWith ended the only dial, want an error")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Connect() never returned after CloseWith")
	}
}

// TestClientCloseWithDuringBackoff is TestClientCloseWithDuringDial's
// backoff counterpart: the first dial fails (HTTP 503, recoverable) and the
// backoff timer never fires on its own, so the client is parked in
// waitBackoff -- confirmed via State() == "backoff" before CloseWith runs.
// CloseWith must return promptly and no second dial may reach the server.
func TestClientCloseWithDuringBackoff(t *testing.T) {
	var requests atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	t.Cleanup(srv.Close)
	rc := ReconnectOptions{Base: 10 * time.Millisecond, Cap: time.Second, ConnectTimeout: 2 * time.Second}
	rc.after = func(time.Duration) <-chan time.Time { return make(chan time.Time) }
	rc.randFloat64 = fixedRand(0.5)
	cl := NewClient("ws"+srv.URL[len("http"):]+"/tunnel", StaticToken("tok"), ClientConfig{Reconnect: rc})
	connectErr := make(chan error, 1)
	go func() { connectErr <- cl.Connect(context.Background()) }()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && cl.State() != "backoff" {
		time.Sleep(2 * time.Millisecond)
	}
	if got := cl.State(); got != "backoff" {
		t.Fatalf("State() before CloseWith = %s, want backoff", got)
	}

	start := time.Now()
	if err := cl.CloseWith(context.Background(), "bye"); err != nil {
		t.Fatalf("CloseWith: %v", err)
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("CloseWith() took %v while backing off", d)
	}
	if got := cl.State(); got != "closed" {
		t.Errorf("State() = %s, want closed", got)
	}
	if got := requests.Load(); got != 1 {
		t.Errorf("server saw %d dials, want exactly 1 (no dial after CloseWith)", got)
	}
	select {
	case err := <-connectErr:
		if err == nil {
			t.Error("Connect() = nil after CloseWith during backoff, want an error")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Connect() never returned after CloseWith")
	}
}

// TestClientCloseWithAfterServerDrain is TestClientCloseAfterServerDrain's
// CloseWith counterpart, with reconnect enabled: the server drains, the
// client sets retiringConn and starts its parallel reconnect, and that dial
// is held mid-upgrade so CloseWith provably lands with both in place. The
// retiring conn must still get error{14,"bye"} + 4014, no dial may start
// after CloseWith returns, and State() must read "closed".
func TestClientCloseWithAfterServerDrain(t *testing.T) {
	connCh := make(chan *Conn, 4)
	inner := newTestListener(testAcceptHandler{
		Options: Options{Logger: discardLogger()},
		OnConn:  func(c *Conn) { connCh <- c },
	})
	var dials atomic.Int32
	reconnectDialing := make(chan struct{})
	gate := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if dials.Add(1) == 2 {
			close(reconnectDialing)
			<-gate
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
	// An open stream keeps serverConn alive past its drain until the 10s
	// deadline, so the retiring conn is still live when CloseWith runs.
	if _, err := serverConn.OpenStream(context.Background()); err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	go func() {
		_ = serverConn.Drain(context.Background(), "rollout", DrainOptions{Deadline: 10 * time.Second})
	}()

	// handleServerDrain sets retiringConn before it spawns the attemptLoop
	// whose dial reaches the server, so this alone orders the two.
	select {
	case <-reconnectDialing:
	case <-time.After(2 * time.Second):
		t.Fatal("the drain's parallel reconnect never dialed")
	}
	cl.mu.Lock()
	retiringSet := cl.retiringConn != nil
	cl.mu.Unlock()
	if !retiringSet {
		t.Fatal("retiringConn not set while the drain's parallel dial is in flight")
	}

	closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer closeCancel()
	if err := cl.CloseWith(closeCtx, "bye"); err != nil {
		t.Fatalf("CloseWith: %v", err)
	}
	dialsAtClose := dials.Load()

	select {
	case <-serverConn.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("retiring server-side connection never closed")
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
	if got := cl.State(); got != "closed" {
		t.Errorf("State() = %s, want closed", got)
	}
	time.Sleep(100 * time.Millisecond)
	if got := dials.Load(); got != dialsAtClose || got != 2 {
		t.Errorf("server saw %d dials (%d at CloseWith return), want exactly 2: no dial after CloseWith", got, dialsAtClose)
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
