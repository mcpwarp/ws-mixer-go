package wsmixer

// Test gaps flagged by the v0.4.0 client review (nit 8): MaxAttempts
// exhaustion, 429 Retry-After honored, reconnect Disabled, Stats().
// ProtocolViolations incrementing, Client.Stats() cumulative across
// reconnects, and a regression for should-fix 3 (the drain-race stale-conn
// bail).

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// TestClientMaxAttemptsExhaustion pins WIRE.md section 2.9's exhaustion
// path: once a bounded ReconnectOptions.MaxAttempts is used up, the final
// disconnect is reported exactly once, fatally, and the state machine ends
// closed instead of scheduling yet another attempt.
func TestClientMaxAttemptsExhaustion(t *testing.T) {
	// The first connect succeeds (so Connect() itself returns nil, and the
	// exhaustion is observable purely as a later OnDisconnect, matching the
	// gap this pins: "MaxAttempts exhaustion (final OnDisconnect Fatal)").
	// The server then drops that connection and refuses every dial after
	// it, so every subsequent attempt fails at the dial/handshake layer
	// (never reaching a welcome, which would otherwise reset cl.attempt to
	// 0 per WIRE.md section 2.9 and prevent exhaustion from ever happening).
	var accepted atomic.Int32
	inner := newTestListener(testAcceptHandler{
		Options: Options{Logger: discardLogger()},
		OnConn: func(c *Conn) {
			go func() { _ = c.Close(uint32(ProtocolErrorCode), "drop after first connect") }()
		},
	})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if accepted.Add(1) == 1 {
			inner.ServeHTTP(w, r)
			return
		}
		http.Error(w, "gone", http.StatusBadRequest)
	}))
	t.Cleanup(srv.Close)
	url := "ws" + srv.URL[len("http"):] + "/tunnel"

	rec := newDisconnectRecorder()
	fa := &fakeAfter{}
	rc := testReconnectOptions(fa, 0)
	rc.MaxAttempts = 2
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

	var last DisconnectReason
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		last = rec.waitNext(t)
		if last.Fatal {
			break
		}
	}
	if !last.Fatal {
		t.Fatalf("final reason.Fatal = false, want true: %+v", last)
	}
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && cl.State() != "closed" {
		time.Sleep(10 * time.Millisecond)
	}
	if got := cl.State(); got != "closed" {
		t.Errorf("State() = %s, want closed after MaxAttempts exhausted", got)
	}
}

// TestClient429RetryAfterHonored pins CLIENT-SDK.md/WIRE.md section 2.9's
// "429: honour Retry-After" rule: a 429 upgrade response with a
// Retry-After header drives the next attempt's delay directly, bypassing
// full-jitter backoff.
func TestClient429RetryAfterHonored(t *testing.T) {
	var n atomic.Int32
	inner := newTestListener(testAcceptHandler{Options: Options{Logger: discardLogger()}})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if n.Add(1) == 1 {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		inner.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	url := "ws" + srv.URL[len("http"):] + "/tunnel"

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
	defer cl.Close(context.Background())

	r := rec.waitNext(t)
	if r.HTTPStatus != 429 {
		t.Fatalf("reason.HTTPStatus = %d, want 429", r.HTTPStatus)
	}
	calls := waitForCalls(t, fa, 1)
	if calls[0] != time.Second {
		t.Errorf("delay after 429 = %v, want exactly 1s (Retry-After, not full-jitter backoff)", calls[0])
	}
}

// TestClientReconnectDisabled pins ReconnectOptions.Disabled (nit 7): a
// recoverable disconnect with reconnect Disabled is reported exactly once,
// fatally, with no attempt to redial.
func TestClientReconnectDisabled(t *testing.T) {
	_, url := startTestServer(t, testAcceptHandler{OnConn: func(c *Conn) {
		go func() { _ = c.Close(uint32(ProtocolErrorCode), "boom") }()
	}})
	rec := newDisconnectRecorder()
	fa := &fakeAfter{}
	rc := testReconnectOptions(fa, 0)
	rc.Disabled = true
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
	if !r.Fatal {
		t.Errorf("reason.Fatal = false, want true (reconnect Disabled)")
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && cl.State() != "closed" {
		time.Sleep(10 * time.Millisecond)
	}
	if got := cl.State(); got != "closed" {
		t.Errorf("State() = %s, want closed", got)
	}
	if calls := fa.snapshot(); len(calls) != 0 {
		t.Errorf("backoff delays recorded = %v, want none (Disabled never reconnects)", calls)
	}
}

// TestClientStatsProtocolViolationIncrements pins Client.Stats()'s
// ProtocolViolations counter: the server sends a frame that is illegal on
// stream 0 (dispatch.go: only DATA is legal there), which the client's
// dispatch loop must fail the connection over and count.
func TestClientStatsProtocolViolationIncrements(t *testing.T) {
	_, url := startTestServer(t, testAcceptHandler{OnConn: func(c *Conn) {
		c.sendControlFrame(EncodeWindow(0, 1))
	}})
	fa := &fakeAfter{}
	rc := testReconnectOptions(fa, 0)
	rc.Disabled = true
	cl := NewClient(url, StaticToken("tok"), ClientConfig{Reconnect: rc})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := cl.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer cl.Close(context.Background())

	deadline := time.Now().Add(2 * time.Second)
	var s Stats
	for time.Now().Before(deadline) {
		s = cl.Stats()
		if s.ProtocolViolations >= 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if s.ProtocolViolations < 1 {
		t.Errorf("Stats().ProtocolViolations = %d, want >= 1", s.ProtocolViolations)
	}
}

// TestClientStatsCumulativeAcrossReconnects pins Client.Stats()'s
// cumulative-across-reconnects promise: bytes transferred on a connection
// that has since been replaced by a reconnect must still be reflected in
// Client.Stats(), added to whatever the new, live connection has done.
func TestClientStatsCumulativeAcrossReconnects(t *testing.T) {
	_, url := startTestServer(t, testAcceptHandler{})
	fa := &fakeAfter{}
	cl := NewClient(url, StaticToken("tok"), ClientConfig{Reconnect: testReconnectOptions(fa, 0)})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := cl.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer cl.Close(context.Background())

	c1 := cl.Conn()
	if err := c1.SendApp(context.Background(), map[string]any{"x": 1}); err != nil {
		t.Fatalf("SendApp on c1: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && c1.Stats().BytesOut == 0 {
		time.Sleep(5 * time.Millisecond)
	}
	before := c1.Stats().BytesOut
	if before == 0 {
		t.Fatal("c1 never recorded any BytesOut")
	}

	go func() { _ = c1.Close(uint32(ProtocolErrorCode), "force a reconnect") }()
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && (cl.Conn() == nil || cl.Conn() == c1) {
		time.Sleep(5 * time.Millisecond)
	}
	c2 := cl.Conn()
	if c2 == nil || c2 == c1 {
		t.Fatal("client never reconnected to a new *Conn")
	}
	if err := c2.SendApp(context.Background(), map[string]any{"y": 2}); err != nil {
		t.Fatalf("SendApp on c2: %v", err)
	}

	deadline = time.Now().Add(2 * time.Second)
	var total Stats
	for time.Now().Before(deadline) {
		total = cl.Stats()
		if total.BytesOut > before {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if total.BytesOut <= before {
		t.Errorf("Client.Stats().BytesOut = %d, want > %d (c1's bytes plus c2's)", total.BytesOut, before)
	}
}

// TestClientDrainStaleConnBail pins should-fix 3: a drain arriving on a conn
// that is no longer cl.conn by the time handleServerDrain gets past the
// user's own OnDrain callback must bail instead of starting a second
// parallel reconnect on top of whatever already happened -- and must not
// leak the stale conn.
func TestClientDrainStaleConnBail(t *testing.T) {
	connCh := make(chan *Conn, 8)
	_, url := startTestServer(t, testAcceptHandler{OnConn: func(c *Conn) { connCh <- c }})

	fa := &fakeAfter{}
	onDrainStarted := make(chan struct{})
	releaseOnDrain := make(chan struct{})
	var onDrainCount atomic.Int32
	cl := NewClient(url, StaticToken("tok"), ClientConfig{
		Reconnect: testReconnectOptions(fa, 0),
		OnDrain: func(*DrainMsg) {
			if onDrainCount.Add(1) == 1 {
				close(onDrainStarted)
				<-releaseOnDrain // block the first drain's handler mid-flight
			}
		},
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := cl.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer cl.Close(context.Background())

	c1 := <-connCh
	if _, err := c1.OpenStream(context.Background()); err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	go func() { _ = c1.Drain(context.Background(), "rollout", DrainOptions{Deadline: 10 * time.Second}) }()
	<-onDrainStarted // handleServerDrain(c1, ...) is now blocked inside OnDrain

	// While the first drain's OnDrain is still blocked (so handleServerDrain
	// hasn't yet re-locked to check cl.conn == c1), force c1 out from under
	// it via an ordinary Close, and let a normal reconnect happen.
	if err := c1.Close(uint32(ProtocolErrorCode), "unrelated"); err != nil {
		t.Fatalf("c1.Close: %v", err)
	}
	// c2 is the new server-side accepted connection (a different object than
	// the client-side cl.Conn(), which is never directly comparable to it --
	// they're the two ends of two different sockets). Its arrival on connCh
	// is exactly "the client reconnected".
	select {
	case <-connCh:
	case <-time.After(3 * time.Second):
		t.Fatal("client never reconnected after c1 closed")
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && cl.State() != "connected" {
		time.Sleep(5 * time.Millisecond)
	}
	clientConnAfterReconnect := cl.Conn()
	if cl.State() != "connected" || clientConnAfterReconnect == nil {
		t.Fatalf("client never became connected again (state=%s)", cl.State())
	}

	// Now release the first drain's OnDrain: handleServerDrain(c1, ...)
	// resumes and must see cl.conn != the stale conn it was bound to, bail
	// without touching state, and make sure the stale conn -- already
	// closed above, so this is a no-op here, but exercises the "ensure an
	// orphaned loser gets closed" path for a conn that, were it NOT already
	// closed, would otherwise leak.
	close(releaseOnDrain)

	// The real assertion: no second, spurious reconnect gets triggered by
	// the stale drain, and the client stays cleanly on its current conn.
	select {
	case extra := <-connCh:
		t.Fatalf("stale drain triggered an unwanted extra reconnect: %v", extra)
	case <-time.After(500 * time.Millisecond):
	}
	if cl.Conn() != clientConnAfterReconnect {
		t.Errorf("Conn() changed (%v -> %v): the stale drain must not have touched state", clientConnAfterReconnect, cl.Conn())
	}
}

// TestClientConnectConcurrentIdempotent pins nit 6: two concurrent Connect
// calls must both observe the same result instead of one of them blocking
// forever (a 1-buffered chan error, written once, can only ever be received
// by one of two concurrent readers).
func TestClientConnectConcurrentIdempotent(t *testing.T) {
	_, url := startTestServer(t, testAcceptHandler{})
	cl := NewClient(url, StaticToken("tok"), ClientConfig{})
	defer cl.Close(context.Background())

	const n = 8
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			errs <- cl.Connect(ctx)
		}()
	}
	for i := 0; i < n; i++ {
		select {
		case err := <-errs:
			if err != nil {
				t.Errorf("concurrent Connect() #%d = %v, want nil", i, err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("only %d/%d concurrent Connect() calls returned", i, n)
		}
	}
}

// TestClientNewClientNilTokenPanics pins nit 6's other half: NewClient must
// fail fast and clearly on a nil TokenProvider instead of panicking obscurely
// inside the first dial.
func TestClientNewClientNilTokenPanics(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("NewClient(nil token) did not panic")
		}
	}()
	NewClient("ws://example.invalid/tunnel", nil, ClientConfig{})
}
