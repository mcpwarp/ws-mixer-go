package wsmixer

// Regression tests pinning blockers 1-3 and should-fixes 1/3 from the v0.4.0
// client review, ported from the reviewer's repro tests
// (zz_repro[1-4]_test.go) into the real suite so each blocker stays caught.

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

// TestClientDrainFlagStuck pins blocker 1: after a drain-triggered parallel
// reconnect where the NEW conn wins the race (lands before the old one
// closes), drainReconnectScheduled must be cleared so the next, unrelated
// disconnect of the (now active) new conn still triggers a reconnect
// instead of silently reporting and doing nothing forever.
func TestClientDrainFlagStuck(t *testing.T) {
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
	defer cl.Close(context.Background())

	c1 := <-connCh
	// Keep one stream live so c1 does not close the instant it drains.
	if _, err := c1.OpenStream(context.Background()); err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	// Long deadline: c1 stays alive until the replacement lands and supersedes it.
	go func() { _ = c1.Drain(context.Background(), "rollout", DrainOptions{Deadline: 10 * time.Second}) }()

	c2 := <-connCh // the parallel reconnect
	for i := 0; i < 300 && onConnectCount.Load() < 2; i++ {
		time.Sleep(10 * time.Millisecond)
	}
	if onConnectCount.Load() < 2 {
		t.Fatal("reconnect never landed")
	}
	// Wait for the superseded c1 to actually be gone.
	select {
	case <-c1.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("old conn never closed")
	}
	time.Sleep(200 * time.Millisecond)

	// Now an ordinary, unrelated disconnect of the active connection.
	go func() { _ = c2.Close(uint32(ProtocolErrorCode), "unrelated failure") }()

	select {
	case <-connCh:
		// good: client reconnected
	case <-time.After(3 * time.Second):
		t.Fatalf("client never reconnected after an unrelated disconnect (state=%s)", cl.State())
	}
}

// TestClientCloseDuringDial pins should-fix 1: Close() while a dial is in
// flight must return promptly, not wait out the per-attempt ConnectTimeout.
func TestClientCloseDuringDial(t *testing.T) {
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
	rc := ReconnectOptions{Base: time.Millisecond, Cap: time.Millisecond, ConnectTimeout: 8 * time.Second}
	cl := NewClient("ws://"+ln.Addr().String()+"/tunnel", StaticToken("tok"), ClientConfig{Reconnect: rc})
	go func() { _ = cl.Connect(context.Background()) }()
	time.Sleep(300 * time.Millisecond)
	start := time.Now()
	_ = cl.Close(context.Background())
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("Close() took %v while a dial was in flight (ConnectTimeout=8s)", d)
	}
}

// TestClientCloseFromOnDisconnectAsync pins blocker 2: Close() called (from
// its own goroutine) inside OnDisconnect -- the natural "on fatal
// disconnect, hang up" user pattern -- must not deadlock against Close's own
// wg.Wait().
func TestClientCloseFromOnDisconnectAsync(t *testing.T) {
	_, url := startTestServer(t, testAcceptHandler{OnConn: func(c *Conn) {
		go func() { _ = c.Close(uint32(ProtocolErrorCode), "boom") }()
	}})
	fa := &fakeAfter{}
	done := make(chan struct{})
	var cl *Client
	cl = NewClient(url, StaticToken("tok"), ClientConfig{
		Reconnect: testReconnectOptions(fa, 0),
		OnDisconnect: func(DisconnectReason) {
			go func() {
				_ = cl.Close(context.Background())
				close(done)
			}()
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
		t.Fatal("Close() called from OnDisconnect never returned (deadlock)")
	}
}

// TestClientCloseFromOnDisconnectSync pins blocker 2's synchronous variant:
// calling Close() directly, inline, from OnDisconnect -- which runs on
// Client's own watchConn/reportAndSchedule goroutine -- must not deadlock.
func TestClientCloseFromOnDisconnectSync(t *testing.T) {
	_, url := startTestServer(t, testAcceptHandler{OnConn: func(c *Conn) {
		go func() { _ = c.Close(uint32(ProtocolErrorCode), "boom") }()
	}})
	fa := &fakeAfter{}
	done := make(chan struct{})
	var cl *Client
	cl = NewClient(url, StaticToken("tok"), ClientConfig{
		Reconnect: testReconnectOptions(fa, 0),
		OnDisconnect: func(DisconnectReason) {
			_ = cl.Close(context.Background()) // the natural thing a user writes
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
		t.Fatal("Close() called synchronously from OnDisconnect never returned (deadlock)")
	}
}

// TestClientCloseFromOnConnectSync pins blocker 2's OnConnect variant:
// calling Close() directly, inline, from OnConnect -- which runs on
// Client's own attemptLoop goroutine -- must not deadlock.
func TestClientCloseFromOnConnectSync(t *testing.T) {
	_, url := startTestServer(t, testAcceptHandler{})
	fa := &fakeAfter{}
	done := make(chan struct{})
	var cl *Client
	cl = NewClient(url, StaticToken("tok"), ClientConfig{
		Reconnect: testReconnectOptions(fa, 0),
		OnConnect: func(*Conn, *WelcomeMsg) {
			_ = cl.Close(context.Background())
			close(done)
		},
	})
	go func() { _ = cl.Connect(context.Background()) }()
	select {
	case <-done:
	case <-time.After(4 * time.Second):
		t.Fatal("Close() called synchronously from OnConnect never returned (deadlock)")
	}
}

// TestClientCloseAfterServerDrain pins blocker 3: Client.Close() after the
// server has already sent drain must still terminate promptly. Without the
// fix, conn.Drain() no-ops (the conn is already draining), so the client
// never sends its own drain{client_requested} and never closes the socket
// -- Close blocks on wg.Wait until the server's own drain deadline.
func TestClientCloseAfterServerDrain(t *testing.T) {
	connCh := make(chan *Conn, 4)
	_, url := startTestServer(t, testAcceptHandler{OnConn: func(c *Conn) { connCh <- c }})
	fa := &fakeAfter{}
	rc := testReconnectOptions(fa, 0)
	rc.Disabled = true // keep the drained conn as the active one
	drained := make(chan struct{})
	cl := NewClient(url, StaticToken("tok"), ClientConfig{
		Reconnect: rc,
		OnDrain:   func(*DrainMsg) { close(drained) },
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := cl.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	c1 := <-connCh
	if _, err := c1.OpenStream(context.Background()); err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	go func() { _ = c1.Drain(context.Background(), "rollout", DrainOptions{Deadline: 10 * time.Second}) }()
	<-drained
	time.Sleep(100 * time.Millisecond)

	done := make(chan struct{})
	go func() { _ = cl.Close(context.Background()); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Client.Close() did not return within 2s after a server drain")
	}
}
