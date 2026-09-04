//go:build conformance

package adapter

// Regression test for the write/close_write ordering race described in
// docs/CONFORMANCE.md section 1.1 and conformance/README.md: `write` acks
// asynchronously (a goroutine per call in the pre-fix adapter) while
// `close_write` used to run synchronously in the command-reading loop, so a
// close_write issued before an in-flight write's ack could send CLOSE --
// which jumps the writer loop's DATA rotation entirely (go/wsmixer's
// sched.go: "Control frames jump the DATA rotation entirely") -- before that
// write's chunk ever reached the wire, truncating the stream.
//
// This drives the adapter's own handleCommand exactly the way main()'s
// stdin-reading loop does: three "write" commands immediately followed by a
// "close_write", none of their acks awaited, all against a real go/wsmixer
// server<->client pair over a real TCP loopback connection. It asserts the
// peer stream receives every byte, in order, followed by a clean EOF.

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/mcpwarp/ws-mixer-go/wsmixer"
)

// testAcceptBackend implements ServerBackend directly on wsmixer.AcceptConn,
// with no HTTP-layer policy of its own -- the same wiring as
// cmd/conformance-adapter's acceptBackend, which this package deliberately
// does not export (docs/MIGRATION.md section 2.3: the real acceptBackend
// belongs in that thin main, not in this library). Duplicated here, byte-for
// -byte equivalent, so these whitebox tests of adapterState/handleCommand can
// drive a real listener without importing the cmd package -- the same
// approach docs/MIGRATION.md section 0.5 takes for wsmixer's own
// testAcceptHandler.
type testAcceptBackend struct{}

func (testAcceptBackend) Handler(cfg ServerConfig) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		bearer := testBearerToken(r.Header.Get("Authorization"))
		ws, err := websocket.Accept(w, r, &websocket.AcceptOptions{
			Subprotocols:    []string{wsmixer.Subprotocol},
			CompressionMode: websocket.CompressionDisabled,
		})
		if err != nil {
			return
		}
		defer ws.CloseNow()

		opts := cfg.Options
		opts.SetDefaults()
		ws.SetReadLimit(opts.ReadLimit)

		c, err := wsmixer.AcceptConn(r.Context(), ws, bearer, wsmixer.AcceptOptions{
			Options:      opts,
			Authenticate: cfg.Authenticate,
			Request:      r,
		})
		if err != nil {
			return
		}
		cfg.OnConn(c)
		c.Run()
		<-c.Done()
	})
}

// testBearerToken mirrors cmd/conformance-adapter's bearerToken (see
// testAcceptBackend).
func testBearerToken(header string) string {
	const prefix = "bearer "
	if len(header) <= len(prefix) || !strings.EqualFold(header[:len(prefix)], prefix) {
		return ""
	}
	return strings.TrimSpace(header[len(prefix):])
}

// captureEmittedEvents redirects the process's real stdout (fd 1, which the
// package-level `out` writer was bound to at package init) through a pipe
// for the duration of fn, via dup2 -- swapping the Go-level os.Stdout
// variable would not work here, since `out` captured the *os.File pointer
// at init and never looks at os.Stdout again, but a dup2'd fd 1 is seen by
// every write through that pointer regardless. It returns every JSON-lines
// event emit() wrote during fn, in the exact order they were written, so a
// test can assert not just that some ack/error happened but which of two
// concurrently-emitted events landed first.
func captureEmittedEvents(t *testing.T, fn func()) []map[string]any {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	savedFd, err := syscall.Dup(1)
	if err != nil {
		t.Fatalf("dup stdout: %v", err)
	}
	if err := syscall.Dup2(int(w.Fd()), 1); err != nil {
		t.Fatalf("dup2 stdout: %v", err)
	}

	eventsCh := make(chan []map[string]any, 1)
	go func() {
		var events []map[string]any
		sc := bufio.NewScanner(r)
		sc.Buffer(make([]byte, 0, 64*1024), 8<<20)
		for sc.Scan() {
			line := bytes.TrimSpace(sc.Bytes())
			if len(line) == 0 {
				continue
			}
			var e map[string]any
			if err := json.Unmarshal(line, &e); err == nil {
				events = append(events, e)
			}
		}
		eventsCh <- events
	}()

	fn()
	// emit() already flushes under outMu on every call; take the same lock
	// here too (rather than calling out.Flush() unguarded) so this doesn't
	// race with a concurrent emit() from a goroutine fn() kicked off but
	// didn't wait for (e.g. a worker's own cmdErr after a cancelled write).
	outMu.Lock()
	out.Flush()
	outMu.Unlock()
	w.Close()
	if err := syscall.Dup2(savedFd, 1); err != nil {
		t.Fatalf("restore stdout: %v", err)
	}
	syscall.Close(savedFd)
	return <-eventsCh
}

func TestWriteCloseWriteOrderingRace(t *testing.T) {
	st := newState(testAcceptBackend{})

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	listener := testAcceptBackend{}.Handler(ServerConfig{
		Options: st.options(),
		Authenticate: func(_ context.Context, h *wsmixer.Hello) (wsmixer.WelcomeMeta, error) {
			return wsmixer.WelcomeMeta{}, nil
		},
		OnConn: func(c *wsmixer.Conn) {
			st.setConn(c)
		},
	})
	mux := http.NewServeMux()
	mux.Handle("/v1/tunnel", listener)
	srv := &http.Server{Handler: mux}
	go func() { _ = srv.Serve(ln) }()
	defer srv.Close()

	streamOpened := make(chan *wsmixer.Stream, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	clientConn, err := wsmixer.Dial(ctx, "ws://"+ln.Addr().String()+"/v1/tunnel", wsmixer.ClientOptions{
		Options: st.options(),
		Token:   "race-test-token",
		Agent:   wsmixer.AgentInfo{SDK: "race-test", SDKVersion: "0.0.0"},
		OnStream: func(s *wsmixer.Stream) {
			streamOpened <- s
		},
	})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer clientConn.Close(0, "")

	// Wait for the server-role Conn to be recorded (set in OnConn, which
	// runs on the accept goroutine concurrently with Dial's return).
	deadline := time.Now().Add(5 * time.Second)
	var srvConn *wsmixer.Conn
	for {
		if srvConn = st.getConn(); srvConn != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("server-side Conn never appeared")
		}
		time.Sleep(time.Millisecond)
	}

	s, err := srvConn.OpenStream(ctx)
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	st.storeStream(s)

	var peer *wsmixer.Stream
	select {
	case peer = <-streamOpened:
	case <-time.After(5 * time.Second):
		t.Fatal("client never saw OnStream")
	}

	// The race: three "write" commands immediately followed by
	// "close_write", exactly as main()'s stdin loop would dispatch them
	// line-by-line, without waiting for any of the writes' "ack" events.
	chunks := [][]byte{[]byte("AAAA"), []byte("BBBB"), []byte("CCCC")}
	want := append(append(append([]byte{}, chunks[0]...), chunks[1]...), chunks[2]...)
	for i, chunk := range chunks {
		handleCommand(st, "write", float64(100+i), map[string]any{
			"cmd": "write", "seq": float64(100 + i),
			"id": float64(s.ID()), "data_b64": base64.StdEncoding.EncodeToString(chunk),
		})
	}
	handleCommand(st, "close_write", 200, map[string]any{
		"cmd": "close_write", "seq": float64(200), "id": float64(s.ID()),
	})

	// Read from the peer until EOF, with an overall deadline so a
	// regression (truncated read, or a hang) fails the test instead of
	// blocking forever.
	done := make(chan struct{})
	var got []byte
	var readErr error
	go func() {
		defer close(done)
		buf := make([]byte, 64)
		for {
			n, err := peer.Read(buf)
			if n > 0 {
				got = append(got, buf[:n]...)
			}
			if err != nil {
				readErr = err
				return
			}
		}
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out reading from peer stream; got %d/%d bytes: %q", len(got), len(want), got)
	}

	if readErr != io.EOF {
		t.Fatalf("peer read ended with %v, want io.EOF", readErr)
	}
	if string(got) != string(want) {
		t.Fatalf("peer received %q, want %q (truncated write/close_write race)", got, want)
	}
}

// settleGoroutines polls runtime.NumGoroutine until it stops changing (or a
// deadline passes), to give background goroutines (worker exits, conn
// teardown) a chance to actually finish before comparing counts -- a bare
// before/after snapshot is flaky since none of this is synchronous from the
// test's point of view.
func settleGoroutines(t *testing.T) int {
	t.Helper()
	var last int
	stable := 0
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		n := runtime.NumGoroutine()
		if n == last {
			stable++
			if stable >= 5 {
				return n
			}
		} else {
			stable = 0
			last = n
		}
		time.Sleep(20 * time.Millisecond)
	}
	return last
}

// TestResetUnblocksBlockedWrite is the regression test for RESET's
// out-of-band handling (OVERVIEW.md section 2.5: RESET is abortive -- it
// discards buffered data and unblocks writers). It exhausts the peer's
// receive window so a queued "write" blocks indefinitely inside
// WriteContext (waiting for send credit that will never arrive), then
// issues "reset" for the same stream id and asserts:
//
//   - reset returns promptly instead of waiting behind the blocked write;
//   - the blocked write's own ack (seq 300) is an *error* ack whose message
//     reflects a cancellation, not a successful write racing in on late
//     credit -- and it is captured (via captureEmittedEvents, which redirects
//     the adapter's real stdout) at or before reset's own ack (seq 301) in
//     emission order, i.e. the runner never sees reset "complete" before it
//     has already been told the write it unblocked failed;
//   - the peer observes the RESET itself, having read nothing from the
//     stream before "reset" was even dispatched (so no credit was returned
//     to the sender ahead of time -- the write can only have unblocked via
//     RESET's cancellation, never via fresh credit);
//   - the stream's FIFO worker and bookkeeping are torn down (queue
//     drained, not left running against an already-reset stream), and no
//     goroutine leaks once things settle.
//
// Mutation coverage (verified by hand in a scratch copy of main.go, not
// re-run by `go test`): routing "reset" back through the per-stream FIFO
// instead of resetStream's out-of-band path makes this test fail because
// reset itself then queues behind the still-blocked write and neither ever
// completes within the capture window -- the test fails at "never observed
// an ack/error for the blocked write (seq 300)", not the reset-latency
// check. Changing the write job from s.WriteContext(ctx, data) to
// s.Write(data) (dropping the cancelable context) doesn't stop the write
// from unblocking -- Stream.Reset still closes notifyCh, which wakes
// reserveSendCredit regardless of ctx -- but the write's error then reflects
// s.err (a *StreamError formatted as "... error CANCEL: abort") instead of
// ctx.Err()'s "context canceled", so it fails the cancel-reason check below
// instead.
func TestResetUnblocksBlockedWrite(t *testing.T) {
	st := newState(testAcceptBackend{})
	st.window = 16384 // go/wsmixer's floor for hello.window (control.go's
	// windowMin validation rejects anything smaller); a write of several
	// times this size will exhaust it and block on credit after its first
	// chunk, since the peer never reads (no credit is ever returned).

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	listener := testAcceptBackend{}.Handler(ServerConfig{
		Options: st.options(),
		Authenticate: func(_ context.Context, h *wsmixer.Hello) (wsmixer.WelcomeMeta, error) {
			return wsmixer.WelcomeMeta{}, nil
		},
		OnConn: func(c *wsmixer.Conn) {
			st.setConn(c)
		},
	})
	mux := http.NewServeMux()
	mux.Handle("/v1/tunnel", listener)
	srv := &http.Server{Handler: mux}
	go func() { _ = srv.Serve(ln) }()
	defer srv.Close()

	streamOpened := make(chan *wsmixer.Stream, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	clientConn, err := wsmixer.Dial(ctx, "ws://"+ln.Addr().String()+"/v1/tunnel", wsmixer.ClientOptions{
		Options: st.options(),
		Token:   "race-test-token",
		Agent:   wsmixer.AgentInfo{SDK: "race-test", SDKVersion: "0.0.0"},
		OnStream: func(s *wsmixer.Stream) {
			streamOpened <- s
		},
	})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer clientConn.Close(0, "")

	deadline := time.Now().Add(5 * time.Second)
	var srvConn *wsmixer.Conn
	for {
		if srvConn = st.getConn(); srvConn != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("server-side Conn never appeared")
		}
		time.Sleep(time.Millisecond)
	}

	// Baseline is taken once the connection itself (client+server Conn
	// pairs, each with its own steady-state reader/writer/ping/watchdog/
	// delivery goroutines) is fully up, so it doesn't misattribute those
	// long-lived, per-connection (not per-stream) goroutines to a leak.
	baseline := settleGoroutines(t)

	s, err := srvConn.OpenStream(ctx)
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	st.storeStream(s)

	var peer *wsmixer.Stream
	select {
	case peer = <-streamOpened:
	case <-time.After(5 * time.Second):
		t.Fatal("client never saw OnStream")
	}

	// Nothing reads from peer at all until well after "reset" has been
	// dispatched below -- no credit is ever returned to the sender ahead of
	// time, so the blocked write below can only unblock via RESET's
	// cancellation, never via fresh credit arriving first.

	// Kick off a write far larger than the window; nothing ever reads from
	// peer, so no credit is returned and the write blocks inside
	// WriteContext once it exhausts its initial 4 bytes of credit.
	// Everything from here through "reset" settling is captured so the
	// relative order and content of the write's and reset's acks can be
	// asserted, not just inferred from side effects.
	payload := make([]byte, 5*16384)
	var resetElapsed time.Duration
	events := captureEmittedEvents(t, func() {
		handleCommand(st, "write", 300, map[string]any{
			"cmd": "write", "seq": float64(300),
			"id": float64(s.ID()), "data_b64": base64.StdEncoding.EncodeToString(payload),
		})

		// Give the write a moment to actually reach the blocked state on
		// the worker goroutine before firing the reset -- otherwise a fast
		// reset could race ahead of the write even having started, which
		// wouldn't exercise the "unblock a blocked write" path at all.
		time.Sleep(200 * time.Millisecond)

		resetStart := time.Now()
		handleCommand(st, "reset", 301, map[string]any{
			"cmd": "reset", "seq": float64(301), "id": float64(s.ID()),
			"code": float64(wsmixer.CancelCode), "message": "abort",
		})
		resetElapsed = time.Since(resetStart)

		// Give the worker goroutine, unblocked by reset's context
		// cancellation, a moment to actually run its own cmdErr for seq
		// 300 before the capture window closes.
		time.Sleep(200 * time.Millisecond)
	})
	if resetElapsed > time.Second {
		t.Fatalf("reset took %v; want it to return promptly instead of waiting behind the blocked write", resetElapsed)
	}

	var writeAckIdx, resetAckIdx = -1, -1
	var writeEvent, resetEvent map[string]any
	for i, e := range events {
		seq, _ := e["seq"].(float64)
		switch seq {
		case 300:
			if writeAckIdx == -1 {
				writeAckIdx, writeEvent = i, e
			}
		case 301:
			if resetAckIdx == -1 {
				resetAckIdx, resetEvent = i, e
			}
		}
	}
	if writeEvent == nil {
		t.Fatalf("never observed an ack/error for the blocked write (seq 300); events: %v", events)
	}
	if resetEvent == nil {
		t.Fatalf("never observed an ack/error for reset (seq 301); events: %v", events)
	}
	if writeEvent["event"] != "error" {
		t.Fatalf("blocked write (seq 300) got event %v, want \"error\" -- a successful ack here means it was NOT actually cancelled by RESET", writeEvent["event"])
	}
	if msg, _ := writeEvent["message"].(string); !strings.Contains(strings.ToLower(msg), "context canceled") && !strings.Contains(strings.ToLower(msg), "cancel") {
		t.Fatalf("blocked write's error message = %q, want it to reflect RESET's cancellation", msg)
	}
	if resetEvent["event"] != "ack" {
		t.Fatalf("reset (seq 301) got event %v, want \"ack\"", resetEvent["event"])
	}
	if writeAckIdx > resetAckIdx {
		t.Fatalf("blocked write's error ack (seq 300, event #%d) arrived after reset's ack (seq 301, event #%d); want it at or before", writeAckIdx, resetAckIdx)
	}

	// The peer must observe the RESET -- draining whatever DATA already
	// reached the wire before the reset first (the first ~16KiB chunk
	// legitimately got sent before send credit ran out). This is the
	// stream's very first Read call: it has not read (and so has not
	// returned any credit) before this point.
	buf := make([]byte, 4096)
	var readErr error
	readDeadline := time.Now().Add(5 * time.Second)
	for {
		_, readErr = peer.Read(buf)
		if readErr != nil || time.Now().After(readDeadline) {
			break
		}
	}
	se, ok := readErr.(*wsmixer.StreamError)
	if !ok {
		t.Fatalf("peer.Read error = %v (%T), want *wsmixer.StreamError", readErr, readErr)
	}
	if se.Code != wsmixer.CancelCode {
		t.Fatalf("peer saw RESET code %v, want %v", se.Code, wsmixer.CancelCode)
	}

	// The stream's bookkeeping (and its FIFO worker) must be gone: the
	// queue was drained rather than left running against an already-reset
	// stream.
	st.mu.Lock()
	_, workerStillTracked := st.streamWorkers[s.ID()]
	_, streamStillTracked := st.streams[s.ID()]
	st.mu.Unlock()
	if workerStillTracked {
		t.Fatal("stream worker still tracked after reset; want it torn down")
	}
	if streamStillTracked {
		t.Fatal("stream still tracked after reset; want it torn down")
	}

	after := settleGoroutines(t)
	if after > baseline+2 { // small slack for test-runtime goroutines unrelated to this stream
		t.Fatalf("goroutine count grew from %d to %d after reset; suspect a leaked worker", baseline, after)
	}
}

// TestNoWorkerLeakAfterManyOpenCloseCycles opens, writes, and gracefully
// closes 50 streams in sequence and asserts none of their FIFO worker
// goroutines (or their streams/streamWorkers bookkeeping) survive the
// stream reaching closed -- see teardownStream's call sites.
func TestNoWorkerLeakAfterManyOpenCloseCycles(t *testing.T) {
	st := newState(testAcceptBackend{})

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	listener := testAcceptBackend{}.Handler(ServerConfig{
		Options: st.options(),
		Authenticate: func(_ context.Context, h *wsmixer.Hello) (wsmixer.WelcomeMeta, error) {
			return wsmixer.WelcomeMeta{}, nil
		},
		OnConn: func(c *wsmixer.Conn) {
			st.setConn(c)
		},
	})
	mux := http.NewServeMux()
	mux.Handle("/v1/tunnel", listener)
	srv := &http.Server{Handler: mux}
	go func() { _ = srv.Serve(ln) }()
	defer srv.Close()

	streamOpened := make(chan *wsmixer.Stream, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	clientConn, err := wsmixer.Dial(ctx, "ws://"+ln.Addr().String()+"/v1/tunnel", wsmixer.ClientOptions{
		Options: st.options(),
		Token:   "race-test-token",
		Agent:   wsmixer.AgentInfo{SDK: "race-test", SDKVersion: "0.0.0"},
		OnStream: func(s *wsmixer.Stream) {
			streamOpened <- s
		},
	})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer clientConn.Close(0, "")

	deadline := time.Now().Add(5 * time.Second)
	var srvConn *wsmixer.Conn
	for {
		if srvConn = st.getConn(); srvConn != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("server-side Conn never appeared")
		}
		time.Sleep(time.Millisecond)
	}

	// Baseline is taken once the connection itself is fully up (see the
	// identical comment in TestResetUnblocksBlockedWrite), so the
	// connection's own steady-state goroutines aren't misattributed to a
	// per-stream leak.
	baseline := settleGoroutines(t)

	const cycles = 50
	for i := 0; i < cycles; i++ {
		s, err := srvConn.OpenStream(ctx)
		if err != nil {
			t.Fatalf("cycle %d: OpenStream: %v", i, err)
		}
		st.storeStream(s)
		go autoRead(st, s) // mirrors the "open_stream" command handler

		var peer *wsmixer.Stream
		select {
		case peer = <-streamOpened:
		case <-time.After(5 * time.Second):
			t.Fatalf("cycle %d: client never saw OnStream", i)
		}

		handleCommand(st, "write", float64(1000+2*i), map[string]any{
			"cmd": "write", "seq": float64(1000 + 2*i),
			"id": float64(s.ID()), "data_b64": base64.StdEncoding.EncodeToString([]byte("hi")),
		})
		handleCommand(st, "close_write", float64(1001+2*i), map[string]any{
			"cmd": "close_write", "seq": float64(1001 + 2*i), "id": float64(s.ID()),
		})

		// Drain the peer to EOF (server's CLOSE), then close the peer's own
		// write side so the server's autoRead sees EOF too -- only then are
		// both directions closed and teardownStream fires.
		buf := make([]byte, 64)
		for {
			_, rerr := peer.Read(buf)
			if rerr != nil {
				if rerr != io.EOF {
					t.Fatalf("cycle %d: peer read error %v, want io.EOF", i, rerr)
				}
				break
			}
		}
		if err := peer.CloseWrite(); err != nil {
			t.Fatalf("cycle %d: peer.CloseWrite: %v", i, err)
		}

		// Wait for the server side to observe the full close and tear the
		// stream down before starting the next cycle.
		teardownDeadline := time.Now().Add(5 * time.Second)
		for {
			st.mu.Lock()
			_, tracked := st.streams[s.ID()]
			st.mu.Unlock()
			if !tracked {
				break
			}
			if time.Now().After(teardownDeadline) {
				t.Fatalf("cycle %d: stream %d never torn down", i, s.ID())
			}
			time.Sleep(time.Millisecond)
		}
	}

	st.mu.Lock()
	remainingWorkers := len(st.streamWorkers)
	remainingStreams := len(st.streams)
	st.mu.Unlock()
	if remainingWorkers != 0 {
		t.Fatalf("streamWorkers not empty after %d cycles: %d entries remain", cycles, remainingWorkers)
	}
	if remainingStreams != 0 {
		t.Fatalf("streams not empty after %d cycles: %d entries remain", cycles, remainingStreams)
	}

	after := settleGoroutines(t)
	if after > baseline+2 { // small slack for test-runtime goroutines unrelated to these streams
		t.Fatalf("goroutine count grew from %d to %d after %d open/close cycles; suspect a leaked per-stream worker", baseline, after, cycles)
	}
}

// newTestConn spins up a real go/wsmixer server<->client pair over TCP
// loopback (identical wiring to the tests above) and returns st (with the
// server-side Conn already recorded), the server-side Conn itself, and a
// channel of client-side streams as they're opened -- the shared setup the
// remaining tests in this file build on.
func newTestConn(t *testing.T, configure func(*adapterState)) (st *adapterState, srvConn *wsmixer.Conn, streamOpened chan *wsmixer.Stream) {
	t.Helper()
	st = newState(testAcceptBackend{})
	if configure != nil {
		configure(st)
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { ln.Close() })

	listener := testAcceptBackend{}.Handler(ServerConfig{
		Options: st.options(),
		Authenticate: func(_ context.Context, h *wsmixer.Hello) (wsmixer.WelcomeMeta, error) {
			return wsmixer.WelcomeMeta{}, nil
		},
		OnConn: func(c *wsmixer.Conn) { st.setConn(c) },
	})
	mux := http.NewServeMux()
	mux.Handle("/v1/tunnel", listener)
	srv := &http.Server{Handler: mux}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { srv.Close() })

	streamOpened = make(chan *wsmixer.Stream, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	clientConn, err := wsmixer.Dial(ctx, "ws://"+ln.Addr().String()+"/v1/tunnel", wsmixer.ClientOptions{
		Options: st.options(),
		Token:   "race-test-token",
		Agent:   wsmixer.AgentInfo{SDK: "race-test", SDKVersion: "0.0.0"},
		OnStream: func(s *wsmixer.Stream) {
			streamOpened <- s
		},
	})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { clientConn.Close(0, "") })

	deadline := time.Now().Add(5 * time.Second)
	for {
		if srvConn = st.getConn(); srvConn != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("server-side Conn never appeared")
		}
		time.Sleep(time.Millisecond)
	}
	return st, srvConn, streamOpened
}

// TestEnqueueOnStreamQueueFull exercises enqueueOnStream's bounded FIFO:
// once a worker is stalled behind a job that never returns and its channel
// (cap streamQueueCap) has filled up behind it, the next enqueue must not
// block waiting for room -- it must fail fast with a "queue_full" error ack
// instead.
func TestEnqueueOnStreamQueueFull(t *testing.T) {
	st, srvConn, streamOpened := newTestConn(t, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s, err := srvConn.OpenStream(ctx)
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	st.storeStream(s)
	select {
	case <-streamOpened:
	case <-time.After(5 * time.Second):
		t.Fatal("client never saw OnStream")
	}
	defer st.teardownStream(s.ID())

	block := make(chan struct{})
	defer close(block)
	noopJob := func(*wsmixer.Stream) func(context.Context) { return func(context.Context) {} }
	blockingJob := func(*wsmixer.Stream) func(context.Context) {
		return func(context.Context) { <-block }
	}

	// The first job occupies the worker until block is closed, so every
	// subsequent enqueue just piles up in the channel behind it.
	if ok := st.enqueueOnStream(s.ID(), 9000, blockingJob); !ok {
		t.Fatal("enqueueOnStream(9000): stream not found")
	}
	// Give the worker goroutine a moment to actually pick up and start
	// running the blocking job, so it isn't still sitting in the channel
	// occupying one of the queue's slots.
	time.Sleep(50 * time.Millisecond)

	// Fill the queue to exactly streamQueueCap no-op jobs behind the
	// blocked worker, then push one more: only that last one should be
	// rejected.
	var events []map[string]any
	events = captureEmittedEvents(t, func() {
		for i := 0; i < streamQueueCap+1; i++ {
			st.enqueueOnStream(s.ID(), float64(9001+i), noopJob)
		}
	})
	if len(events) != 1 {
		t.Fatalf("got %d events for %d enqueues (1 blocked + %d queued + 1 overflow), want exactly 1 (the overflow's queue_full error): %v", len(events), streamQueueCap+2, streamQueueCap, events)
	}
	if events[0]["event"] != "error" {
		t.Fatalf("overflow event = %v, want event \"error\"", events[0])
	}
	if msg, _ := events[0]["message"].(string); !strings.Contains(msg, "queue_full") {
		t.Fatalf("overflow message = %q, want it to mention queue_full", msg)
	}
	if seq, _ := events[0]["seq"].(float64); seq != float64(9001+streamQueueCap) {
		t.Fatalf("queue_full error reported seq %v, want %v", seq, 9001+streamQueueCap)
	}
}

// TestWriteAfterTeardownGetsCleanErrorAck asserts the ordinary (non-racy)
// half of the atomicity fix in enqueueOnStream: once a stream has been torn
// down (closed both ways, reset, or the connection failing), a "write"
// command that arrives afterward for that id must get a clean "no such
// stream" error ack -- not an orphan worker, not a silently dropped
// command, not a panic.
func TestWriteAfterTeardownGetsCleanErrorAck(t *testing.T) {
	st, srvConn, streamOpened := newTestConn(t, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s, err := srvConn.OpenStream(ctx)
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	st.storeStream(s)
	select {
	case <-streamOpened:
	case <-time.After(5 * time.Second):
		t.Fatal("client never saw OnStream")
	}

	st.teardownStream(s.ID())

	events := captureEmittedEvents(t, func() {
		handleCommand(st, "write", 9500, map[string]any{
			"cmd": "write", "seq": float64(9500),
			"id": float64(s.ID()), "data_b64": base64.StdEncoding.EncodeToString([]byte("late")),
		})
	})
	if len(events) != 1 || events[0]["event"] != "error" {
		t.Fatalf("late write got events %v, want a single clean error ack", events)
	}
	if msg, _ := events[0]["message"].(string); !strings.Contains(msg, "no such stream") {
		t.Fatalf("late write error message = %q, want it to say no such stream", msg)
	}

	st.mu.Lock()
	_, streamLeft := st.streams[s.ID()]
	_, workerLeft := st.streamWorkers[s.ID()]
	st.mu.Unlock()
	if streamLeft || workerLeft {
		t.Fatalf("late write after teardown left bookkeeping behind: stream tracked=%v worker tracked=%v", streamLeft, workerLeft)
	}
}

// TestConcurrentTeardownVsWriteRace stresses the atomicity fix in
// enqueueOnStream: a "write" command's stream lookup and its enqueue onto
// (or creation of) that stream's FIFO worker happen in the very same
// critical section (s.mu) that teardownStream uses to remove a stream.
// Without that, a lookup that found the stream just before a concurrent
// teardownStream (as run here, and as autoRead/watchDisconnect do for
// real) removed it could still go on to create a brand new worker for an id
// nothing will ever tear down again -- an orphan entry in streamWorkers
// (and, per its field comment, a leaked goroutine) with no matching entry
// in streams. It runs 200 iterations, each pitting a real "write" command
// against a concurrent teardownStream for the same freshly opened stream
// under -race, and asserts that once both finish, neither streams nor
// streamWorkers still has an entry for that id.
func TestConcurrentTeardownVsWriteRace(t *testing.T) {
	const iterations = 200
	// teardownStream only clears the adapter's own bookkeeping -- it never
	// resets/closes the underlying wsmixer.Stream, so each iteration's
	// stream stays live (and counted) at the protocol level for the rest
	// of the test; maxStreams must cover all of them or later OpenStream
	// calls block on the server's own stream-limit enforcement.
	st, srvConn, streamOpened := newTestConn(t, func(st *adapterState) {
		st.maxStreams = iterations + 16
	})

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	for i := 0; i < iterations; i++ {
		s, err := srvConn.OpenStream(ctx)
		if err != nil {
			t.Fatalf("iteration %d: OpenStream: %v", i, err)
		}
		st.storeStream(s)
		select {
		case <-streamOpened:
		case <-time.After(5 * time.Second):
			t.Fatalf("iteration %d: client never saw OnStream", i)
		}

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			st.teardownStream(s.ID())
		}()
		go func(seq float64) {
			defer wg.Done()
			handleCommand(st, "write", seq, map[string]any{
				"cmd": "write", "seq": seq,
				"id": float64(s.ID()), "data_b64": base64.StdEncoding.EncodeToString([]byte("x")),
			})
		}(float64(20000 + i))
		wg.Wait()

		st.mu.Lock()
		_, streamLeft := st.streams[s.ID()]
		_, workerLeft := st.streamWorkers[s.ID()]
		st.mu.Unlock()
		if streamLeft {
			t.Fatalf("iteration %d: stream %d still tracked after concurrent teardown+write", i, s.ID())
		}
		if workerLeft {
			t.Fatalf("iteration %d: worker %d still tracked after concurrent teardown+write", i, s.ID())
		}
	}
}
