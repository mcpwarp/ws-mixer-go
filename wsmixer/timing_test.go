package wsmixer

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"runtime"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// dialRaw completes a bare ws-mixer.v1 WebSocket upgrade (offering the
// subprotocol and a well-formed, if meaningless, bearer token so the
// pre-upgrade checks pass) without ever sending a hello frame, so tests can
// exercise server-side handshake timeouts.
func dialRaw(ctx context.Context, url string) (*websocket.Conn, error) {
	c, _, err := websocket.Dial(ctx, url, &websocket.DialOptions{
		Subprotocols:    []string{Subprotocol},
		CompressionMode: websocket.CompressionDisabled,
		HTTPHeader:      http.Header{"Authorization": []string{"Bearer raw-dial-no-hello"}},
	})
	return c, err
}

// These tests cover the three sequence fixtures that depend on wall-clock
// timeouts (hello_timeout.json, ping_pong_then_dead_peer_timeout.json,
// drain_with_inflight_timeout.json) -- the only fixtures under
// spec/fixtures/sequences/ with a wait_ms step. sequence_test.go's generic
// harness deliberately skips these three; per this task's explicit
// allowance, they are covered here behaviorally with short, scaled real
// Options instead of literally replaying the fixtures' wait_ms values (which
// would cost the suite a combined ~2 real minutes).

// TestHelloTimeout mirrors hello_timeout.json: no hello arrives within
// HelloTimeout of the WebSocket accept -> error{PROTOCOL_ERROR} + close 4001.
func TestHelloTimeout(t *testing.T) {
	ln := NewListener(ServerOptions{
		Options: Options{HelloTimeout: 100 * time.Millisecond, Logger: discardLogger()},
	})
	srv := httptest.NewServer(ln)
	defer srv.Close()

	// A raw dial with no hello ever sent.
	url := "ws" + srv.URL[len("http"):] + "/tunnel"
	rawDialAndWaitForClose(t, url)
}

// TestHelloTimeoutNearSimultaneous drives performServerHandshake directly
// against a fake transport many times with a very short HelloTimeout and the
// hello frame delivered right around the deadline (sometimes just before,
// sometimes just after), so the timeout callback and the post-Read success
// path in performServerHandshake are racing for real. Run with -race, this
// catches a callback that writes error{}/closes the socket concurrently with
// (or after) the handshake having already proceeded toward welcome: exactly
// one of the two outcomes must win, and the wire must carry only the frame(s)
// that outcome implies.
func TestHelloTimeoutNearSimultaneous(t *testing.T) {
	for i := 0; i < 200; i++ {
		fake := newFakeWS()
		opts := Options{
			Window: 262144, MaxStreams: 64,
			PingInterval: time.Hour, PingTimeout: 2 * time.Hour,
			HelloTimeout: time.Millisecond,
			Logger:       discardLogger(), Metrics: NoopMetrics{},
		}
		c := newConn(fake, RoleServer, opts)

		hb, err := json.Marshal(syntheticHello())
		if err != nil {
			t.Fatalf("marshaling synthetic hello: %v", err)
		}
		go func(i int) {
			// Jitter straddling the 1ms deadline: some iterations land the
			// hello before it fires, some after.
			time.Sleep(time.Duration(i%5) * 250 * time.Microsecond)
			fake.feedInbound(EncodeData(0, hb))
		}(i)

		hsErr := performServerHandshake(context.Background(), c, harnessHelloToken, serverHandshakeOptions{
			Options: opts, Authenticate: harnessAuthenticate,
		})

		select {
		case b := <-fake.outbound:
			frame, ferr := DecodeFrame(b)
			if ferr != nil || frame.StreamID != 0 {
				t.Fatalf("iteration %d: unexpected non-control frame: %v (err=%v)", i, frame, ferr)
			}
			msg, perr := ParseControl(frame.Payload)
			if perr != nil {
				t.Fatalf("iteration %d: parsing control payload: %v", i, perr)
			}
			switch msg.(type) {
			case *WelcomeMsg:
				if hsErr != nil {
					t.Fatalf("iteration %d: got welcome{} on the wire but performServerHandshake returned %v", i, hsErr)
				}
			case *ErrorMsg:
				if hsErr == nil {
					t.Fatalf("iteration %d: got error{} on the wire but performServerHandshake returned nil", i)
				}
			default:
				t.Fatalf("iteration %d: unexpected control message %T", i, msg)
			}
			// The two outcomes are mutually exclusive: no second frame
			// (e.g. a welcome after an error, or vice versa) may follow. Give
			// c.fail's asynchronous close-path goroutine a moment to run
			// before checking, so this isn't just racing to peek before it
			// would write a second frame.
			time.Sleep(5 * time.Millisecond)
			select {
			case b2 := <-fake.outbound:
				t.Fatalf("iteration %d: unexpected extra frame on the wire: %x", i, b2)
			default:
			}
		case <-time.After(time.Second):
			t.Fatalf("iteration %d: no frame written to the wire", i)
		}

		fake.CloseNow()
	}
}

// TestKeepaliveTimeout mirrors ping_pong_then_dead_peer_timeout.json: a peer
// that stops answering ping is disconnected with KEEPALIVE_TIMEOUT once
// elapsed time since the last pong exceeds PingTimeout.
func TestKeepaliveTimeout(t *testing.T) {
	// Drives *Conn directly against a fake transport (as sequence_test.go
	// does): the peer never sends a pong, so the watchdog is the only thing
	// that can end the connection -- a real dialed client would always
	// auto-reply to ping regardless of its own PingInterval/PingTimeout,
	// since that reply is unconditional protocol behavior, not a policy this
	// test can disable from the outside.
	fake := newFakeWS()
	opts := Options{
		Window: 262144, MaxStreams: 64,
		PingInterval: 20 * time.Millisecond, PingTimeout: 60 * time.Millisecond,
		Logger: discardLogger(), Metrics: NoopMetrics{},
	}
	c := newConn(fake, RoleServer, opts)
	c.session = "test"
	c.ourWindow = opts.Window
	c.peerWindow = opts.Window
	c.maxStreams = opts.MaxStreams
	c.pingInterval = opts.PingInterval
	c.pingTimeout = opts.PingTimeout
	c.finishHandshake()
	c.run()
	defer fake.Close(0, "")

	select {
	case <-c.closed:
	case <-time.After(2 * time.Second):
		t.Fatal("connection never closed on keepalive timeout")
	}
	ce, ok := c.Err().(*ConnError)
	if !ok {
		t.Fatalf("expected *ConnError, got %#v", c.Err())
	}
	if ce.Code != KeepaliveTimeout {
		t.Errorf("error code = %s, want KEEPALIVE_TIMEOUT", ce.Code)
	}
	if ce.CloseCode() != 4013 {
		t.Errorf("close code = %d, want 4013", ce.CloseCode())
	}
}

// TestClientKeepaliveTimeout mirrors client_dead_peer_timeout.json (skipped
// from sequence_test.go's generic harness alongside the other wait_ms
// fixtures): the CLIENT side's own watchdog fires when the server never
// answers any of its pings.
func TestClientKeepaliveTimeout(t *testing.T) {
	fake := newFakeWS()
	opts := Options{
		Window: 262144, MaxStreams: 64,
		PingInterval: 20 * time.Millisecond, PingTimeout: 60 * time.Millisecond,
		Logger: discardLogger(), Metrics: NoopMetrics{},
	}
	c := newConn(fake, RoleClient, opts)
	c.session = "test"
	c.ourWindow = opts.Window
	c.peerWindow = opts.Window
	c.maxStreams = opts.MaxStreams
	c.pingInterval = opts.PingInterval
	c.pingTimeout = opts.PingTimeout
	c.finishHandshake()
	c.run()
	defer fake.Close(0, "")

	select {
	case <-c.closed:
	case <-time.After(2 * time.Second):
		t.Fatal("connection never closed on keepalive timeout")
	}
	ce, ok := c.Err().(*ConnError)
	if !ok {
		t.Fatalf("expected *ConnError, got %#v", c.Err())
	}
	if ce.Code != KeepaliveTimeout {
		t.Errorf("error code = %s, want KEEPALIVE_TIMEOUT", ce.Code)
	}
	if ce.CloseCode() != 4013 {
		t.Errorf("close code = %d, want 4013", ce.CloseCode())
	}
}

// TestDrainWithInflightTimeout mirrors drain_with_inflight_timeout.json: a
// stream still open at the drain deadline is RESET(CANCEL), then the
// connection closes with GOING_AWAY/4012.
func TestDrainWithInflightTimeout(t *testing.T) {
	connCh := make(chan *Conn, 1)
	ln := NewListener(ServerOptions{OnConn: func(c *Conn) { connCh <- c }})
	srv := httptest.NewServer(ln)
	defer srv.Close()
	url := "ws" + srv.URL[len("http"):] + "/tunnel"

	streamCh := make(chan *Stream, 1)
	client := dialTestClient(t, url, ClientOptions{OnStream: func(st *Stream) { streamCh <- st }})
	_ = client

	var serverConn *Conn
	select {
	case serverConn = <-connCh:
	case <-time.After(2 * time.Second):
		t.Fatal("no server connection observed")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	st, err := serverConn.OpenStream(ctx)
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	var clientStream *Stream
	select {
	case clientStream = <-streamCh:
	case <-time.After(2 * time.Second):
		t.Fatal("client never observed the opened stream")
	}
	_ = clientStream // left open deliberately: this is the survivor

	drainErrCh := make(chan error, 1)
	go func() {
		drainErrCh <- serverConn.Drain(context.Background(), "rollout", DrainOptions{Deadline: 100 * time.Millisecond})
	}()

	select {
	case <-drainErrCh:
	case <-time.After(2 * time.Second):
		t.Fatal("Drain never returned")
	}

	if st.LastError() == nil {
		t.Error("in-flight stream was not reset by the drain deadline")
	} else if se, ok := st.LastError().(*StreamError); !ok || se.Code != CancelCode {
		t.Errorf("stream error = %v, want StreamError{CancelCode}", st.LastError())
	}

	ce, ok := serverConn.Err().(*ConnError)
	if !ok {
		t.Fatalf("expected *ConnError after drain, got %#v", serverConn.Err())
	}
	if ce.Code != GoingAwayCode || ce.CloseCode() != 4012 {
		t.Errorf("drain close = %s/%d, want GOING_AWAY/4012", ce.Code, ce.CloseCode())
	}
}

// TestStreamIDExhaustion checks that OpenStream refuses to hand out an id
// past the 31-bit space, instead sends drain{reason:"id_exhausted"} and
// closes, and leaves no goroutine behind, per OVERVIEW.md section 2.5.
func TestStreamIDExhaustion(t *testing.T) {
	before := runtime.NumGoroutine()

	fake := newFakeWS()
	opts := Options{Window: 262144, MaxStreams: 1 << 20, Logger: discardLogger(), Metrics: NoopMetrics{}}
	c := newConn(fake, RoleServer, opts)
	c.session = "test"
	c.ourWindow = opts.Window
	c.peerWindow = opts.Window
	c.maxStreams = opts.MaxStreams
	c.pingInterval = time.Hour
	c.pingTimeout = 2 * time.Hour
	c.finishHandshake()
	c.run()

	c.nextStreamID = maxStreamIDValue + 2 // one past the last legal odd id (2^31-1)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := c.OpenStream(ctx)
	if err == nil {
		t.Fatal("expected OpenStream to fail once the id space is exhausted")
	}

	var sawDrain bool
	deadline := time.After(2 * time.Second)
	for !sawDrain {
		select {
		case b := <-fake.outbound:
			frame, ferr := DecodeFrame(b)
			if ferr != nil || frame.StreamID != 0 {
				continue
			}
			if dm, ok := mustParseControl(t, frame.Payload).(*DrainMsg); ok {
				if dm.Reason != "id_exhausted" {
					t.Errorf("drain.reason = %q, want id_exhausted", dm.Reason)
				}
				sawDrain = true
			}
		case <-deadline:
			t.Fatal("no drain{reason:id_exhausted} frame observed after id exhaustion")
		}
	}

	select {
	case <-c.closed:
	case <-time.After(2 * time.Second):
		t.Fatal("connection never closed after id exhaustion drain")
	}
	fake.Close(0, "")

	waitForGoroutines(t, before)
}

func mustParseControl(t *testing.T, payload []byte) any {
	t.Helper()
	msg, err := ParseControl(payload)
	if err != nil {
		t.Fatalf("parse control payload: %v", err)
	}
	return msg
}

// waitForGoroutines fails the test if the goroutine count has not settled
// back down near baseline within a couple of seconds.
func waitForGoroutines(t *testing.T, before int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	var after int
	for {
		after = runtime.NumGoroutine()
		if after <= before+2 || time.Now().After(deadline) { // small slack for test/runtime goroutines
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if after > before+2 {
		t.Errorf("goroutine count grew from %d to %d and did not settle back down", before, after)
	}
}

func rawDialAndWaitForClose(t *testing.T, url string) {
	t.Helper()
	// A minimal client that completes the WS upgrade with the right
	// subprotocol but never sends a hello frame, to exercise the server's
	// hello timeout independent of this package's own Dial (which would
	// send hello immediately).
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c, err := dialRaw(ctx, url)
	if err != nil {
		t.Fatalf("raw dial: %v", err)
	}
	defer c.CloseNow()

	// The server must write the stream-0 error{PROTOCOL_ERROR} frame before
	// closing the socket (OVERVIEW.md section 2.8's three-step sequence).
	_, data, err := c.Read(ctx)
	if err != nil {
		t.Fatalf("expected an error{} frame before the close, got a read error: %v", err)
	}
	frame, ferr := DecodeFrame(data)
	if ferr != nil || frame.StreamID != 0 {
		t.Fatalf("expected a stream-0 control frame, got %v (decode err=%v)", frame, ferr)
	}
	msg, perr := ParseControl(frame.Payload)
	if perr != nil {
		t.Fatalf("parsing error{} control message: %v", perr)
	}
	em, ok := msg.(*ErrorMsg)
	if !ok {
		t.Fatalf("expected error{}, got %T", msg)
	}
	if em.Code != uint32(ProtocolErrorCode) {
		t.Errorf("error.code = %d, want PROTOCOL_ERROR (%d)", em.Code, ProtocolErrorCode)
	}

	_, _, err = c.Read(ctx)
	if err == nil {
		t.Fatal("expected the server to close the connection after the hello timeout")
	}
	if code := websocket.CloseStatus(err); code != 4001 {
		t.Errorf("close code = %d (%v), want 4001 (PROTOCOL_ERROR)", code, err)
	}
}
