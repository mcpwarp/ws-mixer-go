package wsmixer

// Tests for surfacing the peer's WebSocket close-frame reason string
// (Conn.PeerCloseReason, DisconnectReason.CloseReason): a server that closes
// with, say, 4009 and a human-readable reason -- with or without a
// preceding ws-mixer stream-0 error{} frame -- should let a consumer see
// that reason, not just the bare close code.

import (
	"context"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// --- Conn-level: handleReadError extracts the close reason -----------------

func TestConnPeerCloseReason(t *testing.T) {
	ws := newFakeWS()
	c := newConn(ws, RoleClient, Options{})
	const wantReason = "too many agent connections"
	ws.closeWithError(websocket.CloseError{Code: 4009, Reason: wantReason})

	c.readerLoop()

	if got := c.PeerCloseCode(); got != 4009 {
		t.Errorf("PeerCloseCode() = %d, want 4009", got)
	}
	if got := c.PeerCloseReason(); got != wantReason {
		t.Errorf("PeerCloseReason() = %q, want %q", got, wantReason)
	}
}

func TestConnPeerCloseReasonEmptyOnAbnormalClose(t *testing.T) {
	ws := newFakeWS()
	c := newConn(ws, RoleClient, Options{})
	ws.CloseNow() // no websocket.CloseError -- Read just returns io.EOF

	c.readerLoop()

	if got := c.PeerCloseCode(); got != -1 {
		t.Errorf("PeerCloseCode() = %d, want -1 (abnormal closure)", got)
	}
	if got := c.PeerCloseReason(); got != "" {
		t.Errorf("PeerCloseReason() = %q, want empty", got)
	}
}

// --- Client-level: connected phase ------------------------------------------

// TestClientBareCloseReasonPostConnect: the peer closes 4009 with a reason
// and no preceding error{} frame -- exactly the case the error{} frame can
// be lost for. CloseReason must carry the reason even though no ws-mixer
// error was ever seen (HasErrorCode false), and Message is unchanged from
// today's generic "peer closed with code %d" text.
func TestClientBareCloseReasonPostConnect(t *testing.T) {
	var raw atomic.Pointer[websocket.Conn]
	_, url := startTestServer(t, testAcceptHandler{OnRawConn: func(ws *websocket.Conn) { raw.Store(ws) }})

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

	ws := raw.Load()
	if ws == nil {
		t.Fatal("server never captured the raw *websocket.Conn (OnRawConn runs before hello/welcome, so Connect returning means it already ran)")
	}
	const wantReason = "CONNECTION_LIMIT: too many agent connections for this account (limit 10)"
	go func() { _ = ws.Close(4009, wantReason) }()

	r := rec.waitNext(t)
	if r.Phase != PhaseConnected {
		t.Errorf("Phase = %v, want connected", r.Phase)
	}
	if r.WSCode != 4009 {
		t.Errorf("WSCode = %d, want 4009", r.WSCode)
	}
	if r.HasErrorCode {
		t.Errorf("HasErrorCode = true, want false: no error{} frame preceded the close")
	}
	if r.CloseReason != wantReason {
		t.Errorf("CloseReason = %q, want %q", r.CloseReason, wantReason)
	}
	wantMessage := "ws-mixer: peer closed with code 4009"
	if r.Message != wantMessage {
		t.Errorf("Message = %q, want %q (unchanged today's generic text)", r.Message, wantMessage)
	}
}

// TestClientSelfInitiatedCloseReasonIsEmpty: Client.Close's own graceful
// shutdown writes a real reason to the wire (conn.Drain/conn.Close), but
// this side initiated that close itself -- coder/websocket's close
// handshake can hand the blocked read loop back a CloseError carrying that
// same outgoing text, echoed by the peer (see Conn.localCloseInitiated), and
// CloseReason must not surface it: it is reserved for a reason the peer
// actually originated.
func TestClientSelfInitiatedCloseReasonIsEmpty(t *testing.T) {
	_, url := startTestServer(t, testAcceptHandler{})

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

	if err := cl.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}

	r := rec.waitNext(t)
	if r.CloseReason != "" {
		t.Errorf("CloseReason = %q, want empty: this side initiated the close itself", r.CloseReason)
	}
}

// TestServerFailInitiatedCloseReasonIsEmpty: a real, live connection's fail()
// path (here, a protocol violation manufactured from OnConn) sends error{}
// and a real reason on the wire itself -- checked on the SERVER Conn's own
// PeerCloseReason directly (the client's read loop stops at the error{}
// message before ever reading the close frame at all, exactly like
// TestClientErrorThenCloseReasonUnobservedPostConnect, so it cannot exercise
// this side of the fix): the server's own blocked Read can be handed back a
// CloseError carrying the message it just sent itself, once the client
// reacts to error{} by closing in turn and its close frame echoes back
// through the server's own close handshake -- CloseReason must not surface
// that, for the same self-initiated-close reason as
// TestClientSelfInitiatedCloseReasonIsEmpty, this time exercising fail()
// rather than Close().
func TestServerFailInitiatedCloseReasonIsEmpty(t *testing.T) {
	connCh := make(chan *Conn, 1)
	_, url := startTestServer(t, testAcceptHandler{OnConn: func(c *Conn) {
		select {
		case connCh <- c:
		default: // a reconnect would land here too if Disabled below didn't prevent one
		}
		go c.failProtocol(newConnErrorf(ProtocolErrorCode, "manufactured for TestServerFailInitiatedCloseReasonIsEmpty"))
	}})

	rec := newDisconnectRecorder()
	fa := &fakeAfter{}
	rc := testReconnectOptions(fa, 0.5)
	rc.Disabled = true // one failure is all this test needs; PROTOCOL_ERROR is non-fatal and would otherwise reconnect forever
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

	var serverConn *Conn
	select {
	case serverConn = <-connCh:
	case <-time.After(2 * time.Second):
		t.Fatal("no server connection observed")
	}

	r := rec.waitNext(t)
	if !r.HasErrorCode || r.ErrorCode != ProtocolErrorCode {
		t.Fatalf("HasErrorCode/ErrorCode = %v/%v, want true/PROTOCOL_ERROR", r.HasErrorCode, r.ErrorCode)
	}
	if r.CloseReason != "" {
		t.Errorf("client-side CloseReason = %q, want empty: the server initiated this close via fail()", r.CloseReason)
	}

	select {
	case <-serverConn.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("server connection never closed")
	}
	if got := serverConn.PeerCloseReason(); got != "" {
		t.Errorf("server-side PeerCloseReason() = %q, want empty: fail() initiated this close itself", got)
	}
}

// TestClientErrorThenCloseReasonUnobservedPostConnect: the peer sends
// stream-0 error{} and then closes. dispatch.go's ErrorMsg case ends the
// read loop as soon as it parses the error{} message (handlePeerError closes
// this side's own socket without reading any further), so the close frame
// the peer subsequently sends is never actually read here -- CloseReason
// stays empty, while HasErrorCode/Message still carry the ConnError exactly
// as they did before this change.
func TestClientErrorThenCloseReasonUnobservedPostConnect(t *testing.T) {
	const wantMessage = "too many agent connections"
	_, url := startTestServer(t, testAcceptHandler{OnConn: func(c *Conn) {
		go func() { _ = c.Close(uint32(EnhanceYourCalm), wantMessage) }()
	}})
	rec := newDisconnectRecorder()
	fa := &fakeAfter{}
	rc := testReconnectOptions(fa, 0.5)
	rc.MaxAttempts = 1 // one failure is all this test needs; EnhanceYourCalm is non-fatal and would otherwise reconnect forever
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
	if !r.HasErrorCode || r.ErrorCode != EnhanceYourCalm {
		t.Fatalf("HasErrorCode/ErrorCode = %v/%v, want true/EnhanceYourCalm", r.HasErrorCode, r.ErrorCode)
	}
	if r.Message != wantMessage {
		t.Errorf("Message = %q, want %q (the ConnError message, unchanged)", r.Message, wantMessage)
	}
	if r.CloseReason != "" {
		t.Errorf("CloseReason = %q, want empty (unobservable: the read loop never reads the peer's close frame after error{})", r.CloseReason)
	}
}

// --- Client-level: handshake phase ------------------------------------------

// TestClientHandshakePhaseBareCloseReason: the peer upgrades, then closes
// with a reason before ever sending welcome (or an error{} frame) -- e.g.
// rejected during auth. clientHandshake's own hello->welcome Read must
// surface this as PhaseHandshake with the close code/reason, not PhaseDial
// with a misleading "no welcome within Nms" timeout message.
// TestClientHandshakePhaseWelcomeTimeout: the server upgrades, then stays
// completely silent -- never sends welcome, never closes -- so the client's
// own HelloTimeout is what ends the handshake. WIRE.md section 2.9 ("no
// welcome within 10s on the client side -> close 4001 and retry with
// backoff") and CLIENT-SDK.md's "Handshake-phase close" row: this must be
// phase handshake with the locally generated wsCode 4001, non-fatal, a
// scheduled retry -- and the peer must actually receive PROTOCOL_ERROR/4001
// on the wire, not be left with a bare 1006 it can never explain.
func TestClientHandshakePhaseWelcomeTimeout(t *testing.T) {
	closeCodeCh := make(chan websocket.StatusCode, 1)
	_, url := startTestServer(t, testAcceptHandler{OnRawConn: func(ws *websocket.Conn) {
		// Stay silent forever (read and discard whatever the client sends --
		// its hello -- but never reply): this call is synchronous in
		// testAcceptHandler.ServeHTTP, so it also keeps AcceptConn from ever
		// running and racing this same *websocket.Conn's Read. Returns once
		// the client's own HelloTimeout fires and it closes, recording the
		// close code this side actually observed.
		for {
			_, _, err := ws.Read(context.Background())
			if err != nil {
				select {
				case closeCodeCh <- websocket.CloseStatus(err):
				default:
				}
				return
			}
		}
	}})

	rec := newDisconnectRecorder()
	fa := &fakeAfter{}
	rc := testReconnectOptions(fa, 0.5) // rand=0 would make fullJitter's delay exactly 0, which waitBackoff special-cases into never calling rc.after at all
	rc.MaxAttempts = 1                  // this server never welcomes on any attempt
	cl := NewClient(url, StaticToken("tok"), ClientConfig{
		Options:      Options{HelloTimeout: 50 * time.Millisecond},
		Reconnect:    rc,
		OnDisconnect: rec.onDisconnect,
	})

	done := make(chan struct{})
	go func() {
		_ = cl.Connect(context.Background())
		close(done)
	}()
	defer func() {
		cl.Close(context.Background())
		<-done
	}()

	r := rec.waitNext(t)
	if r.Phase != PhaseHandshake {
		t.Errorf("Phase = %v, want handshake", r.Phase)
	}
	if r.WSCode != 4001 {
		t.Errorf("WSCode = %d, want 4001", r.WSCode)
	}
	if !r.HasErrorCode || r.ErrorName != "PROTOCOL_ERROR" {
		t.Errorf("HasErrorCode/ErrorName = %v/%q, want true/PROTOCOL_ERROR", r.HasErrorCode, r.ErrorName)
	}
	if r.CloseReason != "" {
		t.Errorf("CloseReason = %q, want empty", r.CloseReason)
	}
	if r.Fatal {
		t.Errorf("Fatal = true, want false")
	}
	waitForCalls(t, fa, 1) // a retry was scheduled, not treated as fatal/exhausted

	select {
	case code := <-closeCodeCh:
		if code != 4001 {
			t.Errorf("server observed close code %d, want 4001 (not a bare 1006)", code)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server never observed the client's close")
	}
}

// TestClientHandshakePhaseWelcomeTimeoutShippedRatio regression-tests the
// bug two independent reviewers found in the fix above: dialAndHandshake
// (client_reconnect.go) put a single ctx, sized to ConnectTimeout alone,
// over both the dial and clientHandshake's welcome wait -- and that ctx is
// exactly what reaches c.ws.Read. With ConnectTimeout and HelloTimeout equal
// (the ratio that actually shipped: both default to 10s), the ctx deadline
// and the hello timer race, and whichever fires first decides the outcome:
// when the ctx deadline wins, coder/websocket tears the transport down with
// no close frame at all, reproducing the pre-fix Phase=dial/WSCode=0/bare-
// 1006 behaviour instead of the intended Phase=handshake/WSCode=4001.
// TestClientHandshakePhaseWelcomeTimeout above never caught this because
// testReconnectOptions's ConnectTimeout (2s) so outweighs its HelloTimeout
// (50ms) that the ctx deadline never has a realistic chance to win -- this
// test uses equal values instead, and runs the scenario several times since
// the old behaviour was a coin flip, not a certainty.
func TestClientHandshakePhaseWelcomeTimeoutShippedRatio(t *testing.T) {
	const iterations = 5
	for i := 0; i < iterations; i++ {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			closeCodeCh := make(chan websocket.StatusCode, 1)
			_, url := startTestServer(t, testAcceptHandler{OnRawConn: func(ws *websocket.Conn) {
				for {
					_, _, err := ws.Read(context.Background())
					if err != nil {
						select {
						case closeCodeCh <- websocket.CloseStatus(err):
						default:
						}
						return
					}
				}
			}})

			rec := newDisconnectRecorder()
			fa := &fakeAfter{}
			rc := testReconnectOptions(fa, 0.5)
			rc.MaxAttempts = 1
			rc.ConnectTimeout = 150 * time.Millisecond // shipped ratio: equal to HelloTimeout below, not ConnectTimeout >> HelloTimeout
			cl := NewClient(url, StaticToken("tok"), ClientConfig{
				Options:      Options{HelloTimeout: 150 * time.Millisecond},
				Reconnect:    rc,
				OnDisconnect: rec.onDisconnect,
			})

			done := make(chan struct{})
			go func() {
				_ = cl.Connect(context.Background())
				close(done)
			}()
			defer func() {
				cl.Close(context.Background())
				<-done
			}()

			r := rec.waitNext(t)
			if r.Phase != PhaseHandshake {
				t.Errorf("Phase = %v, want handshake", r.Phase)
			}
			if r.WSCode != 4001 {
				t.Errorf("WSCode = %d, want 4001", r.WSCode)
			}

			select {
			case code := <-closeCodeCh:
				if code != 4001 {
					t.Errorf("server observed close code %d, want 4001 (not a bare 1006)", code)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("server never observed the client's close")
			}
		})
	}
}

func TestClientHandshakePhaseBareCloseReason(t *testing.T) {
	const wantReason = "CONNECTION_LIMIT: too many agent connections for this account (limit 10)"
	_, url := startTestServer(t, testAcceptHandler{OnRawConn: func(ws *websocket.Conn) {
		_ = ws.Close(4009, wantReason)
	}})

	rec := newDisconnectRecorder()
	fa := &fakeAfter{}
	rc := testReconnectOptions(fa, 0)
	rc.MaxAttempts = 1 // bound the retry loop: this server closes on every attempt
	cl := NewClient(url, StaticToken("tok"), ClientConfig{
		Reconnect:    rc,
		OnDisconnect: rec.onDisconnect,
	})

	done := make(chan struct{})
	go func() {
		_ = cl.Connect(context.Background())
		close(done)
	}()
	defer func() {
		cl.Close(context.Background())
		<-done
	}()

	r := rec.waitNext(t)
	if r.Phase != PhaseHandshake {
		t.Errorf("Phase = %v, want handshake", r.Phase)
	}
	if r.WSCode != 4009 {
		t.Errorf("WSCode = %d, want 4009", r.WSCode)
	}
	if r.CloseReason != wantReason {
		t.Errorf("CloseReason = %q, want %q", r.CloseReason, wantReason)
	}
	if r.Fatal {
		t.Errorf("Fatal = true, want false")
	}
}
