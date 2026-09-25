package wsmixer

// Tests for surfacing the peer's WebSocket close-frame reason string
// (Conn.PeerCloseReason, DisconnectReason.CloseReason): a server that closes
// with, say, 4009 and a human-readable reason -- with or without a
// preceding ws-mixer stream-0 error{} frame -- should let a consumer see
// that reason, not just the bare close code.

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
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
// error{} message was ever seen; ErrorCode/ErrorName are still derived from
// the bare close code itself (deriveBareCloseErrorCode, D-2026-09-20-09),
// and Message is unchanged from today's generic "peer closed with code %d"
// text (Message, unlike ErrorCode/ErrorName, is never synthesized from the
// bare close code).
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
	if !r.HasErrorCode || r.ErrorCode != EnhanceYourCalm || r.ErrorName != "ENHANCE_YOUR_CALM" {
		t.Errorf("HasErrorCode/ErrorCode/ErrorName = %v/%v/%q, want true/EnhanceYourCalm/ENHANCE_YOUR_CALM (derived from the bare 4009 close, D-2026-09-20-09)", r.HasErrorCode, r.ErrorCode, r.ErrorName)
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
// (client_reconnect.go) used to put a single ctx, sized to ConnectTimeout
// alone, over both the dial and clientHandshake's welcome wait -- and that
// ctx is exactly what reaches c.ws.Read. Fixed by sizing that ctx to
// ConnectTimeout+HelloTimeout (the sum, covering both budgets in sequence)
// instead. This test pins the shipped-defaults ratio specifically
// (ConnectTimeout == HelloTimeout, both default to 10s): with the fix, the
// combined ctx (300ms here) has a comfortable margin over the HelloTimeout
// AfterFunc (150ms, starting only once the welcome wait itself begins,
// after a ~0ms loopback dial) -- not a coin flip, the AfterFunc reliably
// wins and this reports Phase=handshake/WSCode=4001, with the server
// actually observing 4001 on the wire (not a bare 1006). What this
// regression-tests is the fix staying in effect at exactly this ratio: were
// the combined ctx to regress back to ConnectTimeout alone (100ms here,
// less than the 150ms AfterFunc it now races against unfairly early), the
// ctx would win instead, tearing the transport down with no close frame at
// all and reproducing the pre-fix Phase=dial/WSCode=0/bare-1006 behaviour.
// TestClientHandshakePhaseWelcomeTimeout above never caught the original
// bug because testReconnectOptions's ConnectTimeout (2s) so outweighs its
// HelloTimeout (50ms) that even the pre-fix, ConnectTimeout-alone ctx still
// had enough headroom to not matter -- this test uses the shipped ratio
// instead, and runs the scenario several times as a sanity margin (a slow
// CI host could in principle still narrow it), not because the outcome is
// expected to be a coin flip. A genuinely competitive race between the
// combined ctx and the AfterFunc needs a SLOW DIAL eating into the shared
// budget instead (see the sibling
// TestClientHandshakePhaseSlowUpgradeCtxDeadlineDuringWelcomeWait).
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

// TestClientHandshakePhaseSlowUpgradeCtxDeadlineDuringWelcomeWait pins
// review item 6, probed: when the DIAL itself (the HTTP upgrade to the
// 101) eats deep enough into dialAndHandshake's combined
// ConnectTimeout+HelloTimeout ctx budget, that shared ctx's own deadline
// can expire strictly AFTER the 101 (while clientHandshake is waiting for
// welcome) but strictly BEFORE its own separate HelloTimeout AfterFunc
// ever gets a chance to fire (which is timed fresh from when the welcome
// wait itself starts, not from when the ctx was created) -- a genuine
// post-101 timeout, not an application cancellation. Before this fix,
// Dial/clientHandshake both used a bare `ctx.Err() != nil` check to detect
// "the caller is abandoning this dial", which a naturally-expired deadline
// also satisfies -- misclassifying this case Phase=dial instead of
// Phase=handshake. Using errors.Is(ctx.Err(), context.Canceled) instead
// (Close/CloseWith's own cancellation, or a plain Dial caller's explicit
// ctx cancellation) fixes it: this scenario's ctx.Err() is
// context.DeadlineExceeded, not Canceled, so it is no longer conflated with
// an application-cancelled dial.
func TestClientHandshakePhaseSlowUpgradeCtxDeadlineDuringWelcomeWait(t *testing.T) {
	// Delays the 101 itself well past ConnectTimeout, then -- once
	// upgraded -- reads the client's hello and goes silent forever (never
	// sends welcome): both halves are needed to actually force a failure
	// here. A quick, normal welcome sent right after the delayed 101 would
	// still land comfortably inside both the shared ctx budget and the
	// HelloTimeout AfterFunc's own window (it only starts counting once the
	// welcome wait itself begins, after the 101) -- succeeding outright
	// instead of exercising the race this test targets.
	inner := newTestListener(testAcceptHandler{
		Options: Options{Logger: discardLogger()},
		OnRawConn: func(ws *websocket.Conn) {
			for {
				_, _, err := ws.Read(context.Background())
				if err != nil {
					return
				}
			}
		},
	})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(1200 * time.Millisecond) // delay the 101 itself well past ConnectTimeout
		inner.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	url := "ws" + srv.URL[len("http"):] + "/tunnel"

	rec := newDisconnectRecorder()
	fa := &fakeAfter{}
	rc := testReconnectOptions(fa, 0.5)
	rc.MaxAttempts = 1
	rc.ConnectTimeout = 1 * time.Second
	cl := NewClient(url, StaticToken("tok"), ClientConfig{
		Options:      Options{HelloTimeout: 1 * time.Second},
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
		t.Errorf("Phase = %v, want handshake (a slow-but-genuine upgrade eating into the shared ctx budget is not an application cancellation)", r.Phase)
	}
	if r.Fatal {
		t.Errorf("Fatal = true, want false")
	}
}

// TestDialPostUpgradeDeadlineExceededStillDetectable pins the other half
// of review item 6: even though a post-101 timeout is now wrapped in
// postUpgradeError (so Client can classify it Phase=handshake), a plain
// Dial caller's own errors.Is(err, context.DeadlineExceeded) check must
// still see straight through that wrapping via Unwrap, exactly as it did
// before postUpgradeError existed.
func TestDialPostUpgradeDeadlineExceededStillDetectable(t *testing.T) {
	_, url := startTestServer(t, testAcceptHandler{OnRawConn: func(ws *websocket.Conn) {
		for {
			_, _, err := ws.Read(context.Background())
			if err != nil {
				return
			}
		}
	}})

	// ctx deliberately much shorter than HelloTimeout, so the ctx deadline
	// reliably wins the race against the HelloTimeout AfterFunc (unlike
	// this file's own shipped-ratio test, which deliberately keeps them
	// equal to probe the coin-flip case) -- this test only cares about the
	// ctx-wins outcome specifically.
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := Dial(ctx, url, ClientOptions{
		Token:   "tok",
		Options: Options{HelloTimeout: 5 * time.Second},
	})
	if err == nil {
		t.Fatal("expected an error")
	}
	var pue *postUpgradeError
	if !errors.As(err, &pue) {
		t.Fatalf("err is not a *postUpgradeError (test premise broken): %v", err)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("errors.Is(err, context.DeadlineExceeded) = false, want true (through postUpgradeError's Unwrap): %v", err)
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
	if !r.HasErrorCode || r.ErrorCode != EnhanceYourCalm || r.ErrorName != "ENHANCE_YOUR_CALM" {
		t.Errorf("HasErrorCode/ErrorCode/ErrorName = %v/%v/%q, want true/EnhanceYourCalm/ENHANCE_YOUR_CALM (derived from the bare 4009 close, D-2026-09-20-09)", r.HasErrorCode, r.ErrorCode, r.ErrorName)
	}
	if r.CloseReason != wantReason {
		t.Errorf("CloseReason = %q, want %q", r.CloseReason, wantReason)
	}
	if r.Fatal {
		t.Errorf("Fatal = true, want false")
	}
}

// TestClientHandshakePhaseTransportDeath: the server upgrades (the 101
// succeeds), then drops the TCP connection outright with no WebSocket close
// frame at all -- ws.CloseNow()'s documented behaviour, and exactly what a
// half-open connection dying or a TCP reset looks like on the wire. This
// happened strictly after the upgrade and strictly before welcome, so it
// must be reported Phase=handshake per D-2026-09-20 change 3 (marking where
// the failure happened, client.go's postUpgradeError, rather than guessing
// from the error's type) -- not the misleading Phase=dial/WSCode=0 this
// used to fall through to, and WSCode must stay 0 (never fabricated: no
// close frame was actually observed), CloseReason "", non-fatal, with a
// scheduled retry.
func TestClientHandshakePhaseTransportDeath(t *testing.T) {
	_, url := startTestServer(t, testAcceptHandler{OnRawConn: func(ws *websocket.Conn) {
		_ = ws.CloseNow()
	}})

	rec := newDisconnectRecorder()
	fa := &fakeAfter{}
	rc := testReconnectOptions(fa, 0)
	rc.MaxAttempts = 1 // bound the retry loop: this server drops every attempt
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
	if r.WSCode != 0 {
		t.Errorf("WSCode = %d, want 0 (no close frame was ever observed -- never fabricate one)", r.WSCode)
	}
	if r.CloseReason != "" {
		t.Errorf("CloseReason = %q, want empty", r.CloseReason)
	}
	if r.Fatal {
		t.Errorf("Fatal = true, want false")
	}
	if want := "wsmixer: connection lost before welcome completed the handshake"; !strings.Contains(r.Message, want) {
		t.Errorf("Message = %q, want it to contain %q (not a fabricated welcome-timeout message)", r.Message, want)
	}
}

// TestClientHandshakePhaseMalformedFrameBeforeWelcome: the server upgrades,
// then writes a structurally invalid frame (too short to even contain a
// frame header) instead of welcome. clientHandshake's own DecodeFrame call
// already turns this into a *ConnError -- D-2026-09-20 change 3 doesn't
// change this path at all, just pins that it stays Phase=handshake
// alongside the newly reclassified transport-death case above.
func TestClientHandshakePhaseMalformedFrameBeforeWelcome(t *testing.T) {
	_, url := startTestServer(t, testAcceptHandler{OnRawConn: func(ws *websocket.Conn) {
		_ = ws.Write(context.Background(), websocket.MessageBinary, []byte{0x00, 0x01}) // shorter than a frame header
	}})

	rec := newDisconnectRecorder()
	fa := &fakeAfter{}
	rc := testReconnectOptions(fa, 0)
	rc.MaxAttempts = 1 // bound the retry loop: this server sends garbage every attempt
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
	if !r.HasErrorCode || r.ErrorCode != ProtocolErrorCode {
		t.Errorf("ErrorCode = %v (has=%v), want ProtocolErrorCode", r.ErrorCode, r.HasErrorCode)
	}
	if r.Fatal {
		t.Errorf("Fatal = true, want false")
	}
}

// TestClientCloseDuringHandshakeWaitReportsNoDisconnect pins the one case
// D-2026-09-20 change 3 deliberately leaves alone: Client.Close() while a
// dial is blocked waiting for welcome (after the 101 already succeeded)
// cancels dialAndHandshake's shared ctx (its closeCh watcher), which is
// exactly the same ctx clientHandshake reads with -- but this is the
// application abandoning the dial, not the peer or the transport failing
// it, so Dial's postUpgradeError wrapping deliberately skips this case (its
// own errors.Is(ctx.Err(), context.Canceled) check -- review item 6). Today,
// and unchanged by change 3: attemptLoop's own
// closingNow check (client_reconnect.go) discards a dial failure that lands
// after Close() has already started before onAttemptFailed/phaseFor ever
// run at all -- no OnDisconnect fires for it, only Connect's own "closed
// before the first connection completed" error. Pinned here so any future
// change to that reporting is deliberate, not accidental.
func TestClientCloseDuringHandshakeWaitReportsNoDisconnect(t *testing.T) {
	blocked := make(chan struct{})
	_, url := startTestServer(t, testAcceptHandler{OnRawConn: func(ws *websocket.Conn) {
		close(blocked)
		// Read the client's hello and then go silent forever -- Close()
		// below cancels the ctx clientHandshake is waiting on before
		// HelloTimeout ever has a chance to fire.
		_, _, _ = ws.Read(context.Background())
		<-context.Background().Done()
	}})

	rec := newDisconnectRecorder()
	fa := &fakeAfter{}
	rc := testReconnectOptions(fa, 0)
	rc.ConnectTimeout = 30 * time.Second
	cl := NewClient(url, StaticToken("tok"), ClientConfig{
		Options:      Options{HelloTimeout: 30 * time.Second},
		Reconnect:    rc,
		OnDisconnect: rec.onDisconnect,
	})

	done := make(chan struct{})
	var connectErr error
	go func() {
		connectErr = cl.Connect(context.Background())
		close(done)
	}()
	select {
	case <-blocked:
	case <-time.After(2 * time.Second):
		t.Fatal("server never observed the upgrade")
	}
	time.Sleep(50 * time.Millisecond) // let clientHandshake actually reach its Read

	closeDone := make(chan struct{})
	go func() { _ = cl.Close(context.Background()); close(closeDone) }()
	select {
	case <-closeDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Close() during the handshake wait did not return")
	}
	<-done

	if connectErr == nil {
		t.Error("Connect() should report the client closed before the first connection completed")
	}
	select {
	case r := <-rec.ch:
		t.Errorf("unexpected OnDisconnect for a dial Close() raced ahead of: %+v", r)
	case <-time.After(200 * time.Millisecond):
		// none, as today: attemptLoop's closingNow check discards this dial
		// failure before it ever reaches onAttemptFailed/phaseFor.
	}
}
