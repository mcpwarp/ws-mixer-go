package wsmixer

// Tests for Conn.Close called before Run: the writer loop is not running
// yet, so Close must write the error{} frame (and flush anything already
// queued via SendApp) synchronously, the same way fail() already does for a
// pre-run handshake failure -- or the frame is lost with the socket.

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func newUnrunConn(ws *fakeWS) *Conn {
	c := newConn(ws, RoleServer, Options{})
	c.opts.SetDefaults()
	c.session = "test-session"
	c.ourWindow = 262144
	c.peerWindow = 262144
	c.maxStreams = 64
	c.pingInterval = time.Minute
	c.pingTimeout = 5 * time.Minute
	c.finishHandshake()
	return c
}

// wantErrorMsg decodes one stream-0 DATA frame off ws.outbound and asserts
// it is the error{} message with the given code/message.
func wantErrorMsg(t *testing.T, ws *fakeWS, code ErrorCode, message string) {
	t.Helper()
	var raw []byte
	select {
	case raw = <-ws.outbound:
	case <-time.After(2 * time.Second):
		t.Fatal("no frame written to the transport")
	}
	frame, err := DecodeFrame(raw)
	if err != nil {
		t.Fatalf("DecodeFrame: %v", err)
	}
	if frame.StreamID != 0 {
		t.Fatalf("frame.StreamID = %d, want 0", frame.StreamID)
	}
	msg, err := ParseControl(frame.Payload)
	if err != nil {
		t.Fatalf("ParseControl: %v", err)
	}
	em, ok := msg.(*ErrorMsg)
	if !ok {
		t.Fatalf("expected *ErrorMsg, got %T", msg)
	}
	if em.Code != uint32(code) {
		t.Errorf("Code = %d, want %d", em.Code, uint32(code))
	}
	if em.Message != message {
		t.Errorf("Message = %q, want %q", em.Message, message)
	}
}

func TestCloseBeforeRunWritesErrorThenCloses(t *testing.T) {
	ws := newFakeWS()
	c := newUnrunConn(ws)

	if err := c.Close(uint32(EnhanceYourCalm), "refused: over capacity"); err != nil {
		t.Fatalf("Close: %v", err)
	}

	wantErrorMsg(t, ws, EnhanceYourCalm, "refused: over capacity")

	select {
	case <-ws.closed:
	default:
		t.Fatal("ws.Close was never called")
	}
	ws.mu.Lock()
	gotCode, gotReason := ws.closeCode, ws.closeReason
	ws.mu.Unlock()
	if want := websocket.StatusCode(EnhanceYourCalm.CloseCode()); gotCode != want {
		t.Errorf("close code = %d, want %d", gotCode, want)
	}
	if gotReason != "refused: over capacity" {
		t.Errorf("close reason = %q, want %q", gotReason, "refused: over capacity")
	}

	select {
	case <-c.closed:
	default:
		t.Error("c.closed was not closed")
	}
}

// TestCloseBeforeRunFlushesQueuedAppFrameBeforeError checks the ordering
// guarantee for a frame the application already enqueued via SendApp right
// before calling Close (a real pattern: a best-effort app-level notice sent
// alongside a refusal): it must still reach the wire, in order, ahead of
// error{}, which stays last as the wire requires.
func TestCloseBeforeRunFlushesQueuedAppFrameBeforeError(t *testing.T) {
	ws := newFakeWS()
	c := newUnrunConn(ws)

	if err := c.SendApp(context.Background(), map[string]any{"reason": "over_capacity"}); err != nil {
		t.Fatalf("SendApp: %v", err)
	}

	if err := c.Close(uint32(EnhanceYourCalm), "refused: over capacity"); err != nil {
		t.Fatalf("Close: %v", err)
	}

	var raw []byte
	select {
	case raw = <-ws.outbound:
	case <-time.After(2 * time.Second):
		t.Fatal("no app frame written to the transport")
	}
	frame, err := DecodeFrame(raw)
	if err != nil {
		t.Fatalf("DecodeFrame: %v", err)
	}
	msg, err := ParseControl(frame.Payload)
	if err != nil {
		t.Fatalf("ParseControl: %v", err)
	}
	if _, ok := msg.(*AppMsg); !ok {
		t.Fatalf("expected the queued *AppMsg first, got %T", msg)
	}

	wantErrorMsg(t, ws, EnhanceYourCalm, "refused: over capacity")
}

// TestRunAfterCloseBeforeRunIsSafe checks that calling Run() on a Conn that
// was already closed before Run does not hang or panic, and -- since Close
// claims preRunClosed in the same critical section Run checks -- starts
// none of its five goroutines at all: c.running stays false, Run's early
// return is the whole story, not a race against goroutines it did start.
func TestRunAfterCloseBeforeRunIsSafe(t *testing.T) {
	ws := newFakeWS()
	c := newUnrunConn(ws)

	if err := c.Close(uint32(NoError), "refused"); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case <-c.closed:
	case <-time.After(2 * time.Second):
		t.Fatal("c.closed was not closed by Close")
	}

	done := make(chan struct{})
	go func() {
		c.Run()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run() did not return promptly on a Conn already closed before Run")
	}

	c.mu.Lock()
	running := c.running
	c.mu.Unlock()
	if running {
		t.Error("Run() started the writer loop on a Conn already closed before Run")
	}
}

// TestCloseClampsUnmappableWSCloseCode: an application error code that maps
// outside the legal WS close-code range (any code > 999, e.g. an
// application code >= 0x1000_0000) can't be sent as a WS close code at all --
// coder/websocket would otherwise send no close frame and just abort the
// transport. Close must clamp the WS close to InternalErrorCode's code
// (4002) while error{} still carries the caller's real, unclamped code.
func TestCloseClampsUnmappableWSCloseCode(t *testing.T) {
	ws := newFakeWS()
	c := newUnrunConn(ws)

	const appCode = uint32(0x10000000)
	if err := c.Close(appCode, "x"); err != nil {
		t.Fatalf("Close: %v", err)
	}

	wantErrorMsg(t, ws, ErrorCode(appCode), "x")

	ws.mu.Lock()
	gotCode := ws.closeCode
	ws.mu.Unlock()
	if want := websocket.StatusCode(InternalErrorCode.CloseCode()); gotCode != want {
		t.Errorf("close code = %d, want %d (clamped to INTERNAL_ERROR)", gotCode, want)
	}
}

// TestCloseApplicationCloseCodeUnclamped: 4014 is a legal WS close code, so
// ApplicationCloseCode must reach the wire unchanged, not clamped.
func TestCloseApplicationCloseCodeUnclamped(t *testing.T) {
	ws := newFakeWS()
	c := newUnrunConn(ws)

	if err := c.Close(uint32(ApplicationCloseCode), "CONNECTION_LIMIT: test"); err != nil {
		t.Fatalf("Close: %v", err)
	}

	ws.mu.Lock()
	gotCode := ws.closeCode
	ws.mu.Unlock()
	if want := websocket.StatusCode(4014); gotCode != want {
		t.Errorf("close code = %d, want 4014 (unclamped)", gotCode)
	}
}

// TestCloseNoErrorUnclamped: NoError maps to 1000, the other legal code this
// clamp must leave untouched.
func TestCloseNoErrorUnclamped(t *testing.T) {
	ws := newFakeWS()
	c := newUnrunConn(ws)

	if err := c.Close(uint32(NoError), "bye"); err != nil {
		t.Fatalf("Close: %v", err)
	}

	ws.mu.Lock()
	gotCode := ws.closeCode
	ws.mu.Unlock()
	if want := websocket.StatusCode(1000); gotCode != want {
		t.Errorf("close code = %d, want 1000 (unclamped)", gotCode)
	}
}

// TestHandlePeerErrorClampsInvalidCloseCode: m.Code in a peer's error{}
// message is bounded only to uint32 by the wire format (control.go) -- a
// peer sending an unmappable code (e.g. an application code >= 0x1000_0000,
// legal only for a stream RESET) must not make this side call ws.Close with
// an invalid WS status code, which coder/websocket reacts to by sending no
// close frame at all and aborting the transport outright. handlePeerError's
// own ws.Close goes through wsCloseCode exactly like fail/Close.
func TestHandlePeerErrorClampsInvalidCloseCode(t *testing.T) {
	ws := newFakeWS()
	c := newUnrunConn(ws)

	const peerCode = uint32(0x10000001)
	b, err := json.Marshal(&ErrorMsg{T: "error", Code: peerCode, Message: "boom"})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	ws.feedInbound(EncodeData(0, b))

	c.readerLoop()

	select {
	case <-c.closed:
	case <-time.After(2 * time.Second):
		t.Fatal("c.closed was not closed")
	}

	ws.mu.Lock()
	gotCode := ws.closeCode
	ws.mu.Unlock()
	if want := websocket.StatusCode(InternalErrorCode.CloseCode()); gotCode != want {
		t.Errorf("close code = %d, want %d (clamped)", gotCode, want)
	}

	ce, ok := c.Err().(*ConnError)
	if !ok {
		t.Fatalf("Err() = %#v, want *ConnError", c.Err())
	}
	if uint32(ce.Code) != peerCode {
		t.Errorf("Err().Code = %#x, want %#x (the peer's real code, unclamped)", uint32(ce.Code), peerCode)
	}
}

// TestCloseAfterRunStillWritesErrorThenCloses checks Close's post-Run path
// (frame handed to the writer loop's queue) still behaves as before this
// change.
func TestCloseAfterRunStillWritesErrorThenCloses(t *testing.T) {
	ws := newFakeWS()
	c := newUnrunConn(ws)
	c.Run()
	defer func() { _ = ws.CloseNow() }()

	if err := c.Close(uint32(GoingAwayCode), "server shutting down"); err != nil {
		t.Fatalf("Close: %v", err)
	}

	wantErrorMsg(t, ws, GoingAwayCode, "server shutting down")

	ws.mu.Lock()
	gotCode := ws.closeCode
	ws.mu.Unlock()
	if want := websocket.StatusCode(GoingAwayCode.CloseCode()); gotCode != want {
		t.Errorf("close code = %d, want %d", gotCode, want)
	}
}
