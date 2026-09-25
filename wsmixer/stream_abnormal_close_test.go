package wsmixer

// Tests for Conn.connClosedErr (conn.go): when a connection dies ABNORMALLY
// -- TCP reset, EOF, a read timeout, the peer calling CloseNow with no close
// frame -- handleReadError (dispatch.go) leaves c.err nil (its `if code !=
// -1 && c.err == nil` guard only fires for an actual observed WS close
// code). Before this fix, Stream.wait/WriteContext returned that nil error
// straight through: ReadContext's loop treats a nil wait() error as "state
// may have changed, recheck" and re-arms on a fresh notifyCh nobody will
// ever close -- since s.conn.closed is already closed, the very next wait()
// call hits that same case again, forever (a hot spin, never returning), and
// WriteContext returned a short byte count with a nil error -- a dead tunnel
// silently reported as success. connClosedErr never returns nil: an
// abnormal closure now surfaces io.ErrUnexpectedEOF instead.

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"
)

// TestReaderLoopAbnormalCloseLeavesConnErrNil pins this test file's premise
// against the REAL production path (not a simulated field write): a bare
// transport failure with no WS close frame -- fakeWS.CloseNow() makes the
// blocked readerLoop's Read return plain io.EOF, which is not a
// websocket.CloseError, so handleReadError's `websocket.CloseStatus`
// returns -1 and its `code != -1` guard never fires -- leaves Conn.Err()
// nil even though the connection is fully closed.
func TestReaderLoopAbnormalCloseLeavesConnErrNil(t *testing.T) {
	ws := newFakeWS()
	c := newConn(ws, RoleServer, Options{})
	c.opts.SetDefaults()
	c.session = "test-session"
	c.ourWindow = 262144
	c.peerWindow = 262144
	c.maxStreams = 64
	c.pingInterval = time.Minute
	c.pingTimeout = 5 * time.Minute
	c.finishHandshake()
	c.Run()
	defer func() { _ = ws.CloseNow() }()

	_ = ws.CloseNow() // abnormal closure: no WS close frame at all

	select {
	case <-c.closed:
	case <-time.After(2 * time.Second):
		t.Fatal("c.closed never closed after the transport died")
	}
	if err := c.Err(); err != nil {
		t.Fatalf("Err() = %v, want nil (this is the premise the rest of this file's tests fix around)", err)
	}
	if err := c.connClosedErr(); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Errorf("connClosedErr() = %v, want io.ErrUnexpectedEOF", err)
	}
}

// TestStreamReadReturnsPromptlyOnAbnormalClose is the core regression: a
// Read blocked on a live stream when the transport dies abnormally (no close
// frame) must return promptly with a non-nil, non-io.EOF error -- not spin
// forever. Drives the real readerLoop/handleReadError path.
func TestStreamReadReturnsPromptlyOnAbnormalClose(t *testing.T) {
	ws := newFakeWS()
	c := newConn(ws, RoleServer, Options{})
	c.opts.SetDefaults()
	c.session = "test-session"
	c.ourWindow = 262144
	c.peerWindow = 262144
	c.maxStreams = 64
	c.pingInterval = time.Minute
	c.pingTimeout = 5 * time.Minute
	c.finishHandshake()
	c.Run()
	defer func() { _ = ws.CloseNow() }()

	st := registerTestStream(c, 1)

	readDone := make(chan struct{})
	var readErr error
	go func() {
		buf := make([]byte, 1)
		_, readErr = st.Read(buf)
		close(readDone)
	}()

	time.Sleep(20 * time.Millisecond) // let Read actually park in wait() first
	_ = ws.CloseNow()                 // abnormal closure: no close frame

	select {
	case <-readDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Read did not return within 2s of an abnormal close (hot spin)")
	}
	if readErr == nil {
		t.Fatal("Read returned a nil error on an abnormal closure")
	}
	if errors.Is(readErr, io.EOF) {
		t.Errorf("Read returned io.EOF on an abnormal closure, want a distinct error -- io.EOF must stay reserved for a clean peer CLOSE")
	}
	if !errors.Is(readErr, io.ErrUnexpectedEOF) {
		t.Errorf("Read error = %v, want io.ErrUnexpectedEOF", readErr)
	}

	// A second Read, after the first already observed the abnormal close,
	// must also return promptly -- this is exactly the hot-spin the bug
	// produced: wait() kept re-arming on a fresh notifyCh nobody would ever
	// close, since s.conn.closed was already closed on every subsequent call.
	done2 := make(chan struct{})
	var err2 error
	go func() {
		buf := make([]byte, 1)
		_, err2 = st.Read(buf)
		close(done2)
	}()
	select {
	case <-done2:
	case <-time.After(2 * time.Second):
		t.Fatal("second Read after an abnormal close did not return within 2s (hot spin)")
	}
	if !errors.Is(err2, io.ErrUnexpectedEOF) {
		t.Errorf("second Read error = %v, want io.ErrUnexpectedEOF", err2)
	}
}

// TestStreamReadReturnsPromptlyWhenStartedAfterAbnormalClose is the same
// regression, but Read is only called once the conn is already dead --
// wait()'s very first call already lands on the s.conn.closed case.
func TestStreamReadReturnsPromptlyWhenStartedAfterAbnormalClose(t *testing.T) {
	ws := newFakeWS()
	c := newConn(ws, RoleServer, Options{})
	c.opts.SetDefaults()
	c.session = "test-session"
	c.ourWindow = 262144
	c.peerWindow = 262144
	c.maxStreams = 64
	c.pingInterval = time.Minute
	c.pingTimeout = 5 * time.Minute
	c.finishHandshake()
	c.Run()
	defer func() { _ = ws.CloseNow() }()

	st := registerTestStream(c, 1)

	_ = ws.CloseNow() // abnormal closure, before Read is ever called
	select {
	case <-c.closed:
	case <-time.After(2 * time.Second):
		t.Fatal("c.closed never closed")
	}

	done := make(chan struct{})
	var err error
	go func() {
		buf := make([]byte, 1)
		_, err = st.Read(buf)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Read started after an abnormal close did not return within 2s")
	}
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Errorf("Read error = %v, want io.ErrUnexpectedEOF", err)
	}
}

// TestStreamWriteFirstSelectReturnsErrorOnAbnormalClose covers
// WriteContext's enqueue select (stream.go, the `case s.outQueue <- pc`
// select): a short byte count with a nil error would look like a complete,
// successful write on a dead tunnel. Built on an un-run *Conn
// (newTestSchedConn/registerTestStream, sched_test.go) so nothing ever
// drains st.outQueue -- outQueue is filled to capacity first so the enqueue
// select deterministically lands on the s.conn.closed case rather than
// racing it against a send that also has room.
func TestStreamWriteFirstSelectReturnsErrorOnAbnormalClose(t *testing.T) {
	c := newTestSchedConn(t)
	st := registerTestStream(c, 1)

	for len(st.outQueue) < cap(st.outQueue) {
		st.outQueue <- &pendingChunk{streamID: st.id, data: []byte("x"), done: make(chan struct{})}
	}
	// Simulate the abnormal-closure path's own field state directly
	// (handleReadError's real effect): c.err stays nil, only c.closed
	// closes.
	close(c.closed)

	_, err := st.WriteContext(context.Background(), []byte("hello"))
	if err == nil {
		t.Fatal("Write returned a nil error on an abnormal closure")
	}
	if errors.Is(err, io.EOF) {
		t.Errorf("Write returned io.EOF on an abnormal closure, want a distinct error")
	}
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Errorf("Write error = %v, want io.ErrUnexpectedEOF", err)
	}
}

// TestStreamWriteSecondSelectReturnsErrorOnAbnormalClose covers
// WriteContext's post-enqueue select (waiting on <-done): the chunk is
// queued successfully while the conn is still open, then the conn dies
// abnormally before anything ever closes done (no writer loop is running on
// this un-run *Conn, so nothing ever will).
func TestStreamWriteSecondSelectReturnsErrorOnAbnormalClose(t *testing.T) {
	c := newTestSchedConn(t)
	st := registerTestStream(c, 1)

	writeDone := make(chan struct{})
	var err error
	go func() {
		_, err = st.WriteContext(context.Background(), []byte("hello"))
		close(writeDone)
	}()

	time.Sleep(20 * time.Millisecond) // let the enqueue succeed and reach the second select
	close(c.closed)

	select {
	case <-writeDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Write (post-enqueue select) did not return within 2s of an abnormal close")
	}
	if err == nil {
		t.Fatal("Write returned a nil error on an abnormal closure")
	}
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Errorf("Write error = %v, want io.ErrUnexpectedEOF", err)
	}
}

// --- normal paths, unchanged by connClosedErr -------------------------------

// TestStreamReadPeerCloseStillReturnsIOEOF: a clean peer CLOSE (no abnormal
// closure involved at all) must keep returning exactly io.EOF, never
// io.ErrUnexpectedEOF -- connClosedErr is only ever consulted once
// s.conn.closed fires, and a plain peer CLOSE resolves entirely inside
// ReadContext's own s.eof check before wait() (let alone connClosedErr) is
// ever reached.
func TestStreamReadPeerCloseStillReturnsIOEOF(t *testing.T) {
	c := newTestSchedConn(t)
	st := registerTestStream(c, 1)

	if err := st.handleClose(); err != nil {
		t.Fatalf("handleClose: %v", err)
	}
	buf := make([]byte, 1)
	_, err := st.Read(buf)
	if err != io.EOF {
		t.Errorf("Read error = %v, want exactly io.EOF", err)
	}
}

// TestStreamReadResetStillReturnsStreamError: a RESET must keep surfacing
// the *StreamError it carries, never connClosedErr's io.ErrUnexpectedEOF --
// resolved via s.err before wait() is ever reached, same as the peer-CLOSE
// case above.
func TestStreamReadResetStillReturnsStreamError(t *testing.T) {
	c := newTestSchedConn(t)
	st := registerTestStream(c, 1)

	st.handleReset(CancelCode, "test reset")
	buf := make([]byte, 1)
	_, err := st.Read(buf)
	se, ok := err.(*StreamError)
	if !ok {
		t.Fatalf("error = %#v, want *StreamError", err)
	}
	if se.Code != CancelCode {
		t.Errorf("error code = %s, want CANCEL", se.Code)
	}
}

// TestStreamReadConnFailStillReturnsConnError: Conn.fail (a real protocol
// violation, not an abnormal closure) sets c.err to a *ConnError before
// c.closed fires -- connClosedErr's io.ErrUnexpectedEOF fallback must never
// override an error the conn actually recorded; Err() is non-nil, so
// connClosedErr returns it verbatim.
func TestStreamReadConnFailStillReturnsConnError(t *testing.T) {
	ws := newFakeWS()
	c := newConn(ws, RoleServer, Options{})
	c.opts.SetDefaults()
	c.session = "test-session"
	c.ourWindow = 262144
	c.peerWindow = 262144
	c.maxStreams = 64
	c.pingInterval = time.Minute
	c.pingTimeout = 5 * time.Minute
	c.finishHandshake()
	c.Run()
	defer func() { _ = ws.CloseNow() }()

	st := registerTestStream(c, 1)

	readDone := make(chan struct{})
	var readErr error
	go func() {
		buf := make([]byte, 1)
		_, readErr = st.Read(buf)
		close(readDone)
	}()

	time.Sleep(20 * time.Millisecond)
	c.fail(newConnErrorf(ProtocolErrorCode, "test protocol violation"))

	select {
	case <-readDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Read did not return within 2s of Conn.fail")
	}
	ce, ok := readErr.(*ConnError)
	if !ok {
		t.Fatalf("error = %#v, want *ConnError", readErr)
	}
	if ce.Code != ProtocolErrorCode {
		t.Errorf("error code = %s, want PROTOCOL_ERROR", ce.Code)
	}
}

// TestOpenStreamOnAbnormallyDeadConnFailsCleanly: every guard inside
// OpenStream's loop (and allocateStreamLocked) only ever checks c.err, never
// c.closed itself -- a conn that died ABNORMALLY (c.err stays nil,
// handleReadError) would otherwise sail through all of them, allocate a
// stream id, enqueue OPEN on a controlQueue nobody drains, and return
// (st, nil) as if it had actually succeeded. The cheap up-front
// `select { case <-c.closed: ...; default: }` guard must catch this before
// any of that happens.
func TestOpenStreamOnAbnormallyDeadConnFailsCleanly(t *testing.T) {
	ws := newFakeWS()
	c := newConn(ws, RoleServer, Options{})
	c.opts.SetDefaults()
	c.session = "test-session"
	c.ourWindow = 262144
	c.peerWindow = 262144
	c.maxStreams = 64
	c.pingInterval = time.Minute
	c.pingTimeout = 5 * time.Minute
	c.finishHandshake()
	c.Run()
	defer func() { _ = ws.CloseNow() }()

	_ = ws.CloseNow() // abnormal closure: c.err stays nil
	select {
	case <-c.closed:
	case <-time.After(2 * time.Second):
		t.Fatal("c.closed never closed")
	}
	if err := c.Err(); err != nil {
		t.Fatalf("Err() = %v, want nil (this test's premise)", err)
	}

	before := c.highestOpened

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	st, err := c.OpenStream(ctx)
	if err == nil {
		t.Fatalf("OpenStream succeeded (%#v) on an abnormally-dead conn, want an error", st)
	}
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Errorf("OpenStream error = %v, want io.ErrUnexpectedEOF", err)
	}
	if st != nil {
		t.Errorf("OpenStream returned a non-nil *Stream (%#v) alongside its error", st)
	}
	if c.highestOpened != before {
		t.Errorf("highestOpened = %d, want unchanged at %d (nothing should have been allocated)", c.highestOpened, before)
	}
}
