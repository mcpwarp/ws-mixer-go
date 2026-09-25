package wsmixer

// Tests for the regression connClosedErr (stream_abnormal_close_test.go)
// introduced: a reader parked in Stream.wait() when a CLOSE/RESET/buffered
// DATA event and the conn's own death become ready in the same instant can
// have wait()'s select pick either case. Before ReadContext's re-check
// (stream.go), taking the <-s.conn.closed case returned wait()'s error
// straight through even though the stream itself had already ended cleanly
// (or with a RESET) -- a clean io.EOF (or a RESET's *StreamError) lost to a
// synthesized io.ErrUnexpectedEOF purely because of which branch the select
// happened to pick.
//
// None of these tests are reachable through stream_abnormal_close_test.go's
// own "normal paths unchanged" tests: those resolve entirely inside
// ReadContext's own s.buf/s.eof/s.err checks, before wait() is ever reached
// at all -- the coverage gap this file closes is specifically a reader
// already PARKED IN wait() when the terminal state lands.
//
// Each test parks a reader (which requires the state to still be "clean" --
// buf empty, err nil, eof false -- at park time, or ReadContext resolves
// before ever reaching wait(), the coverage gap above), then sets the
// terminal state DIRECTLY under s.mu -- deliberately not through
// handleClose/handleReset/handleData, which also close the stream's own
// notifyCh -- so the ONLY channel the parked wait() can ever observe become
// ready is s.conn.closed. This makes the exact race deterministic and
// independent of goroutine-scheduling luck (which, empirically in this
// environment, overwhelmingly lets the notify channel win on its own,
// masking the bug the reviewer measured 400/400 wrong under harsher timing):
// wait() is forced through its <-s.conn.closed branch on every trial, with
// the terminal state already sitting there for ReadContext's re-check to
// find -- exactly the contract under test, decoupled from wait()'s own
// (already-proven-correct, stream_abnormal_close_test.go) channel-select
// nondeterminism.

import (
	"io"
	"testing"
	"time"
)

// parkedRead starts a Read in a goroutine and gives it a moment to actually
// enter Stream.wait()'s blocked select (buf/err/eof must still be clean at
// this point, or ReadContext resolves before ever reaching wait()).
func parkedRead(st *Stream, buf []byte) chan struct {
	n   int
	err error
} {
	resultCh := make(chan struct {
		n   int
		err error
	}, 1)
	go func() {
		n, err := st.Read(buf)
		resultCh <- struct {
			n   int
			err error
		}{n, err}
	}()
	time.Sleep(5 * time.Millisecond)
	return resultCh
}

func waitParkedRead(t *testing.T, resultCh chan struct {
	n   int
	err error
}) (int, error) {
	t.Helper()
	select {
	case r := <-resultCh:
		return r.n, r.err
	case <-time.After(2 * time.Second):
		t.Fatal("parked Read did not return within 2s")
		return 0, nil
	}
}

// TestParkedReadCleanCloseRacingAbnormalConnDeathReturnsIOEOF is the core
// regression: a reader parked in wait() when the peer's clean CLOSE has
// landed (s.eof set) right as the conn dies abnormally (no ws-mixer error at
// all, c.err nil) must still return exactly io.EOF, never
// io.ErrUnexpectedEOF.
func TestParkedReadCleanCloseRacingAbnormalConnDeathReturnsIOEOF(t *testing.T) {
	c := newTestSchedConn(t)
	st := registerTestStream(c, 1)
	buf := make([]byte, 1)
	resultCh := parkedRead(st, buf)

	st.mu.Lock()
	st.remoteClosed = true
	st.eof = true
	st.mu.Unlock()
	close(c.closed) // abnormal conn death: c.err stays nil

	_, err := waitParkedRead(t, resultCh)
	if err != io.EOF {
		t.Fatalf("Read error = %v, want exactly io.EOF (a clean CLOSE must win the race with an abnormal conn death)", err)
	}
}

// TestParkedReadBufferedDataRacingAbnormalConnDeathReturnsData: DATA (still
// unread) has landed, with the conn dying abnormally right behind it. The
// parked Read must return the buffered data (not an error) since data was,
// in fact, received -- and only the NEXT Read, once that data is drained,
// returns io.EOF.
func TestParkedReadBufferedDataRacingAbnormalConnDeathReturnsData(t *testing.T) {
	c := newTestSchedConn(t)
	st := registerTestStream(c, 1)
	buf := make([]byte, 16)
	resultCh := parkedRead(st, buf)

	st.mu.Lock()
	st.buf = []byte("hello")
	st.remoteClosed = true // the peer's CLOSE landed right behind the DATA
	// eof stays false: creditConsumed only sets it once buf actually drains
	// (the real handleClose/creditConsumed behavior this mirrors).
	st.mu.Unlock()
	close(c.closed) // abnormal conn death: c.err stays nil

	n, err := waitParkedRead(t, resultCh)
	if err != nil {
		t.Fatalf("first Read error = %v, want nil (the buffered data must win the race)", err)
	}
	if got := string(buf[:n]); got != "hello" {
		t.Fatalf("first Read = %q, want %q", got, "hello")
	}

	n2, err2 := st.Read(buf)
	if n2 != 0 || err2 != io.EOF {
		t.Fatalf("second Read = (%d, %v), want (0, io.EOF)", n2, err2)
	}
}

// TestParkedReadResetRacingAbnormalConnDeathReturnsStreamError: a RESET
// (which sets s.err) has landed right as the conn dies abnormally -- the
// *StreamError the RESET carries must win, never connClosedErr's
// io.ErrUnexpectedEOF.
func TestParkedReadResetRacingAbnormalConnDeathReturnsStreamError(t *testing.T) {
	c := newTestSchedConn(t)
	st := registerTestStream(c, 1)
	buf := make([]byte, 1)
	resultCh := parkedRead(st, buf)

	st.mu.Lock()
	st.state = streamClosed
	st.buf = nil
	st.err = &StreamError{Code: CancelCode, StreamID: st.id, Message: "test reset racing conn death"}
	st.mu.Unlock()
	close(c.closed) // abnormal conn death: c.err stays nil

	_, err := waitParkedRead(t, resultCh)
	se, ok := err.(*StreamError)
	if !ok {
		t.Fatalf("error = %#v, want *StreamError", err)
	}
	if se.Code != CancelCode {
		t.Fatalf("error code = %s, want CANCEL", se.Code)
	}
}

// TestParkedReadCleanCloseRacingPeerErrorReturnsIOEOF is the non-nil-conn-
// error variant: the conn ends with a REAL ws-mixer error (a peer error{}
// message, via handlePeerError) rather than an abnormal closure, landing
// right as the stream's own clean CLOSE does. A clean io.EOF must still win
// -- connClosedErr's fallback to c.Err() only matters when there is no local
// s.eof/s.err to prefer instead.
func TestParkedReadCleanCloseRacingPeerErrorReturnsIOEOF(t *testing.T) {
	c := newTestSchedConn(t)
	st := registerTestStream(c, 1)
	buf := make([]byte, 1)
	resultCh := parkedRead(st, buf)

	st.mu.Lock()
	st.remoteClosed = true
	st.eof = true
	st.mu.Unlock()
	c.handlePeerError(&ErrorMsg{T: "error", Code: uint32(InternalErrorCode), Message: "test peer error"})

	_, err := waitParkedRead(t, resultCh)
	if err != io.EOF {
		t.Fatalf("Read error = %v, want exactly io.EOF (a clean CLOSE must win even against a real peer error{})", err)
	}
}

// TestParkedReadCleanCloseRacingLocalFailReturnsIOEOF is the same again with
// a LOCAL fail() (a protocol violation this side raised) instead of a peer
// error{}.
func TestParkedReadCleanCloseRacingLocalFailReturnsIOEOF(t *testing.T) {
	c := newTestSchedConn(t)
	st := registerTestStream(c, 1)
	buf := make([]byte, 1)
	resultCh := parkedRead(st, buf)

	st.mu.Lock()
	st.remoteClosed = true
	st.eof = true
	st.mu.Unlock()
	c.fail(newConnErrorf(ProtocolErrorCode, "test local protocol violation racing a clean CLOSE"))

	_, err := waitParkedRead(t, resultCh)
	if err != io.EOF {
		t.Fatalf("Read error = %v, want exactly io.EOF (a clean CLOSE must win even against a local fail())", err)
	}
}
