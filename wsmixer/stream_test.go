package wsmixer

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"
)

func nextControlFrame(t *testing.T, c *Conn) *Frame {
	t.Helper()
	select {
	case b := <-c.controlQueue:
		f, err := DecodeFrame(b)
		if err != nil {
			t.Fatalf("DecodeFrame: %v", err)
		}
		return f
	case <-time.After(time.Second):
		t.Fatal("no control frame enqueued")
		return nil
	}
}

// TestStreamCloseResetsWhenPeerStillSending checks that Close on a stream
// the peer has not half-closed sends CLOSE then RESET(CANCEL): a plain
// CloseWrite would leave the peer writing into a window this side has
// stopped draining, stalling it at credit 0 forever.
func TestStreamCloseResetsWhenPeerStillSending(t *testing.T) {
	c := newTestSchedConn(t)
	st := registerTestStream(c, 1)

	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	f1 := nextControlFrame(t, c)
	if f1.Type != FrameClose || f1.StreamID != st.id {
		t.Fatalf("first frame = %+v, want CLOSE(%d)", f1, st.id)
	}
	f2 := nextControlFrame(t, c)
	if f2.Type != FrameReset || f2.StreamID != st.id {
		t.Fatalf("second frame = %+v, want RESET(%d)", f2, st.id)
	}
	if f2.ResetCode() != CancelCode {
		t.Errorf("reset code = %s, want CANCEL", f2.ResetCode())
	}
	if st.State() != "closed" {
		t.Errorf("state = %s, want closed", st.State())
	}
}

// WIRE.md §2.8: Reset refuses a connection-only code locally (an error
// wrapping ErrResetCode, nothing sent, the stream untouched) on either role,
// and accepts a stream code and an application code.
func TestStreamResetRejectsConnectionOnlyCodes(t *testing.T) {
	for _, role := range []Role{RoleServer, RoleClient} {
		c := newTestSchedConn(t)
		c.role = role
		st := registerTestStream(c, 1)
		for _, code := range []ErrorCode{NoError, FlowControlError, FrameSizeError, UnsupportedCode,
			UnauthorizedCode, GoingAwayCode, KeepaliveTimeout, ApplicationCloseCode} {
			err := st.Reset(code, "x")
			if !errors.Is(err, ErrResetCode) || !strings.Contains(err.Error(), code.String()) {
				t.Fatalf("%s: Reset(%s) = %v, want an ErrResetCode naming the code", role, code, err)
			}
			select {
			case b := <-c.controlQueue:
				t.Fatalf("%s: Reset(%s) sent %x", role, code, b)
			default:
			}
			if st.State() != "open" {
				t.Fatalf("%s: state %s after a refused Reset(%s), want open", role, st.State(), code)
			}
		}
		if err := st.Reset(CancelCode, "x"); err != nil {
			t.Fatalf("%s: Reset(CANCEL) = %v", role, err)
		}
		if f := nextControlFrame(t, c); f.Type != FrameReset || f.ResetCode() != CancelCode {
			t.Fatalf("%s: frame %+v, want RESET(CANCEL)", role, f)
		}
		st2 := registerTestStream(c, 3)
		if err := st2.Reset(ErrorCode(0x1000_0001), "app"); err != nil {
			t.Fatalf("%s: Reset(app code) = %v", role, err)
		}
		nextControlFrame(t, c)
	}
}

// Reset retires the stream before its RESET is queued: a peer frame for the
// id that lands while the RESET is still on its way out (here: the control
// queue is full, so Reset is stuck in its send) is discarded as stale (WIRE.md
// §2.5), never answered with RESET(PROTOCOL_ERROR) for a stream this side
// already reset (window_stale_retired_stream).
func TestStreamResetRetiresBeforeSending(t *testing.T) {
	c := newTestSchedConn(t)
	st := registerTestStream(c, 1)
	for i := 0; i < cap(c.controlQueue); i++ {
		c.controlQueue <- []byte("filler")
	}
	resetDone := make(chan struct{})
	go func() {
		defer close(resetDone)
		_ = st.Reset(CancelCode, "local reset")
	}()
	deadline := time.Now().Add(2 * time.Second)
	for st.State() != "closed" && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond) // Reset is now blocked in its send
	windowDone := make(chan struct{})
	go func() {
		defer close(windowDone)
		c.handleStreamWindow(1, 0)
	}()
	deadline = time.Now().Add(2 * time.Second)
	for c.stats.staleFrames.Load() == 0 && time.Now().Before(deadline) {
		select {
		case <-windowDone:
		default:
		}
		time.Sleep(time.Millisecond)
	}
	if n := c.stats.staleFrames.Load(); n != 1 {
		t.Fatalf("%d stale frames, want the WINDOW discarded as stale", n)
	}
	// Drain the queue: the fillers, then the one RESET(CANCEL), no RESET(PROTOCOL_ERROR).
	var resets []ErrorCode
	for done := false; !done; {
		select {
		case b := <-c.controlQueue:
			if string(b) == "filler" {
				continue
			}
			f, err := DecodeFrame(b)
			if err != nil {
				t.Fatal(err)
			}
			if f.Type == FrameReset {
				resets = append(resets, f.ResetCode())
			}
		case <-time.After(200 * time.Millisecond):
			done = true
		}
	}
	<-resetDone
	<-windowDone
	if len(resets) != 1 || resets[0] != CancelCode {
		t.Fatalf("RESET frames %v, want exactly [CANCEL]", resets)
	}
}

// TestStreamWriteAfterCloseWriteFails is the mutation-testing counterpart to
// spec/fixtures/sequences/data_after_close_toward_client.json: that fixture
// exercises a RECEIVER correctly RESETing a peer that illegally sends DATA
// after its own CLOSE, but says nothing about the SENDER's own guard against
// producing that stray DATA in the first place. CloseWrite sets state to
// half_closed_local (or closed, if the peer had already half-closed); a
// subsequent WriteContext must fail with STREAM_CLOSED rather than silently
// queuing a chunk that would violate WIRE.md §2.5's "MUST NOT
// send" row for half-closed(local)/closed.
func TestStreamWriteAfterCloseWriteFails(t *testing.T) {
	c := newTestSchedConn(t)
	st := registerTestStream(c, 1)

	if err := st.CloseWrite(); err != nil {
		t.Fatalf("CloseWrite: %v", err)
	}
	if st.State() != "half_closed_local" {
		t.Fatalf("state after CloseWrite = %s, want half_closed_local", st.State())
	}

	_, err := st.WriteContext(context.Background(), []byte("stray"))
	if err == nil {
		t.Fatal("WriteContext after CloseWrite succeeded, want an error")
	}
	se, ok := err.(*StreamError)
	if !ok {
		t.Fatalf("error = %#v, want *StreamError", err)
	}
	if se.Code != StreamClosedCode {
		t.Errorf("error code = %s, want STREAM_CLOSED", se.Code)
	}
}

// TestStreamCloseIsPlainWhenPeerAlreadyClosed checks that Close on a stream
// whose peer already sent CLOSE is just CloseWrite plus releasing local
// reads: there is nothing left to cancel.
func TestStreamCloseIsPlainWhenPeerAlreadyClosed(t *testing.T) {
	c := newTestSchedConn(t)
	st := registerTestStream(c, 1)

	st.mu.Lock()
	st.state = streamHalfClosedRemote
	st.remoteClosed = true
	st.eof = true
	st.mu.Unlock()

	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	f1 := nextControlFrame(t, c)
	if f1.Type != FrameClose || f1.StreamID != st.id {
		t.Fatalf("frame = %+v, want CLOSE(%d)", f1, st.id)
	}
	select {
	case b := <-c.controlQueue:
		f, _ := DecodeFrame(b)
		t.Fatalf("unexpected extra control frame: %+v", f)
	case <-time.After(50 * time.Millisecond):
	}
	if st.LastError() != io.EOF {
		t.Errorf("LastError() = %v, want io.EOF", st.LastError())
	}
}
