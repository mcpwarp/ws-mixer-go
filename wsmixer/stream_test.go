package wsmixer

import (
	"io"
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
