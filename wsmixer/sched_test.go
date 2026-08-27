package wsmixer

import (
	"testing"
	"time"
)

// newTestSchedConn builds a minimal *Conn suitable for driving nextChunk
// directly, without running any of its background goroutines.
func newTestSchedConn(t *testing.T) *Conn {
	t.Helper()
	ws := newFakeWS()
	c := newConn(ws, RoleServer, Options{})
	c.opts.setDefaults()
	c.session = "test"
	c.ourWindow = 262144
	c.peerWindow = 262144
	c.maxStreams = 64
	return c
}

func registerTestStream(c *Conn, id uint32) *Stream {
	st := newStream(c, id, c.ourWindow, c.peerWindow)
	c.mu.Lock()
	c.streams[id] = st
	if id > c.highestOpened {
		c.highestOpened = id
	}
	c.mu.Unlock()
	return st
}

// TestNextChunkSkipsEmptiedStreamInRotation is the regression test for the
// nextChunk bug: popping a stream whose outQueue turns out to be empty used
// to abandon the whole rotation (returning nil, false) instead of moving on
// to the next ready stream. Here stream A is marked ready twice ("double
// mark") but its lone chunk is drained out from under the scheduler before
// nextChunk runs; stream B is queued right behind it. nextChunk must skip A
// and return B's chunk in the same call, with no other write needed to make
// progress.
func TestNextChunkSkipsEmptiedStreamInRotation(t *testing.T) {
	c := newTestSchedConn(t)
	a := registerTestStream(c, 1)
	b := registerTestStream(c, 3)

	pcA := &pendingChunk{streamID: a.id, data: []byte("a"), done: make(chan struct{})}
	a.outQueue <- pcA
	c.markStreamReady(a)
	c.markStreamReady(a) // double-mark: a dedupes to one rotation entry

	// Simulate A's chunk having been drained out from under the rotation.
	<-a.outQueue

	pcB := &pendingChunk{streamID: b.id, data: []byte("b"), done: make(chan struct{})}
	b.outQueue <- pcB
	c.markStreamReady(b)

	pc, ok := c.nextChunk()
	if !ok {
		t.Fatal("nextChunk returned false with stream B still queued behind an emptied stream A")
	}
	if pc.streamID != b.id {
		t.Fatalf("nextChunk returned stream %d's chunk, want stream %d's (B)", pc.streamID, b.id)
	}

	// The rotation must now be fully drained: A was skipped (not requeued,
	// since its outQueue is empty) and B's only chunk was just taken.
	if _, ok := c.nextChunk(); ok {
		t.Fatal("nextChunk found another chunk after A and B were both drained")
	}
}

// TestNextChunkNeverSendsDataAfterReset checks that a chunk still sitting in
// outQueue when the stream is RESET is completed with the stream's error
// instead of being written to the wire (OVERVIEW.md section 2.5's sending
// table: no DATA after RESET).
func TestNextChunkNeverSendsDataAfterReset(t *testing.T) {
	c := newTestSchedConn(t)
	st := registerTestStream(c, 1)

	done := make(chan struct{})
	pc := &pendingChunk{streamID: st.id, data: []byte("late"), done: done}
	st.outQueue <- pc
	c.markStreamReady(st)

	// Reset races the in-flight chunk.
	if err := st.Reset(CancelCode, "cancelled"); err != nil {
		t.Fatalf("Reset: %v", err)
	}

	if _, ok := c.nextChunk(); ok {
		t.Fatal("nextChunk returned a chunk to write after the stream was reset")
	}

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("pending chunk was never completed after the stream was reset")
	}
	if pc.err == nil {
		t.Error("pending chunk completed with no error after the stream was reset")
	}
}

// TestNextChunkNeverSendsDataAfterCloseWrite is the CLOSE-side analogue: a
// chunk queued before CloseWrite, still sitting in outQueue when CloseWrite
// runs, must not be written afterwards either.
func TestNextChunkNeverSendsDataAfterCloseWrite(t *testing.T) {
	c := newTestSchedConn(t)
	st := registerTestStream(c, 1)

	done := make(chan struct{})
	pc := &pendingChunk{streamID: st.id, data: []byte("late"), done: done}
	st.outQueue <- pc
	c.markStreamReady(st)

	if err := st.CloseWrite(); err != nil {
		t.Fatalf("CloseWrite: %v", err)
	}

	if _, ok := c.nextChunk(); ok {
		t.Fatal("nextChunk returned a chunk to write after CloseWrite")
	}

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("pending chunk was never completed after CloseWrite")
	}
	if pc.err == nil {
		t.Error("pending chunk completed with no error after CloseWrite")
	}
}
