package wsmixer

import (
	"context"
	"io"
	"sync"
	"time"
)

// streamState is one node of the state machine in OVERVIEW.md section 2.5.
type streamState uint8

const (
	streamIdle streamState = iota
	streamOpen
	streamHalfClosedLocal  // we sent CLOSE; peer may still send DATA
	streamHalfClosedRemote // peer sent CLOSE; we may still send DATA
	streamClosed
)

func (s streamState) String() string {
	switch s {
	case streamIdle:
		return "idle"
	case streamOpen:
		return "open"
	case streamHalfClosedLocal:
		return "half_closed_local"
	case streamHalfClosedRemote:
		return "half_closed_remote"
	case streamClosed:
		return "closed"
	default:
		return "unknown"
	}
}

// maxChunk is the recommended DATA chunk size (OVERVIEW.md section 2.4): a
// sender-side default that bounds head-of-line blocking, not a wire limit.
const maxChunk = 16384

// maxSendWindow is the largest legal cumulative send-credit window: 2^31-1
// (OVERVIEW.md section 2.6 decision 1).
const maxSendWindow = (1 << 31) - 1

// pendingChunk is one outbound DATA chunk queued on its stream's outQueue,
// from which the connection's writer loop takes at most one per turn,
// round-robin across ready streams (OVERVIEW.md section 2.6 rule 3).
type pendingChunk struct {
	streamID uint32
	data     []byte
	done     chan struct{}
	err      error
}

// streamOutQueueCap bounds how many DATA chunks a stream may have queued
// ahead of the writer loop. Kept small (rather than unbounded, as a shared
// connection-wide queue would invite) so a fast writer's Write still blocks
// and backpressure holds, and so the writer's round-robin actually rotates at
// the 16 KiB chunk granularity OVERVIEW.md section 2.6 promises instead of
// one stream being able to race ahead of the others between rotations.
const streamOutQueueCap = 2

// Stream is one ws-mixer byte stream: io.Reader + io.Writer + Close +
// CloseWrite + Reset, per OVERVIEW.md section 3.2.
type Stream struct {
	id   uint32
	conn *Conn

	initialWindow int64
	openedAt      time.Time // for wsmixer_stream_duration_seconds on retirement

	mu         sync.Mutex
	state      streamState
	sendWindow int64 // credit we may still spend sending DATA
	recvWindow int64 // credit we have told the peer it may still spend
	unacked    int64 // bytes delivered to the app, not yet credited back

	buf          []byte // buffered inbound DATA, preserved across CLOSE, dropped on RESET
	eof          bool   // peer's CLOSE reached and buf drained: Read returns io.EOF
	err          error  // sticky terminal read/write error (a *StreamError or *ConnError)
	localClosed  bool   // we sent CLOSE
	remoteClosed bool   // we received CLOSE

	notifyCh chan struct{} // closed and replaced on every state change a waiter should recheck

	// outQueue is this stream's outbound DATA chunk queue. WriteContext
	// enqueues here (blocking, bounded by streamOutQueueCap -- this is where
	// backpressure lives) and only then marks the stream ready; the
	// connection's writer loop (nextChunk, sched.go) is the queue's only
	// reader. A chunk still queued once the stream can no longer send DATA
	// (CLOSE already sent, or RESET) is completed with the stream's error
	// instead of being written -- see nextChunk and sendDone.
	outQueue chan *pendingChunk
}

func newStream(conn *Conn, id uint32, initialWindow int64, sendWindow int64) *Stream {
	return &Stream{
		id:            id,
		conn:          conn,
		initialWindow: initialWindow,
		openedAt:      time.Now(),
		state:         streamOpen,
		sendWindow:    sendWindow,
		recvWindow:    initialWindow,
		notifyCh:      make(chan struct{}),
		outQueue:      make(chan *pendingChunk, streamOutQueueCap),
	}
}

// ID returns the stream's id.
func (s *Stream) ID() uint32 { return s.id }

// State reports the stream's current state, mainly for tests and diagnostics.
func (s *Stream) State() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state.String()
}

// SendWindow reports the remaining local send credit, mainly for tests.
func (s *Stream) SendWindow() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sendWindow
}

// RecvWindow reports the remaining local receive credit, mainly for
// wsmixer_recv_window_bytes sampling.
func (s *Stream) RecvWindow() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.recvWindow
}

// LastError returns the sticky terminal error recorded on this stream (a
// *StreamError from a RESET, or io.EOF once locally closed), or nil while the
// stream is still healthy.
func (s *Stream) LastError() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

func (s *Stream) notifyLocked() {
	close(s.notifyCh)
	s.notifyCh = make(chan struct{})
}

func (s *Stream) wait(ctx context.Context, ch chan struct{}) error {
	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-s.conn.closed:
		return s.conn.Err()
	}
}

// --- reading ---------------------------------------------------------------

// Read implements io.Reader. It returns io.EOF once the peer's CLOSE has been
// received and all buffered data drained, or the *StreamError from a RESET.
func (s *Stream) Read(p []byte) (int, error) { return s.ReadContext(context.Background(), p) }

// ReadContext is Read with an explicit, cancelable context.
func (s *Stream) ReadContext(ctx context.Context, p []byte) (int, error) {
	s.mu.Lock()
	for len(s.buf) == 0 {
		if s.err != nil {
			err := s.err
			s.mu.Unlock()
			return 0, err
		}
		if s.eof {
			s.mu.Unlock()
			return 0, io.EOF
		}
		ch := s.notifyCh
		s.mu.Unlock()
		if err := s.wait(ctx, ch); err != nil {
			return 0, err
		}
		s.mu.Lock()
	}
	n := copy(p, s.buf)
	s.buf = s.buf[n:]
	s.mu.Unlock()
	s.creditConsumed(int64(n))
	return n, nil
}

// creditConsumed implements the receiver-side half of OVERVIEW.md section
// 2.6's flow-control pseudocode: on app read of n, credit half the window
// back to the peer once unacked reaches window/2.
func (s *Stream) creditConsumed(n int64) {
	s.mu.Lock()
	s.unacked += n
	if !s.eof && s.remoteClosed && len(s.buf) == 0 {
		s.eof = true
	}
	threshold := s.initialWindow / 2
	var increment int64
	if s.unacked >= threshold && s.state != streamClosed {
		increment = s.unacked
		s.recvWindow += increment
		s.unacked = 0
	}
	s.mu.Unlock()
	if increment > 0 {
		s.conn.sendControlFrame(EncodeWindow(s.id, uint32(increment)))
	}
}

// --- writing ---------------------------------------------------------------

// Write implements io.Writer. It blocks in the application (never on the
// socket read loop) while the send credit window is exhausted, per
// OVERVIEW.md section 2.6.
func (s *Stream) Write(p []byte) (int, error) { return s.WriteContext(context.Background(), p) }

// WriteContext is Write with an explicit, cancelable context.
func (s *Stream) WriteContext(ctx context.Context, p []byte) (int, error) {
	total := 0
	for len(p) > 0 {
		n, err := s.reserveSendCredit(ctx, len(p))
		if err != nil {
			return total, err
		}
		// Copy the chunk: io.Writer must not retain p after Write returns, but
		// an abandoned queue entry (ctx cancelled, conn closed while pc still
		// sits in outQueue) can outlive this call and be read by writerLoop
		// after the caller has reused or freed p's backing array.
		chunk := append([]byte(nil), p[:n]...)
		p = p[n:]

		done := make(chan struct{})
		pc := &pendingChunk{streamID: s.id, data: chunk, done: done}
		select {
		case s.outQueue <- pc:
		case <-ctx.Done():
			return total, ctx.Err()
		case <-s.conn.closed:
			return total, s.conn.Err()
		}
		s.conn.markStreamReady(s)
		select {
		case <-done:
			if pc.err != nil {
				return total, pc.err
			}
		case <-ctx.Done():
			// pc is already queued and may still reach the wire after this
			// call returns; the returned n does not count those bytes.
			return total, ctx.Err()
		case <-s.conn.closed:
			return total, s.conn.Err()
		}
		total += n
	}
	return total, nil
}

func (s *Stream) reserveSendCredit(ctx context.Context, want int) (int, error) {
	var blockedSince time.Time // zero until the first time this call actually has to wait
	for {
		s.mu.Lock()
		if s.err != nil {
			err := s.err
			s.mu.Unlock()
			return 0, err
		}
		if s.state != streamOpen && s.state != streamHalfClosedRemote {
			s.mu.Unlock()
			return 0, &StreamError{Code: StreamClosedCode, StreamID: s.id, Message: "write on a stream that is not open for sending"}
		}
		if s.sendWindow > 0 {
			n := int64(want)
			if n > s.sendWindow {
				n = s.sendWindow
			}
			if n > maxChunk {
				n = maxChunk
			}
			s.sendWindow -= n
			s.mu.Unlock()
			if !blockedSince.IsZero() {
				s.conn.opts.Metrics.SendWindowBlocked(s.conn.session, s.id, time.Since(blockedSince))
			}
			return int(n), nil
		}
		if blockedSince.IsZero() {
			blockedSince = time.Now()
		}
		ch := s.notifyCh
		s.mu.Unlock()
		if err := s.wait(ctx, ch); err != nil {
			return 0, err
		}
	}
}

// --- half-close / reset ------------------------------------------------------

// CloseWrite sends CLOSE: "I will send no more DATA on this stream." The peer
// may keep sending for as long as it likes.
func (s *Stream) CloseWrite() error {
	s.mu.Lock()
	switch s.state {
	case streamOpen:
		s.state = streamHalfClosedLocal
	case streamHalfClosedRemote:
		s.state = streamClosed
	case streamHalfClosedLocal, streamClosed:
		s.mu.Unlock()
		return nil // already sent, or nothing to send to
	default:
		s.mu.Unlock()
		return &StreamError{Code: ProtocolErrorCode, StreamID: s.id, Message: "CloseWrite on a stream that was never opened"}
	}
	s.localClosed = true
	closedNow := s.state == streamClosed
	s.notifyLocked()
	s.mu.Unlock()

	s.conn.sendControlFrame(EncodeClose(s.id))
	if closedNow {
		s.conn.retireStream(s.id)
	}
	return nil
}

// Close sends CLOSE (if not already sent) and stops delivering further
// reads. If the peer has not yet half-closed its own send side, a plain
// CloseWrite would leave it writing into a window this side has stopped
// draining, stalling it at credit 0 forever -- so Close also RESETs the
// stream with CANCEL in that case. If the peer already sent CLOSE, there is
// nothing left to cancel, and Close is just CloseWrite plus releasing reads.
func (s *Stream) Close() error {
	s.mu.Lock()
	remoteClosed := s.remoteClosed
	s.mu.Unlock()

	if !remoteClosed {
		if err := s.CloseWrite(); err != nil {
			return err
		}
		return s.Reset(CancelCode, "local Close: peer had not finished sending")
	}

	err := s.CloseWrite()
	s.mu.Lock()
	if s.err == nil {
		s.err = io.EOF
	}
	s.notifyLocked()
	s.mu.Unlock()
	return err
}

// Reset aborts the stream in both directions with the given error code and an
// optional human-readable message, discarding any buffered data. It is a
// no-op if the stream is already fully closed. Never call Reset in answer to
// a received RESET (loop prevention, OVERVIEW.md section 2.5).
func (s *Stream) Reset(code ErrorCode, msg string) error {
	s.mu.Lock()
	if s.state == streamClosed {
		s.mu.Unlock()
		return nil
	}
	s.state = streamClosed
	s.buf = nil
	s.err = &StreamError{Code: code, StreamID: s.id, Message: msg}
	s.notifyLocked()
	s.mu.Unlock()

	s.conn.sendControlFrame(EncodeReset(s.id, code, msg))
	s.conn.retireStream(s.id)
	return nil
}

// --- inbound frame handling (called from the connection's read loop) -------

// handleData applies a received DATA frame. n > remaining recv credit is a
// connection-fatal FLOW_CONTROL_ERROR (OVERVIEW.md section 2.6 decision 1);
// the caller is responsible for turning this into a full connection error.
func (s *Stream) handleData(payload []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch s.state {
	case streamOpen, streamHalfClosedLocal:
		// legal: buffer and debit credit.
	case streamHalfClosedRemote:
		return &StreamError{Code: StreamClosedCode, StreamID: s.id, Message: "DATA received after this stream's CLOSE"}
	case streamClosed:
		return &StreamError{Code: StreamClosedCode, StreamID: s.id, Message: "DATA received on a closed stream"}
	}

	n := int64(len(payload))
	if n > s.recvWindow {
		return newConnErrorf(FlowControlError, "stream %d: received %d DATA bytes with %d credit remaining", s.id, n, s.recvWindow)
	}
	s.recvWindow -= n
	s.buf = append(s.buf, payload...)
	s.notifyLocked()
	return nil
}

// handleWindow applies a received WINDOW frame's credit increment.
func (s *Stream) handleWindow(increment uint32) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	// WINDOW must be tolerated on a half-closed or fully-closed stream: it is
	// the one race the ordered transport does not remove (OVERVIEW.md section
	// 2.5). Only reject the cumulative overflow case.
	newWindow := s.sendWindow + int64(increment)
	if newWindow > maxSendWindow {
		return newConnErrorf(FlowControlError, "stream %d: WINDOW would push the send window to %d, past 2^31-1", s.id, newWindow)
	}
	if s.state == streamClosed {
		return nil
	}
	s.sendWindow = newWindow
	s.notifyLocked()
	return nil
}

// handleClose applies a received CLOSE frame.
func (s *Stream) handleClose() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch s.state {
	case streamOpen:
		s.state = streamHalfClosedRemote
	case streamHalfClosedLocal:
		s.state = streamClosed
	case streamHalfClosedRemote:
		return &StreamError{Code: StreamClosedCode, StreamID: s.id, Message: "duplicate CLOSE received"}
	case streamClosed:
		return nil
	}
	s.remoteClosed = true
	if len(s.buf) == 0 {
		s.eof = true
	}
	s.notifyLocked()
	return nil
}

// handleReset applies a received RESET frame: any state, discard the buffer,
// go to closed.
func (s *Stream) handleReset(code ErrorCode, msg string) {
	s.mu.Lock()
	s.state = streamClosed
	s.buf = nil
	s.err = &StreamError{Code: code, StreamID: s.id, Message: msg}
	s.notifyLocked()
	s.mu.Unlock()
}

// isTerminal reports whether the stream is fully closed (CLOSE both ways, or
// RESET either way): the point at which both ends retire the id forever.
func (s *Stream) isTerminal() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state == streamClosed
}

// sendDone reports whether this stream must no longer send DATA -- CLOSE
// already sent (half-closed local or fully closed) or RESET either way, per
// OVERVIEW.md section 2.5's sending table -- and if so, the error a chunk
// still queued for it should be completed with instead of being written.
func (s *Stream) sendDone() (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch s.state {
	case streamHalfClosedLocal, streamClosed:
		if s.err != nil {
			return true, s.err
		}
		return true, &StreamError{Code: StreamClosedCode, StreamID: s.id, Message: "DATA not sent: CLOSE already sent on this stream"}
	default:
		return false, nil
	}
}
