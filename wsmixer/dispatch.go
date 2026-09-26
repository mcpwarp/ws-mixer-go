package wsmixer

// This file holds the connection's frame- and control-message dispatch: the
// read loop calls dispatch() for every decoded frame, which routes stream
// frames (OPEN/DATA/WINDOW/CLOSE/RESET) and stream-0 control messages
// (hello/welcome/ping/pong/drain/error/app) per WIRE.md §2.5 and §2.7.

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/coder/websocket"
)

// --- inbound plumbing --------------------------------------------------------

func (c *Conn) readerLoop() {
	for {
		_, data, err := c.ws.Read(context.Background())
		if err != nil {
			c.handleReadError(err)
			return
		}
		c.stats.bytesIn.Add(int64(len(data)))
		frame, ferr := DecodeFrame(data)
		if ferr != nil {
			switch e := ferr.(type) {
			case *ConnError:
				c.failProtocol(e)
				return
			case *StreamError:
				c.resetStream(e.StreamID, e.Code, e.Message)
			}
			continue
		}
		if !frame.Type.Known() {
			c.opts.Metrics.UnknownFrameType(c.session, uint8(frame.Type))
			c.stats.unknownFrameTypes.Add(1)
			continue
		}
		if c.dispatch(frame) {
			return // connection failed
		}
	}
}

func (c *Conn) handleReadError(err error) {
	code := websocket.CloseStatus(err)
	var ce websocket.CloseError
	reason := ""
	if errors.As(err, &ce) {
		reason = ce.Reason
	}
	c.mu.Lock()
	c.observedCloseCode = int(code)
	// A self-initiated close (fail/Close/handlePeerError) can hand this read
	// back a CloseError carrying this side's own outgoing code/reason: the
	// peer echoes the code/reason it just received verbatim, and this
	// blocked Read (which already holds coder/websocket's read lock) parses
	// that echo the normal way and returns it here -- so only ever record it
	// as the peer's reason when this side did not initiate the close itself.
	if !c.localCloseInitiated {
		c.observedCloseReason = reason
	}
	if code != -1 && c.err == nil {
		c.err = fmt.Errorf("ws-mixer: peer closed with code %d", code)
	}
	c.mu.Unlock()
	c.closeOnce.Do(func() {
		// Guarantee the transport is actually released here: a Read error
		// (peer TCP reset, timeout, malformed close frame) does not always
		// mean the library already tore the socket down, and nothing else on
		// this path calls Close/CloseNow for it (wsmixerserver's Listener has its
		// own defer ws.CloseNow(), but that only fires once ServeHTTP itself
		// returns, and the client has no equivalent).
		_ = c.ws.CloseNow()
		close(c.closed)
	})
}

// dispatch routes one decoded frame. It returns true if the connection has
// been failed and the read loop must stop.
func (c *Conn) dispatch(f *Frame) bool {
	if f.StreamID == 0 {
		if f.Type != FrameData {
			c.failProtocol(newConnErrorf(ProtocolErrorCode, "%s is not legal on stream 0 (control channel)", f.Type))
			return true
		}
		return c.handleControlData(f.Payload)
	}
	if !c.handshakeDone {
		c.failProtocol(newConnErrorf(ProtocolErrorCode, "%s frame received before hello/welcome completed the handshake", f.Type))
		return true
	}
	switch f.Type {
	case FrameOpen:
		return c.handleRemoteOpen(f.StreamID)
	case FrameData:
		return c.handleStreamData(f.StreamID, f.Payload)
	case FrameWindow:
		return c.handleStreamWindow(f.StreamID, f.WindowIncrement())
	case FrameClose:
		return c.handleStreamClose(f.StreamID)
	case FrameReset:
		return c.handleStreamReset(f.StreamID, f.ResetCode(), f.ResetMessage())
	}
	return false
}

func (c *Conn) handleRemoteOpen(id uint32) bool {
	if c.role == RoleServer {
		c.failProtocol(newConnErrorf(ProtocolErrorCode, "OPEN received from the client; only the server opens streams"))
		return true
	}
	c.mu.Lock()
	if id <= c.highestOpened {
		c.mu.Unlock()
		c.failProtocol(newConnErrorf(ProtocolErrorCode, "OPEN for id %d is not greater than highest_opened %d", id, c.highestOpened))
		return true
	}
	// WIRE.md §2.7 Drain: an OPEN with id > last_stream_id after
	// the server has drained means the server violated the boundary it
	// already promised -- connection-fatal, not a stream-scoped
	// REFUSED_STREAM, because only the server opens streams and this is
	// therefore always a peer protocol violation, never self-inflicted.
	if c.draining && id > c.peerLastStreamID {
		lastStreamID := c.peerLastStreamID
		c.mu.Unlock()
		c.failProtocol(newConnErrorf(ProtocolErrorCode, "OPEN for id %d exceeds drain last_stream_id %d", id, lastStreamID))
		return true
	}
	c.highestOpened = id
	overLimit := int64(len(c.streams)) >= c.maxStreams
	declared := c.declaredMaxStreams
	if declared <= 0 {
		declared = c.maxStreams
	}
	var st *Stream
	if !overLimit {
		st = newStream(c, id, c.ourWindow, c.peerWindow)
		c.streams[id] = st
	}
	streamCount := int64(len(c.streams))
	c.mu.Unlock()

	if overLimit {
		c.sendControlFrame(EncodeReset(id, StreamLimitCode, fmt.Sprintf("max_streams=%d exceeded by stream %d", c.maxStreams, id)))
		c.stats.refusedOpens.Add(1)
		// Only an OPEN that also exceeds the max_streams ceiling this side
		// actually declared to the peer (welcome.max_streams / hello) counts
		// as peer misbehavior. A refusal caused purely by this side
		// unilaterally lowering its own effective cap further is expected,
		// self-inflicted behavior (WIRE.md §2.7: the client "MAY
		// lower it further to its own ceiling") and must never escalate.
		if streamCount >= declared && c.repeatRefusedOpen() {
			c.failProtocol(newConnErrorf(EnhanceYourCalm, "%d refused OPENs (STREAM_LIMIT) within %s", c.refusedOpenLimit(), c.refusedOpenWindowDuration()))
			return true
		}
		return false
	}
	c.opts.Metrics.StreamOpened(c.session, id)
	// Route through deliveryQueue (not a bare `go`) so OnStream fires from
	// the same goroutine, and in the same wire order, as OnApp/OnDrain. A
	// handler that blocks for a long time must spawn its own goroutine (see
	// OnStream's doc comment); overflow still fails the connection with
	// ENHANCE_YOUR_CALM via enqueueDelivery. Note that st can already be reset
	// by the time the OnStream callback actually runs off this queue (e.g. a
	// concurrent Drain, see drain.go, resets every surviving stream at its
	// deadline): the callback must not assume st is still live, and Read on
	// it then returns the StreamError that reset it.
	if !c.enqueueDelivery(deliveryEvent{open: st}) {
		return true
	}
	return false
}

// lookupLiveStream classifies an id per WIRE.md §2.5's receiving
// table: neverOpened means "id > highest_opened" (always connection-fatal for
// a non-OPEN frame); otherwise ok reports whether a live *Stream still exists.
func (c *Conn) lookupLiveStream(id uint32) (st *Stream, neverOpened bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if id > c.highestOpened {
		return nil, true
	}
	return c.streams[id], false
}

func (c *Conn) handleStreamData(id uint32, payload []byte) bool {
	st, neverOpened := c.lookupLiveStream(id)
	if neverOpened {
		c.failProtocol(newConnErrorf(ProtocolErrorCode, "DATA for stream %d, which was never opened", id))
		return true
	}
	if st == nil {
		c.discardStale(id, "DATA")
		return false
	}
	if err := st.handleData(payload); err != nil {
		return c.handleStreamErr(id, err)
	}
	c.opts.Metrics.BytesTransferred(c.session, "recv", int64(len(payload)))
	c.opts.Metrics.RecvWindowSample(c.session, id, st.RecvWindow())
	return false
}

func (c *Conn) handleStreamWindow(id uint32, inc uint32) bool {
	st, neverOpened := c.lookupLiveStream(id)
	if neverOpened {
		c.failProtocol(newConnErrorf(ProtocolErrorCode, "WINDOW for stream %d, which was never opened", id))
		return true
	}
	if st == nil {
		c.discardStale(id, "WINDOW")
		return false
	}
	if err := st.handleWindow(inc); err != nil {
		return c.handleStreamErr(id, err)
	}
	c.opts.Metrics.WindowUpdate(c.session, id, inc)
	return false
}

func (c *Conn) handleStreamClose(id uint32) bool {
	st, neverOpened := c.lookupLiveStream(id)
	if neverOpened {
		c.failProtocol(newConnErrorf(ProtocolErrorCode, "CLOSE for stream %d, which was never opened", id))
		return true
	}
	if st == nil {
		c.discardStale(id, "CLOSE")
		return false
	}
	if err := st.handleClose(); err != nil {
		return c.handleStreamErr(id, err)
	}
	if st.isTerminal() {
		c.retireStream(id)
	}
	return false
}

func (c *Conn) handleStreamReset(id uint32, code ErrorCode, msg string) bool {
	st, neverOpened := c.lookupLiveStream(id)
	if neverOpened {
		c.failProtocol(newConnErrorf(ProtocolErrorCode, "RESET for stream %d, which was never opened", id))
		return true
	}
	if st == nil {
		c.discardStale(id, "RESET")
		return false
	}
	st.handleReset(code, msg)
	c.retireStream(id)
	c.opts.Metrics.StreamReset(c.session, id, code)
	return false
}

// handleStreamErr turns a Stream state-machine violation into a RESET (stream
// error) or a full connection failure (connection error).
func (c *Conn) handleStreamErr(id uint32, err error) bool {
	switch e := err.(type) {
	case *ConnError:
		c.failProtocol(e)
		return true
	case *StreamError:
		c.resetStream(id, e.Code, e.Message)
		return false
	}
	return false
}

// resetStream sends RESET for a stream-scoped violation and mirrors the
// closed state locally, e.g. DATA/CLOSE arriving after the peer's CLOSE.
func (c *Conn) resetStream(id uint32, code ErrorCode, msg string) {
	c.sendControlFrame(EncodeReset(id, code, msg))
	c.mu.Lock()
	st := c.streams[id]
	c.mu.Unlock()
	if st != nil {
		st.handleReset(code, msg)
		c.retireStream(id)
	}
}

func (c *Conn) discardStale(id uint32, frameType string) {
	c.opts.Metrics.StaleFrameDiscarded(c.session, id)
	c.stats.staleFrames.Add(1)
	c.opts.Logger.Debug("discarding frame for a stream that is no longer live", "stream_id", id, "frame_type", frameType)
}

// retireStream drops a stream's table entry once it is fully closed: both
// ends then stop tracking credit and the id is retired forever.
func (c *Conn) retireStream(id uint32) {
	c.mu.Lock()
	st, existed := c.streams[id]
	delete(c.streams, id)
	c.notifyConnLocked()
	c.mu.Unlock()
	if existed {
		c.opts.Metrics.StreamClosed(c.session, id, time.Since(st.openedAt))
	}
}

// --- control channel dispatch -------------------------------------------------

func (c *Conn) handleControlData(payload []byte) bool {
	// Token bucket over stream-0 message throughput (WIRE.md §2.7:
	// "app is ... subject to the same stream-0 rate limit as everything
	// else"). Checked before parsing so a flood of junk can't burn CPU on top
	// of exhausting the bucket.
	if !c.stream0Bucket.Allow() {
		c.failProtocol(newConnErrorf(EnhanceYourCalm, "stream-0 message rate exceeded %.0f/s (burst %.0f)", c.stream0Bucket.rate, c.stream0Bucket.capacity))
		return true
	}
	msg, err := ParseControl(payload)
	if err != nil {
		c.failProtocol(err.(*ConnError))
		return true
	}

	// Both hello and welcome are always completed synchronously before Run()
	// starts the read loop (accept.go's AcceptConn, client.go's
	// clientHandshake) — production never has handshakeDone false with the
	// dispatch loop already running, so any stream-0 message seen here in
	// that state is itself a protocol violation, not a handshake step.
	if !c.handshakeDone {
		c.failProtocol(newConnErrorf(ProtocolErrorCode, "frame received before hello/welcome completed the handshake"))
		return true
	}

	switch m := msg.(type) {
	case *HelloMsg:
		c.failProtocol(newConnErrorf(ProtocolErrorCode, "second hello received after the handshake already completed"))
		return true
	case *WelcomeMsg:
		c.failProtocol(newConnErrorf(ProtocolErrorCode, "unexpected welcome after the handshake already completed"))
		return true
	case *PingMsg:
		c.opts.Metrics.ControlMessage(c.session, "ping", "recv")
		_ = c.sendControl(&PongMsg{T: "pong", ID: m.ID, TS: m.TS})
	case *PongMsg:
		c.opts.Metrics.ControlMessage(c.session, "pong", "recv")
		if fatal := c.handlePong(m); fatal {
			return true
		}
	case *DrainMsg:
		c.opts.Metrics.ControlMessage(c.session, "drain", "recv")
		if fatal := c.handleDrainMsg(m); fatal {
			return true
		}
	case *ErrorMsg:
		c.opts.Metrics.ControlMessage(c.session, "error", "recv")
		c.handlePeerError(m)
		return true
	case *AppMsg:
		c.opts.Metrics.ControlMessage(c.session, "app", "recv")
		c.opts.Metrics.AppMessage(c.session, "recv")
		if !c.enqueueDelivery(deliveryEvent{app: m.Body}) {
			return true
		}
	}
	return false
}

// handlePong implements the watermark scheme for ping ids: two integers plus
// the sparse still-outstanding map, instead of an unbounded "seen" set. Every
// id below lowestUnacked has been acked (or pruned as stale) at least once,
// and no id >= nextPingID has ever been sent.
func (c *Conn) handlePong(m *PongMsg) (fatal bool) {
	c.mu.Lock()
	if m.ID >= c.nextPingID {
		c.mu.Unlock()
		c.failProtocol(newConnErrorf(ProtocolErrorCode, "pong for id %d was never sent", m.ID))
		return true
	}
	if m.ID < c.lowestUnacked {
		c.mu.Unlock()
		c.opts.Metrics.DuplicatePong(c.session, m.ID)
		c.stats.duplicatePongs.Add(1)
		return false
	}
	sentAt, wasSent := c.outstandingPings[m.ID]
	if !wasSent {
		// In [lowestUnacked, nextPingID) but not in the map: already acked (or
		// pruned as stale by the watchdog) once before.
		c.mu.Unlock()
		c.opts.Metrics.DuplicatePong(c.session, m.ID)
		c.stats.duplicatePongs.Add(1)
		return false
	}
	delete(c.outstandingPings, m.ID)
	if m.ID == c.lowestUnacked {
		c.lowestUnacked++
		for c.lowestUnacked < c.nextPingID {
			if _, stillOut := c.outstandingPings[c.lowestUnacked]; stillOut {
				break
			}
			c.lowestUnacked++
		}
	}
	c.lastPongAt = time.Now()
	c.mu.Unlock()
	c.opts.Metrics.PingRTT(c.session, time.Since(sentAt))
	return false
}

func (c *Conn) handleDrainMsg(m *DrainMsg) (fatal bool) {
	if c.role == RoleServer && m.Reason != "client_requested" {
		c.failProtocol(newConnErrorf(ProtocolErrorCode, "drain from the client must use reason=client_requested, got %q", m.Reason))
		return true
	}
	reason := m.Reason
	if !KnownDrainReason(reason) {
		reason = "maintenance"
	}
	c.mu.Lock()
	if c.role == RoleServer {
		// The peer (client) asked us to drain; that is not the same as this
		// side having initiated its own Drain() sequence (OPEN refusal,
		// last_stream_id, RESETs, 4012). Record it separately so a server's
		// own Drain() call, made from OnDrain in response, is not a no-op.
		c.peerRequestedDrain = true
	} else {
		c.draining = true
		c.peerLastStreamID = m.LastStreamID
	}
	c.mu.Unlock()
	c.opts.Metrics.DrainReceived(c.session, reason)
	return !c.enqueueDelivery(deliveryEvent{drain: m})
}

func (c *Conn) handlePeerError(m *ErrorMsg) {
	c.opts.Logger.Error("peer reported a connection error", "code", m.Code, "message", m.Message)
	c.mu.Lock()
	if c.err == nil {
		c.err = &ConnError{Code: ErrorCode(m.Code), Message: m.Message}
	}
	// This side is about to call c.ws.Close itself below, with a reason
	// derived from the peer's error{} rather than anything the peer's own
	// close frame carries -- see handleReadError's localCloseInitiated check.
	c.localCloseInitiated = true
	c.mu.Unlock()
	// A peer that receives error MUST NOT reply with another error; just
	// close (WIRE.md §2.7). m.Code is peer-controlled and only
	// bounded to uint32 by the wire format (control.go), so it can map
	// outside the legal WS close-code range just like a caller's own code
	// to fail/Close -- wsCloseCode clamps it the same way.
	c.closeOnce.Do(func() {
		go func() {
			_ = c.ws.Close(wsCloseCode(ErrorCode(m.Code).CloseCode()), truncateCloseReason(m.Message))
			close(c.closed)
		}()
	})
}
