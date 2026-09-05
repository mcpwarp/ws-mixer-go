package wsmixer

import "sync/atomic"

// Stats is a snapshot of the "ignore and count" counters CLIENT-SDK.md
// requires every client SDK to expose: traffic this side discarded or
// logged rather than surfacing as an error, plus raw byte counts. Safe to
// read concurrently with a live connection; see Conn.Stats() and (for the
// cumulative-across-reconnects view) Client.Stats().
type Stats struct {
	// UnknownFrameTypes counts frames whose type byte this side doesn't
	// recognize (FrameType.Known() false): ignored and counted, never a
	// protocol error (OVERVIEW.md section 2.2: "unknown types MUST NOT
	// trigger special behaviour").
	UnknownFrameTypes int64
	// StaleFrames counts frames for a stream id at or below highest_opened
	// with no live *Stream anymore (already fully closed): discarded and
	// logged, not an error (OVERVIEW.md section 2.10 rule 10).
	StaleFrames int64
	// DuplicatePongs counts pong control messages whose id was already
	// acknowledged (or pruned as stale by the watchdog) before this one
	// arrived.
	DuplicatePongs int64
	// RefusedOpens counts OPENs this side refused with RESET(STREAM_LIMIT)
	// because max_streams was already exhausted.
	RefusedOpens int64
	// ProtocolViolations counts connection-fatal ConnErrors raised while
	// dispatching an already-established connection's frames (not handshake
	// failures, and not a keepalive timeout, which have their own distinct
	// disconnect reporting).
	ProtocolViolations int64
	// BytesIn/BytesOut are raw wire byte counts for the connection's steady
	// -state read/write loops (post-handshake frames); the hello/welcome
	// handshake exchange itself is not included.
	BytesIn  int64
	BytesOut int64
}

// connStats holds Stats' counters as atomics so they can be incremented from
// the read/write/dispatch goroutines without a lock.
type connStats struct {
	unknownFrameTypes  atomic.Int64
	staleFrames        atomic.Int64
	duplicatePongs     atomic.Int64
	refusedOpens       atomic.Int64
	protocolViolations atomic.Int64
	bytesIn            atomic.Int64
	bytesOut           atomic.Int64
}

func (s *connStats) snapshot() Stats {
	return Stats{
		UnknownFrameTypes:  s.unknownFrameTypes.Load(),
		StaleFrames:        s.staleFrames.Load(),
		DuplicatePongs:     s.duplicatePongs.Load(),
		RefusedOpens:       s.refusedOpens.Load(),
		ProtocolViolations: s.protocolViolations.Load(),
		BytesIn:            s.bytesIn.Load(),
		BytesOut:           s.bytesOut.Load(),
	}
}

// add accumulates another snapshot into s, for Client.Stats()'s
// cumulative-across-reconnects view.
func (s *Stats) add(o Stats) {
	s.UnknownFrameTypes += o.UnknownFrameTypes
	s.StaleFrames += o.StaleFrames
	s.DuplicatePongs += o.DuplicatePongs
	s.RefusedOpens += o.RefusedOpens
	s.ProtocolViolations += o.ProtocolViolations
	s.BytesIn += o.BytesIn
	s.BytesOut += o.BytesOut
}

// Stats returns a snapshot of this connection's "ignore and count" counters
// (CLIENT-SDK.md). Safe to call at any time, including after the connection
// has closed.
func (c *Conn) Stats() Stats { return c.stats.snapshot() }

// failProtocol fails the connection exactly like fail(), additionally
// counting it in Stats().ProtocolViolations. Used for the ConnErrors raised
// while dispatching an already-handshaken connection's frames (dispatch.go
// and the delivery-queue-overflow check in conn.go) -- mirrors the JS SDK's
// protocolViolations counter, which increments only for a ConnError thrown
// from dispatchFrame, not e.g. a keepalive timeout or a handshake failure
// (both of which get their own distinct disconnect reporting instead).
func (c *Conn) failProtocol(e *ConnError) {
	c.stats.protocolViolations.Add(1)
	c.fail(e)
}
