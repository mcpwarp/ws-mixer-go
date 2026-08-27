package wsmixer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
)

// --- streams -----------------------------------------------------------------

// errStreamSlotGone is allocateStreamLocked's signal that a slot which looked
// free is no longer available by the time openMu was acquired: the caller
// should loop back and re-check from the top rather than treat it as fatal.
var errStreamSlotGone = errors.New("wsmixer: stream slot no longer available")

// allocateStreamLocked allocates the next stream id and registers the new
// Stream. The caller must hold openMu (not c.mu) before calling this; it
// takes c.mu itself only for the duration of this function, then releases
// it. Invariant: the resulting OPEN frame must be enqueued under openMu
// alone, never while also holding c.mu -- failWrite only ever acquires c.mu,
// so holding both while blocked on a full controlQueue would deadlock
// against it.
func (c *Conn) allocateStreamLocked() (uint32, *Stream, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err != nil {
		return 0, nil, c.err
	}
	if c.draining {
		return 0, nil, &StreamError{Code: RefusedStreamCode, Message: "connection is draining, refusing new streams"}
	}
	if int64(len(c.streams)) >= c.maxStreams {
		return 0, nil, errStreamSlotGone
	}
	id := c.nextStreamID
	if id == 0 {
		id = 1
	}
	if id > maxStreamIDValue {
		go c.Drain(context.Background(), "id_exhausted", DrainOptions{})
		return 0, nil, fmt.Errorf("wsmixer: stream id space exhausted")
	}
	c.nextStreamID = id + 2
	c.highestOpened = id
	st := newStream(c, id, c.ourWindow, c.peerWindow)
	c.streams[id] = st
	return id, st, nil
}

// OpenStream opens a new server-initiated stream. It blocks until a slot is
// free if the connection is at MaxStreams. Server-only.
func (c *Conn) OpenStream(ctx context.Context) (*Stream, error) {
	if c.role != RoleServer {
		return nil, newConnErrorf(ProtocolErrorCode, "only the server opens streams")
	}
	for {
		c.mu.Lock()
		if c.err != nil {
			err := c.err
			c.mu.Unlock()
			return nil, err
		}
		if c.draining {
			c.mu.Unlock()
			return nil, &StreamError{Code: RefusedStreamCode, Message: "connection is draining, refusing new streams"}
		}
		if int64(len(c.streams)) >= c.maxStreams {
			ch := c.connNotifyCh
			c.mu.Unlock()
			select {
			case <-ch:
				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-c.closed:
				return nil, c.Err()
			}
		}
		c.mu.Unlock()

		// openMu serializes allocate-id + enqueue-OPEN across concurrent
		// OpenStream calls, so wire order always matches id order (the peer
		// must never see OPEN(id+2) before OPEN(id)). Invariant: openMu is
		// held while blocked on nothing but the controlQueue send below, and
		// failWrite never acquires it (only c.mu) -- so a writer loop stalled
		// on a full controlQueue, which is what unblocks failWrite's
		// teardown, can never be waiting behind a goroutine parked here.
		c.openMu.Lock()
		id, st, err := c.allocateStreamLocked()
		if err != nil {
			c.openMu.Unlock()
			if errors.Is(err, errStreamSlotGone) {
				continue // lost the race for the last slot; re-check from the top
			}
			return nil, err
		}
		select {
		case c.controlQueue <- EncodeOpen(id):
		case <-c.closed:
		}
		c.openMu.Unlock()

		c.opts.Metrics.StreamOpened(c.session, id)
		return st, nil
	}
}

// SendApp sends an opaque `app` message to the peer. Legal in both directions
// at any time after the handshake completes.
func (c *Conn) SendApp(ctx context.Context, body any) error {
	raw, err := json.Marshal(body)
	if err != nil {
		return err
	}
	c.opts.Metrics.AppMessage(c.session, "send")
	return c.sendControl(&AppMsg{T: "app", Body: raw})
}

// OnApp registers the callback invoked for every incoming `app` message. May
// be called at any time -- including concurrently with traffic -- and takes
// effect for events delivered after the call returns; a call while the
// connection is live races only the *choice* of which callback fires next,
// never a torn or partially-updated one. It runs on the same shared delivery
// goroutine as OnStream/OnDrain, in wire order with them: do not block here;
// a handler that has real work to do must spawn its own goroutine, or it will
// hold up delivery of every other stream/app/drain event queued behind it.
func (c *Conn) OnApp(fn func(body json.RawMessage)) {
	c.updateHandlers(func(h *handlers) { h.onApp = fn })
}

// OnStream registers the callback invoked whenever the peer opens a new
// stream. Client-side only (only the server opens streams). It runs on the
// same shared delivery goroutine as OnApp/OnDrain, in wire order with them: a
// handler that blocks for a long time must spawn its own goroutine, or it
// will hold up delivery of every other stream/app/drain event queued behind
// it. May be called at any time; see OnApp. Note that the stream passed to
// the callback may already have been reset by the time it fires (e.g. by a
// concurrent Drain sweeping up survivors, see Drain/drain.go): Read on it
// then returns the StreamError that reset it, rather than any data.
func (c *Conn) OnStream(fn func(*Stream)) {
	c.updateHandlers(func(h *handlers) { h.onStream = fn })
}

// OnDrain registers the callback invoked when a `drain` message arrives. May
// be called at any time; see OnApp. It runs on the same shared delivery
// goroutine as OnStream/OnApp; do not block here -- spawn a goroutine for any
// real work, exactly as OnStream's doc comment describes.
func (c *Conn) OnDrain(fn func(*DrainMsg)) {
	c.updateHandlers(func(h *handlers) { h.onDrain = fn })
}

// PeerRequestedDrain reports whether the peer sent drain{client_requested}.
// This is distinct from whether this side has itself started draining (see
// Drain): a server typically answers a peer-requested drain by calling its
// own Drain(ctx, "client_requested", ...), which still needs to run even
// though the peer already announced its own draining intent. Call Drain from
// a goroutine, not inline from an OnDrain (or OnApp/OnStream) callback: Drain
// blocks until its deadline or the streams table drains, and inline would
// stall the shared delivery goroutine those callbacks run on.
func (c *Conn) PeerRequestedDrain() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.peerRequestedDrain
}
