package wsmixer

import "sync"

// dataSched is the connection's round-robin DATA scheduler (OVERVIEW.md
// section 2.6 rule 3): one <=16 KiB chunk per ready stream, in rotation.
// ready is the FIFO rotation order; inReady dedupes it so a stream already
// waiting its turn is never queued twice. Its lock is a leaf lock: never
// held while touching a Stream's outQueue or c.mu, and never acquired by
// openMu/c.mu-holding code, so the writer loop can never block behind an
// OpenStream call or a teardown.
type dataSched struct {
	mu      sync.Mutex
	ready   []*Stream
	inReady map[uint32]bool
	workCh  chan struct{} // capacity 1: wakes the writer loop when a stream goes empty -> ready
}

func (c *Conn) writerLoop() {
	for {
		// Control frames jump the DATA rotation entirely: drain every control
		// frame currently queued, non-blocking, before considering any DATA.
		for {
			select {
			case msg := <-c.controlQueue:
				if err := c.writeMessage(msg); err != nil {
					c.failWrite(err)
					return
				}
				continue
			default:
			}
			break
		}

		// One <=16 KiB DATA chunk from the next ready stream, round-robin
		// (OVERVIEW.md section 2.6 rule 3).
		if pc, ok := c.nextChunk(); ok {
			err := c.writeMessage(EncodeData(pc.streamID, pc.data))
			pc.err = err
			close(pc.done)
			if err != nil {
				c.failWrite(err)
				return
			}
			c.opts.Metrics.BytesTransferred(c.session, "send", int64(len(pc.data)))
			continue
		}

		// No stream is ready: wait for a control frame, a stream becoming
		// ready, or shutdown.
		select {
		case msg := <-c.controlQueue:
			if err := c.writeMessage(msg); err != nil {
				c.failWrite(err)
				return
			}
		case <-c.sched.workCh:
		case <-c.closed:
			return
		}
	}
}

// markStreamReady adds st to the round-robin DATA rotation if it is not
// already in it, and wakes the writer loop if it is currently idle. The
// caller must have already enqueued a chunk into st.outQueue before calling
// this (see WriteContext).
func (c *Conn) markStreamReady(st *Stream) {
	c.sched.mu.Lock()
	if !c.sched.inReady[st.id] {
		c.sched.inReady[st.id] = true
		c.sched.ready = append(c.sched.ready, st)
	}
	c.sched.mu.Unlock()
	select {
	case c.sched.workCh <- struct{}{}:
	default:
	}
}

// nextChunk pops ready streams in rotation order until it finds one with a
// chunk to send. A stream can reach the front of the rotation with nothing
// left in outQueue (e.g. its only chunk was already claimed elsewhere in the
// same turn) or with a chunk that must not be written -- CLOSE already sent,
// or RESET, per OVERVIEW.md section 2.5's sending table -- and both cases
// must not stall streams still waiting behind it in the rotation. If more
// chunks remain queued behind the one taken, the stream goes back to the end
// of the rotation, so every other ready stream gets a turn before it comes up
// again (OVERVIEW.md section 2.6 rule 3's "one chunk per ready stream,
// round-robin").
func (c *Conn) nextChunk() (*pendingChunk, bool) {
	for {
		c.sched.mu.Lock()
		if len(c.sched.ready) == 0 {
			c.sched.mu.Unlock()
			return nil, false
		}
		st := c.sched.ready[0]
		c.sched.ready = c.sched.ready[1:]
		delete(c.sched.inReady, st.id)
		c.sched.mu.Unlock()

		pc, ok := popChunk(st.outQueue)
		if !ok {
			continue // emptied out from under the rotation; try the next stream
		}
		if done, err := st.sendDone(); done {
			pc.err = err
			close(pc.done)
			continue // never write DATA after this stream's CLOSE/RESET
		}
		if len(st.outQueue) > 0 {
			c.markStreamReady(st)
		}
		return pc, true
	}
}

func popChunk(q chan *pendingChunk) (*pendingChunk, bool) {
	select {
	case pc := <-q:
		return pc, true
	default:
		return nil, false
	}
}
