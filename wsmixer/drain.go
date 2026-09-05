package wsmixer

import (
	"context"
	"fmt"
	"time"
)

// --- drain ---------------------------------------------------------------

// DrainOptions configures a Drain call (OVERVIEW.md section 2.9).
type DrainOptions struct {
	Deadline   time.Duration // grace for in-flight streams; 0 = immediately
	RetryAfter time.Duration // reconnect hint
	Message    string

	// CloseCode overrides the WS close code/error this Drain finishes with.
	// The default (HasCloseCode false) is GOING_AWAY (4012), matching the
	// server-initiated drain sequence in WIRE.md section 2.9. Client's own
	// graceful-shutdown path (WIRE.md section 2.10 rule 14: "drain{reason:
	// client_requested}, wait up to 5s, error{NO_ERROR}, close 1000") sets
	// this to NoError instead.
	CloseCode    ErrorCode
	HasCloseCode bool
}

// isDraining reports whether this side has already initiated its own
// Drain() sequence, or (client-side) already received the peer's drain --
// i.e. whether a further Drain() call would just no-op below. Client.Close
// (blocker 3) uses this to skip straight to closing the socket instead of
// calling Drain and getting exactly that no-op, which would otherwise leave
// it parked on the peer's own drain deadline (or forever).
func (c *Conn) isDraining() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.draining
}

// Drain stops opening new streams, tells the peer, waits up to opts.Deadline
// for in-flight streams to finish, then RESETs every survivor with CANCEL and
// closes with GOING_AWAY (OVERVIEW.md section 2.9). Role-agnostic: WIRE.md
// section 2.9/2.10 has the server send drain for its own policy reasons
// (rollout, idle eviction, load shedding, id exhaustion) and the client send
// it with reason "client_requested" on graceful shutdown (rule 14) -- both
// go through this same method on any live *Conn.
func (c *Conn) Drain(ctx context.Context, reason string, opts DrainOptions) error {
	c.mu.Lock()
	if c.draining {
		c.mu.Unlock()
		return nil
	}
	c.draining = true
	lastID := c.highestOpened
	if c.hasAnnouncedLast && c.announcedLastID < lastID {
		// Endpoints MUST NOT increase last_stream_id across multiple drains;
		// this call is racing an earlier one, keep the smaller value.
		lastID = c.announcedLastID
	}
	c.announcedLastID = lastID
	c.hasAnnouncedLast = true
	c.mu.Unlock()

	c.opts.Metrics.DrainStarted(c.session, reason)

	drainMsg := &DrainMsg{T: "drain", Reason: reason, LastStreamID: lastID}
	if opts.Deadline > 0 {
		ms := opts.Deadline.Milliseconds()
		drainMsg.DeadlineMS = &ms
	} else {
		zero := int64(0)
		drainMsg.DeadlineMS = &zero
	}
	if opts.RetryAfter > 0 {
		ms := opts.RetryAfter.Milliseconds()
		drainMsg.RetryAfterMS = &ms
	}
	if opts.Message != "" {
		drainMsg.Message = opts.Message
	}
	_ = c.sendControl(drainMsg)

	deadline := time.After(opts.Deadline)
	select {
	case <-c.waitForDrainedOrEmpty():
	case <-deadline:
	case <-ctx.Done():
	case <-c.closed:
		return c.Err()
	}

	c.mu.Lock()
	survivors := make([]*Stream, 0, len(c.streams))
	for _, st := range c.streams {
		survivors = append(survivors, st)
	}
	c.mu.Unlock()
	for _, st := range survivors {
		// A survivor here may already be queued for (or mid-) delivery to
		// OnStream, which hasn't run yet: this Reset can beat that callback,
		// so OnStream must not assume the *Stream it receives is still live
		// (see OnStream's doc comment in conn.go and the note in dispatch.go).
		st.Reset(CancelCode, fmt.Sprintf("draining: %s", reason))
		c.opts.Metrics.StreamCancelledOnDrain(c.session, st.ID())
	}

	c.opts.Metrics.DrainCompleted(c.session, reason)
	closeCode := GoingAwayCode
	if opts.HasCloseCode {
		closeCode = opts.CloseCode
	}
	return c.Close(uint32(closeCode), fmt.Sprintf("draining: %s", reason))
}

// waitForDrainedOrEmpty returns a channel closed once the stream table is
// empty.
func (c *Conn) waitForDrainedOrEmpty() <-chan struct{} {
	ch := make(chan struct{})
	go func() {
		defer close(ch)
		for {
			c.mu.Lock()
			empty := len(c.streams) == 0
			notify := c.connNotifyCh
			c.mu.Unlock()
			if empty {
				return
			}
			select {
			case <-notify:
			case <-c.closed:
				return
			}
		}
	}()
	return ch
}
