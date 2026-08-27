package wsmixer

import "time"

// --- keepalive ---------------------------------------------------------------

func (c *Conn) pingLoop() {
	select {
	case <-c.handshakeCh:
	case <-c.closed:
		return
	}
	ticker := time.NewTicker(c.pingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			c.mu.Lock()
			id := c.nextPingID
			c.nextPingID++
			c.outstandingPings[id] = time.Now()
			c.mu.Unlock()
			_ = c.sendControl(&PingMsg{T: "ping", ID: id, TS: time.Now().UnixMilli()})
		case <-c.closed:
			return
		}
	}
}

func (c *Conn) watchdogLoop() {
	select {
	case <-c.handshakeCh:
	case <-c.closed:
		return
	}
	c.mu.Lock()
	c.lastPongAt = time.Now()
	c.mu.Unlock()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			c.mu.Lock()
			elapsed := time.Since(c.lastPongAt)
			now := time.Now()
			for id, sentAt := range c.outstandingPings {
				// Prune ids that will never be usefully acked: their pong, if
				// it ever arrives, is treated as a benign duplicate (see
				// handlePong), not tracked forever.
				if now.Sub(sentAt) > c.pingTimeout {
					delete(c.outstandingPings, id)
				}
			}
			c.mu.Unlock()
			if elapsed > c.pingTimeout {
				c.opts.Metrics.KeepaliveTimeout(c.session)
				c.fail(newConnErrorf(KeepaliveTimeout, "no pong received for %dms", elapsed.Milliseconds()))
				return
			}
		case <-c.closed:
			return
		}
	}
}
