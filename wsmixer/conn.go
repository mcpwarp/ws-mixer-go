package wsmixer

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/coder/websocket"
)

// Role identifies which side of the connection this process is playing. Only
// the server opens streams (OVERVIEW.md section 2.5).
type Role uint8

const (
	RoleServer Role = iota
	RoleClient
)

func (r Role) String() string {
	if r == RoleServer {
		return "server"
	}
	return "client"
}

// Options are the tunables shared by the server Listener and the Dial client
// (OVERVIEW.md section 3.2's Config).
type Options struct {
	Window       int64         // this peer's own receive window; default 262144
	MaxStreams   int64         // streams this peer is willing to accept; default 64
	PingInterval time.Duration // default 30s, floor 5s
	PingTimeout  time.Duration // default 90s, must be >= 2x PingInterval
	HelloTimeout time.Duration // default 10s
	ReadLimit    int64         // default MaxMessageSize (65544)
	Logger       *slog.Logger
	Metrics      Metrics

	// RefusedOpenLimit/RefusedOpenWindow: repeat-offence escalation for
	// refused OPENs. RefusedOpenLimit refused OPENs (STREAM_LIMIT) that also
	// exceed the max_streams ceiling this side actually declared to the peer
	// (welcome.max_streams / hello.max_streams), inside a trailing
	// RefusedOpenWindow, escalate from a per-stream RESET to a full
	// connection ENHANCE_YOUR_CALM. A refusal caused only by this side
	// unilaterally lowering its own effective cap further (OVERVIEW.md
	// section 2.7) never counts toward this. Defaults: 20 within 10s.
	RefusedOpenLimit  int
	RefusedOpenWindow time.Duration

	// Stream0RateLimit/Stream0Burst: a token bucket bounding stream-0 control
	// message throughput (OVERVIEW.md section 2.7: "app is ... subject to the
	// same stream-0 rate limit as everything else"). Exceeding it is
	// ENHANCE_YOUR_CALM. Defaults: 50 messages/s, burst 100.
	Stream0RateLimit float64
	Stream0Burst     float64
}

func (o *Options) setDefaults() {
	if o.Window == 0 {
		o.Window = 262144
	}
	if o.MaxStreams == 0 {
		o.MaxStreams = 64
	}
	if o.PingInterval == 0 {
		o.PingInterval = 30 * time.Second
	}
	if o.PingTimeout == 0 {
		o.PingTimeout = 90 * time.Second
	}
	if o.HelloTimeout == 0 {
		o.HelloTimeout = 10 * time.Second
	}
	if o.ReadLimit == 0 {
		o.ReadLimit = MaxMessageSize
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	if o.Metrics == nil {
		o.Metrics = NoopMetrics{}
	}
	if o.RefusedOpenLimit == 0 {
		o.RefusedOpenLimit = 20
	}
	if o.RefusedOpenWindow == 0 {
		o.RefusedOpenWindow = 10 * time.Second
	}
	if o.Stream0RateLimit == 0 {
		o.Stream0RateLimit = 50
	}
	if o.Stream0Burst == 0 {
		o.Stream0Burst = 100
	}
}

// tokenBucket is a small, mutex-protected token bucket rate limiter.
type tokenBucket struct {
	mu       sync.Mutex
	tokens   float64
	capacity float64
	rate     float64 // tokens/second
	last     time.Time
}

func newTokenBucket(capacity, ratePerSecond float64) *tokenBucket {
	return &tokenBucket{tokens: capacity, capacity: capacity, rate: ratePerSecond, last: time.Now()}
}

// Allow reports whether one token is available and, if so, consumes it.
func (b *tokenBucket) Allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := time.Now()
	b.tokens += now.Sub(b.last).Seconds() * b.rate
	if b.tokens > b.capacity {
		b.tokens = b.capacity
	}
	b.last = now
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

// wsConn is the subset of *websocket.Conn that Conn depends on, so tests can
// substitute a fake transport.
type wsConn interface {
	Read(ctx context.Context) (websocket.MessageType, []byte, error)
	Write(ctx context.Context, typ websocket.MessageType, p []byte) error
	Close(code websocket.StatusCode, reason string) error
	CloseNow() error
	SetReadLimit(n int64)
}

// Conn is one accepted or dialed, handshaken ws-mixer connection.
type Conn struct {
	ws   wsConn
	role Role
	opts Options

	session    string
	peerWindow int64 // peer's advertised receive window -> our streams' send credit
	ourWindow  int64 // our advertised receive window -> our streams' recv credit and threshold
	maxStreams int64 // effective cap this side enforces = min(both sides' asks)
	// declaredMaxStreams is the max_streams value this side actually put on
	// the wire (welcome.max_streams on the server, or what the client learned
	// from welcome.max_streams before optionally lowering maxStreams further
	// via its own opts.MaxStreams ceiling). It is the number the peer is
	// entitled to rely on; see repeatRefusedOpen.
	declaredMaxStreams int64
	helloMeta          json.RawMessage
	welcomeMeta        json.RawMessage
	peerAgent          AgentInfo
	pingInterval       time.Duration
	pingTimeout        time.Duration

	// hp holds the current OnStream/OnApp/OnDrain callbacks. It is read from
	// the delivery goroutine and written by the setters below, which may be
	// called at any time -- an atomic.Pointer swap keeps that race-free
	// without a lock the delivery path would need to take on every dispatch.
	hp atomic.Pointer[handlers]

	handshakeDone bool
	handshakeOnce sync.Once
	handshakeCh   chan struct{}

	// openMu serializes OpenStream's allocate-id + enqueue-OPEN critical
	// section across concurrent callers, so wire order always matches id
	// order. It is deliberately never taken by failWrite or the read path: a
	// writer loop stalled on a full controlQueue blocks whoever holds openMu,
	// but never blocks failWrite (which only needs c.mu) from tearing the
	// connection down and unblocking it.
	openMu sync.Mutex

	mu                 sync.Mutex
	streams            map[uint32]*Stream
	highestOpened      uint32
	nextStreamID       uint32
	draining           bool // this side has initiated its own Drain() sequence
	peerRequestedDrain bool // the peer sent drain{client_requested}; server-only
	announcedLastID    uint32
	hasAnnouncedLast   bool
	outstandingPings   map[int64]time.Time // ids in [lowestUnacked, nextPingID) still awaiting a pong
	lowestUnacked      int64               // watermark: every id below this has been acked at least once
	nextPingID         int64
	lastPongAt         time.Time
	err                error
	running            bool // true once run() has started the writer loop

	closeOnce sync.Once
	closed    chan struct{}

	controlQueue chan []byte

	// sched is the round-robin DATA scheduler (OVERVIEW.md section 2.6 rule
	// 3), split out into sched.go along with markStreamReady/nextChunk/
	// writerLoop. Its lock is intentionally separate from both openMu and
	// c.mu: the writer loop must never need either of those to pick its next
	// chunk.
	sched dataSched

	writeTimeout time.Duration // 0 = defaultWriteTimeout; test-only override

	// deliveryQueue decouples OnApp/OnDrain callback invocation from the read
	// loop (OVERVIEW.md section 2.6 rule 1: "the read loop never blocks on
	// application delivery"). One goroutine drains it in order, so app and
	// drain callbacks still fire in the order their frames arrived, but a slow
	// or blocked handler cannot stall frame parsing/dispatch.
	deliveryQueue chan deliveryEvent

	connNotifyCh chan struct{} // broadcast: a stream slot freed up, or the conn is closing

	stream0Bucket *tokenBucket // rate limit on stream-0 messages (OVERVIEW.md section 2.7)

	// refusedOpenCount/refusedOpenWindowStart: the refused-OPEN repeat-offence
	// escalation counter (see repeatRefusedOpen). Both fields are
	// read-goroutine-owned (only ever touched from dispatch(), which only the
	// read loop calls), so they need no lock of their own.
	refusedOpenCount       int
	refusedOpenWindowStart time.Time
}

// handlers holds the current OnStream/OnApp/OnDrain callbacks as one
// snapshot, so a setter call races with delivery only at the granularity of
// "which whole snapshot is visible," never a half-updated one.
type handlers struct {
	onStream func(*Stream)
	onApp    func(json.RawMessage)
	onDrain  func(*DrainMsg)
}

// updateHandlers atomically applies mutate to a copy of the current handlers
// snapshot and publishes the result, retrying if a concurrent setter raced it.
func (c *Conn) updateHandlers(mutate func(*handlers)) {
	for {
		old := c.hp.Load()
		var next handlers
		if old != nil {
			next = *old
		}
		mutate(&next)
		if c.hp.CompareAndSwap(old, &next) {
			return
		}
	}
}

// deliveryQueueSize is the minimum deliveryQueue capacity. If a peer (or a
// slow application handler) causes the queue to fill up, the connection is
// failed with ENHANCE_YOUR_CALM rather than blocking the read loop or
// silently dropping the event. deliveryQueueCapacity grows this with
// MaxStreams so a legitimate burst of OPENs (delivered through this same
// queue, see handleRemoteOpen) never trips it under normal use.
const deliveryQueueSize = 128

// deliveryQueueMargin is added on top of MaxStreams when that alone would
// exceed deliveryQueueSize.
const deliveryQueueMargin = 64

func deliveryQueueCapacity(maxStreams int64) int {
	n := deliveryQueueSize
	if want := int(maxStreams) + deliveryQueueMargin; want > n {
		n = want
	}
	return n
}

// deliveryEvent is one OnStream/OnApp/OnDrain invocation queued for the
// delivery goroutine, in wire order. Exactly one of the three fields is set.
type deliveryEvent struct {
	open  *Stream
	app   json.RawMessage
	drain *DrainMsg
}

func newConn(ws wsConn, role Role, opts Options) *Conn {
	// Fall back directly here (rather than requiring every caller to have run
	// Options.setDefaults) so a test or embedder building Options by hand
	// doesn't end up with a zero-capacity bucket that rejects everything.
	rate := opts.Stream0RateLimit
	if rate <= 0 {
		rate = 50
	}
	burst := opts.Stream0Burst
	if burst <= 0 {
		burst = 100
	}
	return &Conn{
		ws:               ws,
		role:             role,
		opts:             opts,
		streams:          make(map[uint32]*Stream),
		outstandingPings: make(map[int64]time.Time),
		lastPongAt:       time.Now(),
		closed:           make(chan struct{}),
		controlQueue:     make(chan []byte, 256),
		sched:            dataSched{inReady: make(map[uint32]bool), workCh: make(chan struct{}, 1)},
		deliveryQueue:    make(chan deliveryEvent, deliveryQueueCapacity(opts.MaxStreams)),
		connNotifyCh:     make(chan struct{}),
		handshakeCh:      make(chan struct{}),
		stream0Bucket:    newTokenBucket(burst, rate),
	}
}

// run starts the connection's background goroutines. Must be called exactly
// once, after the handshake has completed.
func (c *Conn) run() {
	c.mu.Lock()
	c.running = true
	c.mu.Unlock()
	go c.writerLoop()
	go c.readerLoop()
	go c.pingLoop()
	go c.watchdogLoop()
	go c.deliveryLoop()
}

// deliveryLoop is the single goroutine that invokes OnStream/OnApp/OnDrain,
// in the order their events were enqueued, so all three fire in wire order
// from one goroutine. An OnStream handler that blocks for a long time must
// spawn its own goroutine, exactly like OnApp/OnDrain: none of the three may
// hold up delivery of the next event. There is no way to preempt a handler
// that is currently running -- a handler that blocks forever (never spawning
// a goroutine for its real work) is an application bug that permanently
// wedges this goroutine, full stop. This loop checks c.closed with priority
// over c.deliveryQueue on every iteration, so at most one more handler may
// start after close (the two cases race in the second select below) before
// it returns, rather than continuing to drain whatever backlog is queued.
func (c *Conn) deliveryLoop() {
	for {
		select {
		case <-c.closed:
			return
		default:
		}
		select {
		case ev := <-c.deliveryQueue:
			h := c.hp.Load()
			if h == nil {
				continue
			}
			if ev.open != nil && h.onStream != nil {
				h.onStream(ev.open)
			}
			if ev.app != nil && h.onApp != nil {
				h.onApp(ev.app)
			}
			if ev.drain != nil && h.onDrain != nil {
				h.onDrain(ev.drain)
			}
		case <-c.closed:
			return
		}
	}
}

// enqueueDelivery hands an OnStream/OnApp/OnDrain event to deliveryLoop
// without blocking the read loop. A full queue means the application isn't
// keeping up: that is excessive load on the one thing bounding this
// connection's memory, so it is ENHANCE_YOUR_CALM, never a silent drop.
func (c *Conn) enqueueDelivery(ev deliveryEvent) (ok bool) {
	select {
	case c.deliveryQueue <- ev:
		return true
	default:
		c.fail(newConnErrorf(EnhanceYourCalm, "application delivery queue full (>%d pending stream/app/drain callbacks)", cap(c.deliveryQueue)))
		return false
	}
}

// Session returns the handshake-assigned connection id (welcome.session).
func (c *Conn) Session() string { return c.session }

// Meta returns hello.meta verbatim (server side) or welcome.meta verbatim
// (client side).
func (c *Conn) Meta() json.RawMessage {
	if c.role == RoleServer {
		return c.helloMeta
	}
	return c.welcomeMeta
}

// Done returns a channel closed once the connection has finished shutting
// down.
func (c *Conn) Done() <-chan struct{} { return c.closed }

// Err returns the error that ended the connection, or nil if it is still
// live or ended gracefully with no error.
func (c *Conn) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.err
}

// finishHandshake marks the handshake complete and releases the ping and
// watchdog loops, which wait on handshakeCh before starting their timers.
func (c *Conn) finishHandshake() {
	c.handshakeOnce.Do(func() {
		c.handshakeDone = true
		close(c.handshakeCh)
	})
}

func (c *Conn) notifyConnLocked() {
	close(c.connNotifyCh)
	c.connNotifyCh = make(chan struct{})
}

func (c *Conn) refusedOpenLimit() int {
	if c.opts.RefusedOpenLimit > 0 {
		return c.opts.RefusedOpenLimit
	}
	return 20
}

func (c *Conn) refusedOpenWindowDuration() time.Duration {
	if c.opts.RefusedOpenWindow > 0 {
		return c.opts.RefusedOpenWindow
	}
	return 10 * time.Second
}

// repeatRefusedOpen records one refused OPEN (STREAM_LIMIT) that also
// exceeded the max_streams ceiling this side actually declared to the peer,
// and reports whether the count within the trailing window has hit the
// escalation limit. Read-goroutine-owned; see the field comments on Conn.
func (c *Conn) repeatRefusedOpen() bool {
	now := time.Now()
	window := c.refusedOpenWindowDuration()
	if c.refusedOpenWindowStart.IsZero() || now.Sub(c.refusedOpenWindowStart) > window {
		c.refusedOpenWindowStart = now
		c.refusedOpenCount = 0
	}
	c.refusedOpenCount++
	return c.refusedOpenCount >= c.refusedOpenLimit()
}

// --- outbound plumbing -------------------------------------------------------

// sendControlFrame enqueues a pre-encoded frame on the control-priority queue
// (WINDOW, CLOSE, RESET, stream-0 DATA): OVERVIEW.md section 2.6 rule 3.
func (c *Conn) sendControlFrame(b []byte) {
	select {
	case c.controlQueue <- b:
	case <-c.closed:
	}
}

// sendControl marshals a control message and enqueues it as a stream-0 DATA
// frame.
func (c *Conn) sendControl(msg any) error {
	b, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	c.sendControlFrame(EncodeData(0, b))
	return nil
}

// failWrite ends the connection after a socket write failure (e.g. the peer
// stopped reading and the write timeout in writeMessage fired). The transport
// is presumed dead, so this skips fail()'s best-effort error{} frame and just
// tears the connection down: closing c.closed is what unblocks every waiter
// parked in sendControlFrame, WriteContext's outQueue send, or Stream.wait —
// none of which would otherwise learn the writer loop is gone.
func (c *Conn) failWrite(err error) {
	c.closeOnce.Do(func() {
		c.mu.Lock()
		if c.err == nil {
			c.err = fmt.Errorf("ws-mixer: write failed: %w", err)
		}
		c.mu.Unlock()
		c.opts.Metrics.ProtocolViolation(c.session, "WRITE_FAILED")
		_ = c.ws.CloseNow()
		close(c.closed)
	})
}

// defaultWriteTimeout bounds a single socket write. writeTimeout on Conn
// exists so tests can shrink this without waiting out the real value.
const defaultWriteTimeout = 10 * time.Second

func (c *Conn) writeMessage(b []byte) error {
	timeout := c.writeTimeout
	if timeout <= 0 {
		timeout = defaultWriteTimeout
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	// Gate the write loop on the socket send buffer: coder/websocket's Write
	// is synchronous and context-bounded, so this blocking call *is* rule 2 of
	// OVERVIEW.md section 2.6.
	return c.ws.Write(ctx, websocket.MessageBinary, b)
}

// --- connection-fatal shutdown -----------------------------------------------

// writeControlNow writes a pre-encoded stream-0 frame synchronously on the
// caller's goroutine. It exists for the window before run() has started the
// writer loop: a handshake failure in that window has nobody reading
// controlQueue, so enqueueing there silently drops the error{} frame. Once
// running, all writes must go through the writer loop's queues instead.
func (c *Conn) writeControlNow(b []byte) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = c.ws.Write(ctx, websocket.MessageBinary, b)
}

// fail is the connection-error path: send error{} on stream 0, then close the
// WebSocket with 4000+code (OVERVIEW.md section 2.8's three steps).
func (c *Conn) fail(e *ConnError) {
	c.closeOnce.Do(func() {
		c.mu.Lock()
		c.err = e
		running := c.running
		c.mu.Unlock()
		c.opts.Metrics.ProtocolViolation(c.session, e.Code.String())

		errMsg := &ErrorMsg{T: "error", Code: uint32(e.Code), Message: e.Message}
		if e.HasStreamID {
			sid := e.StreamID
			errMsg.StreamID = &sid
		}
		if e.HasLastStreamID {
			lsid := e.LastStreamID
			errMsg.LastStreamID = &lsid
		}
		if b, merr := json.Marshal(errMsg); merr == nil {
			if running {
				select {
				case c.controlQueue <- EncodeData(0, b):
				default:
				}
			} else {
				// No writer loop yet (a pre-run handshake failure): write the
				// error{} frame synchronously, right here, or it is lost.
				c.writeControlNow(EncodeData(0, b))
			}
		}
		go func() {
			if running {
				// Best-effort flush window: give the writer loop a moment to
				// send the error{} frame before the socket goes away. Do not
				// wait more than ~2s for the peer's close handshake
				// (OVERVIEW.md section 2.8).
				time.Sleep(50 * time.Millisecond)
			}
			_ = c.ws.Close(websocket.StatusCode(e.CloseCode()), truncateCloseReason(e.Message))
			close(c.closed)
		}()
	})
}

// Close ends the connection gracefully with the given error code and message.
func (c *Conn) Close(code uint32, msg string) error {
	c.closeOnce.Do(func() {
		ec := ErrorCode(code)
		c.mu.Lock()
		c.err = &ConnError{Code: ec, Message: msg}
		c.mu.Unlock()
		errMsg := &ErrorMsg{T: "error", Code: code, Message: msg}
		if b, merr := json.Marshal(errMsg); merr == nil {
			select {
			case c.controlQueue <- EncodeData(0, b):
			default:
			}
		}
		go func() {
			time.Sleep(50 * time.Millisecond)
			_ = c.ws.Close(websocket.StatusCode(ec.CloseCode()), truncateCloseReason(msg))
			close(c.closed)
		}()
	})
	<-c.closed
	return nil
}

// truncateCloseReason trims s to <=123 UTF-8 bytes on a character boundary
// (OVERVIEW.md section 2.4).
func truncateCloseReason(s string) string {
	if len(s) <= 123 {
		return s
	}
	b := []byte(s)[:123]
	for len(b) > 0 && !utf8.RuneStart(b[len(b)-1]) {
		b = b[:len(b)-1]
	}
	// Also drop a truncated final multi-byte rune.
	if len(b) > 0 {
		r, size := utf8.DecodeLastRune(b)
		if r == utf8.RuneError && size <= 1 {
			b = b[:len(b)-1]
		}
	}
	return string(b)
}
