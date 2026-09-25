package wsmixer

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
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

	// allowSubfloorTiming is a test-only escape hatch: when true, the client
	// handshake (applyWelcome, client.go) skips the wire's ping_interval
	// >= 5000ms and ping_timeout >= 2x ping_interval floor checks (OVERVIEW.md
	// section 2.9/2.10), so a scaled-clock test harness (e.g. the conformance
	// runner's --time-scale) can run a real Dial against a welcome carrying
	// values below those floors. Mirrors the JS SDK's ConnOptions._timing.
	// Unexported and settable only through the conformance-hooks.go
	// AllowSubfloorTiming(*Options) function, which is compiled in only
	// under the `conformance` build tag -- never available in a normal
	// build, let alone production.
	allowSubfloorTiming bool
}

// SetDefaults fills in zero-valued fields with their documented defaults.
func (o *Options) SetDefaults() {
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

// WSConn is the subset of *websocket.Conn that Conn depends on. It is
// exported so an already-upgraded socket can be handed to AcceptConn across
// a package boundary, and so tests can substitute a fake transport.
type WSConn interface {
	Read(ctx context.Context) (websocket.MessageType, []byte, error)
	Write(ctx context.Context, typ websocket.MessageType, p []byte) error
	Close(code websocket.StatusCode, reason string) error
	CloseNow() error
	SetReadLimit(n int64)
}

// Conn is one accepted or dialed, handshaken ws-mixer connection.
type Conn struct {
	ws   WSConn
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
	welcomeMsg         *WelcomeMsg // client-side only: the full welcome, for Client's OnConnect
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
	draining           bool // this side has initiated its own Drain() sequence, or (client) the peer's drain was received
	peerRequestedDrain bool // the peer sent drain{client_requested}; server-only
	announcedLastID    uint32
	hasAnnouncedLast   bool
	peerLastStreamID   uint32              // client-only: last_stream_id from the server's drain message
	outstandingPings   map[int64]time.Time // ids in [lowestUnacked, nextPingID) still awaiting a pong
	lowestUnacked      int64               // watermark: every id below this has been acked at least once
	nextPingID         int64
	lastPongAt         time.Time
	err                error
	running            bool // true once Run() has started the writer loop
	// preRunClosed is set, in the same c.mu section that decides running's
	// pre-Run branch, by whichever of fail/Close gets there first while
	// running is still false. Run() checks it under the same lock as its own
	// running=true write, so "decide running and claim the conn" is one
	// atomic step both sides agree on: a Close that observes running==false
	// is guaranteed a Run() racing it will see preRunClosed and start no
	// loops at all, rather than the two of them draining/writing the same
	// controlQueue at once.
	preRunClosed bool

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

	// stats backs Stats(): the "ignore and count" counters CLIENT-SDK.md
	// requires (see stats.go).
	stats connStats

	// observedCloseCode is the raw WebSocket close code this side actually
	// saw when the transport ended (websocket.CloseStatus(err) from
	// handleReadError), independent of whether a ws-mixer *ConnError was
	// also involved. -1 (its zero-value replacement, set in newConn) means
	// none was observed: an abnormal closure -- TCP reset, timeout, EOF --
	// per RFC 6455's 1006 sentinel, which is never actually sent as a frame.
	// Client (client_reconnect.go) needs this to tell "1006/TCP
	// reset/DNS failure" (WIRE.md section 2.9: normal backoff) apart from a
	// clean-looking close that carries no ws-mixer error either.
	observedCloseCode int

	// observedCloseReason is the raw WebSocket close-frame reason string that
	// came with observedCloseCode (websocket.CloseError's Reason, extracted
	// alongside the code in handleReadError), independent of whether a
	// ws-mixer *ConnError was also involved. "" when no close frame was
	// observed, when the peer sent an empty reason, or when this side
	// initiated the close itself (localCloseInitiated) -- when this side
	// calls c.ws.Close, the peer's own coder/websocket echoes the exact
	// code/reason it just received back to us verbatim, and the readerLoop's
	// own blocked Read (already holding coder/websocket's read lock) parses
	// that echo in its normal control-frame handling (read.go's
	// handleControl, not anything close.go does) and returns it as a
	// CloseError -- this side's own outgoing text, which must never be
	// mistaken for something the peer said.
	observedCloseReason string

	// localCloseInitiated records that THIS side called c.ws.Close with a
	// reason (fail, Close, handlePeerError -- every site that passes a
	// non-empty reason to c.ws.Close), set under c.mu in the same critical
	// section that decides the rest of the shutdown. handleReadError checks
	// it before recording observedCloseReason: PeerCloseReason must only
	// ever be a reason the peer actually sent, never an echo of our own.
	localCloseInitiated bool
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

func newConn(ws WSConn, role Role, opts Options) *Conn {
	// Fall back directly here (rather than requiring every caller to have run
	// Options.SetDefaults) so a test or embedder building Options by hand
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
		ws:                ws,
		role:              role,
		opts:              opts,
		streams:           make(map[uint32]*Stream),
		outstandingPings:  make(map[int64]time.Time),
		lastPongAt:        time.Now(),
		closed:            make(chan struct{}),
		controlQueue:      make(chan []byte, 256),
		sched:             dataSched{inReady: make(map[uint32]bool), workCh: make(chan struct{}, 1)},
		deliveryQueue:     make(chan deliveryEvent, deliveryQueueCapacity(opts.MaxStreams)),
		connNotifyCh:      make(chan struct{}),
		handshakeCh:       make(chan struct{}),
		stream0Bucket:     newTokenBucket(burst, rate),
		observedCloseCode: -1,
	}
}

// PeerCloseCode returns the raw WebSocket close code this side actually
// observed when the transport ended, or -1 if none was observed (an
// abnormal closure, per RFC 6455's 1006 sentinel). See the field comment on
// Conn.observedCloseCode.
func (c *Conn) PeerCloseCode() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.observedCloseCode
}

// PeerCloseReason returns the raw WebSocket close-frame reason string this
// side actually observed when the transport ended, or "" if none was
// observed (an abnormal closure), the peer sent an empty reason, or this
// side initiated the close itself. See the field comment on
// Conn.observedCloseReason.
func (c *Conn) PeerCloseReason() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.observedCloseReason
}

// Run starts the connection's background goroutines. Must be called exactly
// once, after the handshake has completed. A Conn already closed by a
// pre-Run fail/Close (preRunClosed) starts nothing at all: the goroutine
// that closed it has already run the WS close handshake itself (coder/
// websocket's Close does its own internal read for the peer's close-frame
// reply, with no reader loop required -- see close.go's waitCloseHandshake),
// c.closed will still close exactly as it would have, and Done() still
// fires -- Run just has no work left to start.
func (c *Conn) Run() {
	c.mu.Lock()
	if c.preRunClosed {
		c.mu.Unlock()
		return
	}
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
// wedges this goroutine, full stop.
//
// On <-c.closed, this loop does not just return: readerLoop (dispatch.go),
// the only producer that ever sends to c.deliveryQueue, has always already
// stopped calling dispatch by the time anything closes c.closed (every
// fail()/Close()/handlePeerError path either runs synchronously on
// readerLoop's own goroutine right before it returns, or closes c.closed from
// a separate goroutine well after readerLoop stopped reading) -- so whatever
// is still sitting in the queue at that point was enqueued from a frame that
// arrived strictly before whatever ended the connection. A real peer's
// error{} is always the LAST message it sends (OVERVIEW.md section 2.8), so
// an app/drain/open event queued ahead of it was received before error{} --
// dropping it here would violate OnApp's "called for every incoming app
// message" promise (and OnStream/OnDrain's equivalent) for an event that has
// already, unambiguously, arrived. flushDeliveryQueue drains exactly that
// backlog, in order, before this returns.
func (c *Conn) deliveryLoop() {
	for {
		select {
		case ev := <-c.deliveryQueue:
			c.deliverEvent(ev)
		case <-c.closed:
			c.flushDeliveryQueue()
			return
		}
	}
}

// flushDeliveryQueue drains whatever is already sitting in c.deliveryQueue,
// in order, once the connection has closed -- see deliveryLoop's doc comment
// for why this backlog is still owed delivery rather than dropped. Bounded at
// cap(c.deliveryQueue) iterations so this always terminates unconditionally:
// readerLoop, the only producer, has already stopped by the time c.closed
// fires, so the queue only ever shrinks from here in the ordinary case, but
// nothing about a non-blocking drain loop on its own guarantees termination
// against every conceivable producer, and this must never be the one thing
// that can hang after close.
func (c *Conn) flushDeliveryQueue() {
	for i := 0; i < cap(c.deliveryQueue); i++ {
		select {
		case ev := <-c.deliveryQueue:
			c.deliverEvent(ev)
		default:
			return
		}
	}
}

// deliverEvent invokes the one handler ev carries -- OnStream, OnApp, or
// OnDrain -- against the current handlers snapshot. Shared by deliveryLoop's
// live path and flushDeliveryQueue's post-close flush so both invoke handlers
// identically and in the same wire order.
func (c *Conn) deliverEvent(ev deliveryEvent) {
	h := c.hp.Load()
	if h == nil {
		return
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
		c.failProtocol(newConnErrorf(EnhanceYourCalm, "application delivery queue full (>%d pending stream/app/drain callbacks)", cap(c.deliveryQueue)))
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

// Welcome returns the full welcome message this client-side connection
// received, or nil on the server side (which never receives one). Client
// (client_reconnect.go) passes this to OnConnect on every successful
// handshake, including reconnects.
func (c *Conn) Welcome() *WelcomeMsg { return c.welcomeMsg }

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

// connClosedErr returns the error that ended the connection, never nil: an
// abnormal closure (no close frame) leaves c.err nil, and a Stream must still
// report "the tunnel died" rather than a nil error -- WIRE.md section 2.9
// requires a handler to tell EOF from a dead tunnel. Use this, never Err(),
// at any point that has already observed c.closed and is about to hand that
// fact to a caller as an error return: a nil error there would be read as
// success (Write) or misread as a clean end-of-stream, not io.EOF, so a
// caller re-checking for exactly io.EOF (as Read's own peer-CLOSE path
// requires) would loop instead of stopping.
func (c *Conn) connClosedErr() error {
	if err := c.Err(); err != nil {
		return err
	}
	return io.ErrUnexpectedEOF
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
	err := c.ws.Write(ctx, websocket.MessageBinary, b)
	if err == nil {
		c.stats.bytesOut.Add(int64(len(b)))
	}
	return err
}

// --- connection-fatal shutdown -----------------------------------------------

// writeControlNow writes a pre-encoded stream-0 frame synchronously on the
// caller's goroutine, bounded by ctx, and reports any write error so the
// caller can stop after the first one instead of hammering a dead socket.
// It exists for the window before Run has started the writer loop: a
// pre-run fail() or Close() in that window has nobody reading controlQueue,
// so enqueueing there would silently drop the frame. The single-writer
// invariant this depends on -- exactly one of {this synchronous path, the
// writer loop} ever writes to c.ws for a given Conn -- is guaranteed by
// fail/Close and Run agreeing, under the same c.mu critical section, on
// which one it is: fail/Close set preRunClosed when they observe running
// still false, and Run checks preRunClosed before it ever sets running or
// starts the writer loop, so the two can never both decide they own the
// socket.
func (c *Conn) writeControlNow(ctx context.Context, b []byte) error {
	return c.ws.Write(ctx, websocket.MessageBinary, b)
}

// wsCloseCode maps an ErrorCode's CloseCode() to the code actually sent on
// the wire: legal WS close codes (1000, or 4000-4999) pass through
// unchanged. Anything else -- an application error code >= 0x1000_0000,
// which stays valid for a stream RESET (errors.go) but was never a legal
// connection-close code to begin with -- is clamped to InternalErrorCode's
// mapped code (4002) instead, since coder/websocket's Close sends no close
// frame at all for an invalid code and just aborts the transport (verified
// in its close.go): the peer would otherwise see a bare abnormal closure
// and nothing else, exactly the failure this whole file exists to prevent.
// error{} carries the caller's real code independently either way.
func wsCloseCode(code int) websocket.StatusCode {
	if code == 1000 || (code >= 4000 && code <= 4999) {
		return websocket.StatusCode(code)
	}
	return websocket.StatusCode(InternalErrorCode.CloseCode())
}

// fail is the connection-error path: send error{} on stream 0, then close the
// WebSocket with 4000+code (OVERVIEW.md section 2.8's three steps).
func (c *Conn) fail(e *ConnError) {
	c.closeOnce.Do(func() {
		c.mu.Lock()
		c.err = e
		c.localCloseInitiated = true
		running := c.running
		if !running {
			c.preRunClosed = true
		}
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
				// Unlike Close (below), this does not also flush whatever is
				// already sitting in controlQueue first: every pre-Run fail()
				// call site is handshake-internal (AcceptConn,
				// runServerHandshake's hello timer, Dial) -- strictly before
				// the application has ever been handed this *Conn -- so the
				// queue is necessarily empty here. An application holding a
				// not-yet-running Conn (e.g. a server's OnConn) can only end
				// it itself via Close or Drain, both of which do flush.
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				_ = c.writeControlNow(ctx, EncodeData(0, b))
				cancel()
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
			_ = c.ws.Close(wsCloseCode(e.CloseCode()), truncateCloseReason(e.Message))
			close(c.closed)
		}()
	})
}

// Close ends the connection gracefully with the given error code and
// message: error{} on stream 0, then WS close at 4000+code, mirroring fail's
// three steps. Before Run has started the writer loop (e.g. inside a
// server's OnConn callback, refusing a connection before it is ever run),
// there is nobody to drain controlQueue: this drains the CONTROL frames
// already queued there synchronously instead, in order, then writes error{}
// last, all against one shared deadline so a dead socket can't block the
// caller far longer than that; a write failure stops the flush and goes
// straight to the WS close below. After Run, error{} is just handed to the
// writer loop's queue with the same best-effort 50ms flush window fail()
// uses. Stream DATA queued before Run (a server's OnConn can legally
// OpenStream+Write on its own not-yet-running Conn) is not silently lost
// either, just for a different reason: Stream.WriteContext blocks on the
// chunk being sent or the conn closing, so a write interrupted by this Close
// returns the conn's own error, never a false success.
//
// An application closing for a reason ws-mixer itself does not interpret
// should use ApplicationCloseCode. Any code > 999 (including every
// application code >= 0x1000_0000, which stays valid for a stream RESET --
// errors.go) maps outside the legal WS close-code range (1000, or
// 4000-4999) and so cannot be expressed as one: the WS close this method
// sends is clamped to InternalErrorCode's code (4002) in that case, while
// error{} still carries the caller's real code.
//
// This does not stop OnStream/OnApp/OnDrain from firing: any stream/app/drain
// event that had already arrived before Close was called -- queued but not
// yet delivered -- is still delivered, on deliveryLoop's own goroutine, which
// can run briefly after this method returns (deliveryLoop's doc comment). A
// caller that tears down per-connection state on Close returning must be able
// to tolerate one more already-in-flight callback invocation after that,
// exactly as it already must tolerate one racing Close from the read/delivery
// side in the first place.
func (c *Conn) Close(code uint32, msg string) error {
	c.closeOnce.Do(func() {
		ec := ErrorCode(code)
		c.mu.Lock()
		c.err = &ConnError{Code: ec, Message: msg}
		c.localCloseInitiated = true
		running := c.running
		if !running {
			c.preRunClosed = true
		}
		c.mu.Unlock()
		errMsg := &ErrorMsg{T: "error", Code: code, Message: msg}
		b, merr := json.Marshal(errMsg)
		if running {
			if merr == nil {
				select {
				case c.controlQueue <- EncodeData(0, b):
				default:
				}
			}
		} else {
			// No writer loop yet: flush at most the n control frames already
			// queued when Close was called (e.g. SendApp called right before
			// it) -- not whatever a racing producer keeps adding -- in
			// order, then write error{} last, or it is lost. One 2s deadline
			// covers the whole flush, not 2s per frame, and the first write
			// failure ends it immediately: there is nothing useful left to
			// attempt on a socket that just failed to write.
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			ok := true
		drain:
			for n := len(c.controlQueue); n > 0; n-- {
				select {
				case queued := <-c.controlQueue:
					if c.writeControlNow(ctx, queued) != nil {
						ok = false
						break drain
					}
				default:
					// Queue emptied early (a concurrent producer may still add
					// more; the snapshot above deliberately doesn't wait for
					// it) -- not a write failure, so error{} below must still
					// be sent.
					break drain
				}
			}
			if ok && merr == nil {
				_ = c.writeControlNow(ctx, EncodeData(0, b))
			}
			cancel()
		}
		go func() {
			if running {
				time.Sleep(50 * time.Millisecond)
			}
			_ = c.ws.Close(wsCloseCode(ec.CloseCode()), truncateCloseReason(msg))
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
