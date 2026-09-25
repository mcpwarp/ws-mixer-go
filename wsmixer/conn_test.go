package wsmixer

import (
	"context"
	"encoding/json"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestWriterLoopFailsConnOnWriteError checks that if the peer stops reading
// and every socket write starts timing out, the writer loop fails the
// connection (not just returns silently), so waiters parked on
// sendControlFrame/outQueue/Stream.wait are unblocked instead of leaking
// forever.
func TestWriterLoopFailsConnOnWriteError(t *testing.T) {
	ws := newFakeWS()
	c := newConn(ws, RoleServer, Options{})
	c.opts.SetDefaults()
	c.writeTimeout = 50 * time.Millisecond // bound the test, not the real default
	c.session = "test-session"
	c.ourWindow = 262144
	c.peerWindow = 262144
	c.maxStreams = 64
	c.pingInterval = time.Minute
	c.pingTimeout = 5 * time.Minute
	c.finishHandshake()
	c.Run()
	defer func() { _ = ws.CloseNow() }()

	// Simulate "the peer stopped reading": fill fakeWS's outbound buffer so
	// every subsequent write blocks on the channel send until writeTimeout
	// expires and ctx.Err() comes back from fakeWS.Write.
	for len(ws.outbound) < cap(ws.outbound) {
		ws.outbound <- []byte("peer isn't draining this")
	}

	if err := c.SendApp(context.Background(), map[string]any{"x": 1}); err != nil {
		t.Fatalf("SendApp: %v", err)
	}

	select {
	case <-c.closed:
	case <-time.After(2 * time.Second):
		t.Fatal("connection did not terminate within the write timeout after the peer stopped reading")
	}
	if c.Err() == nil {
		t.Error("Err() is nil after a write failure; writerLoop must record and surface it")
	}

	// A waiter that shows up after the connection has already failed must not
	// block: sendControlFrame/SendApp select on c.closed.
	done := make(chan struct{})
	go func() {
		_ = c.SendApp(context.Background(), map[string]any{"y": 2})
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("a post-failure sendControlFrame call is stuck: c.closed did not unblock it")
	}
}

// TestAppDeliveryDoesNotBlockReadLoop checks that OnApp does not run on the
// read goroutine: a handler that blocks forever must not stop the read loop
// from continuing to dispatch other frames.
func TestAppDeliveryDoesNotBlockReadLoop(t *testing.T) {
	ws := newFakeWS()
	c := newConn(ws, RoleServer, Options{})
	c.opts.SetDefaults()
	c.session = "test-session"
	c.ourWindow = 262144
	c.peerWindow = 262144
	c.maxStreams = 64
	c.pingInterval = time.Minute
	c.pingTimeout = 5 * time.Minute

	blockCh := make(chan struct{})
	appSeen := make(chan struct{}, 8)
	c.OnApp(func(body json.RawMessage) {
		<-blockCh // first call parks here forever, until the test releases it
		appSeen <- struct{}{}
	})
	c.finishHandshake()
	c.Run()
	defer func() { _ = ws.CloseNow() }()

	ws.feedInbound(EncodeData(0, []byte(`{"t":"app","body":{}}`)))
	// The read loop must still process further frames (a ping) while the
	// first app callback is blocked.
	ws.feedInbound(EncodeData(0, []byte(`{"t":"ping","id":1}`)))

	select {
	case b := <-ws.outbound:
		frame, err := DecodeFrame(b)
		if err != nil || frame.StreamID != 0 {
			t.Fatalf("expected a stream-0 reply, got %v (err=%v)", frame, err)
		}
		msg, _ := ParseControl(frame.Payload)
		if _, ok := msg.(*PongMsg); !ok {
			t.Fatalf("expected pong while app callback is blocked, got %T", msg)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("read loop appears blocked on the OnApp callback")
	}
	close(blockCh)
}

// TestAppDeliveryQueueFullIsEnhanceYourCalm checks the delivery queue's
// overflow rule: a full delivery queue is a connection error, never a silent
// drop.
func TestAppDeliveryQueueFullIsEnhanceYourCalm(t *testing.T) {
	ws := newFakeWS()
	c := newConn(ws, RoleServer, Options{})
	c.opts.SetDefaults()
	c.session = "test-session"
	c.ourWindow = 262144
	c.peerWindow = 262144
	c.maxStreams = 64
	c.pingInterval = time.Minute
	c.pingTimeout = 5 * time.Minute
	c.OnApp(func(json.RawMessage) { select {} }) // never returns
	c.finishHandshake()
	c.Run()
	defer func() { _ = ws.CloseNow() }()

	for i := 0; i < deliveryQueueSize+4; i++ {
		ws.feedInbound(EncodeData(0, []byte(`{"t":"app","body":{}}`)))
	}

	select {
	case <-c.closed:
	case <-time.After(5 * time.Second):
		t.Fatal("connection never failed once the delivery queue filled up")
	}
	ce, ok := c.Err().(*ConnError)
	if !ok {
		t.Fatalf("expected *ConnError, got %#v", c.Err())
	}
	if ce.Code != EnhanceYourCalm {
		t.Errorf("error code = %s, want ENHANCE_YOUR_CALM", ce.Code)
	}
}

// TestRepeatRefusedOpenEscalates checks that repeated STREAM_LIMIT refusals
// within the window escalate to a connection ENHANCE_YOUR_CALM.
func TestRepeatRefusedOpenEscalates(t *testing.T) {
	ws := newFakeWS()
	opts := Options{RefusedOpenLimit: 3, RefusedOpenWindow: time.Minute}
	c := newConn(ws, RoleClient, opts) // only the client receives OPEN
	c.opts.SetDefaults()
	c.opts.RefusedOpenLimit = 3
	c.opts.RefusedOpenWindow = time.Minute
	c.session = "test-session"
	c.ourWindow = 262144
	c.peerWindow = 262144
	c.maxStreams = 1 // trivially over limit for every OPEN after highestOpened advances
	c.pingInterval = time.Minute
	c.pingTimeout = 5 * time.Minute
	c.finishHandshake()
	c.Run()
	defer func() { _ = ws.CloseNow() }()

	// Fill the one allowed slot first so every further OPEN is over limit.
	c.mu.Lock()
	c.streams[1] = newStream(c, 1, c.ourWindow, c.peerWindow)
	c.highestOpened = 1
	c.mu.Unlock()

	for _, id := range []uint32{3, 5, 7, 9} {
		ws.feedInbound(EncodeOpen(id))
	}

	select {
	case <-c.closed:
	case <-time.After(2 * time.Second):
		t.Fatal("connection never escalated after repeated refused OPENs")
	}
	ce, ok := c.Err().(*ConnError)
	if !ok || ce.Code != EnhanceYourCalm {
		t.Errorf("Err() = %#v, want ENHANCE_YOUR_CALM", c.Err())
	}
}

// TestStream0RateLimitEscalates checks that flooding stream-0 with control
// messages past the token bucket is ENHANCE_YOUR_CALM.
func TestStream0RateLimitEscalates(t *testing.T) {
	ws := newFakeWS()
	opts := Options{Stream0RateLimit: 5, Stream0Burst: 5}
	c := newConn(ws, RoleServer, opts)
	c.opts.SetDefaults()
	c.opts.Stream0RateLimit = 5
	c.opts.Stream0Burst = 5
	c.session = "test-session"
	c.ourWindow = 262144
	c.peerWindow = 262144
	c.maxStreams = 64
	c.pingInterval = time.Minute
	c.pingTimeout = 5 * time.Minute
	c.finishHandshake()
	c.Run()
	defer func() { _ = ws.CloseNow() }()

	for i := 0; i < 50; i++ {
		ws.feedInbound(EncodeData(0, []byte(`{"t":"ping","id":1}`)))
	}

	select {
	case <-c.closed:
	case <-time.After(2 * time.Second):
		t.Fatal("connection never failed once the stream-0 token bucket ran out")
	}
	ce, ok := c.Err().(*ConnError)
	if !ok || ce.Code != EnhanceYourCalm {
		t.Errorf("Err() = %#v, want ENHANCE_YOUR_CALM", c.Err())
	}
}

// TestRefusedOpenNoEscalationWhenSelfLimited checks that a client which
// further lowers max_streams below what the peer (server) actually declared
// in welcome does not have those self-inflicted refusals count toward the
// repeat-offence escalation: only OPENs that exceed what this side told the
// peer it could send are evidence of a misbehaving peer (OVERVIEW.md section
// 2.7: the client "MAY lower it further to its own ceiling").
func TestRefusedOpenNoEscalationWhenSelfLimited(t *testing.T) {
	ws := newFakeWS()
	opts := Options{RefusedOpenLimit: 3, RefusedOpenWindow: time.Minute}
	c := newConn(ws, RoleClient, opts) // only the client receives OPEN
	c.opts.SetDefaults()
	c.opts.RefusedOpenLimit = 3
	c.opts.RefusedOpenWindow = time.Minute
	c.session = "test-session"
	c.ourWindow = 262144
	c.peerWindow = 262144
	c.maxStreams = 1           // this side's own, further-lowered ceiling
	c.declaredMaxStreams = 100 // what the server actually told us in welcome
	c.pingInterval = time.Minute
	c.pingTimeout = 5 * time.Minute
	c.finishHandshake()
	c.Run()
	defer func() { _ = ws.CloseNow() }()

	c.mu.Lock()
	c.streams[1] = newStream(c, 1, c.ourWindow, c.peerWindow)
	c.highestOpened = 1
	c.mu.Unlock()

	// Well within the peer's declared max_streams (100) but over this side's
	// self-imposed cap of 1: every one of these is refused, but none of it is
	// the peer's fault, so far more than RefusedOpenLimit refusals must not
	// escalate the connection.
	for _, id := range []uint32{3, 5, 7, 9, 11, 13, 15, 17, 19, 21} {
		ws.feedInbound(EncodeOpen(id))
	}

	select {
	case <-c.closed:
		t.Fatalf("connection failed on self-inflicted refusals: %v", c.Err())
	case <-time.After(300 * time.Millisecond):
	}
}

// TestOpenAboveDrainLastStreamIDIsProtocolError checks that a client which
// has received drain{last_stream_id: N} from the server, then sees an OPEN
// for an id above N, fails the whole connection with PROTOCOL_ERROR rather
// than just refusing the one stream: only the server opens streams, so an
// OPEN above the boundary it already promised is always the server
// violating its own promise, never something the client could have
// self-inflicted (OVERVIEW.md section 2.7 Drain, decision log 2026-08-27).
func TestOpenAboveDrainLastStreamIDIsProtocolError(t *testing.T) {
	ws := newFakeWS()
	c := newConn(ws, RoleClient, Options{}) // only the client receives OPEN
	c.opts.SetDefaults()
	c.session = "test-session"
	c.ourWindow = 262144
	c.peerWindow = 262144
	c.maxStreams = 64
	c.pingInterval = time.Minute
	c.pingTimeout = 5 * time.Minute
	c.finishHandshake()
	c.Run()
	defer func() { _ = ws.CloseNow() }()

	ws.feedInbound(EncodeData(0, []byte(`{"t":"drain","reason":"maintenance","last_stream_id":3,"deadline_ms":0}`)))
	// Give the delivery/dispatch loop a moment to process the drain before
	// the OPEN that violates its boundary.
	time.Sleep(50 * time.Millisecond)
	ws.feedInbound(EncodeOpen(5))

	select {
	case <-c.closed:
	case <-time.After(2 * time.Second):
		t.Fatal("connection never failed on OPEN above drain last_stream_id")
	}
	ce, ok := c.Err().(*ConnError)
	if !ok || ce.Code != ProtocolErrorCode {
		t.Errorf("Err() = %#v, want PROTOCOL_ERROR", c.Err())
	}
}

// TestOpenStreamDoesNotDeadlockWithFailWrite covers the OpenStream/failWrite
// deadlock: OpenStream must never hold c.mu while blocking on a full,
// stalled controlQueue, or a concurrent write failure that needs c.mu to
// record c.err could never complete, and every waiter would hang forever.
func TestOpenStreamDoesNotDeadlockWithFailWrite(t *testing.T) {
	before := runtime.NumGoroutine()

	ws := newFakeWS()
	c := newConn(ws, RoleServer, Options{})
	c.opts.SetDefaults()
	c.writeTimeout = 50 * time.Millisecond // bound the test, not the real default
	c.session = "test-session"
	c.ourWindow = 262144
	c.peerWindow = 262144
	c.maxStreams = 10000 // high enough that no call blocks on the slot limit
	c.pingInterval = time.Minute
	c.pingTimeout = 5 * time.Minute
	c.finishHandshake()
	c.Run()
	defer func() { _ = ws.CloseNow() }()

	// Stall the writer: fill fakeWS's outbound buffer so the first write the
	// writer loop attempts blocks until writeTimeout expires and failWrite
	// fires (same technique as TestWriterLoopFailsConnOnWriteError).
	for len(ws.outbound) < cap(ws.outbound) {
		ws.outbound <- []byte("peer isn't draining this")
	}
	// Fill the control queue too, so a concurrent OpenStream's own OPEN
	// enqueue genuinely blocks instead of landing in a free slot.
	for len(c.controlQueue) < cap(c.controlQueue) {
		c.controlQueue <- EncodeData(0, []byte(`{"t":"app","body":{}}`))
	}

	const nOpens = 20
	done := make(chan struct{}, nOpens)
	for i := 0; i < nOpens; i++ {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_, _ = c.OpenStream(ctx)
			done <- struct{}{}
		}()
	}

	deadline := time.After(2 * time.Second)
	for i := 0; i < nOpens; i++ {
		select {
		case <-done:
		case <-deadline:
			t.Fatal("OpenStream calls did not unblock within 2s of the writer stalling: likely deadlocked against failWrite")
		}
	}

	select {
	case <-c.closed:
	case <-time.After(2 * time.Second):
		t.Fatal("connection never failed after the writer stalled")
	}

	waitForGoroutines(t, before)
}

// --- deliveryLoop flush-on-close tests --------------------------------------
//
// These build a *Conn with newConn directly, without calling Run(): the
// point is to drive deliveryLoop as a bare goroutine against a hand-populated
// c.deliveryQueue and a c.closed the test closes itself, so the
// close-vs-still-queued race that used to drop already-received events
// (conn.go's deliveryLoop, pre-fix: a priority `select { case <-c.closed:
// return; default: }` ahead of the real select) is deterministic instead of
// depending on goroutine scheduling.

// newUnrunConnForDelivery builds a handshake-complete *Conn that Run() has
// never been called on (close_before_run_test.go's newUnrunConn, plus test
// cleanup), mirroring the setup TestWriterLoopRoundRobinsAcrossStreams and
// friends use for driving a single loop directly.
func newUnrunConnForDelivery(t *testing.T) *Conn {
	t.Helper()
	ws := newFakeWS()
	c := newUnrunConn(ws)
	t.Cleanup(func() { _ = ws.CloseNow() })
	return c
}

// runDeliveryLoopToCompletion starts deliveryLoop and waits (bounded) for it
// to return, failing the test if it doesn't -- every flush test needs both
// "the events arrived" and "the loop actually terminated" checked.
func runDeliveryLoopToCompletion(t *testing.T, c *Conn) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		c.deliveryLoop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("deliveryLoop did not return within 2s of c.closed closing")
	}
}

// TestDeliveryLoopFlushesQueuedAppEventsOnClose is the core regression test:
// two app events queued, then c.closed closed (simulating the read loop
// enqueueing both, without yielding, right before the peer's error{} tears
// the connection down) -- both must still be delivered, in order, and
// deliveryLoop must return. Pre-fix, this failed 100% of the time (the
// priority check saw c.closed already closed and returned without draining
// anything).
func TestDeliveryLoopFlushesQueuedAppEventsOnClose(t *testing.T) {
	c := newUnrunConnForDelivery(t)

	var mu sync.Mutex
	var got []string
	c.OnApp(func(body json.RawMessage) {
		mu.Lock()
		got = append(got, string(body))
		mu.Unlock()
	})

	c.deliveryQueue <- deliveryEvent{app: json.RawMessage(`{"seq":1}`)}
	c.deliveryQueue <- deliveryEvent{app: json.RawMessage(`{"seq":2}`)}
	close(c.closed)

	runDeliveryLoopToCompletion(t, c)

	mu.Lock()
	defer mu.Unlock()
	if want := []string{`{"seq":1}`, `{"seq":2}`}; !slicesEqual(got, want) {
		t.Fatalf("delivered app bodies = %v, want %v (in order)", got, want)
	}
}

// TestDeliveryLoopFlushesQueuedDrainEventOnClose is the drain variant of the
// same regression: a drain message queued right before close must still be
// delivered.
func TestDeliveryLoopFlushesQueuedDrainEventOnClose(t *testing.T) {
	c := newUnrunConnForDelivery(t)

	delivered := make(chan *DrainMsg, 1)
	c.OnDrain(func(d *DrainMsg) { delivered <- d })

	want := &DrainMsg{T: "drain", Reason: "maintenance", LastStreamID: 7}
	c.deliveryQueue <- deliveryEvent{drain: want}
	close(c.closed)

	runDeliveryLoopToCompletion(t, c)

	select {
	case got := <-delivered:
		if got != want {
			t.Fatalf("OnDrain delivered %#v, want the same *DrainMsg %#v", got, want)
		}
	default:
		t.Fatal("OnDrain never fired for the queued drain event")
	}
}

// TestDeliveryLoopFlushesQueuedOpenEventOnClose is the stream-OPEN variant:
// an OnStream event queued right before the conn died must still fire, and
// (per handleRemoteOpen's own doc comment) the handler must be able to use
// the *Stream even though the conn is already dead -- Read/Write on it return
// the conn's error promptly rather than hang, since Stream.wait selects on
// s.conn.closed.
func TestDeliveryLoopFlushesQueuedOpenEventOnClose(t *testing.T) {
	c := newUnrunConnForDelivery(t)

	st := newStream(c, 3, c.ourWindow, c.peerWindow)
	delivered := make(chan *Stream, 1)
	c.OnStream(func(s *Stream) { delivered <- s })

	c.deliveryQueue <- deliveryEvent{open: st}
	// Match what every real close path does: c.err is always set before
	// c.closed closes (fail/Close/handlePeerError all set it under c.mu in
	// the same critical section) -- Stream.wait relies on that to turn
	// <-s.conn.closed into a non-nil error, so close c.closed here the same
	// way or ReadContext's err/eof-less loop spins on a nil wait() forever.
	c.mu.Lock()
	c.err = &ConnError{Code: InternalErrorCode, Message: "test: connection died"}
	c.mu.Unlock()
	close(c.closed)

	runDeliveryLoopToCompletion(t, c)

	select {
	case got := <-delivered:
		if got != st {
			t.Fatalf("OnStream delivered %#v, want %#v", got, st)
		}
		// The conn is already dead: a read on this stream must return the
		// conn's own error promptly, never hang.
		buf := make([]byte, 1)
		readDone := make(chan struct{})
		go func() {
			_, _ = got.Read(buf)
			close(readDone)
		}()
		select {
		case <-readDone:
		case <-time.After(2 * time.Second):
			t.Fatal("Read on a stream delivered after the conn died did not return promptly")
		}
	default:
		t.Fatal("OnStream never fired for the queued open event")
	}
}

// TestDeliveryLoopFlushOrderingMixed checks that a mix of open/app/drain
// events flushed on close still delivers in exactly the order they were
// enqueued -- deliverEvent must not reorder across event kinds.
func TestDeliveryLoopFlushOrderingMixed(t *testing.T) {
	c := newUnrunConnForDelivery(t)

	st := newStream(c, 5, c.ourWindow, c.peerWindow)
	drainMsg := &DrainMsg{T: "drain", Reason: "maintenance"}

	var mu sync.Mutex
	var order []string
	c.OnStream(func(*Stream) {
		mu.Lock()
		order = append(order, "open")
		mu.Unlock()
	})
	c.OnApp(func(json.RawMessage) {
		mu.Lock()
		order = append(order, "app")
		mu.Unlock()
	})
	c.OnDrain(func(*DrainMsg) {
		mu.Lock()
		order = append(order, "drain")
		mu.Unlock()
	})

	c.deliveryQueue <- deliveryEvent{app: json.RawMessage(`{}`)}
	c.deliveryQueue <- deliveryEvent{open: st}
	c.deliveryQueue <- deliveryEvent{drain: drainMsg}
	c.deliveryQueue <- deliveryEvent{app: json.RawMessage(`{}`)}
	close(c.closed)

	runDeliveryLoopToCompletion(t, c)

	mu.Lock()
	defer mu.Unlock()
	if want := []string{"app", "open", "drain", "app"}; !slicesEqual(order, want) {
		t.Fatalf("flush order = %v, want %v", order, want)
	}
}

// TestDeliveryLoopFlushIsBounded checks that a queue filled to capacity is
// still fully flushed and deliveryLoop still returns -- the flush's
// cap(c.deliveryQueue) iteration bound must never cut the drain short for a
// legitimately full backlog (only guard against a producer that somehow
// outlives readerLoop, which cannot happen in practice).
func TestDeliveryLoopFlushIsBounded(t *testing.T) {
	c := newUnrunConnForDelivery(t)

	n := cap(c.deliveryQueue)
	var count atomic.Int64
	c.OnApp(func(json.RawMessage) { count.Add(1) })

	for i := 0; i < n; i++ {
		c.deliveryQueue <- deliveryEvent{app: json.RawMessage(`{}`)}
	}
	close(c.closed)

	runDeliveryLoopToCompletion(t, c)

	if got := count.Load(); got != int64(n) {
		t.Fatalf("delivered %d of %d queued events, want all %d flushed", got, n, n)
	}
}

func slicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestWriterLoopRoundRobinsAcrossStreams is a deterministic, non-timing-based
// unit test of the writer's DATA scheduler (OVERVIEW.md section 2.6 rule 3):
// 4 streams each have 8 chunks queued directly on their outQueue (bypassing
// WriteContext's credit/context plumbing, which is exercised elsewhere), and
// the wire order the writer loop produces must be strictly interleaved --
// one chunk per ready stream, round-robin -- rather than draining one
// stream's whole backlog before moving to the next. Unlike
// TestIntegrationConcurrentStreamsFairness (which measures fairness through
// real goroutine/OS scheduling and can only assert "more interleaving than a
// serial writer" to avoid flaking), this drives nextChunk directly against a
// fully-populated rotation, so the expected order is exact.
func TestWriterLoopRoundRobinsAcrossStreams(t *testing.T) {
	const nStreams = 4
	const nChunks = 8

	ws := newFakeWS()
	c := newConn(ws, RoleServer, Options{})
	c.opts.SetDefaults()
	c.session = "test-session"
	c.ourWindow = 262144
	c.peerWindow = 262144
	c.maxStreams = 64
	c.pingInterval = time.Minute
	c.pingTimeout = 5 * time.Minute
	c.finishHandshake()

	streams := make([]*Stream, nStreams)
	for i := 0; i < nStreams; i++ {
		id := uint32(2*i + 1) // server-opened ids are odd (OVERVIEW.md section 2.5)
		st := newStream(c, id, c.ourWindow, c.peerWindow)
		// Room for all nChunks at once: this test pushes a stream's whole
		// backlog before the writer loop starts draining it, unlike
		// WriteContext's real streamOutQueueCap-bounded, one-at-a-time usage.
		st.outQueue = make(chan *pendingChunk, nChunks)
		c.mu.Lock()
		c.streams[id] = st
		c.highestOpened = id
		c.mu.Unlock()
		streams[i] = st
	}

	// Queue all 8 chunks per stream before the writer loop ever runs, so the
	// rotation starts fully populated and the resulting order is determined
	// solely by nextChunk's round-robin logic, not by goroutine timing.
	for i, st := range streams {
		for n := 0; n < nChunks; n++ {
			pc := &pendingChunk{streamID: st.id, data: []byte{byte(i)}, done: make(chan struct{})}
			st.outQueue <- pc // outQueue's capacity must accommodate all nChunks for this test
		}
		c.markStreamReady(st)
	}

	go c.writerLoop()
	defer func() { _ = ws.CloseNow() }()

	const total = nStreams * nChunks
	got := make([]uint32, 0, total)
	for i := 0; i < total; i++ {
		select {
		case msg := <-ws.outbound:
			f, err := DecodeFrame(msg)
			if err != nil {
				t.Fatalf("DecodeFrame: %v", err)
			}
			got = append(got, f.StreamID)
		case <-time.After(2 * time.Second):
			t.Fatalf("only %d/%d frames written before timeout: %v", i, total, got)
		}
	}

	want := make([]uint32, 0, total)
	for n := 0; n < nChunks; n++ {
		for _, st := range streams {
			want = append(want, st.id)
		}
	}
	if len(got) != len(want) {
		t.Fatalf("got %d frames, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("wire order diverges at index %d: got %v, want %v", i, got, want)
		}
	}
}
