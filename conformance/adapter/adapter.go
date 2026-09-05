//go:build conformance

// Package adapter is the ws-mixer conformance runner's Go SDK harness
// (docs/CONFORMANCE.md section 1): a thin shell over go/wsmixer's public API
// (accept.go, client.go, conn.go, stream.go) plus the one test-only timing
// hook (wsmixer.AllowSubfloorTiming, go/wsmixer/conformance_hooks.go). It
// reads JSON-lines commands on stdin and writes JSON-lines events on stdout;
// it has no protocol logic of its own -- every wire behaviour comes from
// go/wsmixer itself.
//
// The go-server matrix role is factored behind the one-method ServerBackend
// interface below (docs/MIGRATION.md section 2.3), so this package has no
// hard dependency on any concrete HTTP upgrade/auth layer: a caller supplies
// a ServerBackend and calls Run. ws-mixer-go's own thin main
// (cmd/conformance-adapter) wires an acceptBackend directly on
// wsmixer.AcceptConn -- proving the protocol core conforms in both roles with
// no dependency on the separate HTTP/upgrade/auth layer. ws-mixer-server's
// thin main (its own cmd/conformance-adapter, importing this package) wires
// a listenerBackend on wsmixerserver.NewListener instead, proving the
// production server conforms.
//
// Requires `-tags conformance` to build (conformance/README.md): this
// package calls wsmixer.AllowSubfloorTiming
// (go/wsmixer/conformance_hooks.go), which only exists under that tag, so
// every consumer -- including ws-mixer-server's import of this package --
// must also build with `-tags conformance`.
package adapter

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mcpwarp/ws-mixer-go/wsmixer"
)

var (
	outMu sync.Mutex
	out   = bufio.NewWriter(os.Stdout)

	// startTime anchors t_ms (docs/CONFORMANCE.md section 1): milliseconds
	// since this adapter process started, monotonic, stamped on every data,
	// stream_closed, and stream_reset event.
	startTime = time.Now()
)

func tMs() int64 { return time.Since(startTime).Milliseconds() }

func emit(e map[string]any) {
	b, _ := json.Marshal(e)
	outMu.Lock()
	defer outMu.Unlock()
	out.Write(b)
	out.WriteByte('\n')
	out.Flush()
}

func ack(seq float64) { emit(map[string]any{"event": "ack", "seq": seq}) }
func cmdErr(seq float64, msg string) {
	emit(map[string]any{"event": "error", "seq": seq, "message": msg})
}
func adapterErr(msg string) { emit(map[string]any{"event": "error", "message": msg}) }

type adapterState struct {
	mu sync.Mutex

	// set_options
	window     int64
	maxStreams int64
	pingIntMs  int64
	pingTOMs   int64
	helloTOMs  int64
	timeScale  float64
	floorMs    int64
	allowSubfl bool

	conn    *wsmixer.Conn
	client  *wsmixer.Client // set only by connectReconnecting (S4(b)/(c))
	streams map[uint32]*wsmixer.Stream
	// streamOwner records which *wsmixer.Conn each tracked stream id
	// belongs to, when known (see teardownAllStreams). Only
	// connectReconnecting's OnStream populates this; the plain Dial and
	// server-accept paths leave an id unrecorded, which teardownAllStreams
	// treats as "belongs to whatever conn is tearing down".
	streamOwner map[uint32]*wsmixer.Conn
	listener    net.Listener
	httpSrv     *http.Server
	backend     ServerBackend

	// sdk/sdkVersion identify the running binary on the "ready" event and
	// (as sdk+"-conformance", the same sdkVersion) on the client role's
	// hello.agent -- set once from the Config passed to Run, zero-valued
	// otherwise (e.g. in tests that build an *adapterState directly and
	// never exercise the "connect" command).
	sdk        string
	sdkVersion string

	// closed tracks, per stream id, which half of a graceful close has
	// happened locally: read (this side saw EOF) and write (this side sent
	// CLOSE). Once both are true the adapter emits an extra
	// stream_closed{direction:"both"} alongside the existing per-direction
	// event (docs/CONFORMANCE.md section 1.2).
	closed map[uint32]closedDirs

	// pendingHello carries the peer's hello.agent/hello.meta from the
	// Authenticate hook (the only place a server-role adapter ever sees
	// them, go/wsmixer has no exported Conn accessor for either) across to
	// OnConn, which fires immediately afterward on the same handshake
	// goroutine (server.go: OnConn runs once the handshake completes). See
	// the "connected" event emitted from OnConn below, and
	// docs/CONFORMANCE.md section 1.2's event table.
	pendingHello *helloInfo

	// streamWorkers serializes write/close_write per stream id
	// (docs/CONFORMANCE.md section 1.1): `write` acks asynchronously (a
	// goroutine per call) while `close_write` used to run synchronously in
	// the command-reading loop, so a close_write issued before an
	// in-flight write's ack could send CLOSE (which jumps the writer
	// loop's DATA rotation entirely, sched.go's writerLoop) before that
	// write's chunk ever reached the wire, truncating the stream. Each
	// stream id gets its own FIFO worker goroutine fed by a bounded
	// (cap 64) channel, so commands for one stream take effect in the
	// order the runner sent them regardless of ack timing; acks are still
	// emitted from inside each queued job, unchanged in shape or timing
	// relative to their own command. The channel is a bound, not an
	// unbounded queue: a worker stalled behind a blocked write (e.g. send
	// credit exhausted) must not wedge the stdin-reading loop, so
	// enqueueOnStream never blocks on it -- once the 64-deep buffer is
	// full it error-acks the new command with "queue_full" instead of
	// waiting for room.
	//
	// RESET (OVERVIEW.md section 2.5: abortive, discards buffered data and
	// unblocks writers) deliberately does NOT go through this FIFO --
	// resetStream below cancels the worker's context (unblocking any
	// WriteContext call that is queued and currently blocked on send
	// credit), calls Stream.Reset directly, and drains any operations
	// still sitting in the channel with an error ack, rather than waiting
	// behind them.
	streamWorkers map[uint32]*streamWorker
}

// streamQueueCap bounds the per-stream FIFO worker's channel: how many
// write/close_write commands may be queued ahead of a stalled worker before
// enqueueOnStream starts rejecting new ones with a "queue_full" error ack
// instead of blocking the stdin-reading loop.
const streamQueueCap = 64

// resetWorkerDrainBudget bounds how long resetStream will wait for a job
// that was already running on the stream's worker to notice cancellation
// and report its own outcome before reset's own ack (see resetStream). It
// is not a normal-path latency: a job respecting its context (WriteContext)
// unblocks near-instantly once cancelled, so this only matters for a job
// that doesn't (e.g. CloseWrite has no context), where it caps the cost of
// waiting instead of leaving RESET's promptness unbounded.
const resetWorkerDrainBudget = 500 * time.Millisecond

// streamJob is one FIFO-queued write or close_write, carrying the seq its
// error ack (if any) should be reported against -- needed so a RESET that
// drains the queue out from under a stalled worker can still emit an
// aborted-command error ack per dropped job (see streamWorkers' comment).
type streamJob struct {
	seq float64
	run func(ctx context.Context)
}

// streamWorker is one stream id's FIFO worker: a bounded job channel plus a
// per-stream cancelable context, so RESET can unblock a job (e.g. a Write
// stuck on exhausted send credit) that is already running when it fires.
// done is closed by the worker's own goroutine right as it returns (always
// via ctx being cancelled -- see the goroutine in enqueueOnStream), which
// resetStream waits on (bounded) after cancelling: a job already running
// when RESET fires reports its own outcome (e.g. a blocked write's "write:
// context canceled" error ack) from inside itself before the goroutine
// loop notices ctx.Done() and returns, so waiting for done makes RESET's
// own ack observably follow that job's outcome rather than racing it.
type streamWorker struct {
	jobs   chan streamJob
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
}

type closedDirs struct{ read, write bool }

// helloInfo is the subset of go/wsmixer's *Hello the server-role "connected"
// event reports (docs/CONFORMANCE.md section 1.2's event table): the peer's
// agent identity and opaque meta, exactly as seen by the Authenticate hook.
type helloInfo struct {
	agent wsmixer.AgentInfo
	meta  json.RawMessage
}

func newState(be ServerBackend) *adapterState {
	return &adapterState{
		window: 262144, maxStreams: 64,
		pingIntMs: 30000, pingTOMs: 90000, helloTOMs: 10000,
		timeScale: 1, floorMs: 300,
		streams:       make(map[uint32]*wsmixer.Stream),
		streamOwner:   make(map[uint32]*wsmixer.Conn),
		closed:        make(map[uint32]closedDirs),
		streamWorkers: make(map[uint32]*streamWorker),
		backend:       be,
	}
}

// setPendingHello/takePendingHello hand the Authenticate hook's *Hello across
// to OnConn (see the pendingHello field comment).
func (s *adapterState) setPendingHello(h *wsmixer.Hello) {
	s.mu.Lock()
	s.pendingHello = &helloInfo{agent: h.Agent, meta: h.Meta}
	s.mu.Unlock()
}

func (s *adapterState) takePendingHello() *helloInfo {
	s.mu.Lock()
	defer s.mu.Unlock()
	h := s.pendingHello
	s.pendingHello = nil
	return h
}

// enqueueOnStream atomically looks up stream id and enqueues a job onto its
// FIFO worker (write and close_write only -- see the streamWorkers field
// comment; reset bypasses this entirely via resetStream), starting the
// worker's goroutine lazily on first use. The lookup, any worker creation,
// and the send onto the worker's channel all happen under s.mu, in the same
// critical section teardownStream/resetStream use to remove a stream --
// without that, a lookup that found the stream just before a concurrent
// teardownStream (from autoRead or watchDisconnect) removed it could still
// go on to create a brand new worker for an id nothing will ever tear down
// again, leaking its goroutine and map entry forever. Returns false if
// stream id is not (or no longer) tracked, in which case the caller should
// report a "no such stream" error; buildJob receives the exact
// *wsmixer.Stream found under the lock, so the job always acts on the
// stream that was actually still live at enqueue time.
//
// Commands for the same stream id run strictly in the order they were
// enqueued, regardless of how long any individual job takes (e.g. a
// blocking WriteContext). The worker's channel is bounded (streamQueueCap):
// if a stalled worker has let it fill up, enqueueOnStream does not block the
// stdin-reading loop waiting for room -- it error-acks seq with
// "queue_full" instead.
func (s *adapterState) enqueueOnStream(id uint32, seq float64, buildJob func(strm *wsmixer.Stream) func(ctx context.Context)) bool {
	s.mu.Lock()
	strm, ok := s.streams[id]
	if !ok {
		s.mu.Unlock()
		return false
	}
	w, wok := s.streamWorkers[id]
	if !wok {
		ctx, cancel := context.WithCancel(context.Background())
		w = &streamWorker{jobs: make(chan streamJob, streamQueueCap), ctx: ctx, cancel: cancel, done: make(chan struct{})}
		s.streamWorkers[id] = w
		go func() {
			defer close(w.done)
			for {
				select {
				case j := <-w.jobs:
					j.run(w.ctx)
				case <-w.ctx.Done():
					return
				}
			}
		}()
	}
	s.mu.Unlock()

	select {
	case w.jobs <- streamJob{seq: seq, run: buildJob(strm)}:
	default:
		cmdErr(seq, fmt.Sprintf("queue_full: stream %d's write/close_write queue (cap %d) is full", id, streamQueueCap))
	}
	return true
}

// teardownStream removes id's bookkeeping (streams, closed, streamWorkers)
// and stops its FIFO worker goroutine. Called once a stream reaches a
// terminal state -- closed both ways (noteHalfClosed returning true) or
// reset by either side -- or when the connection itself fails
// (teardownAllStreams). Without this the streamWorkers/streams map entries
// and the worker goroutine leak for the rest of the process's life.
func (s *adapterState) teardownStream(id uint32) {
	s.mu.Lock()
	delete(s.streams, id)
	delete(s.streamOwner, id)
	delete(s.closed, id)
	w, ok := s.streamWorkers[id]
	delete(s.streamWorkers, id)
	s.mu.Unlock()
	if ok {
		w.cancel()
	}
}

// teardownAllStreams tears down every still-tracked stream owned by conn,
// used once that connection itself fails (watchDisconnect, and
// connectReconnecting's per-conn watcher). Passing nil tears down every
// tracked stream regardless of owner -- used by the plain (non-reconnecting)
// Dial and server-accept paths, which never have more than one live conn at
// a time and so never stored an owner in the first place (S4(a):
// storeStream is only given a *wsmixer.Conn where a wsmixer.Client can
// legitimately have two live conns during a drain-triggered parallel
// reconnect -- connectReconnecting).
//
// Scoping by owner matters specifically for that Client case: OnConnect for
// the replacement conn can fire before the superseded conn's own
// disconnection is even observed (the drain race blockers 1/3 are about),
// so an unconditional "tear down everything" here used to wipe out the
// replacement conn's already-open streams along with the old conn's.
//
// Iterates the union of streams and streamWorkers -- not just streams -- so
// a worker whose id has no (or no longer has a) streams entry still gets
// found and torn down; relying on streams alone would miss it.
func (s *adapterState) teardownAllStreams(conn *wsmixer.Conn) {
	owns := func(id uint32) bool {
		if conn == nil {
			return true
		}
		owner, tracked := s.streamOwner[id]
		return !tracked || owner == conn
	}
	s.mu.Lock()
	idSet := make(map[uint32]struct{}, len(s.streams)+len(s.streamWorkers))
	for id := range s.streams {
		if owns(id) {
			idSet[id] = struct{}{}
		}
	}
	for id := range s.streamWorkers {
		if owns(id) {
			idSet[id] = struct{}{}
		}
	}
	ids := make([]uint32, 0, len(idSet))
	for id := range idSet {
		ids = append(ids, id)
	}
	s.mu.Unlock()
	for _, id := range ids {
		s.teardownStream(id)
	}
}

// drainAbortedJobs error-acks every job still buffered in w's channel
// (queued write/close_write commands that never got to run) after a RESET
// has cancelled the worker -- see resetStream and OVERVIEW.md section 2.5
// ("RESET is abortive; it discards buffered data and unblocks writers").
func drainAbortedJobs(w *streamWorker) {
	for {
		select {
		case j := <-w.jobs:
			cmdErr(j.seq, "reset: queued command aborted by RESET")
		default:
			return
		}
	}
}

// resetStream runs RESET out-of-band, immediately, bypassing the per-stream
// FIFO that serializes write/close_write (see the streamWorkers field
// comment and OVERVIEW.md section 2.5): it cancels the stream's worker
// context first, so a write already running on the worker and blocked on
// exhausted send credit (WriteContext) unblocks right away instead of
// stalling the reset behind it, then calls Stream.Reset (which discards
// buffered data and unblocks the peer), then drains anything still queued
// behind it with an error ack instead of letting it run against an
// already-reset stream.
//
// resetStream waits up to resetWorkerDrainBudget (500ms) on the worker's
// command loop for that already-running job to notice the cancellation and
// finish before proceeding. If the job ignores ctx and outlives the budget,
// resetStream proceeds anyway rather than blocking indefinitely: Stream.Reset
// still runs and drainAbortedJobs still error-acks the queue, but a job that
// was still queued behind the slow one may get scheduled on the worker
// before drainAbortedJobs reaches it and end up observing an already-reset
// stream instead of a clean abort. This is a bounded, accepted race -- the
// alternative is an unbounded wait that would defeat RESET's own promptness
// guarantee -- not a correctness bug.
func (s *adapterState) resetStream(id uint32, strm *wsmixer.Stream, code wsmixer.ErrorCode, msg string, seq float64) {
	s.mu.Lock()
	w, hasWorker := s.streamWorkers[id]
	delete(s.streamWorkers, id)
	delete(s.streams, id)
	delete(s.closed, id)
	s.mu.Unlock()

	if hasWorker {
		w.cancel()
		// Wait (bounded) for a job that was already running on this worker
		// to notice the cancellation and report its own outcome -- e.g. a
		// blocked write's "write: context canceled" error ack -- before
		// this reset's own ack below, so the runner never observes RESET
		// "complete" ahead of being told the write it unblocked failed. A
		// job respecting ctx (WriteContext) unblocks near-instantly, so
		// this bound is only ever a real wait for a pathological job that
		// ignores ctx entirely; it must not be unbounded, or such a job
		// would defeat RESET's own promptness guarantee.
		select {
		case <-w.done:
		case <-time.After(resetWorkerDrainBudget):
		}
	}

	if err := strm.Reset(code, msg); err != nil {
		cmdErr(seq, "reset: "+err.Error())
	} else {
		ack(seq)
	}

	if hasWorker {
		drainAbortedJobs(w)
	}
}

func (s *adapterState) options() wsmixer.Options {
	s.mu.Lock()
	defer s.mu.Unlock()
	opts := wsmixer.Options{
		Window:       s.window,
		MaxStreams:   s.maxStreams,
		PingInterval: time.Duration(s.pingIntMs) * time.Millisecond,
		PingTimeout:  time.Duration(s.pingTOMs) * time.Millisecond,
		HelloTimeout: time.Duration(s.helloTOMs) * time.Millisecond,
	}
	if s.allowSubfl {
		// wsmixer.AllowSubfloorTiming is only compiled in under the
		// `conformance` build tag (go/wsmixer/conformance_hooks.go); this
		// adapter must be built with `-tags conformance` (README.md).
		wsmixer.AllowSubfloorTiming(&opts)
	}
	return opts
}

// scaleMs applies the run's --time-scale to a duration this adapter receives
// verbatim from a command (as opposed to set_options's own already-scaled
// ping_interval_ms/ping_timeout_ms/hello_timeout_ms fields): docs/CONFORMANCE.md
// section 3.4, "every duration is multiplied ... on both sides." The floor
// is the runner's own effective timing floor, carried in set_options.floor_ms
// (coordination contract with conformance/runner; falls back to 300ms if the
// runner never sent one).
func (s *adapterState) scaleMs(ms int64) time.Duration {
	s.mu.Lock()
	scale := s.timeScale
	floor := s.floorMs
	s.mu.Unlock()
	if floor <= 0 {
		floor = 300
	}
	d := time.Duration(float64(ms)*scale) * time.Millisecond
	floorD := time.Duration(floor) * time.Millisecond
	if d < floorD {
		d = floorD
	}
	return d
}

// noteHalfClosed records that direction ("read" or "write") of stream id has
// closed locally, and reports whether both directions are now closed.
func (s *adapterState) noteHalfClosed(id uint32, direction string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	c := s.closed[id]
	switch direction {
	case "read":
		c.read = true
	case "write":
		c.write = true
	}
	s.closed[id] = c
	return c.read && c.write
}

// storeStream tracks st, owned by conn when known (nil for the plain Dial
// and server-accept paths, which never need the distinction -- see
// teardownAllStreams).
func (s *adapterState) storeStream(conn *wsmixer.Conn, st *wsmixer.Stream) {
	s.mu.Lock()
	s.streams[st.ID()] = st
	if conn != nil {
		s.streamOwner[st.ID()] = conn
	}
	s.mu.Unlock()
}

func (s *adapterState) stream(id uint32) *wsmixer.Stream {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.streams[id]
}

// setConn/getConn, setListener, and setHTTPSrv guard st.conn/listener/httpSrv
// with the same mutex as the rest of adapterState: OnConn's callback (running
// on the wsmixer accept goroutine) and the connect/listen command handlers
// write these fields concurrently with reads from later commands
// (open_stream, write, send_app, drain, close) on the stdin-reading
// goroutine.
func (s *adapterState) setConn(c *wsmixer.Conn) {
	s.mu.Lock()
	s.conn = c
	s.mu.Unlock()
}

func (s *adapterState) getConn() *wsmixer.Conn {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.conn
}

// setClient/getClient guard st.client, set only by connectReconnecting
// (S4(b)/(c)): the "close" and "shutdown" commands need to know whether a
// wsmixer.Client is driving the connection so they can act on it directly
// instead of on its current *wsmixer.Conn, which a live Client would just
// reconnect out from under them.
func (s *adapterState) setClient(cl *wsmixer.Client) {
	s.mu.Lock()
	s.client = cl
	s.mu.Unlock()
}

func (s *adapterState) getClient() *wsmixer.Client {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.client
}

func (s *adapterState) setListener(ln net.Listener) {
	s.mu.Lock()
	s.listener = ln
	s.mu.Unlock()
}

func (s *adapterState) setHTTPSrv(srv *http.Server) {
	s.mu.Lock()
	s.httpSrv = srv
	s.mu.Unlock()
}

// ServerBackend produces an http.Handler that yields handshaken *wsmixer.Conn
// (docs/MIGRATION.md section 2.3): the go-server matrix role is parameterized
// over this so the harness below has no hard dependency on a concrete
// upgrade/auth layer. Each consumer's own thin main supplies its own
// ServerBackend (see the package doc comment).
type ServerBackend interface {
	Handler(cfg ServerConfig) http.Handler
}

// ServerConfig is everything a ServerBackend needs to build its handler: the
// negotiated Options, the Authenticate hook, and the OnConn callback that
// hands the harness its *wsmixer.Conn.
type ServerConfig struct {
	Options      wsmixer.Options
	Authenticate func(ctx context.Context, h *wsmixer.Hello) (wsmixer.WelcomeMeta, error)
	OnConn       func(*wsmixer.Conn)
}

// Config is everything Run needs from its caller's thin main: the
// ServerBackend that wires the go-server matrix role, and the sdk identity
// reported on the "ready" event and (as sdk+"-conformance") on the
// client-role hello.agent.
type Config struct {
	Backend    ServerBackend
	SDK        string
	SDKVersion string
}

// Run is the adapter's stdin/stdout harness loop (docs/MIGRATION.md section
// 2.3): it emits "ready", then reads JSON-lines commands from stdin and
// dispatches them to handleCommand until a "shutdown" command is processed
// or stdin closes. It returns an exit code (currently always 0; reserved so
// a caller's thin main can `os.Exit(adapter.Run(cfg))`).
func Run(cfg Config) int {
	st := newState(cfg.Backend)
	st.sdk = cfg.SDK
	st.sdkVersion = cfg.SDKVersion
	emit(map[string]any{
		"event": "ready", "sdk": cfg.SDK, "sdk_version": cfg.SDKVersion,
		"roles": []string{"server", "client"},
	})

	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 0, 64*1024), 8<<20)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(strings.TrimSpace(string(line))) == 0 {
			continue
		}
		var cmd map[string]any
		if err := json.Unmarshal(line, &cmd); err != nil {
			adapterErr("bad command json: " + err.Error())
			continue
		}
		name, _ := cmd["cmd"].(string)
		seq, _ := cmd["seq"].(float64)
		handleCommand(st, name, seq, cmd)
		if name == "shutdown" {
			return 0
		}
	}
	return 0
}

func handleCommand(st *adapterState, name string, seq float64, cmd map[string]any) {
	switch name {
	case "set_options":
		st.mu.Lock()
		if v, ok := cmd["window"].(float64); ok {
			st.window = int64(v)
		}
		if v, ok := cmd["max_streams"].(float64); ok {
			st.maxStreams = int64(v)
		}
		if v, ok := cmd["ping_interval_ms"].(float64); ok {
			st.pingIntMs = int64(v)
		}
		if v, ok := cmd["ping_timeout_ms"].(float64); ok {
			st.pingTOMs = int64(v)
		}
		if v, ok := cmd["hello_timeout_ms"].(float64); ok {
			st.helloTOMs = int64(v)
		}
		if v, ok := cmd["time_scale"].(float64); ok {
			st.timeScale = v
		}
		if v, ok := cmd["floor_ms"].(float64); ok {
			st.floorMs = int64(v)
		}
		if v, ok := cmd["allow_subfloor_timing"].(bool); ok {
			st.allowSubfl = v
		}
		st.mu.Unlock()
		ack(seq)

	case "listen":
		addr, _ := cmd["addr"].(string)
		if addr == "" {
			addr = "127.0.0.1:0"
		}
		ln, err := net.Listen("tcp", addr)
		if err != nil {
			cmdErr(seq, "listen: "+err.Error())
			return
		}
		st.setListener(ln)
		handler := st.backend.Handler(ServerConfig{
			Options: st.options(),
			Authenticate: func(_ context.Context, h *wsmixer.Hello) (wsmixer.WelcomeMeta, error) {
				// docs/CONFORMANCE.md section 1: the adapter has no protocol
				// logic of its own; the only fixture that needs an auth
				// failure (auth_failure.json) triggers it via the raw actor
				// deliberately mismatching its own Authorization header
				// against hello.token, which go/wsmixer's built-in
				// AcceptConn rejects before this hook even runs.
				//
				// Stash h so OnConn (which fires right after this hook
				// succeeds) can report the peer's hello.agent/hello.meta on
				// the server-role "connected" event (docs/CONFORMANCE.md
				// section 1.2) -- go/wsmixer's Conn has no exported accessor
				// for either on the server side (Conn.Meta() only returns
				// hello.meta, not agent).
				st.setPendingHello(h)
				return wsmixer.WelcomeMeta{}, nil
			},
			OnConn: func(c *wsmixer.Conn) {
				st.setConn(c)
				hello := st.takePendingHello()
				helloObj := map[string]any{}
				if hello != nil {
					helloObj["agent"] = hello.agent
					if len(hello.meta) > 0 {
						var v any
						_ = json.Unmarshal(hello.meta, &v)
						helloObj["meta"] = v
					}
				}
				emit(map[string]any{
					"event": "connected", "role": "server",
					"session": c.Session(), "hello": helloObj,
				})
				c.OnApp(func(body json.RawMessage) {
					var v any
					_ = json.Unmarshal(body, &v)
					emit(map[string]any{"event": "app", "body": v})
				})
				c.OnDrain(func(d *wsmixer.DrainMsg) {
					emit(drainEvent(d))
				})
				go watchDisconnect(st, c)
			},
		})
		mux := http.NewServeMux()
		mux.Handle("/v1/tunnel", handler)
		srv := &http.Server{Handler: mux}
		st.setHTTPSrv(srv)
		go func() { _ = srv.Serve(ln) }()
		ack(seq)
		emit(map[string]any{"event": "listening", "seq": seq, "url": "ws://" + ln.Addr().String() + "/v1/tunnel"})

	case "connect":
		url, _ := cmd["url"].(string)
		token, _ := cmd["token"].(string)
		reconnectEnabled := false
		if rc, ok := cmd["reconnect"].(map[string]any); ok {
			if enabled, _ := rc["enabled"].(bool); enabled {
				reconnectEnabled = true
			}
		}
		if reconnectEnabled {
			go connectReconnecting(st, seq, url, token)
			return
		}
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			c, err := wsmixer.Dial(ctx, url, wsmixer.ClientOptions{
				Options: st.options(),
				Token:   token,
				Agent:   wsmixer.AgentInfo{SDK: st.sdk + "-conformance", SDKVersion: st.sdkVersion},
				OnStream: func(s *wsmixer.Stream) {
					st.storeStream(nil, s)
					emit(map[string]any{"event": "stream_opened", "id": s.ID()})
					go autoRead(st, s)
				},
				OnApp: func(body json.RawMessage) {
					var v any
					_ = json.Unmarshal(body, &v)
					emit(map[string]any{"event": "app", "body": v})
				},
				OnDrain: func(d *wsmixer.DrainMsg) { emit(drainEvent(d)) },
			})
			if err != nil {
				cmdErr(seq, "connect: "+err.Error())
				return
			}
			st.setConn(c)
			ack(seq)
			emit(map[string]any{
				"event": "connected", "seq": seq,
				"session": c.Session(),
				"welcome": map[string]any{"session": c.Session()},
			})
			go watchDisconnect(st, c)
		}()

	case "open_stream":
		conn := st.getConn()
		if conn == nil {
			cmdErr(seq, "open_stream before listen/connect completed")
			return
		}
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			s, err := conn.OpenStream(ctx)
			if err != nil {
				cmdErr(seq, "open_stream: "+err.Error())
				return
			}
			st.storeStream(conn, s)
			ack(seq)
			emit(map[string]any{"event": "stream_opened", "seq": seq, "id": s.ID()})
			go autoRead(st, s)
		}()

	case "write":
		id := uint32ID(cmd["id"])
		data := decodeB64(cmd["data_b64"])
		// Looked up and queued on this stream's own worker atomically (see
		// enqueueOnStream): write acks asynchronously, so a close_write/reset
		// for the same id that arrives before this ack must still wait
		// behind this write's chunk actually reaching the wire, or
		// CLOSE/RESET (which jump the writer loop's DATA rotation,
		// sched.go) could truncate it.
		ok := st.enqueueOnStream(id, seq, func(s *wsmixer.Stream) func(ctx context.Context) {
			return func(ctx context.Context) {
				if _, err := s.WriteContext(ctx, data); err != nil {
					cmdErr(seq, "write: "+err.Error())
					return
				}
				ack(seq)
			}
		})
		if !ok {
			cmdErr(seq, fmt.Sprintf("write: no such stream %d", id))
		}

	case "close_write":
		id := uint32ID(cmd["id"])
		ok := st.enqueueOnStream(id, seq, func(s *wsmixer.Stream) func(ctx context.Context) {
			return func(ctx context.Context) {
				if err := s.CloseWrite(); err != nil {
					cmdErr(seq, "close_write: "+err.Error())
					return
				}
				ack(seq)
				emit(map[string]any{"event": "stream_closed", "id": id, "direction": "write", "t_ms": tMs()})
				if st.noteHalfClosed(id, "write") {
					emit(map[string]any{"event": "stream_closed", "id": id, "direction": "both", "t_ms": tMs()})
					st.teardownStream(id)
				}
			}
		})
		if !ok {
			cmdErr(seq, fmt.Sprintf("close_write: no such stream %d", id))
		}

	case "reset":
		// RESET is abortive (OVERVIEW.md section 2.5) and runs out-of-band,
		// immediately -- it must not wait behind a blocked write on the
		// per-stream FIFO the way write/close_write do. See resetStream.
		id := uint32ID(cmd["id"])
		code, _ := cmd["code"].(float64)
		msg, _ := cmd["message"].(string)
		s := st.stream(id)
		if s == nil {
			cmdErr(seq, fmt.Sprintf("reset: no such stream %d", id))
			return
		}
		st.resetStream(id, s, wsmixer.ErrorCode(uint32(code)), msg, seq)

	case "send_app":
		conn := st.getConn()
		if conn == nil {
			cmdErr(seq, "send_app before connection established")
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := conn.SendApp(ctx, cmd["body"]); err != nil {
			cmdErr(seq, "send_app: "+err.Error())
			return
		}
		ack(seq)

	case "drain":
		conn := st.getConn()
		if conn == nil {
			cmdErr(seq, "drain before connection established")
			return
		}
		reason, _ := cmd["reason"].(string)
		var deadline time.Duration
		if v, ok := cmd["deadline_ms"].(float64); ok && v > 0 {
			deadline = st.scaleMs(int64(v))
		}
		ack(seq)
		go func() {
			_ = conn.Drain(context.Background(), reason, wsmixer.DrainOptions{Deadline: deadline})
		}()

	case "close":
		// S4(c): under a live wsmixer.Client, closing st.getConn() directly
		// just looks like an ordinary disconnect to the Client's own
		// reconnect state machine -- it reconnects instead of actually
		// closing. Route through the Client when one is driving this
		// connection; code/message are moot there (Client.Close always
		// finishes its own WIRE.md section 2.10 rule-14 sequence), so only
		// the plain (non-reconnecting) path still honours them.
		if cl := st.getClient(); cl != nil {
			ack(seq)
			go func() { _ = cl.Close(context.Background()) }()
			return
		}
		conn := st.getConn()
		if conn == nil {
			cmdErr(seq, "close before connection established")
			return
		}
		code, _ := cmd["code"].(float64)
		msg, _ := cmd["message"].(string)
		ack(seq)
		go func() { _ = conn.Close(uint32(code), msg) }()

	case "shutdown":
		// S4(b): connectReconnecting's wsmixer.Client owns background
		// goroutines (attemptLoop/watchConn/its callback loop) that outlive
		// the conn it happens to be holding at any moment -- closing just
		// st.conn would leave them running. Close the Client here so
		// nothing leaks past this command, matching Run()'s promise that
		// "shutdown" ends the adapter's own state cleanly (important for
		// in-process callers like race_test.go, which reuse the process
		// across many Run() calls; a real standalone binary's os.Exit would
		// mask the leak).
		if cl := st.getClient(); cl != nil {
			_ = cl.Close(context.Background())
		}
		ack(seq)

	default:
		cmdErr(seq, "unsupported command "+name)
	}
}

func drainEvent(d *wsmixer.DrainMsg) map[string]any {
	e := map[string]any{"event": "drain", "reason": d.Reason, "last_stream_id": d.LastStreamID}
	if d.DeadlineMS != nil {
		e["deadline_ms"] = *d.DeadlineMS
	}
	if d.Message != "" {
		e["message"] = d.Message
	}
	return e
}

func autoRead(st *adapterState, s *wsmixer.Stream) {
	buf := make([]byte, 16384)
	for {
		n, err := s.Read(buf)
		if n > 0 {
			emit(map[string]any{"event": "data", "id": s.ID(), "data_b64": encodeB64(buf[:n]), "t_ms": tMs()})
		}
		if err != nil {
			if err == io.EOF {
				emit(map[string]any{"event": "stream_closed", "id": s.ID(), "direction": "read", "t_ms": tMs()})
				if st.noteHalfClosed(s.ID(), "read") {
					// Both directions closed: the stream is fully terminal
					// (OVERVIEW.md section 2.5), so there is nothing further
					// to legally watch for -- tear down now rather than
					// leaking a poll goroutine per stream for the rest of
					// the connection's life (see the 50-cycle open/close
					// case in race_test.go).
					emit(map[string]any{"event": "stream_closed", "id": s.ID(), "direction": "both", "t_ms": tMs()})
					st.teardownStream(s.ID())
				} else {
					// Only the read side closed (half-closed-remote); this
					// side may still send, and the peer may still illegally
					// send more DATA later (OVERVIEW.md section 2.5's
					// half-closed(remote) row: "recv DATA -> RESET(STREAM_CLOSED)"
					// -- see spec/fixtures/sequences/data_after_close_toward_client.json).
					// Stream.Read would busy-loop returning io.EOF forever
					// once its internal eof flag is set (it has no blocking
					// wait for a later terminal error alone), so watch
					// LastError() instead of calling Read() again --
					// bounded by the stream itself reaching a terminal
					// teardown (e.g. this side's own later close_write
					// completing "both") or the connection closing.
					waitForLateStreamReset(st, s)
				}
			} else if se, ok := err.(*wsmixer.StreamError); ok {
				emitStreamReset(st, s, se)
			}
			return
		}
	}
}

func emitStreamReset(st *adapterState, s *wsmixer.Stream, se *wsmixer.StreamError) {
	emit(map[string]any{
		"event": "stream_reset", "id": s.ID(),
		"code": uint32(se.Code), "name": se.Code.String(), "message": se.Message,
		"t_ms": tMs(),
	})
	st.teardownStream(s.ID())
}

// waitForLateStreamReset polls Stream.LastError() (there is no blocking
// public API for "wait until a stream that already saw read-EOF later gets
// RESET") until either a *wsmixer.StreamError appears (emitted as
// stream_reset) or the connection itself closes.
func waitForLateStreamReset(st *adapterState, s *wsmixer.Stream) {
	conn := st.getConn()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := s.LastError(); err != nil {
			if se, ok := err.(*wsmixer.StreamError); ok {
				emitStreamReset(st, s, se)
			}
			return
		}
		if conn != nil {
			select {
			case <-conn.Done():
				return
			default:
			}
		}
		if st.stream(s.ID()) == nil {
			// The stream was already torn down by another path (this
			// side's own later close_write completing "both", or a RESET
			// handled elsewhere) -- once that happens there is nothing
			// further to legally watch for, and continuing to poll would
			// leak this goroutine for the rest of the connection's life.
			return
		}
		<-ticker.C
	}
}

// connectReconnecting runs the "connect" command's connect.reconnect.enabled
// path (docs/CONFORMANCE.md section 1.1, only the drain_reconnect scenario
// sets this): built on wsmixer.Client, the Go SDK's own reconnecting client
// (client_reconnect.go), instead of the single-attempt wsmixer.Dial the
// plain "connect" path above uses. OnConnect fires on every successful
// welcome, including reconnects; the first one is reported as the ordinary
// "connected" ack/event (matching the plain path's shape), and every one
// after that as `reconnected` (coordination contract (b), mirrors the JS
// adapter's own onConnect handling in adapter.mjs).
func connectReconnecting(st *adapterState, seq float64, url, token string) {
	var connectedOnce atomic.Bool
	// cl is referenced from inside its own ClientConfig (OnStream), so it
	// has to be declared before the wsmixer.NewClient call that assigns it.
	var cl *wsmixer.Client

	// N4: cl.Conn() read right after a successful Connect() can already be
	// nil -- Connect only waits for the first welcome, not for OnConnect's
	// enqueued callback to have actually run, and an instant post-welcome
	// disconnect can also race cl.conn back to nil before this goroutine
	// gets to read it. firstConnCh instead captures the exact *wsmixer.Conn
	// OnConnect handed us for the first (and, since it's only ever
	// buffer-1'd on a successful non-blocking send, only the first) welcome
	// -- safe to read fields like Session() off even after that conn has
	// since ended, since those are fixed at construction.
	firstConnCh := make(chan *wsmixer.Conn, 1)

	cl = wsmixer.NewClient(url, wsmixer.StaticToken(token), wsmixer.ClientConfig{
		Options: st.options(),
		Agent:   wsmixer.AgentInfo{SDK: st.sdk + "-conformance", SDKVersion: st.sdkVersion},
		OnStream: func(s *wsmixer.Stream) {
			// nit 4: s.Conn() is the conn this specific stream was opened
			// on, fixed at construction -- unlike cl.Conn(), which reads
			// whatever the client's *current* active conn happens to be and
			// races the window between a new conn's dispatch loop starting
			// (and delivering this very OnStream) and onAttemptSucceeded
			// actually assigning it to cl.conn. Recording the right owner is
			// what lets teardownAllStreams (S4(a)) scope a disconnect's
			// cleanup to its own streams instead of also sweeping up
			// streams that already belong to a reconnect's replacement conn.
			st.storeStream(s.Conn(), s)
			emit(map[string]any{"event": "stream_opened", "id": s.ID()})
			go autoRead(st, s)
		},
		OnApp: func(body json.RawMessage) {
			var v any
			_ = json.Unmarshal(body, &v)
			emit(map[string]any{"event": "app", "body": v})
		},
		OnDrain: func(d *wsmixer.DrainMsg) { emit(drainEvent(d)) },
		OnConnect: func(c *wsmixer.Conn, _ *wsmixer.WelcomeMsg) {
			st.setConn(c)
			// S4(a): tear down only c's own streams once it ends, in a
			// watcher bound to this specific conn -- not from OnDisconnect,
			// which carries no *wsmixer.Conn to scope by and used to call
			// the unscoped teardownAllStreams() unconditionally. That could
			// (and did) run after a drain-triggered parallel reconnect's
			// OnConnect had already fired for the replacement conn, wiping
			// out streams that belong to the new connection, not the one
			// that actually disconnected.
			go func(c *wsmixer.Conn) {
				<-c.Done()
				st.teardownAllStreams(c)
			}(c)
			if connectedOnce.Swap(true) {
				emit(map[string]any{"event": "reconnected", "session": c.Session()})
			} else {
				select {
				case firstConnCh <- c:
				default:
				}
			}
		},
		OnDisconnect: func(r wsmixer.DisconnectReason) {
			e := map[string]any{"event": "disconnected", "message": r.Message, "fatal": r.Fatal}
			if r.WSCode != 0 {
				e["ws_code"] = r.WSCode
			}
			if r.HasErrorCode {
				e["error_code"] = uint32(r.ErrorCode)
				e["error_name"] = r.ErrorName
			}
			emit(e)
		},
	})
	st.setClient(cl)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := cl.Connect(ctx); err != nil {
		cmdErr(seq, "connect: "+err.Error())
		return
	}
	// N4: block for the conn OnConnect actually handed us rather than
	// racing cl.Conn() -- Connect() returning nil only guarantees OnConnect
	// was enqueued, not that callbackLoop has run it yet, and the
	// conn may have already disconnected by the time this goroutine gets
	// scheduled either way. See firstConnCh's declaration above.
	c := <-firstConnCh
	ack(seq)
	emit(map[string]any{
		"event": "connected", "seq": seq,
		"session": c.Session(),
		"welcome": map[string]any{"session": c.Session()},
	})
}

func watchDisconnect(st *adapterState, c *wsmixer.Conn) {
	<-c.Done()
	// The connection itself is gone: none of its still-open streams will
	// ever reach a graceful terminal state on their own, so tear them all
	// down here rather than leaking their FIFO worker goroutines forever.
	st.teardownAllStreams(c)
	err := c.Err()
	e := map[string]any{"fatal": false}
	if ce, ok := err.(*wsmixer.ConnError); ok {
		e["error_code"] = uint32(ce.Code)
		e["error_name"] = ce.Code.String()
		e["ws_code"] = ce.CloseCode()
		e["message"] = ce.Message
		e["fatal"] = ce.Code != wsmixer.NoError
	} else if err != nil {
		e["message"] = err.Error()
	} else {
		e["message"] = "closed"
	}
	e["event"] = "disconnected"
	emit(e)
}

func uint32ID(v any) uint32 {
	f, _ := v.(float64)
	return uint32(f)
}

func decodeB64(v any) []byte {
	s, _ := v.(string)
	b, _ := base64.StdEncoding.DecodeString(s)
	return b
}

func encodeB64(b []byte) string { return base64.StdEncoding.EncodeToString(b) }
