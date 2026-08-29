// Command go-adapter is the ws-mixer conformance runner's Go SDK adapter
// (docs/CONFORMANCE.md section 1): a thin shell over go/wsmixer's public API
// (server.go, client.go, conn.go, stream.go) plus the one test-only timing
// hook (wsmixer.AllowSubfloorTiming, go/wsmixer/conformance_hooks.go). It
// reads JSON-lines commands on stdin and writes JSON-lines events on stdout;
// it has no protocol logic of its own -- every wire behaviour comes from
// go/wsmixer itself.
//
// Must be built with `-tags conformance` (conformance/README.md, the
// Makefile note there) -- without that tag, go/wsmixer/conformance_hooks.go
// is excluded from the build and wsmixer.AllowSubfloorTiming does not exist.
package main

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
	"time"

	"github.com/mcpwarp/ws-mixer/go/wsmixer"
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

	conn     *wsmixer.Conn
	streams  map[uint32]*wsmixer.Stream
	listener net.Listener
	httpSrv  *http.Server

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

func newState() *adapterState {
	return &adapterState{
		window: 262144, maxStreams: 64,
		pingIntMs: 30000, pingTOMs: 90000, helloTOMs: 10000,
		timeScale: 1, floorMs: 300,
		streams:       make(map[uint32]*wsmixer.Stream),
		closed:        make(map[uint32]closedDirs),
		streamWorkers: make(map[uint32]*streamWorker),
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
	delete(s.closed, id)
	w, ok := s.streamWorkers[id]
	delete(s.streamWorkers, id)
	s.mu.Unlock()
	if ok {
		w.cancel()
	}
}

// teardownAllStreams tears down every still-tracked stream, used once the
// connection itself fails (watchDisconnect): none of those streams will
// ever reach a graceful terminal state on their own once the socket is
// gone, so their FIFO workers would otherwise leak forever. Iterates the
// union of streams and streamWorkers -- not just streams -- so a worker
// whose id has no (or no longer has a) streams entry still gets found and
// torn down; relying on streams alone would miss it.
func (s *adapterState) teardownAllStreams() {
	s.mu.Lock()
	idSet := make(map[uint32]struct{}, len(s.streams)+len(s.streamWorkers))
	for id := range s.streams {
		idSet[id] = struct{}{}
	}
	for id := range s.streamWorkers {
		idSet[id] = struct{}{}
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

func (s *adapterState) storeStream(st *wsmixer.Stream) {
	s.mu.Lock()
	s.streams[st.ID()] = st
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

func main() {
	st := newState()
	emit(map[string]any{
		"event": "ready", "sdk": "ws-mixer-go", "sdk_version": "0.1.0",
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
			return
		}
	}
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
		listener := wsmixer.NewListener(wsmixer.ServerOptions{
			Options: st.options(),
			Authenticate: func(_ context.Context, h *wsmixer.Hello) (wsmixer.WelcomeMeta, error) {
				// docs/CONFORMANCE.md section 1: the adapter has no protocol
				// logic of its own; the only fixture that needs an auth
				// failure (auth_failure.json) triggers it via the raw actor
				// deliberately mismatching its own Authorization header
				// against hello.token, which go/wsmixer's built-in
				// performServerHandshake rejects before this hook even runs.
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
		mux.Handle("/v1/tunnel", listener)
		srv := &http.Server{Handler: mux}
		st.setHTTPSrv(srv)
		go func() { _ = srv.Serve(ln) }()
		ack(seq)
		emit(map[string]any{"event": "listening", "seq": seq, "url": "ws://" + ln.Addr().String() + "/v1/tunnel"})

	case "connect":
		// wsmixer.Dial is a minimal client with no reconnect/backoff loop of
		// its own (client.go: "that is the JS SDK's job") -- a scenario that
		// asks for reconnect (docs/CONFORMANCE.md section 1.1's
		// connect.reconnect.enabled, only drain_reconnect) is structurally
		// inapplicable to a Go client peer, the same way listen/open_stream
		// are inapplicable to the JS adapter. Reply unsupported so the
		// runner SKIPs this cell instead of timing out waiting for a
		// `reconnected` event the Go SDK can never emit.
		if rc, ok := cmd["reconnect"].(map[string]any); ok {
			if enabled, _ := rc["enabled"].(bool); enabled {
				emit(map[string]any{
					"event": "error", "seq": seq, "ok": false, "unsupported": true,
					"error": "unsupported", "message": "connect.reconnect.enabled: the Go SDK client has no reconnect loop",
				})
				return
			}
		}
		url, _ := cmd["url"].(string)
		token, _ := cmd["token"].(string)
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			c, err := wsmixer.Dial(ctx, url, wsmixer.ClientOptions{
				Options: st.options(),
				Token:   token,
				Agent:   wsmixer.AgentInfo{SDK: "ws-mixer-go-conformance", SDKVersion: "0.1.0"},
				OnStream: func(s *wsmixer.Stream) {
					st.storeStream(s)
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
			st.storeStream(s)
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

func watchDisconnect(st *adapterState, c *wsmixer.Conn) {
	<-c.Done()
	// The connection itself is gone: none of its still-open streams will
	// ever reach a graceful terminal state on their own, so tear them all
	// down here rather than leaking their FIFO worker goroutines forever.
	st.teardownAllStreams()
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
