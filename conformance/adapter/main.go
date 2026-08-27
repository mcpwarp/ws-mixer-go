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
)

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
}

type closedDirs struct{ read, write bool }

func newState() *adapterState {
	return &adapterState{
		window: 262144, maxStreams: 64,
		pingIntMs: 30000, pingTOMs: 90000, helloTOMs: 10000,
		timeScale: 1, floorMs: 300,
		streams: make(map[uint32]*wsmixer.Stream),
		closed:  make(map[uint32]closedDirs),
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
				return wsmixer.WelcomeMeta{}, nil
			},
			OnConn: func(c *wsmixer.Conn) {
				st.setConn(c)
				c.OnApp(func(body json.RawMessage) {
					var v any
					_ = json.Unmarshal(body, &v)
					emit(map[string]any{"event": "app", "body": v})
				})
				c.OnDrain(func(d *wsmixer.DrainMsg) {
					emit(drainEvent(d))
				})
				go watchDisconnect(c)
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
			go watchDisconnect(c)
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
		s := st.stream(id)
		if s == nil {
			cmdErr(seq, fmt.Sprintf("write: no such stream %d", id))
			return
		}
		go func() {
			if _, err := s.Write(data); err != nil {
				cmdErr(seq, "write: "+err.Error())
				return
			}
			ack(seq)
		}()

	case "close_write":
		id := uint32ID(cmd["id"])
		s := st.stream(id)
		if s == nil {
			cmdErr(seq, fmt.Sprintf("close_write: no such stream %d", id))
			return
		}
		if err := s.CloseWrite(); err != nil {
			cmdErr(seq, "close_write: "+err.Error())
			return
		}
		ack(seq)
		emit(map[string]any{"event": "stream_closed", "id": id, "direction": "write"})
		if st.noteHalfClosed(id, "write") {
			emit(map[string]any{"event": "stream_closed", "id": id, "direction": "both"})
		}

	case "reset":
		id := uint32ID(cmd["id"])
		code, _ := cmd["code"].(float64)
		msg, _ := cmd["message"].(string)
		s := st.stream(id)
		if s == nil {
			cmdErr(seq, fmt.Sprintf("reset: no such stream %d", id))
			return
		}
		if err := s.Reset(wsmixer.ErrorCode(uint32(code)), msg); err != nil {
			cmdErr(seq, "reset: "+err.Error())
			return
		}
		ack(seq)

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
			emit(map[string]any{"event": "data", "id": s.ID(), "data_b64": encodeB64(buf[:n])})
		}
		if err != nil {
			if err == io.EOF {
				emit(map[string]any{"event": "stream_closed", "id": s.ID(), "direction": "read"})
				if st.noteHalfClosed(s.ID(), "read") {
					emit(map[string]any{"event": "stream_closed", "id": s.ID(), "direction": "both"})
				}
				// The read side reached a clean EOF, but the peer may still
				// illegally send more DATA later (OVERVIEW.md section 2.5's
				// half-closed(remote) row: "recv DATA -> RESET(STREAM_CLOSED)"
				// -- see spec/fixtures/sequences/data_after_close_toward_client.json).
				// Stream.Read would busy-loop returning io.EOF forever once
				// its internal eof flag is set (it has no blocking wait for
				// a later terminal error alone), so watch LastError() instead
				// of calling Read() again.
				waitForLateStreamReset(st, s)
			} else if se, ok := err.(*wsmixer.StreamError); ok {
				emitStreamReset(s, se)
			}
			return
		}
	}
}

func emitStreamReset(s *wsmixer.Stream, se *wsmixer.StreamError) {
	emit(map[string]any{
		"event": "stream_reset", "id": s.ID(),
		"code": uint32(se.Code), "name": se.Code.String(), "message": se.Message,
	})
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
				emitStreamReset(s, se)
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
		<-ticker.C
	}
}

func watchDisconnect(c *wsmixer.Conn) {
	<-c.Done()
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
