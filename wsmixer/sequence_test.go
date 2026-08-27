package wsmixer

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// This harness drives spec/fixtures/sequences/*.json against a real *Conn
// wired to an in-memory fake transport (fakews_test.go), per spec/README.md's
// "Consuming these fixtures from an SDK test harness" section.
//
// Fixtures that depend on wall-clock timeouts (hello_timeout,
// ping_pong_then_dead_peer_timeout, drain_with_inflight_timeout: the only
// three with a wait_ms step) are intentionally out of scope for this generic
// harness and are instead covered behaviorally by dedicated tests in
// timing_test.go using scaled-down Options, per this task's explicit
// allowance to "use a fake clock or short real timeouts scaled via Options".
var sequenceWaitMsFixtures = map[string]bool{
	"hello_timeout.json":                    true,
	"ping_pong_then_dead_peer_timeout.json": true,
	"drain_with_inflight_timeout.json":      true,
}

type sequenceStep struct {
	Recv   json.RawMessage `json:"recv"`
	Send   json.RawMessage `json:"send"`
	WaitMs *int            `json:"wait_ms"`
	Expect map[string]any  `json:"expect"`
}

type sequenceFixture struct {
	Name        string
	Description string         `json:"description"`
	Role        string         `json:"role"`
	Steps       []sequenceStep `json:"steps"`
}

func loadSequenceFixtures(t *testing.T) []sequenceFixture {
	t.Helper()
	dir := filepath.Join("..", "..", "spec", "fixtures", "sequences")
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("reading sequences dir: %v", err)
	}
	var out []sequenceFixture
	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".json" {
			continue
		}
		if sequenceWaitMsFixtures[e.Name()] {
			continue
		}
		data, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			t.Fatalf("reading %s: %v", e.Name(), err)
		}
		var f sequenceFixture
		if err := json.Unmarshal(data, &f); err != nil {
			t.Fatalf("parsing %s: %v", e.Name(), err)
		}
		f.Name = e.Name()
		out = append(out, f)
	}
	if len(out) == 0 {
		t.Fatal("no sequence fixtures found")
	}
	return out
}

func TestSequenceFixtures(t *testing.T) {
	for _, seq := range loadSequenceFixtures(t) {
		seq := seq
		t.Run(seq.Name, func(t *testing.T) {
			runSequence(t, seq)
		})
	}
}

type seqRuntime struct {
	t         *testing.T
	c         *Conn
	fake      *fakeWS
	streams   map[uint32]*Stream
	cur       *Stream
	openCh    chan *Stream
	lastFrame *Frame // last frame captured by doSend's passive path, for streams with no live *Stream (e.g. refused at OPEN)
}

// harnessHelloToken is the token the harness's synthetic hello (built for a
// fixture that starts mid-connection with no hello of its own) carries, and
// the bearer performServerHandshake is told to expect for it.
const harnessHelloToken = "harness-synthetic-token"

// harnessAuthenticate is the sequence harness's Authenticate hook: it accepts
// every token except the one fixture (auth_failure.json) that names a
// specifically revoked one, exactly the way OVERVIEW.md section 3.4 expects a
// real Authenticate hook to reject a bad/expired token.
func harnessAuthenticate(ctx context.Context, h *Hello) (WelcomeMeta, error) {
	if h.Token == "stale-or-revoked-token" {
		return WelcomeMeta{}, Unauthorized("token invalid: signature verification failed")
	}
	return WelcomeMeta{}, nil
}

func syntheticHello() *HelloMsg {
	return &HelloMsg{
		T: "hello", V: 1, Token: harnessHelloToken,
		Agent: AgentInfo{SDK: "ws-mixer-go-harness", SDKVersion: "0.0.0"},
	}
}

func runSequence(t *testing.T, seq sequenceFixture) {
	t.Helper()
	fake := newFakeWS()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	opts := Options{
		Window: 262144, MaxStreams: 64,
		PingInterval: time.Hour, PingTimeout: 2 * time.Hour, HelloTimeout: time.Hour,
		Logger: logger, Metrics: NoopMetrics{},
	}
	applyWelcomeStepOptions(seq, &opts)
	var role Role
	if seq.Role == "client" {
		role = RoleClient
	} else {
		role = RoleServer
	}
	c := newConn(fake, role, opts)

	rt := &seqRuntime{t: t, c: c, fake: fake, streams: map[uint32]*Stream{}, openCh: make(chan *Stream, 16)}
	c.OnStream(func(st *Stream) {
		rt.openCh <- st
	})

	// The handshake (real or, for a fixture that starts mid-connection,
	// synthetic-but-real) always completes before run() starts, exactly like
	// production: performHandshakePrelude returns the index of the first
	// step not already consumed by it.
	startIdx := performHandshakePrelude(t, rt, seq, opts)

	if scriptsAutonomousPing(seq) {
		// pong_for_unsent_id.json scripts a "send ping" step: the real
		// pingLoop must actually fire within doSend's 2s wait bound. This
		// only shortens the ticker pingLoop reads post-handshake -- the
		// negotiated welcome.ping_interval that just went out over the wire
		// (and must satisfy the protocol's 5000ms floor) is untouched.
		c.pingInterval = 20 * time.Millisecond
	}

	c.run()
	defer func() { _ = fake.Close(0, "") }()

	for i := startIdx; i < len(seq.Steps); i++ {
		step := seq.Steps[i]
		switch {
		case step.Recv != nil:
			rt.doRecv(step.Recv)
		case step.Send != nil:
			rt.doSend(step.Send)
		case step.WaitMs != nil:
			time.Sleep(time.Millisecond) // negligible: only non-timing fixtures reach here
		case step.Expect != nil:
			rt.checkExpect(step.Expect)
		default:
			t.Fatalf("step %d has none of recv/send/wait_ms/expect", i)
		}
	}
}

// performHandshakePrelude completes the handshake for real, before run()
// starts, exactly as production does (server.go's performServerHandshake,
// client.go's clientHandshake) — the one exception being a fixture that
// deliberately tests pre-handshake behavior (its first step is a "recv" of
// something other than hello), which is left alone with handshakeDone still
// false. It returns the index of the first step the main loop should still
// process; steps consumed here are not replayed.
//
// Classification is keyed on the fixture's first step shape (never on a
// timeout):
//   - role client, first step "recv welcome": the fixture never scripts its
//     own "send hello" (e.g. max_streams_exceeded.json) -- run the real
//     client handshake (clientHandshake) against the scripted welcome.
//   - role server, first step "recv hello": the fixture scripts its own real
//     handshake -- run performServerHandshake against the scripted hello,
//     consuming just that one step so the loop's next step ("send welcome"
//     or "send error") asserts against what performServerHandshake actually
//     produced.
//   - role server, first step "send" (a welcome, or a stream frame like
//     OPEN): the fixture starts mid-connection with no hello of its own
//     (e.g. pong_for_unsent_id.json, window_after_close.json,
//     late_data_after_reset.json, request_response_half_close.json) -- run
//     performServerHandshake against a synthetic hello so the Conn is
//     genuinely handshaken, then let the loop run from the top.
//   - anything else (a "recv" of something other than hello/welcome): the
//     fixture is deliberately exercising pre-handshake behavior
//     (app_before_welcome.json, frame_before_hello.json,
//     data_for_never_opened_stream.json, unknown_control_message_type.json)
//     -- no prelude.
func performHandshakePrelude(t *testing.T, rt *seqRuntime, seq sequenceFixture, opts Options) int {
	t.Helper()
	if len(seq.Steps) == 0 {
		return 0
	}
	c := rt.c
	step0 := seq.Steps[0]
	ctx := context.Background()

	if seq.Role == "client" {
		if step0.Recv == nil {
			return 0
		}
		var probe genericStepProbe
		_ = json.Unmarshal(step0.Recv, &probe)
		if probe.T != "welcome" {
			return 0
		}
		rt.fake.feedInbound(EncodeData(0, step0.Recv))
		if err := clientHandshake(ctx, c, ClientOptions{
			Options: opts, Token: harnessHelloToken,
			Agent: AgentInfo{SDK: "ws-mixer-go-harness", SDKVersion: "0.0.0"},
		}); err != nil {
			t.Fatalf("prelude client handshake: %v", err)
		}
		// clientHandshake wrote a real hello frame before reading the welcome
		// we just fed it; no step scripts that hello, so drain it here.
		<-rt.fake.outbound
		return 1
	}

	if step0.Recv != nil {
		var probe genericStepProbe
		_ = json.Unmarshal(step0.Recv, &probe)
		if probe.T != "hello" {
			return 0
		}
		var h struct {
			Token string `json:"token"`
		}
		_ = json.Unmarshal(step0.Recv, &h)
		rt.fake.feedInbound(EncodeData(0, step0.Recv))
		err := performServerHandshake(ctx, c, h.Token, serverHandshakeOptions{
			Options: opts, Authenticate: harnessAuthenticate,
		})
		if err != nil {
			ce, ok := err.(*ConnError)
			if !ok {
				t.Fatalf("prelude server handshake: %v", err)
			}
			c.fail(ce)
		}
		return 1
	}

	if step0.Send == nil {
		return 0
	}
	var probe genericStepProbe
	_ = json.Unmarshal(step0.Send, &probe)
	hb, err := json.Marshal(syntheticHello())
	if err != nil {
		t.Fatalf("marshaling synthetic hello: %v", err)
	}
	rt.fake.feedInbound(EncodeData(0, hb))
	if err := performServerHandshake(ctx, c, harnessHelloToken, serverHandshakeOptions{
		Options: opts, Authenticate: harnessAuthenticate,
	}); err != nil {
		t.Fatalf("prelude server handshake: %v", err)
	}
	if probe.T != "welcome" {
		// The scripted first step is not itself "send welcome" (e.g. it's
		// "send OPEN"), so no step will consume the real welcome the prelude
		// just produced -- drain it here instead of leaving it ahead of the
		// frame the first real step expects on the wire.
		<-rt.fake.outbound
	}
	return 0
}

// applyWelcomeStepOptions scans a fixture's steps for a welcome message
// (either side may script it) and pulls window/max_streams into opts, so a
// server-role fixture's own advertised window (e.g. a deliberately small one
// to set up a flow-control test) actually governs the Conn under test rather
// than a one-size-fits-all default.
func applyWelcomeStepOptions(seq sequenceFixture, opts *Options) {
	for _, step := range seq.Steps {
		for _, raw := range [][]byte{step.Recv, step.Send} {
			if raw == nil {
				continue
			}
			var w struct {
				T          string `json:"t"`
				Window     int64  `json:"window"`
				MaxStreams int64  `json:"max_streams"`
			}
			_ = json.Unmarshal(raw, &w)
			if w.T != "welcome" {
				continue
			}
			if w.Window > 0 {
				opts.Window = w.Window
			}
			if w.MaxStreams > 0 {
				opts.MaxStreams = w.MaxStreams
			}
		}
	}
}

// scriptsAutonomousPing reports whether a fixture scripts a "send ping" step
// (only pong_for_unsent_id.json does): runSequence uses this to shorten the
// Conn's ping ticker after the handshake completes, so a real autonomous
// ping actually fires within doSend's 2s wait bound.
func scriptsAutonomousPing(seq sequenceFixture) bool {
	for _, step := range seq.Steps {
		if step.Send == nil {
			continue
		}
		var p genericStepProbe
		_ = json.Unmarshal(step.Send, &p)
		if p.T == "ping" {
			return true
		}
	}
	return false
}

// --- building/decoding wire bytes from fixture step content -----------------

type genericStepProbe struct {
	Type string `json:"type"`
	T    string `json:"t"`
}

type frameStepContent struct {
	Type          string  `json:"type"`
	StreamID      uint32  `json:"stream_id"`
	PayloadHex    string  `json:"payload_hex"`
	PayloadLength *int    `json:"payload_length"`
	Increment     *uint32 `json:"increment"`
	Code          *uint32 `json:"code"`
	Message       *string `json:"message"`
}

func buildFrameBytes(fs frameStepContent) []byte {
	switch fs.Type {
	case "OPEN":
		return EncodeOpen(fs.StreamID)
	case "DATA":
		var payload []byte
		if fs.PayloadHex != "" {
			payload, _ = hex.DecodeString(fs.PayloadHex)
		} else if fs.PayloadLength != nil {
			payload = make([]byte, *fs.PayloadLength)
		}
		return EncodeData(fs.StreamID, payload)
	case "WINDOW":
		inc := uint32(0)
		if fs.Increment != nil {
			inc = *fs.Increment
		}
		return EncodeWindow(fs.StreamID, inc)
	case "CLOSE":
		return EncodeClose(fs.StreamID)
	case "RESET":
		code := ErrorCode(0)
		if fs.Code != nil {
			code = ErrorCode(*fs.Code)
		}
		msg := ""
		if fs.Message != nil {
			msg = *fs.Message
		}
		return EncodeReset(fs.StreamID, code, msg)
	}
	return nil
}

// doRecv feeds one fixture step to the connection under test as if it arrived
// from the peer.
func (rt *seqRuntime) doRecv(raw json.RawMessage) {
	var probe genericStepProbe
	_ = json.Unmarshal(raw, &probe)
	if probe.Type != "" {
		var fs frameStepContent
		_ = json.Unmarshal(raw, &fs)
		rt.fake.feedInbound(buildFrameBytes(fs))
		if fs.Type == "OPEN" && fs.StreamID != 0 {
			// Client role: the server "opened" this stream; wait briefly for
			// the resulting OnStream callback. It may never arrive if the
			// connection refuses the OPEN outright (e.g. before the
			// handshake completes, or over max_streams).
			rt.cur = rt.tryStreamFor(fs.StreamID)
		} else if fs.StreamID != 0 {
			rt.cur = rt.streams[fs.StreamID]
		}
		return
	}
	// A control message: the raw fixture JSON is already valid stream-0 DATA
	// payload content, verbatim.
	rt.fake.feedInbound(EncodeData(0, raw))
}

// doSend performs whatever local action (if any) produces this step's
// expected outbound message, then waits for it to actually be written and
// asserts it matches.
func (rt *seqRuntime) doSend(raw json.RawMessage) {
	t := rt.t
	var probe genericStepProbe
	_ = json.Unmarshal(raw, &probe)
	ctx := context.Background()

	if probe.Type != "" {
		var fs frameStepContent
		_ = json.Unmarshal(raw, &fs)
		switch fs.Type {
		case "OPEN":
			st, err := rt.c.OpenStream(ctx)
			if err != nil {
				t.Fatalf("OpenStream: %v", err)
			}
			if st.ID() != fs.StreamID {
				t.Fatalf("OpenStream assigned id %d, fixture expects %d", st.ID(), fs.StreamID)
			}
			rt.streams[st.ID()] = st
			rt.cur = st
		case "DATA":
			st := rt.streamFor(fs.StreamID)
			var payload []byte
			if fs.PayloadHex != "" {
				payload, _ = hex.DecodeString(fs.PayloadHex)
			} else if fs.PayloadLength != nil {
				payload = make([]byte, *fs.PayloadLength)
			}
			if _, err := st.WriteContext(ctx, payload); err != nil {
				t.Fatalf("stream %d Write: %v", fs.StreamID, err)
			}
		case "CLOSE":
			st := rt.streamFor(fs.StreamID)
			if err := st.CloseWrite(); err != nil {
				t.Fatalf("stream %d CloseWrite: %v", fs.StreamID, err)
			}
		case "RESET":
			// Only actively reset a stream we (or the peer, via OnStream)
			// actually have live: a RESET for an id the connection refused
			// outright (e.g. over max_streams) is emitted autonomously and
			// has no backing *Stream to call Reset() on.
			if st, exists := rt.streams[fs.StreamID]; exists {
				code := ErrorCode(0)
				if fs.Code != nil {
					code = ErrorCode(*fs.Code)
				}
				msg := ""
				if fs.Message != nil {
					msg = *fs.Message
				}
				_ = st.Reset(code, msg)
			}
		}
		rt.cur = rt.streams[fs.StreamID]
	} else if probe.T == "app" {
		var am struct {
			Body json.RawMessage `json:"body"`
		}
		_ = json.Unmarshal(raw, &am)
		var body any
		_ = json.Unmarshal(am.Body, &body)
		if err := rt.c.SendApp(ctx, body); err != nil {
			t.Fatalf("SendApp: %v", err)
		}
	} else if probe.T == "hello" {
		// The real client sends hello synchronously at Dial() time, before
		// this harness's Conn even starts running; simulate that here by
		// forwarding the fixture's exact hello bytes verbatim.
		rt.c.sendControlFrame(EncodeData(0, raw))
	}
	// Everything else (welcome, ping, pong, error, drain-as-reaction) is
	// emitted autonomously by the dispatch/handshake/keepalive/fail path
	// already exercised above -- including, for a fixture that starts
	// mid-connection, the real handshake prelude run before this loop. Wait
	// for it to actually land on the wire, and fail if it doesn't: no
	// fabricated fallback.
	var got []byte
	if probe.T == "ping" {
		got = rt.popOutbound(2 * time.Second)
	} else {
		// A step asserting something other than a ping must tolerate a real
		// autonomous ping (pingLoop) landing on the wire first.
		got = rt.popOutboundSkippingPings(2 * time.Second)
	}
	rt.assertMatches(got, raw, probe)
}

// streamFor returns the *Stream for id, waiting briefly for an asynchronous
// OnStream callback (client role, receiving a server-opened stream) to have
// registered it.
func (rt *seqRuntime) streamFor(id uint32) *Stream {
	if st, ok := rt.streams[id]; ok {
		return st
	}
	select {
	case st := <-rt.openCh:
		rt.streams[st.ID()] = st
		if st.ID() == id {
			return st
		}
		return rt.streamFor(id)
	case <-time.After(2 * time.Second):
		rt.t.Fatalf("no stream %d registered in time", id)
		return nil
	}
}

// tryStreamFor is streamFor without the fatal timeout: it returns nil if the
// connection never actually created a live stream for id (e.g. the OPEN was
// refused before hello/welcome completed, or over max_streams).
func (rt *seqRuntime) tryStreamFor(id uint32) *Stream {
	if st, ok := rt.streams[id]; ok {
		return st
	}
	select {
	case st := <-rt.openCh:
		rt.streams[st.ID()] = st
		if st.ID() == id {
			return st
		}
		return rt.tryStreamFor(id)
	case <-time.After(150 * time.Millisecond):
		return nil
	}
}

func (rt *seqRuntime) popOutbound(timeout time.Duration) []byte {
	select {
	case b := <-rt.fake.outbound:
		return b
	case <-time.After(timeout):
		rt.t.Fatalf("timed out waiting for an outbound message")
		return nil
	}
}

// popOutboundSkippingPings is popOutbound, but silently discards any
// autonomous ping frame found first: pingLoop runs on its own ticker (sped up
// for pong_for_unsent_id.json, see scriptsAutonomousPing) and its output can
// legitimately interleave with whatever else the connection sends, so a step
// asserting a non-ping frame must not choke on one arriving in between.
func (rt *seqRuntime) popOutboundSkippingPings(timeout time.Duration) []byte {
	deadline := time.Now().Add(timeout)
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			rt.t.Fatalf("timed out waiting for an outbound message")
			return nil
		}
		select {
		case b := <-rt.fake.outbound:
			if frame, err := DecodeFrame(b); err == nil && frame.StreamID == 0 {
				if msg, perr := ParseControl(frame.Payload); perr == nil {
					if _, isPing := msg.(*PingMsg); isPing {
						continue
					}
				}
			}
			return b
		case <-time.After(remaining):
			rt.t.Fatalf("timed out waiting for an outbound message")
			return nil
		}
	}
}

func (rt *seqRuntime) assertMatches(got []byte, wantRaw json.RawMessage, probe genericStepProbe) {
	t := rt.t
	frame, err := DecodeFrame(got)
	if err != nil {
		t.Fatalf("captured outbound bytes failed to decode: %v", err)
	}
	rt.lastFrame = frame
	if probe.Type != "" {
		var fs frameStepContent
		_ = json.Unmarshal(wantRaw, &fs)
		if frame.Type.String() != fs.Type {
			t.Errorf("sent frame type = %s, want %s", frame.Type, fs.Type)
		}
		if frame.StreamID != fs.StreamID {
			t.Errorf("sent frame stream_id = %d, want %d", frame.StreamID, fs.StreamID)
		}
		if fs.PayloadHex != "" {
			if got := hex.EncodeToString(frame.Payload); got != fs.PayloadHex {
				t.Errorf("sent payload_hex = %s, want %s", got, fs.PayloadHex)
			}
		}
		if fs.PayloadLength != nil && len(frame.Payload) != *fs.PayloadLength {
			t.Errorf("sent payload length = %d, want %d", len(frame.Payload), *fs.PayloadLength)
		}
		if fs.Code != nil && frame.Type == FrameReset {
			if frame.ResetCode() != ErrorCode(*fs.Code) {
				t.Errorf("sent RESET code = %d, want %d", frame.ResetCode(), *fs.Code)
			}
		}
		return
	}

	if frame.StreamID != 0 {
		t.Fatalf("expected a control message, got a frame on stream %d", frame.StreamID)
	}
	msg, perr := ParseControl(frame.Payload)
	if perr != nil {
		t.Fatalf("captured control message failed to parse: %v", perr)
	}
	gotT := controlType(msg)
	if gotT != probe.T {
		t.Errorf("sent control t = %s, want %s", gotT, probe.T)
	}
	switch probe.T {
	case "ping":
		// Sequence ping ids are the sender's own counter: nextPingID starts
		// at 0, so a fixture's ping ids must be written 0-based to match what
		// the Conn under test actually sends (spec/README.md).
		var want struct {
			ID int64 `json:"id"`
		}
		_ = json.Unmarshal(wantRaw, &want)
		pm, ok := msg.(*PingMsg)
		if ok && pm.ID != want.ID {
			t.Errorf("ping.id = %d, want %d", pm.ID, want.ID)
		}
	case "error":
		var want struct {
			Code int `json:"code"`
		}
		_ = json.Unmarshal(wantRaw, &want)
		em := msg.(*ErrorMsg)
		if int(em.Code) != want.Code {
			t.Errorf("error.code = %d, want %d", em.Code, want.Code)
		}
	case "pong":
		// OVERVIEW.md section 2.7: "A pong MUST carry the same id and ts and
		// jump ahead of queued DATA."
		var want struct {
			ID int64 `json:"id"`
			TS int64 `json:"ts"`
		}
		_ = json.Unmarshal(wantRaw, &want)
		pm := msg.(*PongMsg)
		if pm.ID != want.ID {
			t.Errorf("pong.id = %d, want %d (must echo the ping verbatim)", pm.ID, want.ID)
		}
		if want.TS != 0 && pm.TS != want.TS {
			t.Errorf("pong.ts = %d, want %d (must echo the ping verbatim)", pm.TS, want.TS)
		}
	case "drain":
		var want struct {
			LastStreamID uint32 `json:"last_stream_id"`
		}
		_ = json.Unmarshal(wantRaw, &want)
		dm := msg.(*DrainMsg)
		if dm.LastStreamID != want.LastStreamID {
			t.Errorf("drain.last_stream_id = %d, want %d", dm.LastStreamID, want.LastStreamID)
		}
	case "app":
		var want struct {
			Body json.RawMessage `json:"body"`
		}
		_ = json.Unmarshal(wantRaw, &want)
		am := msg.(*AppMsg)
		var gotBody, wantBody any
		_ = json.Unmarshal(am.Body, &gotBody)
		_ = json.Unmarshal(want.Body, &wantBody)
		gb, _ := json.Marshal(gotBody)
		wb, _ := json.Marshal(wantBody)
		if string(gb) != string(wb) {
			t.Errorf("app.body = %s, want %s", gb, wb)
		}
	}
}

func controlType(msg any) string {
	switch msg.(type) {
	case *HelloMsg:
		return "hello"
	case *WelcomeMsg:
		return "welcome"
	case *PingMsg:
		return "ping"
	case *PongMsg:
		return "pong"
	case *DrainMsg:
		return "drain"
	case *ErrorMsg:
		return "error"
	case *AppMsg:
		return "app"
	}
	return ""
}

// --- expect ------------------------------------------------------------------

func (rt *seqRuntime) checkExpect(want map[string]any) {
	t := rt.t
	c := rt.c

	if v, ok := want["error_code"]; ok {
		if v == nil {
			time.Sleep(20 * time.Millisecond)
			if err := c.Err(); err != nil {
				t.Errorf("expected no connection error, got %v", err)
			}
		} else {
			select {
			case <-c.closed:
			case <-time.After(2 * time.Second):
				t.Fatalf("timed out waiting for the connection to fail")
			}
			ce, ok := c.Err().(*ConnError)
			if !ok {
				t.Fatalf("expected *ConnError, got %#v", c.Err())
			}
			if ce.Code.String() != v.(string) {
				t.Errorf("error_code = %s, want %s", ce.Code, v)
			}
		}
	}
	if v, ok := want["close_code"]; ok && v != nil {
		select {
		case <-c.closed:
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for the connection to close")
		}
		ce, ok := c.Err().(*ConnError)
		if !ok {
			t.Fatalf("no *ConnError to read close_code from (got %#v)", c.Err())
		}
		if want := int(v.(float64)); ce.CloseCode() != want {
			t.Errorf("close_code = %d, want %d", ce.CloseCode(), want)
		}
	}
	// Stream-level checks poll briefly: a preceding "recv" step only enqueues
	// bytes on the fake transport, and the connection's read loop applies
	// them asynchronously.
	if v, ok := want["stream_state"]; ok {
		want := v.(string)
		switch {
		case rt.cur != nil:
			got := pollUntil(300*time.Millisecond, func() (string, bool) {
				s := rt.cur.State()
				return s, s == want
			})
			if got != want {
				t.Errorf("stream_state = %s, want %s", got, want)
			}
		case want == "closed" && rt.lastFrame != nil && rt.lastFrame.Type == FrameReset:
			// No live *Stream because the connection refused it outright (a
			// captured RESET with nothing to track, e.g.
			// max_streams_exceeded.json's RESET(STREAM_LIMIT)): an untracked,
			// refused stream is definitionally closed.
		default:
			t.Fatalf("no current stream (and no captured RESET) to check stream_state against -- the stream was never tracked")
		}
	}
	if v, ok := want["send_window"]; ok {
		if rt.cur == nil {
			t.Fatalf("no current stream to check send_window against")
		}
		want := int64(v.(float64))
		got := pollUntil(300*time.Millisecond, func() (int64, bool) {
			w := rt.cur.SendWindow()
			return w, w == want
		})
		if got != want {
			t.Errorf("send_window = %d, want %d", got, want)
		}
	}
	if v, ok := want["stream_reset_code"]; ok {
		want := v.(string)
		switch {
		case rt.cur != nil:
			got := pollUntil(300*time.Millisecond, func() (string, bool) {
				se, ok := rt.cur.LastError().(*StreamError)
				if !ok {
					return "", false
				}
				return se.Code.String(), se.Code.String() == want
			})
			if got != want {
				t.Errorf("stream_reset_code = %s, want %s", got, want)
			}
		case rt.lastFrame != nil && rt.lastFrame.Type == FrameReset:
			if got := rt.lastFrame.ResetCode().String(); got != want {
				t.Errorf("stream_reset_code = %s, want %s", got, want)
			}
		default:
			t.Fatalf("no current stream (or captured RESET frame) to check stream_reset_code against")
		}
	}
}

// pollUntil retries check every 5ms until it reports ok or timeout elapses,
// returning the last observed value either way.
func pollUntil[T any](timeout time.Duration, check func() (T, bool)) T {
	deadline := time.Now().Add(timeout)
	for {
		v, ok := check()
		if ok || time.Now().After(deadline) {
			return v
		}
		time.Sleep(5 * time.Millisecond)
	}
}
