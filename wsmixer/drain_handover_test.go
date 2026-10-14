package wsmixer

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// An OPEN crossing this side's own drain{client_requested} is refused for
// that stream only (RESET REFUSED_STREAM), never a connection error.
func TestClientOwnDrainRefusesLateOpen(t *testing.T) {
	drained := make(chan struct{}, 1)
	connCh := make(chan *Conn, 1)
	_, url := startTestServer(t, testAcceptHandler{OnConn: func(c *Conn) {
		c.OnDrain(func(*DrainMsg) { drained <- struct{}{} })
		connCh <- c
	}})
	held := make(chan *Stream, 1)
	cc := dialTestClient(t, url, ClientOptions{OnStream: func(st *Stream) { held <- st }})
	sc := <-connCh

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	sA, err := sc.OpenStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = sA.Write([]byte("a"))
	<-held // the client holds stream A, so its drain waits out the grace
	go func() { _ = cc.Drain(ctx, "client_requested", DrainOptions{Deadline: 3 * time.Second}) }()
	<-drained

	sB, err := sc.OpenStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = sB.Write([]byte("b"))
	_, err = sB.ReadContext(ctx, make([]byte, 1))
	var se *StreamError
	if !errors.As(err, &se) || se.Code != RefusedStreamCode {
		t.Fatalf("late stream ended with %v, want RESET REFUSED_STREAM", err)
	}
	select {
	case <-cc.Done():
		t.Fatalf("the client connection ended (%v); a late OPEN must not be a connection error", cc.Err())
	default:
	}
}

// drainHandover connects a Client, opens one stream the client holds, and
// sends a raw drain (the server itself never closes) with deadlineMS.
func drainHandover(t *testing.T, deadlineMS int64) (c1 *Conn, srvStream, cliStream *Stream, rec *disconnectRecorder, welcomed <-chan time.Time) {
	t.Helper()
	connCh := make(chan *Conn, 4)
	_, url := startTestServer(t, testAcceptHandler{OnConn: func(c *Conn) { connCh <- c }})
	rec = newDisconnectRecorder()
	held := make(chan *Stream, 4)
	var connects atomic.Int32
	w := make(chan time.Time, 1)
	cl := NewClient(url, StaticToken("tok"), ClientConfig{
		Reconnect:    testReconnectOptions(&fakeAfter{}, 0),
		OnDisconnect: rec.onDisconnect,
		OnStream:     func(st *Stream) { held <- st },
		OnConnect: func(*Conn, *WelcomeMsg) {
			if connects.Add(1) == 2 {
				w <- time.Now()
			}
		},
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := cl.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cl.Close(context.Background()) })
	c1 = <-connCh
	srvStream, err := c1.OpenStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = srvStream.Write([]byte("x"))
	cliStream = <-held
	ms := deadlineMS
	b, _ := json.Marshal(&DrainMsg{T: tDrain, Reason: "rollout", LastStreamID: srvStream.ID(), DeadlineMS: &ms})
	c1.sendControlFrame(EncodeData(0, b))
	<-connCh // the replacement
	return c1, srvStream, cliStream, rec, w
}

// waitSuperseded waits for the superseded report and checks it is the only one.
func waitSuperseded(t *testing.T, rec *disconnectRecorder) DisconnectReason {
	t.Helper()
	superseded := func() []DisconnectReason {
		var out []DisconnectReason
		for _, r := range rec.snapshotAll() {
			if r.Superseded {
				out = append(out, r)
			}
		}
		return out
	}
	deadline := time.Now().Add(5 * time.Second)
	for len(superseded()) == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(200 * time.Millisecond)
	got := superseded()
	if len(got) != 1 {
		t.Fatalf("%d superseded reports, want exactly 1: %+v", len(got), rec.snapshotAll())
	}
	return got[0]
}

// The drained connection keeps serving after its replacement's welcome and
// is retired at deadline_ms + 1000 ms: survivors RESET CANCEL, then
// error{NO_ERROR} + close 1000, reported once as Superseded.
func TestClientRetiresDrainedConnAtDeadline(t *testing.T) {
	c1, srvStream, _, rec, welcomed := drainHandover(t, 200)
	at := <-welcomed
	select {
	case <-c1.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("the drained connection was never retired")
	}
	if el := time.Since(at); el < 1100*time.Millisecond || el > 3*time.Second {
		t.Fatalf("retired %v after the replacement's welcome, want deadline_ms 200 + 1000 ms", el)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := srvStream.ReadContext(ctx, make([]byte, 8))
	var se *StreamError
	if !errors.As(err, &se) || se.Code != CancelCode {
		t.Fatalf("surviving stream ended with %v, want RESET CANCEL", err)
	}
	var ce *ConnError
	if !errors.As(c1.Err(), &ce) || ce.Code != NoError {
		t.Fatalf("server saw %v, want error{NO_ERROR}", c1.Err())
	}
	if r := waitSuperseded(t, rec); r.Fatal || r.Message != supersededMessage {
		t.Fatalf("superseded report %+v", r)
	}
}

// ... or earlier, as soon as no stream is left on it.
func TestClientRetiresDrainedConnWhenEmpty(t *testing.T) {
	c1, _, cliStream, rec, welcomed := drainHandover(t, 10000)
	<-welcomed
	select {
	case <-c1.Done():
		t.Fatal("retired while a stream was still open")
	case <-time.After(200 * time.Millisecond):
	}
	_ = cliStream.Close()
	select {
	case <-c1.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("not retired once its last stream ended")
	}
	waitSuperseded(t, rec)
}

// After a drain hand-over each connection's counters are in Client.Stats()
// exactly once.
func TestClientStatsAfterHandoverCountEachConnOnce(t *testing.T) {
	connCh := make(chan *Conn, 4)
	_, url := startTestServer(t, testAcceptHandler{OnConn: func(c *Conn) { connCh <- c }})
	clientConns := make(chan *Conn, 4)
	rec := newDisconnectRecorder()
	cl := NewClient(url, StaticToken("tok"), ClientConfig{
		Reconnect:    testReconnectOptions(&fakeAfter{}, 0),
		OnConnect:    func(c *Conn, _ *WelcomeMsg) { clientConns <- c },
		OnDisconnect: rec.onDisconnect,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := cl.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	defer cl.Close(context.Background())
	old := <-clientConns
	srv := <-connCh
	ms := int64(100)
	b, _ := json.Marshal(&DrainMsg{T: tDrain, Reason: "rollout", LastStreamID: 0, DeadlineMS: &ms})
	srv.sendControlFrame(EncodeData(0, b))
	repl := <-clientConns
	select {
	case <-old.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("the drained connection was never retired")
	}
	if old.Stats().BytesIn == 0 {
		t.Fatal("the drained connection counted nothing; the check below would prove nothing")
	}
	// The retirement is complete once it has been reported; the old conn's
	// own watchConn folds its counters in around the same time, so give it
	// a moment, then the total must have settled at old + replacement (a
	// second fold would land past it).
	waitSuperseded(t, rec)
	time.Sleep(200 * time.Millisecond)
	want := old.Stats()
	want.add(repl.Stats())
	if got := cl.Stats(); got != want {
		t.Fatalf("Client.Stats() = %+v, want old + replacement = %+v", got, want)
	}
}

// CloseImmediately: error{NO_ERROR} + close 1000 at once, no
// drain{client_requested}, no grace for an open stream.
func TestClientCloseImmediately(t *testing.T) {
	connCh := make(chan *Conn, 1)
	var drains atomic.Int32
	_, url := startTestServer(t, testAcceptHandler{OnConn: func(c *Conn) {
		c.OnDrain(func(*DrainMsg) { drains.Add(1) })
		connCh <- c
	}})
	held := make(chan *Stream, 1)
	cl := NewClient(url, StaticToken("tok"), ClientConfig{OnStream: func(st *Stream) { held <- st }})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := cl.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	sc := <-connCh
	st, _ := sc.OpenStream(ctx)
	_, _ = st.Write([]byte("x"))
	<-held
	start := time.Now()
	if err := cl.CloseImmediately(ctx); err != nil {
		t.Fatal(err)
	}
	<-sc.Done()
	if el := time.Since(start); el > time.Second {
		t.Fatalf("CloseImmediately took %v", el)
	}
	var ce *ConnError
	if !errors.As(sc.Err(), &ce) || ce.Code != NoError || drains.Load() != 0 {
		t.Fatalf("server saw %v and %d drains, want error{NO_ERROR} and none", sc.Err(), drains.Load())
	}
	if cl.State() != "closed" {
		t.Fatalf("state %s", cl.State())
	}
}

// Integer fields accept any integral JSON number, never a fraction or a bool.
func TestControlIntegerFieldsAcceptIntegralNumbers(t *testing.T) {
	for raw, want := range map[string]int64{`1`: 1, `1.0`: 1, `1e3`: 1000, `25.000`: 25} {
		msg, err := ParseControl([]byte(`{"t":"ping","id":` + raw + `}`))
		if err != nil {
			t.Fatalf("id %s: %v", raw, err)
		}
		if got := msg.(*PingMsg).ID; got != want {
			t.Fatalf("id %s = %d, want %d", raw, got, want)
		}
	}
	for _, raw := range []string{`1.5`, `true`, `"1"`, `1e-1`} {
		if _, err := ParseControl([]byte(`{"t":"ping","id":` + raw + `}`)); err == nil {
			t.Fatalf("id %s accepted", raw)
		}
	}
}

// String limits count code points (JSON Schema maxLength), not UTF-8 bytes.
func TestControlStringLimitsCountCodePoints(t *testing.T) {
	msg := func(n int) []byte {
		return []byte(`{"t":"error","code":1,"message":"` + strings.Repeat("é", n) + `"}`)
	}
	if _, err := ParseControl(msg(1024)); err != nil {
		t.Fatalf("1024 code points (2048 bytes) rejected: %v", err)
	}
	if _, err := ParseControl(msg(1025)); err == nil {
		t.Fatal("1025 code points accepted")
	}
	drain := func(n int) []byte {
		return []byte(`{"t":"drain","reason":"rollout","last_stream_id":0,"message":"` + strings.Repeat("ü", n) + `"}`)
	}
	if _, err := ParseControl(drain(256)); err != nil {
		t.Fatalf("256 code points rejected: %v", err)
	}
	if _, err := ParseControl(drain(257)); err == nil {
		t.Fatal("257 code points accepted")
	}
}
