package wsmixer

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// These tests exercise the real AcceptConn (via testAcceptHandler) + Dial
// path end to end over an actual loopback WebSocket (httptest.Server),
// unlike sequence_test.go which drives *Conn directly against a fake
// transport. The production HTTP-layer glue (wsmixerserver.Listener) is
// tested in that package instead (docs/MIGRATION.md section 0.5).

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func startTestServer(t *testing.T, opts testAcceptHandler) (*httptest.Server, string) {
	t.Helper()
	if opts.Logger == nil {
		opts.Logger = discardLogger()
	}
	ln := newTestListener(opts)
	srv := httptest.NewServer(ln)
	t.Cleanup(srv.Close)
	url := "ws" + srv.URL[len("http"):] + "/tunnel"
	return srv, url
}

func dialTestClient(t *testing.T, url string, opts ClientOptions) *Conn {
	t.Helper()
	if opts.Logger == nil {
		opts.Logger = discardLogger()
	}
	if opts.Token == "" {
		opts.Token = "test-token"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := Dial(ctx, url, opts)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = c.Close(uint32(NoError), "test done") })
	return c
}

func TestIntegrationHandshakeAndRequestResponse(t *testing.T) {
	var gotHello *Hello
	var mu sync.Mutex
	var serverConn *Conn
	connCh := make(chan *Conn, 1)
	ln := newTestListener(testAcceptHandler{
		Authenticate: func(ctx context.Context, h *Hello) (WelcomeMeta, error) {
			mu.Lock()
			gotHello = h
			mu.Unlock()
			return WelcomeMeta{Meta: map[string]any{"ok": true}}, nil
		},
		OnConn: func(c *Conn) { connCh <- c },
	})
	srv := httptest.NewServer(ln)
	defer srv.Close()
	url := "ws" + srv.URL[len("http"):] + "/tunnel"

	client := dialTestClient(t, url, ClientOptions{Token: "abc", Meta: map[string]any{"hi": 1}})

	select {
	case serverConn = <-connCh:
	case <-time.After(2 * time.Second):
		t.Fatal("server never observed the connection")
	}
	if gotHello == nil || gotHello.Token != "abc" {
		t.Errorf("Authenticate did not see the expected hello: %+v", gotHello)
	}

	if client.Session() == "" {
		t.Error("client session id is empty")
	}
	if serverConn.Session() != client.Session() {
		t.Errorf("session mismatch: server=%s client=%s", serverConn.Session(), client.Session())
	}

	var received []byte
	streamDone := make(chan struct{})
	client.OnStream(func(st *Stream) {
		defer close(streamDone)
		req, err := io.ReadAll(st)
		if err != nil {
			t.Errorf("client stream read: %v", err)
			return
		}
		received = req
		if _, err := st.Write([]byte("world")); err != nil {
			t.Errorf("client stream write: %v", err)
		}
		_ = st.CloseWrite()
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	st, err := serverConn.OpenStream(ctx)
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	if _, err := st.Write([]byte("hello")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := st.CloseWrite(); err != nil {
		t.Fatalf("CloseWrite: %v", err)
	}
	resp, err := io.ReadAll(st)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(resp, []byte("world")) {
		t.Errorf("response = %q, want %q", resp, "world")
	}

	select {
	case <-streamDone:
	case <-time.After(2 * time.Second):
		t.Fatal("client never finished handling the stream")
	}
	if !bytes.Equal(received, []byte("hello")) {
		t.Errorf("request = %q, want %q", received, "hello")
	}
}

func TestIntegrationAppRoundTrip(t *testing.T) {
	connCh := make(chan *Conn, 1)
	_, url := startTestServer(t, testAcceptHandler{OnConn: func(c *Conn) { connCh <- c }})

	appCh := make(chan json.RawMessage, 1)
	client := dialTestClient(t, url, ClientOptions{
		OnApp: func(body json.RawMessage) { appCh <- body },
	})

	var serverConn *Conn
	select {
	case serverConn = <-connCh:
	case <-time.After(2 * time.Second):
		t.Fatal("no server connection observed")
	}

	if err := serverConn.SendApp(context.Background(), map[string]any{"op": "register"}); err != nil {
		t.Fatalf("SendApp: %v", err)
	}
	select {
	case body := <-appCh:
		var m map[string]any
		if err := json.Unmarshal(body, &m); err != nil {
			t.Fatalf("unmarshal app body: %v", err)
		}
		if m["op"] != "register" {
			t.Errorf("app.body.op = %v, want register", m["op"])
		}
	case <-time.After(2 * time.Second):
		t.Fatal("client never received the app message")
	}
	_ = client
}

// spyWS wraps a WSConn and records every inbound message's decoded
// StreamID, in the exact order the transport delivered them, so a test can
// assert on fairness at the wire instead of on client-side read timing.
type spyWS struct {
	WSConn
	mu  sync.Mutex
	ids []uint32
}

func (s *spyWS) Read(ctx context.Context) (websocket.MessageType, []byte, error) {
	typ, data, err := s.WSConn.Read(ctx)
	if err == nil {
		if f, ferr := DecodeFrame(data); ferr == nil {
			s.mu.Lock()
			s.ids = append(s.ids, f.StreamID)
			s.mu.Unlock()
		}
	}
	return typ, data, err
}

func (s *spyWS) sequence() []uint32 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]uint32(nil), s.ids...)
}

// dialWithSpy performs the same handshake Dial does, but over a spyWS so the
// test can inspect the exact wire order of inbound frames.
func dialWithSpy(t *testing.T, url string, opts ClientOptions) (*Conn, *spyWS) {
	t.Helper()
	opts.Options.SetDefaults()
	if opts.Token == "" {
		opts.Token = "test-token"
	}
	header := opts.HTTPHeader.Clone()
	if header == nil {
		header = http.Header{}
	}
	header.Set("Authorization", "Bearer "+opts.Token)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ws, _, err := websocket.Dial(ctx, url, &websocket.DialOptions{
		Subprotocols:    []string{Subprotocol},
		HTTPHeader:      header,
		CompressionMode: websocket.CompressionDisabled,
	})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	ws.SetReadLimit(opts.ReadLimit)
	spy := &spyWS{WSConn: ws}

	c := newConn(spy, RoleClient, opts.Options)
	c.hp.Store(&handlers{onStream: opts.OnStream, onApp: opts.OnApp, onDrain: opts.OnDrain})
	if err := clientHandshake(ctx, c, opts); err != nil {
		t.Fatalf("clientHandshake: %v", err)
	}
	c.Run()
	t.Cleanup(func() { _ = c.Close(uint32(NoError), "test done") })
	return c, spy
}

// TestIntegrationConcurrentStreamsFairness opens several streams that each
// write a large payload and checks every stream makes steady progress
// instead of one hogging the connection (OVERVIEW.md section 2.6 rule 3:
// round-robin one <=16KiB DATA chunk per ready stream).
func TestIntegrationConcurrentStreamsFairness(t *testing.T) {
	connCh := make(chan *Conn, 1)
	_, url := startTestServer(t, testAcceptHandler{OnConn: func(c *Conn) { connCh <- c }})

	const nStreams = 4
	const chunkSize = 16384 // == maxChunk in stream.go
	const payloadSize = 8 * chunkSize

	var wg sync.WaitGroup
	wg.Add(nStreams) // known up front: avoids racing Add (in the OnStream goroutine) against Wait
	received := make([][]byte, nStreams)
	client, spy := dialWithSpy(t, url, ClientOptions{
		OnStream: func(st *Stream) {
			go func() {
				defer wg.Done()
				var buf bytes.Buffer
				chunk := make([]byte, 4096)
				for {
					n, err := st.Read(chunk)
					if n > 0 {
						buf.Write(chunk[:n])
					}
					if err != nil {
						if err != io.EOF {
							t.Errorf("client read: %v", err)
						}
						break
					}
				}
				if b := buf.Bytes(); len(b) > 0 {
					received[int(b[0])] = b
				}
			}()
		},
	})
	_ = client

	var serverConn *Conn
	select {
	case serverConn = <-connCh:
	case <-time.After(2 * time.Second):
		t.Fatal("no server connection observed")
	}

	streams := make([]*Stream, nStreams)
	streamIDs := make(map[uint32]bool, nStreams)
	for i := range streams {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		st, err := serverConn.OpenStream(ctx)
		cancel()
		if err != nil {
			t.Fatalf("OpenStream: %v", err)
		}
		streams[i] = st
		streamIDs[st.ID()] = true
	}

	// Write in lockstep rounds: every stream's chunk for round r is released
	// at the same instant via the `start` barrier, so the writer must choose
	// among genuinely-simultaneously-ready chunks each round rather than
	// relying on incidental goroutine-scheduling luck to interleave at all.
	const rounds = payloadSize / chunkSize
	for r := 0; r < rounds; r++ {
		var rwg sync.WaitGroup
		start := make(chan struct{})
		for i := 0; i < nStreams; i++ {
			i := i
			rwg.Add(1)
			go func() {
				defer rwg.Done()
				<-start
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				chunk := bytes.Repeat([]byte{byte(i)}, chunkSize)
				if _, err := streams[i].WriteContext(ctx, chunk); err != nil {
					t.Errorf("stream %d round %d write: %v", i, r, err)
				}
			}()
		}
		close(start)
		rwg.Wait()
	}
	for _, st := range streams {
		_ = st.CloseWrite()
	}
	wg.Wait()

	for i, buf := range received {
		if len(buf) != payloadSize {
			t.Errorf("stream %d: got %d bytes, want %d", i, len(buf), payloadSize)
			continue
		}
		if buf[0] != byte(i) || buf[len(buf)-1] != byte(i) {
			t.Errorf("stream %d: payload corrupted", i)
		}
	}

	// Interleaving, measured at the wire: spy recorded every inbound frame's
	// StreamID in the exact order coder/websocket delivered them. A writer
	// that serialized streams (all of stream 0's chunks, then all of stream
	// 1's, ...) would produce exactly nStreams-1 "switches" in that sequence,
	// no matter how many chunks each stream sends, since every stream's block
	// only borders its neighbours once. Round-robin switches on nearly every
	// DATA frame instead, so requiring materially more than nStreams-1
	// switches catches a regression to a serial writer.
	var got []uint32
	for _, id := range spy.sequence() {
		if streamIDs[id] { // exclude stream 0 (welcome/ping/pong)
			got = append(got, id)
		}
	}
	switches := 0
	for i := 1; i < len(got); i++ {
		if got[i] != got[i-1] {
			switches++
		}
	}
	if switches <= nStreams-1 {
		t.Errorf("no interleaving observed at the wire: only %d switches in %v (serial writer?)", switches, got)
	}
}

// TestIntegrationStressConcurrentOpens opens streams from many goroutines at
// once and checks the wire order matches id order: OpenStream must allocate
// the id and enqueue its OPEN frame as one atomic step (serialized by
// openMu), or a lagging goroutine can enqueue OPEN(id) after a later id
// already went out, which the peer reports as "OPEN for id X is not greater
// than highest_opened Y". Safe to run with `go test -count=N`.
func TestIntegrationStressConcurrentOpens(t *testing.T) {
	connCh := make(chan *Conn, 1)
	_, url := startTestServer(t, testAcceptHandler{OnConn: func(c *Conn) { connCh <- c }})

	const nOpens = 50

	var swg sync.WaitGroup
	swg.Add(nOpens) // known up front: avoids racing Add (in the OnStream goroutine) against Wait
	client := dialTestClient(t, url, ClientOptions{
		OnStream: func(st *Stream) {
			go func() {
				defer swg.Done()
				_, _ = io.ReadAll(st)
			}()
		},
	})

	var serverConn *Conn
	select {
	case serverConn = <-connCh:
	case <-time.After(2 * time.Second):
		t.Fatal("no server connection observed")
	}

	var owg sync.WaitGroup
	errCh := make(chan error, nOpens)
	for i := 0; i < nOpens; i++ {
		owg.Add(1)
		go func() {
			defer owg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			st, err := serverConn.OpenStream(ctx)
			if err != nil {
				errCh <- err
				return
			}
			_ = st.CloseWrite()
		}()
	}
	owg.Wait()
	close(errCh)
	for err := range errCh {
		t.Errorf("OpenStream: %v", err)
	}

	select {
	case <-serverConn.Done():
		t.Fatalf("connection failed during concurrent opens: %v", serverConn.Err())
	case <-time.After(200 * time.Millisecond):
	}

	done := make(chan struct{})
	go func() { swg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("client never finished reading all 50 streams")
	}
	_ = client
}

// TestNoGoroutineLeak checks that closing a connection tears down its
// background goroutines (writer/reader/ping/watchdog).
func TestNoGoroutineLeak(t *testing.T) {
	before := runtime.NumGoroutine()

	connCh := make(chan *Conn, 1)
	_, url := startTestServer(t, testAcceptHandler{OnConn: func(c *Conn) { connCh <- c }})

	client := dialTestClient(t, url, ClientOptions{})
	var serverConn *Conn
	select {
	case serverConn = <-connCh:
	case <-time.After(2 * time.Second):
		t.Fatal("no server connection observed")
	}

	_ = client.Close(uint32(NoError), "bye")
	select {
	case <-serverConn.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("server connection never closed after client hung up")
	}

	deadline := time.Now().Add(2 * time.Second)
	var after int
	for {
		after = runtime.NumGoroutine()
		if after <= before+2 || time.Now().After(deadline) { // small slack for test/runtime goroutines
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if after > before+2 {
		t.Errorf("goroutine count grew from %d to %d after closing the connection", before, after)
	}
}
