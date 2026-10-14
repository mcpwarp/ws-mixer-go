package mcpwarp

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/mcpwarp/ws-mixer-go/wsmixer"
)

// fakeEdge is a minimal mcpwarp edge for tests: an httptest server running
// the real ws-mixer server handshake (wsmixer.AcceptConn, the same glue as
// cmd/testserver and wsmixer/accepthandler_test.go). Each accepted
// connection is handed to the test on conns; the test plays the edge's
// envelope side by hand.
type fakeEdge struct {
	url   string
	conns chan *edgeConn
	// rejectNext makes the next hellos fail with error{UNAUTHORIZED} (4011).
	rejectNext atomic.Int32
	hellos     atomic.Int32

	mu  sync.Mutex
	all []*edgeConn
}

type edgeConn struct {
	c      *wsmixer.Conn
	hello  *wsmixer.Hello
	header http.Header
	apps   chan json.RawMessage
	drains atomic.Int32 // drain frames the agent sent
}

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func bearerToken(header string) string {
	const prefix = "bearer "
	if len(header) <= len(prefix) || !strings.EqualFold(header[:len(prefix)], prefix) {
		return ""
	}
	return strings.TrimSpace(header[len(prefix):])
}

func newFakeEdge(t *testing.T) *fakeEdge {
	t.Helper()
	e := &fakeEdge{conns: make(chan *edgeConn, 16)}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := websocket.Accept(w, r, &websocket.AcceptOptions{
			Subprotocols:    []string{wsmixer.Subprotocol},
			CompressionMode: websocket.CompressionDisabled,
		})
		if err != nil {
			return
		}
		defer ws.CloseNow()
		ws.SetReadLimit(wsmixer.MaxMessageSize)
		var hello *wsmixer.Hello
		c, err := wsmixer.AcceptConn(r.Context(), ws, bearerToken(r.Header.Get("Authorization")), wsmixer.AcceptOptions{
			Options: wsmixer.Options{Logger: discardLogger()},
			Authenticate: func(_ context.Context, h *wsmixer.Hello) (wsmixer.WelcomeMeta, error) {
				e.hellos.Add(1)
				if e.rejectNext.Add(-1) >= 0 {
					return wsmixer.WelcomeMeta{}, wsmixer.Unauthorized("token rejected")
				}
				e.rejectNext.Store(0)
				hello = h
				return wsmixer.WelcomeMeta{}, nil
			},
			Request: r,
		})
		if err != nil {
			return
		}
		ec := &edgeConn{c: c, hello: hello, header: r.Header.Clone(), apps: make(chan json.RawMessage, 64)}
		c.OnDrain(func(*wsmixer.DrainMsg) { ec.drains.Add(1) })
		c.OnApp(func(body json.RawMessage) {
			select {
			case ec.apps <- append(json.RawMessage(nil), body...):
			default:
			}
		})
		e.mu.Lock()
		e.all = append(e.all, ec)
		e.mu.Unlock()
		e.conns <- ec
		c.Run()
		<-c.Done()
	}))
	t.Cleanup(func() {
		e.mu.Lock()
		all := e.all
		e.mu.Unlock()
		for _, ec := range all {
			_ = ec.c.Close(uint32(wsmixer.NoError), "test done")
		}
		srv.Close()
	})
	e.url = "ws" + strings.TrimPrefix(srv.URL, "http") + "/v1/tunnel"
	return e
}

func (e *fakeEdge) nextConn(t *testing.T) *edgeConn {
	t.Helper()
	select {
	case ec := <-e.conns:
		return ec
	case <-time.After(5 * time.Second):
		t.Fatal("the agent never connected")
		return nil
	}
}

// noConn asserts no further connection arrives within d.
func (e *fakeEdge) noConn(t *testing.T, d time.Duration) {
	t.Helper()
	select {
	case <-e.conns:
		t.Fatal("the agent dialed again")
	case <-time.After(d):
	}
}

func (ec *edgeConn) send(t *testing.T, body string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := ec.c.SendApp(ctx, json.RawMessage(body)); err != nil {
		t.Fatalf("edge SendApp: %v", err)
	}
}

func (ec *edgeConn) nextApp(t *testing.T) json.RawMessage {
	t.Helper()
	select {
	case b := <-ec.apps:
		return b
	case <-time.After(5 * time.Second):
		t.Fatal("no app message from the agent")
		return nil
	}
}

// noApp asserts the agent sends no app message within d.
func (ec *edgeConn) noApp(t *testing.T, d time.Duration) {
	t.Helper()
	select {
	case b := <-ec.apps:
		t.Fatalf("unexpected app message %s", b)
	case <-time.After(d):
	}
}

func (ec *edgeConn) expectRegister(t *testing.T, name string) {
	t.Helper()
	if got, want := string(ec.nextApp(t)), string(EncodeRegister(name, KindHTTP)); got != want {
		t.Fatalf("app = %s, want register %s", got, want)
	}
}

func (ec *edgeConn) waitDone(t *testing.T) {
	t.Helper()
	select {
	case <-ec.c.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("the connection did not end")
	}
}

// roundTrip opens one stream, writes in (head first and the rest only once
// headFirst bytes of response have arrived, when headFirst > 0), half-closes,
// and returns everything the agent wrote and how the stream ended.
func (ec *edgeConn) roundTrip(t *testing.T, in []byte, headLen, headFirst int) ([]byte, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	st, err := ec.c.OpenStream(ctx)
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	var out []byte
	buf := make([]byte, 32*1024)
	read := func(until int) error {
		for until < 0 || len(out) < until {
			n, err := st.ReadContext(ctx, buf)
			out = append(out, buf[:n]...)
			if err != nil {
				return err
			}
		}
		return nil
	}
	if headFirst > 0 {
		if _, err := st.Write(in[:headLen]); err != nil {
			t.Fatalf("edge write: %v", err)
		}
		if err := read(headFirst); err != nil {
			t.Fatalf("the agent did not answer before the body (got %q): %v", out, err)
		}
		in = in[headLen:]
	}
	if _, err := st.Write(in); err != nil {
		t.Fatalf("edge write: %v", err)
	}
	if err := st.CloseWrite(); err != nil {
		t.Fatalf("edge CloseWrite: %v", err)
	}
	err = read(-1)
	if errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("the stream did not end (got %q)", out)
	}
	return out, err
}

// eventLog records an agent's events.
type eventLog struct {
	mu     sync.Mutex
	events []event
}

func (l *eventLog) add(ev event) {
	l.mu.Lock()
	l.events = append(l.events, ev)
	l.mu.Unlock()
}

func (l *eventLog) of(kind eventKind) []event {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []event
	for _, ev := range l.events {
		if ev.kind == kind {
			out = append(out, ev)
		}
	}
	return out
}

const testIntegration = "0F8FAD5B-D9CB-469F-A165-70867728950E"

// newTestAgent builds an agent for service "notes" against edgeURL with an
// echo handler, then applies mod. It is closed at cleanup without waiting
// out the unregister budget.
func newTestAgent(t *testing.T, edgeURL string, mod func(*agentConfig)) (*agent, *eventLog) {
	t.Helper()
	log := &eventLog{}
	cfg := agentConfig{
		url:         edgeURL,
		token:       wsmixer.StaticToken("t"),
		name:        "notes",
		integration: testIntegration,
		options:     wsmixer.Options{Logger: discardLogger()},
		reconnect:   wsmixer.ReconnectOptions{Base: 10 * time.Millisecond, Cap: 50 * time.Millisecond},
		handler:     echoHandler(nil),
		emit:        log.add,
	}
	if mod != nil {
		mod(&cfg)
	}
	a := newAgent(cfg)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		a.close(ctx)
	})
	return a, log
}

func echoHandler(record func(*http.Request, []byte)) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if record != nil {
			record(r, body)
		}
		if ct := r.Header.Get("Content-Type"); ct != "" {
			w.Header().Set("Content-Type", ct)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
	})
}

func waitState(t *testing.T, a *agent, want exposeState) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for a.session.currentState() != want {
		if time.Now().After(deadline) {
			t.Fatalf("state %s, want %s", a.session.currentState(), want)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

const registeredNotes = `{"mcpwarp":{"v":1,"op":"registered","services":[{"name":"notes","id":"srv_notes","url":"https://notes-tester.mcpwarp.test/mcp","created":true}],"errors":[]}}`

// startRegistered starts a, answers its first register, and waits for
// state registered.
func startRegistered(t *testing.T, e *fakeEdge, a *agent) *edgeConn {
	t.Helper()
	a.start()
	ec := e.nextConn(t)
	ec.expectRegister(t, "notes")
	ec.send(t, registeredNotes)
	waitState(t, a, stateRegistered)
	return ec
}
