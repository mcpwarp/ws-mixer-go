package mcpwarp

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mcpwarp/ws-mixer-go/internal/version"
	"github.com/mcpwarp/ws-mixer-go/wsmixer"
)

// §3.1: every dial calls the token provider and carries hello.token,
// Authorization, the lowercased integration in hello.meta and
// X-Mcpwarp-Integration, hello.agent and User-Agent; §3.2: register on
// every welcome.
func TestAgentIdentityAndRegisterOnEveryDial(t *testing.T) {
	edge := newFakeEdge(t)
	var calls atomic.Int32
	a, _ := newTestAgent(t, edge.url, func(c *agentConfig) {
		c.token = func(context.Context) (string, error) {
			n := calls.Add(1)
			return "tok-" + string(rune('0'+n)), nil
		}
	})
	a.start()
	lower := strings.ToLower(testIntegration)
	uaRE := regexp.MustCompile(`^ws-mixer-go/` + regexp.QuoteMeta(version.SDK) + ` \(go/\S+ \S+/\S+\)$`)
	for i := 1; i <= 2; i++ {
		ec := edge.nextConn(t)
		wantTok := "tok-" + string(rune('0'+i))
		if ec.hello.Token != wantTok || ec.header.Get("Authorization") != "Bearer "+wantTok {
			t.Fatalf("dial %d: hello.token %q, Authorization %q, want %q", i, ec.hello.Token, ec.header.Get("Authorization"), wantTok)
		}
		if got := ec.header.Get("X-Mcpwarp-Integration"); got != lower {
			t.Fatalf("X-Mcpwarp-Integration = %q, want %q", got, lower)
		}
		var meta map[string]string
		if err := json.Unmarshal(ec.hello.Meta, &meta); err != nil {
			t.Fatalf("hello.meta %s: %v", ec.hello.Meta, err)
		}
		ag := ec.hello.Agent
		want := map[string]string{"client": "sdk-go", "version": version.SDK, "os": ag.OS, "integration": lower}
		if len(meta) != len(want) || meta["client"] != want["client"] || meta["version"] != want["version"] || meta["os"] != want["os"] || meta["integration"] != want["integration"] {
			t.Fatalf("hello.meta = %v, want %v", meta, want)
		}
		if ag.SDK != "ws-mixer-go" || ag.SDKVersion != version.SDK || !strings.HasPrefix(ag.Runtime, "go/") || !strings.Contains(ag.OS, "/") {
			t.Fatalf("hello.agent = %+v", ag)
		}
		if ua := ec.header.Get("User-Agent"); !uaRE.MatchString(ua) || !strings.Contains(ua, "("+ag.Runtime+" "+ag.OS+")") {
			t.Fatalf("User-Agent = %q", ua)
		}
		ec.expectRegister(t, "notes")
		if i == 1 {
			// Not a drain: the client reconnects with backoff and must register again.
			_ = ec.c.Close(uint32(wsmixer.InternalErrorCode), "edge restart")
		}
	}
	if n := calls.Load(); n != 2 {
		t.Fatalf("token provider called %d times, want once per dial", n)
	}
}

// hangingHandler blocks every request until it is cancelled, and reports
// each one on started.
func hangingHandler(started chan<- struct{}) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started <- struct{}{}
		<-r.Context().Done()
	})
}

// openHangingRequest opens a stream, writes a POST /mcp the hanging handler
// holds, and waits until the handler has it.
func openHangingRequest(t *testing.T, ec *edgeConn, started <-chan struct{}) *wsmixer.Stream {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	st, err := ec.c.OpenStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Write([]byte("POST /mcp HTTP/1.1\r\nHost: notes-tester.mcpwarp.test\r\nContent-Length: 2\r\n\r\n{}")); err != nil {
		t.Fatal(err)
	}
	_ = st.CloseWrite()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("the handler never got the request")
	}
	return st
}

// requireCutByTakeOver: the in-flight stream was RESET CANCEL by the agent.
func requireCutByTakeOver(t *testing.T, st *wsmixer.Stream) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := io.ReadAll(readerCtx{ctx, st})
	var se *wsmixer.StreamError
	if !errors.As(err, &se) || se.Code != wsmixer.CancelCode || se.Message != takenOverMessage {
		t.Fatalf("in-flight stream ended with %v, want RESET CANCEL %q", err, takenOverMessage)
	}
}

type readerCtx struct {
	ctx context.Context
	st  *wsmixer.Stream
}

func (r readerCtx) Read(p []byte) (int, error) { return r.st.ReadContext(r.ctx, p) }

// X-Mcpwarp-Integration is omitted, not sent empty, without an integration.
func TestAgentNoEmptyIntegrationHeader(t *testing.T) {
	edge := newFakeEdge(t)
	a, _ := newTestAgent(t, edge.url, func(c *agentConfig) { c.integration = "" })
	a.start()
	ec := edge.nextConn(t)
	if v, ok := ec.header["X-Mcpwarp-Integration"]; ok {
		t.Fatalf("X-Mcpwarp-Integration sent as %q", v)
	}
}

// §3.3 Sticky: a matching conflict cuts in-flight streams and closes the
// client at once, with no unregister, and nothing reconnects.
func TestAgentStickyConflictClosesClient(t *testing.T) {
	edge := newFakeEdge(t)
	started := make(chan struct{}, 1)
	a, log := newTestAgent(t, edge.url, func(c *agentConfig) { c.handler = hangingHandler(started) })
	ec := startRegistered(t, edge, a)
	st := openHangingRequest(t, ec, started)

	start := time.Now()
	ec.send(t, `{"mcpwarp":{"v":1,"op":"conflict","id":"srv_notes","name":"notes","reason":"taken_over"}}`)
	requireCutByTakeOver(t, st)
	ec.waitDone(t)
	if el := time.Since(start); el > time.Second {
		t.Fatalf("closing took %v; a take-over must not wait for in-flight streams", el)
	}
	if st := a.session.currentState(); st != stateConflict {
		t.Fatalf("state %s, want conflict", st)
	}
	if got := a.client().State(); got != "closed" {
		t.Fatalf("client state %s, want closed", got)
	}
	var ce *wsmixer.ConnError
	if !errors.As(ec.c.Err(), &ce) || ce.Code != wsmixer.NoError {
		t.Fatalf("edge saw the connection end with %v, want error{NO_ERROR}", ec.c.Err())
	}
	ec.noApp(t, 100*time.Millisecond) // no unregister
	if n := ec.drains.Load(); n != 0 {
		t.Fatalf("the take-over close sent %d drain frames, want none (v0.6.2 R4)", n)
	}
	edge.noConn(t, 300*time.Millisecond)
	if ev := log.of(evConflict); len(ev) != 1 || ev[0].conflict != (Conflict{ServerID: "srv_notes", Name: "notes", Reason: "taken_over"}) {
		t.Fatalf("conflict events = %+v", ev)
	}

	// close() afterwards still sends nothing.
	a.close(context.Background())
	if st := a.session.currentState(); st != stateClosed {
		t.Fatalf("state %s after close, want closed", st)
	}
}

// §3.3 Drain replaced: a take-over, never a hand-over -- the client is
// closed before it can dial a replacement, no unregister, state conflict.
func TestAgentDrainReplacedIsTakeOver(t *testing.T) {
	edge := newFakeEdge(t)
	started := make(chan struct{}, 1)
	a, log := newTestAgent(t, edge.url, func(c *agentConfig) { c.handler = hangingHandler(started) })
	ec := startRegistered(t, edge, a)
	// The in-flight stream must be cut by the agent, not left to the edge's
	// 5 s drain deadline. Who sends the final close is a race here (once the
	// stream is cut the edge's own drain may finish first), so the close
	// code is asserted only in the conflict test above.
	st := openHangingRequest(t, ec, started)

	start := time.Now()
	go func() {
		_ = ec.c.Drain(context.Background(), "replaced", wsmixer.DrainOptions{Deadline: 5 * time.Second})
	}()
	requireCutByTakeOver(t, st)
	ec.waitDone(t)
	if el := time.Since(start); el > time.Second {
		t.Fatalf("the taken-over connection took %v to close; it must close at once, not wait out the drain", el)
	}
	if st := a.session.currentState(); st != stateConflict {
		t.Fatalf("state %s, want conflict", st)
	}
	if got := a.client().State(); got != "closed" {
		t.Fatalf("client state %s, want closed (no replacement dial)", got)
	}
	ec.noApp(t, 100*time.Millisecond)
	if n := ec.drains.Load(); n != 0 {
		t.Fatalf("the take-over close sent %d drain frames, want none (v0.6.2 R4)", n)
	}
	edge.noConn(t, 300*time.Millisecond)
	if ev := log.of(evConflict); len(ev) != 1 || ev[0].conflict != (Conflict{ServerID: "srv_notes", Name: "notes", Reason: "replaced"}) {
		t.Fatalf("conflict events = %+v", ev)
	}
}

// Any other drain reason is a hand-over: the replacement registers on its
// welcome and the drained connection's end changes nothing.
func TestAgentDrainHandOverRegistersOnReplacement(t *testing.T) {
	edge := newFakeEdge(t)
	a, _ := newTestAgent(t, edge.url, nil)
	ec := startRegistered(t, edge, a)

	go func() {
		_ = ec.c.Drain(context.Background(), "rollout", wsmixer.DrainOptions{Deadline: 3 * time.Second})
	}()
	ec2 := edge.nextConn(t)
	ec2.expectRegister(t, "notes")
	ec.waitDone(t)
	if st := a.session.currentState(); st != stateRegistering {
		t.Fatalf("state %s, want registering (the drained connection's end must not change it)", st)
	}
	ec2.send(t, registeredNotes)
	waitState(t, a, stateRegistered)
}

// §3.2 Unregister: close() sends unregister on the live connection and
// waits for unregistered, then closes the client.
func TestAgentCloseUnregisters(t *testing.T) {
	edge := newFakeEdge(t)
	a, log := newTestAgent(t, edge.url, nil)
	ec := startRegistered(t, edge, a)

	done := make(chan struct{})
	go func() {
		defer close(done)
		a.close(context.Background())
	}()
	if got, want := string(ec.nextApp(t)), string(EncodeUnregister("notes")); got != want {
		t.Fatalf("app = %s, want %s", got, want)
	}
	start := time.Now()
	ec.send(t, `{"mcpwarp":{"v":1,"op":"unregistered","services":[{"name":"notes","id":"srv_notes"}]}}`)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("close() did not finish")
	}
	if el := time.Since(start); el > time.Second {
		t.Fatalf("close() waited %v after unregistered", el)
	}
	ec.waitDone(t)
	if st := a.session.currentState(); st != stateClosed {
		t.Fatalf("state %s, want closed", st)
	}
	if ev := log.of(evClose); len(ev) != 1 || ev[0].err != nil {
		t.Fatalf("close events = %+v", ev)
	}
}

// §2.5: a refusal before the first registration ends the agent.
func TestAgentRefusalBeforeFirstRegistrationIsFatal(t *testing.T) {
	edge := newFakeEdge(t)
	a, _ := newTestAgent(t, edge.url, nil)
	a.start()
	ec := edge.nextConn(t)
	ec.expectRegister(t, "notes")
	ec.send(t, `{"mcpwarp":{"v":1,"op":"registered","services":[],"errors":[{"name":"notes","code":"QUOTA_EXCEEDED","message":"quota exceeded"}]}}`)
	select {
	case <-a.done:
	case <-time.After(5 * time.Second):
		t.Fatal("the agent did not end")
	}
	var e *Error
	if !errors.As(a.err, &e) || e.Code != "QUOTA_EXCEEDED" || !e.Fatal || e.ServiceName != "notes" {
		t.Fatalf("agent ended with %#v", a.err)
	}
	ec.waitDone(t)
	edge.noConn(t, 200*time.Millisecond)
}
