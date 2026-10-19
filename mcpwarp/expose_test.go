package mcpwarp

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mcpwarp/ws-mixer-go/wsmixer"
)

// §5.2: validation is synchronous, first failure wins, in table order, and
// no message echoes the offending value.
func TestExposeValidationOrder(t *testing.T) {
	ok := ExposeOptions{Token: "t", Name: "notes", Integration: testIntegration, Handler: http.NotFoundHandler()}
	cases := []struct {
		name string
		mod  func(*ExposeOptions)
		msg  string
	}{
		{"1 no adapter", func(o *ExposeOptions) { o.Handler = nil }, "mcpwarp: pass exactly one of `handler` or `target`"},
		{"1 both", func(o *ExposeOptions) { o.Target = "http://127.0.0.1:1/mcp" }, "mcpwarp: pass exactly one of `handler` or `target`"},
		{"2 timeout with handler", func(o *ExposeOptions) { o.TargetTimeout = time.Second }, "mcpwarp: `targetTimeoutMs` applies only to `target`"},
		{"3 bad target", func(o *ExposeOptions) { o.Handler, o.Target = nil, "::nope" }, "mcpwarp: `target` is not a valid URL"},
		{"4 timeout range", func(o *ExposeOptions) {
			o.Handler, o.Target, o.TargetTimeout = nil, "http://127.0.0.1:1/mcp", -5*time.Millisecond
		},
			"mcpwarp: targetTimeoutMs must be a positive number of at most 2147483647 ms, got -5"},
		{"5 name", func(o *ExposeOptions) { o.Name = "Bad Name" }, "mcpwarp: invalid service `name`: lowercase letters, digits and inner dashes, at most 30 characters"},
		{"6 integration missing", func(o *ExposeOptions) { o.Integration = "" }, "mcpwarp: `integration` (your integration's registration UUID) is required"},
		{"7 integration syntax", func(o *ExposeOptions) { o.Integration = "not-a-uuid" }, "mcpwarp: `integration` must be an RFC 4122 UUID in text form; the server silently ignores anything else"},
		{"8 kind", func(o *ExposeOptions) { o.Kind = "sse" }, "mcpwarp: `kind` must be \"http\" or \"stdio\""},
		{"9 token", func(o *ExposeOptions) { o.Token = "" }, "mcpwarp: `token` is required"},
		{"10 agent", func(o *ExposeOptions) { o.Agent.Runtime = strings.Repeat("x", 65) }, "mcpwarp: `agent.runtime` must be 1-64 printable ASCII characters"},
		{"11 reconnect", func(o *ExposeOptions) { o.Reconnect.StableAfter = -1 }, "mcpwarp: `reconnect.stableAfter` must be a finite number >= 0"},
	}
	// field groups the rows that set the same option (rows 1-4 the adapter).
	field := func(name string) string {
		switch name[:strings.IndexByte(name, ' ')] {
		case "1", "2", "3", "4":
			return "adapter"
		case "6", "7":
			return "integration"
		}
		return name
	}
	for i, c := range cases {
		o := ok
		c.mod(&o)
		// Every later row on another option broken too: the first failure must still win.
		broken := map[string]bool{field(c.name): true, "adapter": true}
		for _, later := range cases[i+1:] {
			if f := field(later.name); !broken[f] {
				broken[f] = true
				later.mod(&o)
			}
		}
		_, err := Expose(context.Background(), o)
		if !errors.Is(err, ErrInvalidOptions) || err.Error() != c.msg {
			t.Errorf("%s: Expose = %v\nwant %q", c.name, err, c.msg)
		}
	}
	// 12: an already-cancelled ctx: nothing is dialed.
	edge := newFakeEdge(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	o := ok
	o.URL = edge.url
	if _, err := Expose(ctx, o); !errors.Is(err, context.Canceled) {
		t.Fatalf("Expose with a cancelled ctx = %v", err)
	}
	edge.noConn(t, 200*time.Millisecond)
}

func exposeOpts(edgeURL string) ExposeOptions {
	return ExposeOptions{
		URL: edgeURL, Token: "t", Name: "notes", Integration: testIntegration,
		Handler: http.NotFoundHandler(), Reconnect: ReconnectOptions{Base: 10 * time.Millisecond, Cap: 50 * time.Millisecond},
	}
}

type exposeResult struct {
	h   *Handle
	err error
}

func exposeAsync(ctx context.Context, o ExposeOptions) <-chan exposeResult {
	ch := make(chan exposeResult, 1)
	go func() {
		h, err := Expose(ctx, o)
		ch <- exposeResult{h, err}
	}()
	return ch
}

func await(t *testing.T, ch <-chan exposeResult) exposeResult {
	t.Helper()
	select {
	case r := <-ch:
		return r
	case <-time.After(5 * time.Second):
		t.Fatal("Expose did not return")
		return exposeResult{}
	}
}

func nextEvent(t *testing.T, h *Handle, kind EventKind) Event {
	t.Helper()
	timeout := time.After(5 * time.Second)
	for {
		select {
		case ev, ok := <-h.Events():
			if !ok {
				t.Fatalf("events closed before %v", kind)
			}
			if ev.Kind == kind {
				return ev
			}
		case <-timeout:
			t.Fatalf("no %v event", kind)
		}
	}
}

// §5.4-§5.7: resolves registered with the handle; Close unregisters, then
// EventClose is the last event and the channel closes.
func TestExposeRegistersAndCloses(t *testing.T) {
	edge := newFakeEdge(t)
	res := exposeAsync(context.Background(), exposeOpts(edge.url))
	ec := edge.nextConn(t)
	ec.expectRegister(t, "notes")
	ec.send(t, registeredNotes)
	r := await(t, res)
	if r.err != nil {
		t.Fatal(r.err)
	}
	h := r.h
	reg, ok := h.Registration()
	if h.URL() != "https://notes-tester.mcpwarp.test/mcp" || h.ServerID() != "srv_notes" || !ok || !reg.Created ||
		h.Path() != "/mcp" || h.State() != ExposeRegistered || h.Client() == nil {
		t.Fatalf("handle: url %q id %q reg %+v/%v path %q state %s", h.URL(), h.ServerID(), reg, ok, h.Path(), h.State())
	}
	if ev := nextEvent(t, h, EventRegistered); ev.Registration != reg {
		t.Fatalf("EventRegistered %+v, want %+v", ev.Registration, reg)
	}

	closed := make(chan error, 1)
	go func() { closed <- h.Close(context.Background()) }()
	if got := string(ec.nextApp(t)); got != string(EncodeUnregister("notes")) {
		t.Fatalf("app %s, want unregister", got)
	}
	ec.send(t, `{"mcpwarp":{"v":1,"op":"unregistered","services":[{"name":"notes","id":"srv_notes"}]}}`)
	if err := <-closed; err != nil {
		t.Fatal(err)
	}
	if ev := nextEvent(t, h, EventClose); ev.Err != nil {
		t.Fatalf("EventClose err %v", ev.Err)
	}
	if _, ok := <-h.Events(); ok {
		t.Fatal("an event after EventClose")
	}
	if h.State() != ExposeClosed || h.Close(context.Background()) != nil {
		t.Fatal("close is not idempotent or the state is not closed")
	}
}

// §5.9 Held disable at the handle: the registered that brings a held
// disable's id is EventRegistered then EventDisabled, back to back; the
// handle keeps the registration and ends disabled.
func TestExposeHeldDisableRegisteredThenDisabled(t *testing.T) {
	edge := newFakeEdge(t)
	res := exposeAsync(context.Background(), exposeOpts(edge.url))
	ec := edge.nextConn(t)
	ec.expectRegister(t, "notes")
	ec.send(t, `{"mcpwarp":{"v":1,"op":"disable","id":"srv_notes","reason":"disabled"}}`)
	ec.send(t, registeredNotes)
	r := await(t, res)
	if r.err != nil {
		t.Fatal(r.err)
	}
	h := r.h
	ev := nextEvent(t, h, EventRegistered)
	if ev.Registration.ServerID != "srv_notes" {
		t.Fatalf("EventRegistered %+v", ev.Registration)
	}
	nextEvent(t, h, EventDisabled)
	waitFor(t, func() bool { return h.State() == ExposeDisabled })
	if reg, ok := h.Registration(); !ok || reg.ServerID != "srv_notes" || h.URL() == "" {
		t.Fatalf("Registration %+v/%v, url %q; want the registration kept", reg, ok, h.URL())
	}
}

func TestExposeRejections(t *testing.T) {
	t.Run("refusal before registration", func(t *testing.T) {
		edge := newFakeEdge(t)
		res := exposeAsync(context.Background(), exposeOpts(edge.url))
		ec := edge.nextConn(t)
		ec.expectRegister(t, "notes")
		ec.send(t, `{"mcpwarp":{"v":1,"op":"registered","errors":[{"name":"notes","code":"QUOTA_EXCEEDED","message":"quota exceeded"}]}}`)
		var e *Error
		if r := await(t, res); !errors.As(r.err, &e) || e.Code != "QUOTA_EXCEEDED" || !e.Fatal || e.Message != "quota exceeded" || e.ServiceName != "notes" {
			t.Fatalf("Expose = %#v", r.err)
		}
		ec.waitDone(t)
	})
	t.Run("taken over before registration", func(t *testing.T) {
		edge := newFakeEdge(t)
		res := exposeAsync(context.Background(), exposeOpts(edge.url))
		ec := edge.nextConn(t)
		ec.expectRegister(t, "notes")
		ec.send(t, `{"mcpwarp":{"v":1,"op":"conflict","id":"srv_notes","name":"notes"}}`)
		var e *Error
		r := await(t, res)
		if !errors.As(r.err, &e) || e.Code != ErrCodeTakenOver || e.Message != `mcpwarp: service "notes" was taken over by another connection before it registered` {
			t.Fatalf("Expose = %#v", r.err)
		}
		ec.waitDone(t)
		edge.noConn(t, 300*time.Millisecond)
	})
	t.Run("unauthorized", func(t *testing.T) {
		edge := newFakeEdge(t)
		edge.rejectNext.Store(1)
		var e *Error
		r := await(t, exposeAsync(context.Background(), exposeOpts(edge.url)))
		if !errors.As(r.err, &e) || e.Code != ErrCodeUnauthorized || !e.Fatal {
			t.Fatalf("Expose = %#v", r.err)
		}
		var dr wsmixer.DisconnectReason
		if !errors.As(e, &dr) || !dr.Fatal {
			t.Fatalf("cause %v, want the fatal disconnect reason", e.Cause)
		}
	})
	t.Run("ctx cancelled before registration", func(t *testing.T) {
		edge := newFakeEdge(t)
		ctx, cancel := context.WithCancel(context.Background())
		res := exposeAsync(ctx, exposeOpts(edge.url))
		ec := edge.nextConn(t)
		cancel()
		if r := await(t, res); !errors.Is(r.err, context.Canceled) || r.h != nil {
			t.Fatalf("Expose = (%v, %v)", r.h, r.err)
		}
		ec.waitDone(t)
	})
}

// §5.8: cancelling ctx after Expose returned closes the handle.
func TestExposeContextCancelAfterSettleCloses(t *testing.T) {
	edge := newFakeEdge(t)
	ctx, cancel := context.WithCancel(context.Background())
	res := exposeAsync(ctx, exposeOpts(edge.url))
	ec := edge.nextConn(t)
	ec.expectRegister(t, "notes")
	ec.send(t, registeredNotes)
	h := await(t, res).h
	cancel()
	if got := string(ec.nextApp(t)); got != string(EncodeUnregister("notes")) {
		t.Fatalf("app %s, want unregister", got)
	}
	ec.send(t, `{"mcpwarp":{"v":1,"op":"unregistered"}}`)
	nextEvent(t, h, EventClose)
}

// §5.9: OnRetry before every transport redial (backoff, drain, the
// token-refresh redial) and every register re-send (OVERLOADED).
func TestExposeOnRetry(t *testing.T) {
	edge := newFakeEdge(t)
	edge.rejectNext.Store(1) // the first hello: one token-refresh redial
	var mu sync.Mutex
	var retries []RetryInfo
	var tokens atomic.Int32
	o := exposeOpts(edge.url)
	o.Token, o.TokenProvider = "", func(context.Context) (string, error) {
		tokens.Add(1)
		return "t", nil
	}
	o.OnRetry = func(ri RetryInfo) {
		mu.Lock()
		retries = append(retries, ri)
		mu.Unlock()
	}
	count := func(pred func(RetryInfo) bool) int {
		mu.Lock()
		defer mu.Unlock()
		n := 0
		for _, r := range retries {
			if pred(r) {
				n++
			}
		}
		return n
	}
	// seen: exactly one matching OnRetry call, and no second one shortly after.
	seen := func(pred func(RetryInfo) bool) bool {
		deadline := time.Now().Add(5 * time.Second)
		for count(pred) == 0 && time.Now().Before(deadline) {
			time.Sleep(5 * time.Millisecond)
		}
		time.Sleep(100 * time.Millisecond)
		return count(pred) == 1
	}
	res := exposeAsync(context.Background(), o)
	ec := edge.nextConn(t)
	if !seen(func(r RetryInfo) bool { return r == RetryInfo{Attempt: 1, Delay: 0, Reason: "token-refresh"} }) {
		t.Fatalf("no token-refresh OnRetry in %+v", retries)
	}
	ec.expectRegister(t, "notes")
	ec.send(t, `{"mcpwarp":{"v":1,"op":"error","code":"OVERLOADED","message":"busy"}}`)
	if !seen(func(r RetryInfo) bool {
		return r == RetryInfo{Attempt: 1, Delay: time.Second, Reason: "register failed: OVERLOADED: busy"}
	}) {
		t.Fatalf("no OVERLOADED OnRetry in %+v", retries)
	}
	ec.expectRegister(t, "notes") // the re-send
	ec.send(t, registeredNotes)
	h := await(t, res).h
	defer h.Close(context.Background())

	_ = ec.c.Close(uint32(wsmixer.InternalErrorCode), "edge restart")
	if !seen(func(r RetryInfo) bool {
		return r.Attempt >= 1 && r.Reason == "edge restart" && r.Delay <= 50*time.Millisecond
	}) {
		t.Fatalf("no backoff OnRetry in %+v", retries)
	}
	ec2 := edge.nextConn(t)
	ec2.expectRegister(t, "notes")
	ec2.send(t, registeredNotes)

	go func() {
		_ = ec2.c.Drain(context.Background(), "rollout", wsmixer.DrainOptions{Deadline: 3 * time.Second})
	}()
	if !seen(func(r RetryInfo) bool { return r.Attempt == 0 && r.Reason == "drain" && r.Delay <= 2*time.Second }) {
		t.Fatalf("no drain OnRetry in %+v", retries)
	}
	edge.nextConn(t).expectRegister(t, "notes")
	if n := tokens.Load(); n < 4 {
		t.Fatalf("token provider called %d times, want once per dial (4)", n)
	}
}

// §8: the caller's HTTP client performs the upgrade.
func TestExposeWebSocketInjection(t *testing.T) {
	edge := newFakeEdge(t)
	var used atomic.Int32
	o := exposeOpts(edge.url)
	o.WebSocket = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		used.Add(1)
		return http.DefaultTransport.RoundTrip(r)
	})}
	res := exposeAsync(context.Background(), o)
	ec := edge.nextConn(t)
	ec.expectRegister(t, "notes")
	ec.send(t, registeredNotes)
	h := await(t, res).h
	defer h.Close(context.Background())
	if used.Load() != 1 {
		t.Fatalf("the injected client made %d requests, want the one upgrade", used.Load())
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// §5.6: a take-over after Expose returned is EventConflict and state
// conflict; drain{replaced} carries the known server id.
func TestExposeConflictAfterSettle(t *testing.T) {
	edge := newFakeEdge(t)
	res := exposeAsync(context.Background(), exposeOpts(edge.url))
	ec := edge.nextConn(t)
	ec.expectRegister(t, "notes")
	ec.send(t, registeredNotes)
	h := await(t, res).h
	go func() { _ = ec.c.Drain(context.Background(), "replaced", wsmixer.DrainOptions{Deadline: time.Second}) }()
	ev := nextEvent(t, h, EventConflict)
	if ev.Conflict != (Conflict{ServerID: "srv_notes", Name: "notes", Reason: "replaced"}) || h.State() != ExposeConflict {
		t.Fatalf("conflict %+v state %s", ev.Conflict, h.State())
	}
	ec.waitDone(t)
	edge.noConn(t, 300*time.Millisecond)
	if err := h.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	nextEvent(t, h, EventClose)
}

// The real transport under a Tunnel: configure goes online over a live edge.
func TestTunnelOverRealTransport(t *testing.T) {
	edge := newFakeEdge(t)
	tun, err := NewTunnel(TunnelOptions{Integration: testIntegration, Handler: http.NotFoundHandler()})
	if err != nil {
		t.Fatal(err)
	}
	online := make(chan TunnelStatus, 8)
	tun.Subscribe(func(s TunnelStatus) { online <- s })
	if err := tun.Configure(context.Background(), TunnelConfig{Enabled: true, Token: "t", Name: "notes", URL: edge.url}); err != nil {
		t.Fatal(err)
	}
	ec := edge.nextConn(t)
	ec.expectRegister(t, "notes")
	ec.send(t, registeredNotes)
	timeout := time.After(5 * time.Second)
wait:
	for {
		select {
		case s := <-online:
			if s.State == StateOnline {
				if s.URL != "https://notes-tester.mcpwarp.test/mcp" {
					t.Fatalf("online %+v", s)
				}
				break wait
			}
		case <-timeout:
			t.Fatalf("the tunnel never went online (status %+v)", tun.Status())
		}
	}
	go func() {
		app := ec.nextApp(t)
		if string(app) == string(EncodeUnregister("notes")) {
			ec.send(t, `{"mcpwarp":{"v":1,"op":"unregistered"}}`)
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := tun.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if got := tun.Status(); got != (TunnelStatus{State: StateOff}) {
		t.Fatalf("status after Stop %+v", got)
	}
	ec.waitDone(t)
}
