package mcpwarp

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/mcpwarp/ws-mixer-go/wsmixer"
)

func groupConfig(names ...string) ServiceGroupConfig {
	c := ServiceGroupConfig{Enabled: true, Token: "t"}
	for _, n := range names {
		c.Services = append(c.Services, ServiceConfig{Name: n, Handler: theGroupHandler})
	}
	return c
}

func (r *tunnelRunner) groupOnline(g *ServiceGroup, c ServiceGroupConfig, names ...string) {
	g.configureAsync(c)
	r.quiesce()
	r.groupConnEvent("connected", map[string]any{})
	r.quiesce()
	r.groupConnEvent("welcome", map[string]any{})
	r.quiesce()
	r.matchSent(map[string]any{"op": "register", "names": toAny(names)})
	var svcs []any
	for _, n := range names {
		svcs = append(svcs, map[string]any{"name": n, "id": "srv_" + n, "url": "https://" + n + "-t.x/mcp"})
	}
	r.groupConnEvent("registered", map[string]any{"services": svcs})
	r.quiesce()
}

func toAny(xs []string) []any {
	out := make([]any, len(xs))
	for i, x := range xs {
		out[i] = x
	}
	return out
}

// §6A.2: an identical config changes nothing; a services-only change is
// diffed on the live connection without a redial; a token change restarts.
func TestServiceGroupConfigureDiff(t *testing.T) {
	r, g := newFakeGroup(t, nil)
	r.groupOnline(g, groupConfig("alpha", "beta"), "alpha", "beta")
	snaps := len(r.groupSnaps)
	if ch := g.configureAsync(groupConfig("alpha", "beta")); !isClosed(ch) {
		t.Fatal("an identical configure did not resolve at once")
	}
	r.quiesce()
	if len(r.groupSnaps) != snaps || len(r.sentFrames) != 0 {
		t.Fatalf("an identical configure delivered %d snapshots, sent %+v", len(r.groupSnaps)-snaps, r.sentFrames)
	}
	// A reorder sends nothing and delivers nothing.
	g.configureAsync(groupConfig("beta", "alpha"))
	r.quiesce()
	if len(r.groupSnaps) != snaps || len(r.sentFrames) != 0 {
		t.Fatalf("a reorder delivered %d snapshots, sent %+v", len(r.groupSnaps)-snaps, r.sentFrames)
	}
	g.configureAsync(groupConfig("beta", "gamma"))
	r.quiesce()
	r.matchSent(map[string]any{"op": "unregister", "names": toAny([]string{"alpha"})})
	if len(r.dials) != 1 {
		t.Fatalf("%d dials after a services-only change, want 1", len(r.dials))
	}
	if st := g.Status().Services; st["alpha"].State != ServiceUnregistered || st["gamma"].State != ServicePending {
		t.Fatalf("services %+v, want alpha unregistered, gamma pending", st)
	}
	r.groupConnEvent("unregistered", map[string]any{"services": []any{map[string]any{"name": "alpha", "id": "srv_alpha"}}})
	r.quiesce()
	r.matchSent(map[string]any{"op": "register", "names": toAny([]string{"gamma"})})

	c := groupConfig("beta", "gamma")
	c.Token = "t2"
	g.configureAsync(c)
	r.quiesce()
	if len(r.dials) != 2 {
		t.Fatalf("%d dials after a token change, want 2 (a restart)", len(r.dials))
	}
}

// §6A.2 checks: a duplicate name is INVALID_CONFIG, a bad name INVALID_NAME,
// an entry with no handler or target INVALID_CONFIG; none of them dials.
func TestServiceGroupConfigureValidation(t *testing.T) {
	for _, tc := range []struct {
		cfg  ServiceGroupConfig
		code StatusCode
	}{
		{groupConfig("alpha", "alpha"), CodeInvalidConfig},
		{groupConfig("Alpha"), CodeInvalidName},
		{ServiceGroupConfig{Enabled: true, Token: "t", Services: []ServiceConfig{{Name: "alpha"}}}, CodeInvalidConfig},
		{ServiceGroupConfig{Enabled: true, Token: "t", Services: []ServiceConfig{{Name: "alpha", Handler: theGroupHandler, Kind: "ftp"}}}, CodeInvalidConfig},
	} {
		r, g := newFakeGroup(t, nil)
		g.configureAsync(tc.cfg)
		r.quiesce()
		if st := g.Status(); st.State != StateError || st.Code != tc.code || len(st.Services) != 0 {
			t.Errorf("%+v: status %+v, want error/%s with no services", tc.cfg.Services, st, tc.code)
		}
		if len(r.dials) != 0 {
			t.Errorf("%+v: %d dials", tc.cfg.Services, len(r.dials))
		}
	}
	r, g := newFakeGroup(t, nil)
	g.configureAsync(ServiceGroupConfig{Enabled: true, Token: "t"})
	r.quiesce()
	if st := g.Status(); st.State != StateOff {
		t.Fatalf("no services: %+v, want off (§6A.2 check 10)", st)
	}
}

// §6A.8: Stop unregisters every name the connection may hold in one frame
// and goes {off, {}}; a local disable survives a restart, Enable clears it.
func TestServiceGroupStopAndLocalDisable(t *testing.T) {
	r, g := newFakeGroup(t, nil)
	r.groupOnline(g, groupConfig("alpha", "beta"), "alpha", "beta")
	_ = g.Disable(context.Background(), "beta")
	r.quiesce()
	r.matchSent(map[string]any{"op": "unregister", "names": toAny([]string{"beta"})})
	if st := g.Status().Services["beta"]; st.State != ServiceDisabled || st.Code != "" {
		t.Fatalf("beta %+v, want disabled with no code", st)
	}
	r.groupConnEvent("unregistered", map[string]any{"services": []any{map[string]any{"name": "beta", "id": "srv_beta"}}})
	r.quiesce()
	c := groupConfig("alpha", "beta")
	c.Token = "t2"
	g.configureAsync(c)
	r.quiesce()
	if st := g.Status().Services["beta"]; st.State != ServiceDisabled {
		t.Fatalf("after a restart beta %+v, want still locally disabled", st)
	}
	r.groupConnEvent("connected", map[string]any{})
	r.groupConnEvent("welcome", map[string]any{})
	r.quiesce()
	r.matchSent(map[string]any{"op": "register", "names": toAny([]string{"alpha"})})
	_ = g.Enable(context.Background(), "beta")
	r.quiesce()
	if len(r.sentFrames) != 0 {
		t.Fatalf("Enable sent %+v while alpha's register was outstanding, want it queued", r.sentFrames)
	}
	r.groupConnEvent("registered", map[string]any{"services": []any{map[string]any{"name": "alpha", "id": "srv_alpha", "url": "https://alpha-t.x/mcp"}}})
	r.quiesce()
	r.matchSent(map[string]any{"op": "register", "names": toAny([]string{"beta"})})
	r.groupConnEvent("registered", map[string]any{"services": []any{map[string]any{"name": "beta", "id": "srv_beta", "url": "https://beta-t.x/mcp"}}})
	r.quiesce()

	ch := g.stopAsync()
	r.quiesce()
	if !isClosed(ch) {
		t.Fatal("Stop did not resolve once the fake edge answered its unregister")
	}
	if st := g.Status(); st.State != StateOff || len(st.Services) != 0 {
		t.Fatalf("after Stop %+v, want {off, {}}", st)
	}
}

// A TokenSourceFunc on a group: an error wrapping ErrTokenUnavailable is a
// failed dial with the normal backoff (connecting/RETRYING, not fatal); a
// first 401 adds its non-fatal disconnect before the immediate token-refresh
// redial, and the group still comes online.
func TestServiceGroupTokenSourceUnavailableAndRefresh(t *testing.T) {
	r, g := newFakeGroup(t, nil)
	c := groupConfig("alpha")
	c.Token, c.TokenSource = "", r.providerFor("t") // a TokenSourceFunc
	r.providerResults = []string{"unavailable"}
	g.configureAsync(c)
	r.quiesce()
	if st := g.Status(); st.State != StateConnecting || st.Code != CodeRetrying || st.LastError != lastRetryDial {
		t.Fatalf("after an unavailable token: %+v, want connecting/RETRYING", st)
	}
	if len(r.dials) != 0 {
		t.Fatalf("%d dials, want none (the provider failed before the socket)", len(r.dials))
	}
	r.matchRetry("dial", fakeBackoff)
	r.advance(fakeBackoff)
	if len(r.dials) != 1 {
		t.Fatalf("%d dials after the backoff, want 1", len(r.dials))
	}
	r.groupConnEvent("dial_failed", map[string]any{"httpStatus": 401.0})
	r.quiesce()
	r.matchRetry("token-refresh", 0)
	if len(r.dials) != 2 {
		t.Fatalf("%d dials after the first 401, want 2 (the refresh redial)", len(r.dials))
	}
	r.groupConnEvent("connected", map[string]any{})
	r.groupConnEvent("welcome", map[string]any{})
	r.quiesce()
	r.matchSent(map[string]any{"op": "register", "names": toAny([]string{"alpha"})})
	r.groupConnEvent("registered", map[string]any{"services": []any{map[string]any{"name": "alpha", "id": "srv_alpha", "url": "https://alpha-t.x/mcp"}}})
	r.quiesce()
	if st := g.Status(); st.State != StateOnline {
		t.Fatalf("%+v, want online", st)
	}
}

// v0.7.1 (iii): a restart through Configure starts a new transient counter
// (it carries across reconnects, not across restarts).
func TestServiceGroupRestartResetsTransientCounter(t *testing.T) {
	r, g := newFakeGroup(t, nil)
	transient := map[string]any{"errors": []any{map[string]any{"name": "alpha", "code": "INTERNAL", "message": "x"}}}
	step := func(delay time.Duration) {
		t.Helper()
		r.groupConnEvent("registered", transient)
		r.quiesce()
		if len(r.retries) == 0 || r.retries[len(r.retries)-1].Delay != delay {
			t.Fatalf("retries %+v, want the last at %v", r.retries, delay)
		}
	}
	g.configureAsync(groupConfig("alpha"))
	r.quiesce()
	r.groupConnEvent("connected", map[string]any{})
	r.groupConnEvent("welcome", map[string]any{})
	r.quiesce()
	r.matchSent(map[string]any{"op": "register", "names": toAny([]string{"alpha"})})
	step(time.Second)
	r.advance(time.Second)
	r.matchSent(map[string]any{"op": "register", "names": toAny([]string{"alpha"})})
	step(2 * time.Second)

	c := groupConfig("alpha")
	c.Token = "t2"
	g.configureAsync(c)
	r.quiesce()
	r.groupConnEvent("connected", map[string]any{})
	r.groupConnEvent("welcome", map[string]any{})
	r.quiesce()
	r.matchSent(map[string]any{"op": "register", "names": toAny([]string{"alpha"})})
	step(time.Second)
}

// openedMetrics records ConnectionOpened; everything else is discarded.
type openedMetrics struct {
	wsmixer.NoopMetrics
	opened chan string
}

func (m *openedMetrics) ConnectionOpened(session, _ string) {
	select {
	case m.opened <- session:
	default:
	}
}

// Client is the live connection's wsmixer.Client (nil before the first
// Configure and after Dispose), and ServiceGroupOptions.Metrics reaches it.
// Real transport.
func TestServiceGroupClientAndMetrics(t *testing.T) {
	edge := newFakeEdge(t)
	m := &openedMetrics{opened: make(chan string, 1)}
	g, err := NewServiceGroup(ServiceGroupOptions{Integration: testIntegration, Metrics: m})
	if err != nil {
		t.Fatal(err)
	}
	if g.Client() != nil {
		t.Fatal("Client() before the first Configure, want nil")
	}
	c := groupConfig("alpha")
	c.URL = edge.url
	_ = g.Configure(context.Background(), c)
	ec := edge.nextConn(t)
	ec.nextApp(t)
	var session string
	select {
	case session = <-m.opened:
	case <-time.After(5 * time.Second):
		t.Fatal("ServiceGroupOptions.Metrics saw no ConnectionOpened")
	}
	cl := g.Client()
	if cl == nil || cl.Conn() == nil || cl.Conn().Session() != session {
		t.Fatalf("Client() = %v, want the live client whose connection has session %q", cl, session)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = g.Dispose(ctx)
	if g.Client() != nil {
		t.Fatal("Client() after Dispose, want nil")
	}
}

// Status and Subscribe hand out copies: a listener mutating its map does
// not change the group's status.
func TestServiceGroupStatusIsolation(t *testing.T) {
	r, g := newFakeGroup(t, nil)
	r.groupOnline(g, groupConfig("alpha"), "alpha")
	g.Subscribe(func(s ServiceGroupStatus) { s.Services["alpha"] = ServiceStatus{State: ServiceRejected} })
	st := g.Status()
	st.Services["alpha"] = ServiceStatus{}
	if got := g.Status().Services["alpha"].State; got != ServiceActive {
		t.Fatalf("alpha %s after the callers mutated their copies, want active", got)
	}
}

// idleTransport never dials: a group over it applies configs without a
// connection.
type idleTransport struct{}

func (idleTransport) start()                     {}
func (idleTransport) active() appConn            { return nil }
func (idleTransport) close(_ bool, done func())  { done() }
func (idleTransport) client() *wsmixer.Client    { return nil }
func newIdleTransport(transportConfig) transport { return idleTransport{} }

// Concurrent AddService calls each edit the desired config read and stored
// under one lock: none is lost to another's stale copy.
func TestServiceGroupConcurrentAddService(t *testing.T) {
	for round := 0; round < 20; round++ {
		g, err := NewServiceGroup(ServiceGroupOptions{Integration: testIntegration, newTransport: newIdleTransport})
		if err != nil {
			t.Fatal(err)
		}
		_ = g.Configure(context.Background(), groupConfig("base"))
		const n = 30
		var wg sync.WaitGroup
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				_ = g.AddService(context.Background(), ServiceConfig{Name: fmt.Sprintf("svc-%02d", i), Handler: theGroupHandler})
			}(i)
		}
		wg.Wait()
		g.mu.Lock()
		got := len(g.desired.Services)
		g.mu.Unlock()
		if got != n+1 {
			t.Fatalf("round %d: %d services desired after %d concurrent AddService, want %d", round, got, n, n+1)
		}
		deadline := time.Now().Add(5 * time.Second)
		for len(g.Status().Services) != n+1 {
			if time.Now().After(deadline) {
				t.Fatalf("round %d: status has %d services, want %d", round, len(g.Status().Services), n+1)
			}
			time.Sleep(time.Millisecond)
		}
		_ = g.Dispose(context.Background())
	}
}

func TestNewServiceGroupValidation(t *testing.T) {
	if _, err := NewServiceGroup(ServiceGroupOptions{}); !errors.Is(err, ErrInvalidOptions) {
		t.Fatalf("no integration: %v, want ErrInvalidOptions", err)
	}
	if _, err := NewServiceGroup(ServiceGroupOptions{Integration: testIntegration, Agent: Agent{Runtime: "\x01"}}); !errors.Is(err, ErrInvalidOptions) {
		t.Fatalf("bad agent: %v, want ErrInvalidOptions", err)
	}
}
