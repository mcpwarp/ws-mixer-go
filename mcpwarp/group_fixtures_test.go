package mcpwarp

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"
)

// Group fixtures (tunnel/ms_*.json, MCPWARP.md §10.6, tunnel/README.md
// "Group fixtures"): the real ServiceGroup, agent and groupSession over the
// same fake ws-mixer client and clock as the tunnel fixtures. The fake edge
// records every register/unregister the SDK sends for the `sent` events and
// answers only a close's unregisters itself (runner rule 5).

// sentFrame is one register/unregister the SDK sent on a connection.
type sentFrame struct {
	conn  int
	op    string
	names []string
}

// groupHandler is the one handler every service gets; a pointer, so a
// config naming it compares equal.
type groupHandler struct{}

func (*groupHandler) ServeHTTP(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }

var theGroupHandler = &groupHandler{}

// groupFrame is the fake edge receiving a group's register/unregister.
func (r *tunnelRunner) groupFrame(ctx context.Context, d *fakeDial, b []byte) {
	var env struct {
		Mcpwarp struct {
			Op       string `json:"op"`
			Services []struct {
				Name string `json:"name"`
			} `json:"services"`
		} `json:"mcpwarp"`
	}
	if err := json.Unmarshal(b, &env); err != nil {
		r.t.Fatalf("fake edge: %s: %v", b, err)
	}
	f := sentFrame{conn: d.ordinal, op: env.Mcpwarp.Op}
	for _, s := range env.Mcpwarp.Services {
		f.names = append(f.names, s.Name)
	}
	if f.op == "unregister" && ctx.Value(closeFrameKey{}) != nil {
		if d.held {
			return
		}
		var svcs []map[string]any
		for _, n := range f.names {
			svcs = append(svcs, map[string]any{"name": n})
		}
		b, _ := json.Marshal(map[string]any{"mcpwarp": map[string]any{"v": 1, "op": "unregistered", "services": svcs}})
		d.tr.cfg.events.app(d, b)
		return
	}
	r.sentFrames = append(r.sentFrames, f)
}

func (r *tunnelRunner) matchSent(p map[string]any) {
	conn := len(r.dials)
	if c, ok := p["conn"].(float64); ok {
		conn = int(c)
	}
	var want []string
	for _, n := range p["names"].([]any) {
		want = append(want, n.(string))
	}
	op := p["op"].(string)
	for i, f := range r.sentFrames {
		if f.conn != conn {
			continue
		}
		if f.op != op || !slices.Equal(f.names, want) {
			r.t.Fatalf("sent: the next frame on conn %d is %s %v, want %s %v", conn, f.op, f.names, op, want)
		}
		r.sentFrames = append(r.sentFrames[:i], r.sentFrames[i+1:]...)
		return
	}
	r.t.Fatalf("sent %s %v on conn %d: no such frame went out (recorded: %+v)", op, want, conn, r.sentFrames)
}

// groupConnEvent is an edge message of a group fixture: entries carry
// their names; everything else is the tunnel fixtures' event.
func (r *tunnelRunner) groupConnEvent(name string, p map[string]any) {
	d := r.dialOf(p)
	delete(p, "conn")
	withOptional := func(env map[string]any, keys ...string) map[string]any {
		for _, k := range keys {
			if v, ok := p[k]; ok {
				env[k] = v
			}
		}
		return env
	}
	switch name {
	case "registered":
		r.overload = 1
		r.appTo(d, withOptional(map[string]any{"op": "registered"}, "services", "errors"))
	case "unregistered":
		r.overload = 1
		r.appTo(d, withOptional(map[string]any{"op": "unregistered"}, "services"))
	case "app_error":
		if c, _ := p["code"].(string); c != string(AppOverloaded) && c != string(AppConnectionLimit) {
			r.overload = 1
		}
		r.appTo(d, withOptional(map[string]any{"op": "error"}, "code", "message"))
	case "disabled":
		r.appTo(d, withOptional(map[string]any{"op": "disable"}, "id", "reason"))
	case "enabled":
		r.appTo(d, withOptional(map[string]any{"op": "enable"}, "id", "name"))
	case "conflict":
		r.appTo(d, withOptional(map[string]any{"op": "conflict"}, "id", "name", "reason"))
	default:
		if c, ok := p["conn"]; ok {
			p["conn"] = c
		}
		r.connEvent(name, p)
	}
}

func groupServicesOf(v any) []ServiceConfig {
	list, _ := v.([]any)
	out := make([]ServiceConfig, 0, len(list))
	for _, s := range list {
		m := s.(map[string]any)
		name, _ := m["name"].(string)
		kind, _ := m["kind"].(string)
		out = append(out, ServiceConfig{Name: name, Kind: ServiceKind(kind), Handler: theGroupHandler})
	}
	return out
}

func (r *tunnelRunner) mergedGroupConfig(base, payload map[string]any, provider bool) ServiceGroupConfig {
	m := map[string]any{}
	for k, v := range base {
		m[k] = v
	}
	for k, v := range payload {
		if v == nil {
			delete(m, k)
		} else {
			m[k] = v
		}
	}
	var c ServiceGroupConfig
	c.Enabled, _ = m["enabled"].(bool)
	c.Token, _ = m["token"].(string)
	c.URL, _ = m["url"].(string)
	c.Services = groupServicesOf(m["services"])
	if provider && c.Token != "" {
		c.TokenSource, c.Token = r.providerFor(c.Token), ""
	}
	return c
}

func decodeGroupExpect(t *testing.T, raw json.RawMessage) []ServiceGroupStatus {
	var list []struct {
		State     TunnelState `json:"state"`
		Code      StatusCode  `json:"code"`
		LastError string      `json:"lastError"`
		Services  map[string]struct {
			State     ServiceState `json:"state"`
			ID        string       `json:"id"`
			URL       string       `json:"url"`
			Code      string       `json:"code"`
			LastError string       `json:"lastError"`
		} `json:"services"`
	}
	if err := json.Unmarshal(raw, &list); err != nil {
		t.Fatalf("expect: %v", err)
	}
	out := make([]ServiceGroupStatus, len(list))
	for i, s := range list {
		g := ServiceGroupStatus{State: s.State, Code: s.Code, LastError: s.LastError, Services: map[string]ServiceStatus{}}
		for name, v := range s.Services {
			g.Services[name] = ServiceStatus{State: v.State, ID: v.ID, URL: v.URL, Code: v.Code, LastError: v.LastError}
		}
		out[i] = g
	}
	return out
}

func groupSnapString(ss []ServiceGroupStatus) string {
	var b strings.Builder
	for _, s := range ss {
		fmt.Fprintf(&b, "\n  {%s %s %q", s.State, s.Code, s.LastError)
		names := make([]string, 0, len(s.Services))
		for n := range s.Services {
			names = append(names, n)
		}
		sort.Strings(names)
		for _, n := range names {
			fmt.Fprintf(&b, " %s:%+v", n, s.Services[n])
		}
		b.WriteString("}")
	}
	return b.String()
}

// newFakeGroup is a ServiceGroup over the fake transport and clock, with
// the primary listener subscribed; mod may adjust the options first.
func newFakeGroup(t *testing.T, mod func(*ServiceGroupOptions)) (*tunnelRunner, *ServiceGroup) {
	r := &tunnelRunner{t: t, clock: &fakeClock{}, overload: 1, group: true}
	opts := ServiceGroupOptions{
		Integration:  testIntegration,
		OnRetry:      func(ri RetryInfo) { r.retries = append(r.retries, ri) },
		after:        r.clock.after,
		newTransport: r.newTransport,
		schedule:     func(fn func()) { r.deferred = append(r.deferred, fn) },
	}
	if mod != nil {
		mod(&opts)
	}
	g, err := NewServiceGroup(opts)
	if err != nil {
		t.Fatal(err)
	}
	g.Subscribe(func(s ServiceGroupStatus) { r.groupSnaps = append(r.groupSnaps, s) })
	return r, g
}

func runGroupFixture(t *testing.T, f *tunnelFixture) {
	r, g := newFakeGroup(t, nil)
	provider := f.usesTokenProvider()
	var pending []<-chan struct{}
	var apply func(name string, raw json.RawMessage)
	apply = func(name string, raw json.RawMessage) {
		var p map[string]any
		_ = json.Unmarshal(raw, &p)
		switch name {
		case "configure":
			pending = append(pending, g.configureAsync(r.mergedGroupConfig(f.Config, p, provider)))
		case "add_service":
			svcs := groupServicesOf([]any{p})
			pending = append(pending, g.editAsync(func(c *ServiceGroupConfig) bool {
				out := slices.DeleteFunc(c.Services, func(s ServiceConfig) bool { return s.Name == svcs[0].Name })
				c.Services = append(out, svcs[0])
				return true
			}))
		case "remove_service":
			_ = g.RemoveService(context.Background(), p["name"].(string))
		case "enable_service":
			_ = g.Enable(context.Background(), p["name"].(string))
		case "disable_service":
			_ = g.Disable(context.Background(), p["name"].(string))
		case "sent":
			r.matchSent(p)
		case "token_provider":
			result, _ := p["result"].(string)
			r.providerResults = append(r.providerResults, result)
		case "stop":
			pending = append(pending, g.stopAsync())
		case "dispose":
			timeout := defaultDisposeTimeout
			if ms, ok := p["timeoutMs"].(float64); ok {
				timeout = time.Duration(ms) * time.Millisecond
			}
			pending = append(pending, g.disposeAsync(timeout))
		case "burst":
			var list []json.RawMessage
			_ = json.Unmarshal(raw, &list)
			for _, ev := range list {
				apply(singleKey(t, ev))
			}
		case "subscribe":
			unsub := g.Subscribe(func(s ServiceGroupStatus) { r.groupSnaps = append(r.groupSnaps, s) })
			unsub()
		case "tick":
			r.advance(time.Duration(p["ms"].(float64)) * time.Millisecond)
		case "retry":
			limit := time.Duration(p["delayMs"].(float64)) * time.Millisecond
			r.matchRetry(p["cause"].(string), limit)
			if d, ok := r.nextTimer(limit); ok {
				r.advance(d)
			} else {
				r.advance(limit)
			}
		default:
			r.groupConnEvent(name, p)
		}
	}
	for _, ev := range f.Events {
		apply(singleKey(t, ev))
		r.quiesce()
	}
	for i, ch := range pending {
		select {
		case <-ch:
		default:
			t.Errorf("configure/stop/dispose/addService call %d never resolved", i+1)
		}
	}
	want := decodeGroupExpect(t, f.ExpectRaw)
	ok := len(r.groupSnaps) == len(want)
	for i := 0; ok && i < len(want); i++ {
		ok = sameGroupStatus(r.groupSnaps[i], want[i])
	}
	if !ok {
		t.Fatalf("snapshots\n got %s\nwant %s", groupSnapString(r.groupSnaps), groupSnapString(want))
	}
	if len(r.sentFrames) > 0 {
		t.Fatalf("frames the fixture has no sent event for: %+v", r.sentFrames)
	}
	if f.ExpectDials != nil && len(r.dials) != *f.ExpectDials {
		t.Fatalf("%d dials, want %d", len(r.dials), *f.ExpectDials)
	}
}
