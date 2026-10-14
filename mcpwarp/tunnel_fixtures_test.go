package mcpwarp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/mcpwarp/ws-mixer-go/wsmixer"
)

// spec/fixtures/mcpwarp/tunnel/*.json (MCPWARP.md §6, §10.4), run against
// the real Tunnel, agent and session over a fake ws-mixer client and a fake
// clock: tunnel/README.md's "runner that fakes the ws-mixer client itself".
// fakeTransport models the client's reconnect rules (WIRE.md §2.9) the way
// the fixtures state them -- a redial after a failed dial or a non-fatal
// loss, a replacement dial on drain, the drained connection serving until
// it ends -- and injects each disconnect's {fatal, code, wsCode} verbatim.
// wsmixer.Client's own verdicts are covered by its tests. Everything runs on
// the test goroutine: after each event the runner drains the deferred apply
// pump, so the SDK is quiescent; only tick and retry move the clock.

const (
	fakeBackoff    = time.Second
	fakeDrainDelay = 500 * time.Millisecond
	fakeGrace      = 5 * time.Second
)

type tunnelRunner struct {
	t        *testing.T
	clock    *fakeClock
	deferred []func()
	dials    []*fakeDial
	retries  []RetryInfo // onRetry calls not yet matched by a retry event
	overload int         // expected attempt of the next OVERLOADED re-send
	snaps    []TunnelStatus
}

func (r *tunnelRunner) quiesce() {
	for len(r.deferred) > 0 {
		f := r.deferred[0]
		r.deferred = r.deferred[1:]
		f()
	}
}

func (r *tunnelRunner) advance(d time.Duration) {
	r.clock.advance(d)
	r.quiesce()
}

type fakeTransport struct {
	r   *tunnelRunner
	cfg transportConfig

	closed     bool // close() called, or a fatal end
	ended      bool
	dones      []func()
	pending    *fakeDial // the dial in progress
	redialStop func() bool
	activeC    *fakeDial
	conns      []*fakeDial
	attempt    int
	serverID   string
}

type fakeDial struct {
	tr                 *fakeTransport
	ordinal            int
	upgraded, welcomed bool
	ended, abandoned   bool
	held, drained      bool
}

func (r *tunnelRunner) newTransport(cfg transportConfig) transport {
	return &fakeTransport{r: r, cfg: cfg}
}

func (tr *fakeTransport) start()                  { tr.dial() }
func (tr *fakeTransport) client() *wsmixer.Client { return nil }

func (tr *fakeTransport) active() appConn {
	if tr.activeC != nil && !tr.activeC.ended {
		return tr.activeC
	}
	return nil
}

func (tr *fakeTransport) dial() {
	d := &fakeDial{tr: tr, ordinal: len(tr.r.dials) + 1}
	tr.r.dials = append(tr.r.dials, d)
	tr.conns = append(tr.conns, d)
	tr.pending = d
}

func (tr *fakeTransport) scheduleRedial(cause string, delay time.Duration, attempt int) {
	if tr.redialStop != nil {
		return
	}
	tr.cfg.events.retry(wsmixer.ReconnectInfo{Attempt: attempt, Delay: delay, Cause: cause})
	tr.redialStop = tr.r.clock.after(delay, func() {
		tr.redialStop = nil
		if !tr.closed {
			tr.dial()
		}
	})
}

func (tr *fakeTransport) backoff(cause string) {
	tr.attempt++
	tr.scheduleRedial(cause, fakeBackoff, tr.attempt)
}

func (tr *fakeTransport) live() []*fakeDial {
	var out []*fakeDial
	for _, d := range tr.conns {
		if d.upgraded && !d.ended && !d.abandoned {
			out = append(out, d)
		}
	}
	return out
}

func (tr *fakeTransport) close(now bool, done func()) {
	if tr.ended {
		if done != nil {
			done()
		}
		return
	}
	if done != nil {
		tr.dones = append(tr.dones, done)
	}
	if tr.closed {
		if now {
			for _, d := range tr.live() {
				tr.endByClose(d)
			}
		}
		tr.checkEnded()
		return
	}
	tr.closed = true
	tr.stopRedial()
	if tr.pending != nil {
		tr.pending.abandoned = true
		tr.pending = nil
	}
	for _, d := range tr.live() {
		if d.held && !now {
			d := d
			tr.r.clock.after(fakeGrace, func() { tr.endByClose(d) })
			continue
		}
		tr.endByClose(d)
	}
	tr.checkEnded()
}

func (tr *fakeTransport) stopRedial() {
	if tr.redialStop != nil {
		tr.redialStop()
		tr.redialStop = nil
	}
}

func (tr *fakeTransport) endByClose(d *fakeDial) {
	if d.ended {
		return
	}
	d.ended = true
	if d.welcomed {
		tr.cfg.events.connEnd(d)
	}
	tr.cfg.events.disconnect(wsmixer.DisconnectReason{Phase: wsmixer.PhaseConnected, Message: "client closing"})
	tr.checkEnded()
}

func (tr *fakeTransport) checkEnded() {
	if !tr.closed || tr.ended || len(tr.live()) > 0 {
		return
	}
	tr.ended = true
	dones := tr.dones
	tr.dones = nil
	for _, f := range dones {
		f()
	}
}

func (d *fakeDial) SendApp(_ context.Context, body any) error {
	if d.ended || d.abandoned || !d.welcomed {
		return errors.New("fake: connection gone")
	}
	b, _ := json.Marshal(body)
	if string(b) == string(EncodeUnregister(d.tr.cfg.name)) && !d.held {
		d.tr.cfg.events.app(d, []byte(fmt.Sprintf(`{"mcpwarp":{"v":1,"op":"unregistered","services":[{"name":%q}]}}`, d.tr.cfg.name)))
	}
	return nil
}

func (d *fakeDial) laterWelcomed() bool {
	for _, o := range d.tr.conns {
		if o.ordinal > d.ordinal && o.welcomed {
			return true
		}
	}
	return false
}

// --- events -------------------------------------------------------------

type tunnelFixture struct {
	Description string            `json:"description"`
	Config      map[string]any    `json:"config"`
	Events      []json.RawMessage `json:"events"`
	Expect      []TunnelStatus    `json:"expect"`
	ExpectDials *int              `json:"expectDials"`
}

func (s *TunnelStatus) UnmarshalJSON(b []byte) error {
	var v struct {
		State     TunnelState `json:"state"`
		URL       string      `json:"url"`
		Code      StatusCode  `json:"code"`
		LastError string      `json:"lastError"`
	}
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	*s = TunnelStatus{State: v.State, URL: v.URL, Code: v.Code, LastError: v.LastError}
	return nil
}

func (r *tunnelRunner) dialOf(p map[string]any) *fakeDial {
	if c, ok := p["conn"].(float64); ok {
		if int(c) < 1 || int(c) > len(r.dials) {
			r.t.Fatalf("conn %v: only %d dials", c, len(r.dials))
		}
		return r.dials[int(c)-1]
	}
	if len(r.dials) == 0 {
		r.t.Fatal("no dial yet")
	}
	return r.dials[len(r.dials)-1]
}

func (r *tunnelRunner) appTo(d *fakeDial, env map[string]any) {
	if d.ended || d.abandoned || !d.welcomed {
		return
	}
	env["v"] = 1
	b, _ := json.Marshal(map[string]any{"mcpwarp": env})
	d.tr.cfg.events.app(d, b)
}

func (d *fakeDial) serverID() string {
	if d.tr.serverID != "" {
		return d.tr.serverID
	}
	return "srv_" + d.tr.cfg.name
}

func strField(p map[string]any, k string) (string, bool) {
	s, ok := p[k].(string)
	return s, ok
}

func (r *tunnelRunner) connEvent(name string, p map[string]any) {
	d := r.dialOf(p)
	tr := d.tr
	switch name {
	case "connected":
		if !d.abandoned && !tr.closed {
			d.upgraded = true
		}
	case "dial_failed":
		if d.abandoned || tr.closed {
			return
		}
		d.ended, tr.pending = true, nil
		tr.cfg.events.disconnect(wsmixer.DisconnectReason{Phase: wsmixer.PhaseDial, Message: "dial failed"})
		tr.backoff("dial failed")
	case "welcome":
		if d.abandoned || tr.closed || !d.upgraded {
			return
		}
		d.welcomed = true
		if tr.pending == d {
			tr.pending = nil
		}
		tr.activeC = d
		tr.cfg.events.connect(d)
	case "registered":
		svcs := p["services"].([]any)
		out := []any{}
		for _, s := range svcs {
			m := s.(map[string]any)
			m["name"] = tr.cfg.name
			tr.serverID, _ = m["id"].(string)
			out = append(out, m)
		}
		if !d.ended {
			r.overload = 1
		}
		r.appTo(d, map[string]any{"op": "registered", "services": out})
	case "register_error":
		e := map[string]any{"name": tr.cfg.name, "code": p["code"]}
		if m, ok := strField(p, "message"); ok {
			e["message"] = m
		}
		if id, ok := strField(p, "id"); ok {
			e["id"] = id
			tr.serverID = id
		}
		r.appTo(d, map[string]any{"op": "registered", "errors": []any{e}})
	case "app_error":
		e := map[string]any{"op": "error", "code": p["code"]}
		if m, ok := strField(p, "message"); ok {
			e["message"] = m
		}
		r.appTo(d, e)
	case "disabled":
		e := map[string]any{"op": "disable", "id": d.serverID()}
		if m, ok := strField(p, "reason"); ok {
			e["reason"] = m
		}
		r.appTo(d, e)
	case "enabled":
		r.appTo(d, map[string]any{"op": "enable", "id": d.serverID(), "name": tr.cfg.name})
	case "conflict":
		e := map[string]any{"op": "conflict", "id": d.serverID(), "name": tr.cfg.name}
		if m, ok := strField(p, "reason"); ok {
			e["reason"] = m
		}
		r.appTo(d, e)
	case "hold":
		d.held = true
	case "drain":
		if d.ended || !d.welcomed {
			return
		}
		reason, ok := strField(p, "reason")
		if !ok {
			reason = "rollout"
		}
		tr.cfg.events.drain(reason)
		if tr.closed {
			return
		}
		d.drained = true
		tr.scheduleRedial(wsmixer.ReconnectDrain, fakeDrainDelay, 0)
	case "disconnect":
		r.disconnect(d, p)
	default:
		r.t.Fatalf("unknown event %q", name)
	}
}

func (r *tunnelRunner) disconnect(d *fakeDial, p map[string]any) {
	tr := d.tr
	if d.ended || d.abandoned || !d.upgraded {
		return
	}
	fatal, _ := p["fatal"].(bool)
	code, _ := strField(p, "code")
	ws, _ := p["wsCode"].(float64)
	reason := wsmixer.DisconnectReason{Phase: wsmixer.PhaseConnected, WSCode: int(ws), Fatal: fatal, Message: fmt.Sprintf("closed %d", int(ws))}
	if code != "" {
		if ec, ok := wsmixer.ParseErrorCode(code); ok {
			reason.ErrorCode, reason.HasErrorCode, reason.ErrorName = ec, true, code
		}
	}
	d.ended = true
	if !d.welcomed {
		reason.Phase = wsmixer.PhaseHandshake
		if tr.pending == d {
			tr.pending = nil
		}
	} else {
		tr.cfg.events.connEnd(d)
	}
	switch {
	case tr.closed:
		reason.Fatal = false
		tr.cfg.events.disconnect(reason)
		tr.checkEnded()
	case d.welcomed && d.laterWelcomed():
		reason.Fatal, reason.Superseded = false, true
		tr.cfg.events.disconnect(reason)
	case fatal:
		tr.closed = true
		tr.stopRedial()
		for _, o := range tr.live() {
			o.ended = true
		}
		tr.ended = true
		tr.cfg.events.disconnect(reason)
		dones := tr.dones
		tr.dones = nil
		for _, f := range dones {
			f()
		}
	case d.welcomed && d.drained && (tr.pending != nil || tr.redialStop != nil):
		// The replacement's dial is the pending reconnect (T44).
		tr.cfg.events.disconnect(reason)
	default:
		tr.cfg.events.disconnect(reason)
		cause := "connection lost"
		if !d.welcomed {
			cause = "handshake failed"
		}
		tr.backoff(cause)
	}
}

// --- running --------------------------------------------------------------

func singleKey(t *testing.T, raw json.RawMessage) (string, json.RawMessage) {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil || len(m) != 1 {
		t.Fatalf("event %s: want exactly one key", raw)
	}
	for k, v := range m {
		return k, v
	}
	return "", nil
}

func (r *tunnelRunner) mergedConfig(base map[string]any, payload map[string]any) TunnelConfig {
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
	var c TunnelConfig
	c.Enabled, _ = m["enabled"].(bool)
	c.Token, _ = m["token"].(string)
	c.Name, _ = m["name"].(string)
	c.URL, _ = m["url"].(string)
	k, _ := m["kind"].(string)
	c.Kind = ServiceKind(k)
	return c
}

// matchRetry checks tunnel/README.md's onRetry contract for one retry
// event: exactly one not yet matched onRetry call fits its cause.
func (r *tunnelRunner) matchRetry(cause string, delay time.Duration) {
	fits := func(ri RetryInfo) bool {
		switch cause {
		case "register":
			return ri.Delay == delay && strings.HasPrefix(ri.Reason, "register failed: ") && !strings.HasPrefix(ri.Reason, "register failed: OVERLOADED: ")
		case "overloaded":
			return ri.Delay == time.Second && strings.HasPrefix(ri.Reason, "register failed: OVERLOADED: ")
		case "drain":
			return ri.Attempt == 0 && ri.Reason == "drain" && ri.Delay <= delay
		case "dial", "lost":
			return ri.Delay <= delay && ri.Reason != "drain" && !strings.HasPrefix(ri.Reason, "register failed")
		}
		r.t.Fatalf("unknown retry cause %q", cause)
		return false
	}
	match := -1
	for i, ri := range r.retries {
		if fits(ri) {
			if match >= 0 {
				r.t.Fatalf("retry %s: more than one matching onRetry call among %+v", cause, r.retries)
			}
			match = i
		}
	}
	if match < 0 {
		r.t.Fatalf("retry %s: no matching onRetry call among %+v", cause, r.retries)
	}
	if cause == "overloaded" {
		if a := r.retries[match].Attempt; a != r.overload {
			r.t.Fatalf("OVERLOADED onRetry attempt %d, want %d", a, r.overload)
		}
		r.overload++
	}
	r.retries = append(r.retries[:match], r.retries[match+1:]...)
}

func (r *tunnelRunner) nextTimer(within time.Duration) (time.Duration, bool) {
	r.clock.mu.Lock()
	defer r.clock.mu.Unlock()
	best, found := time.Duration(0), false
	for _, tm := range r.clock.timers {
		if !tm.stopped && !tm.fired && tm.at <= r.clock.now+within && (!found || tm.at < best) {
			best, found = tm.at, true
		}
	}
	return best - r.clock.now, found
}

// newFakeTunnel is a Tunnel over the fake transport and clock, with the
// primary listener subscribed; mod may adjust the options first.
func newFakeTunnel(t *testing.T, mod func(*TunnelOptions)) (*tunnelRunner, *Tunnel) {
	r := &tunnelRunner{t: t, clock: &fakeClock{}, overload: 1}
	opts := TunnelOptions{
		Integration:  testIntegration,
		Handler:      http.NotFoundHandler(),
		OnRetry:      func(ri RetryInfo) { r.retries = append(r.retries, ri) },
		after:        r.clock.after,
		newTransport: r.newTransport,
		schedule:     func(fn func()) { r.deferred = append(r.deferred, fn) },
	}
	if mod != nil {
		mod(&opts)
	}
	tun, err := NewTunnel(opts)
	if err != nil {
		t.Fatal(err)
	}
	tun.Subscribe(func(s TunnelStatus) { r.snaps = append(r.snaps, s) })
	return r, tun
}

func runTunnelFixture(t *testing.T, f *tunnelFixture) {
	r, tun := newFakeTunnel(t, nil)
	var pending []<-chan struct{}

	var apply func(name string, raw json.RawMessage)
	apply = func(name string, raw json.RawMessage) {
		var p map[string]any
		_ = json.Unmarshal(raw, &p)
		switch name {
		case "configure":
			pending = append(pending, tun.configureAsync(r.mergedConfig(f.Config, p)))
		case "stop":
			pending = append(pending, tun.stopAsync())
		case "dispose":
			timeout := defaultDisposeTimeout
			if ms, ok := p["timeoutMs"].(float64); ok {
				timeout = time.Duration(ms) * time.Millisecond
			}
			pending = append(pending, tun.disposeAsync(timeout))
		case "burst":
			var list []json.RawMessage
			_ = json.Unmarshal(raw, &list)
			for _, ev := range list {
				apply(singleKey(t, ev))
			}
		case "subscribe":
			unsub := tun.Subscribe(func(s TunnelStatus) { r.snaps = append(r.snaps, s) })
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
			r.connEvent(name, p)
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
			t.Errorf("configure/stop/dispose call %d never resolved", i+1)
		}
	}
	if fmt.Sprint(r.snaps) != fmt.Sprint(f.Expect) || len(r.snaps) != len(f.Expect) {
		t.Fatalf("snapshots\n got %s\nwant %s", snapString(r.snaps), snapString(f.Expect))
	}
	for i := range r.snaps {
		if r.snaps[i] != f.Expect[i] {
			t.Fatalf("snapshot %d: got %+v, want %+v", i, r.snaps[i], f.Expect[i])
		}
	}
	if f.ExpectDials != nil && len(r.dials) != *f.ExpectDials {
		t.Fatalf("%d dials, want %d", len(r.dials), *f.ExpectDials)
	}
}

func snapString(ss []TunnelStatus) string {
	var b strings.Builder
	for _, s := range ss {
		fmt.Fprintf(&b, "\n  %+v", s)
	}
	return b.String()
}

// tunnelFixtureSkips are fixtures whose inputs Go's types cannot express.
var tunnelFixtureSkips = map[string]string{
	"07_invalid_config.json": "drives §6.2 checks 4/5 with token/name null; Go's TunnelConfig has string fields, so a null is \"\" " +
		"(an empty token is off, an empty name INVALID_NAME at start). Its kind \"ftp\" case is TestTunnelInvalidKindAtStart",
}

func TestTunnelFixtures(t *testing.T) {
	dir := mcpwarpFixtureDir(t, "tunnel")
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".json") {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)
	if len(names) == 0 {
		t.Fatal("no tunnel fixtures")
	}
	for _, name := range names {
		b, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		var f tunnelFixture
		if err := json.Unmarshal(b, &f); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		t.Run(name, func(t *testing.T) {
			if why, ok := tunnelFixtureSkips[name]; ok {
				t.Skip(why)
			}
			runTunnelFixture(t, &f)
		})
	}
}
