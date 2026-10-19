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

	group      bool        // a group fixture (config.services)
	sentFrames []sentFrame // a group's register/unregister frames not yet matched by a sent event
	groupSnaps []ServiceGroupStatus

	providers       map[string]TokenSource // one source per token value (token_provider fixtures)
	providerResults []string               // results of the next provider calls
	drainRetryAfter time.Duration          // the last drain's retry_after_ms, for its retry event
}

// providerFor is the source for one token value, the same one on every call
// so an unchanged configure stays idempotent (§6.2).
func (r *tunnelRunner) providerFor(token string) TokenSource {
	if p, ok := r.providers[token]; ok {
		return p
	}
	p := TokenSourceFunc(func(context.Context) (string, error) {
		result := "ok"
		if len(r.providerResults) > 0 {
			result, r.providerResults = r.providerResults[0], r.providerResults[1:]
		}
		switch result {
		case "unavailable":
			return "", fmt.Errorf("fixture provider: %w", wsmixer.ErrTokenUnavailable)
		case "error":
			return "", errors.New("fixture provider failed")
		}
		return token, nil
	})
	if r.providers == nil {
		r.providers = map[string]TokenSource{}
	}
	r.providers[token] = p
	return p
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
	refreshed  bool // the one token-refresh redial was spent
}

type fakeDial struct {
	tr                 *fakeTransport
	ordinal            int
	upgraded, welcomed bool
	ended, abandoned   bool
	held, drained      bool
	flushing           bool // delivering flush_after ops after its end
}

func (r *tunnelRunner) newTransport(cfg transportConfig) transport {
	return &fakeTransport{r: r, cfg: cfg}
}

func (tr *fakeTransport) start()                  { tr.attemptDial() }
func (tr *fakeTransport) client() *wsmixer.Client { return nil }

// attemptDial is one dial attempt: the token provider is called first; a
// failed call ends the attempt before the socket factory (no dial counted),
// a marked-temporary one as a failed dial with backoff, any other fatally
// (CLIENT-SDK.md's Provider failure row, as wsmixer.Client decides it).
func (tr *fakeTransport) attemptDial() {
	if _, err := tr.cfg.token(context.Background()); err != nil {
		if errors.Is(err, wsmixer.ErrTokenUnavailable) {
			tr.cfg.events.disconnect(wsmixer.DisconnectReason{Phase: wsmixer.PhaseDial, Message: err.Error(), Cause: err})
			tr.backoff(err.Error())
			return
		}
		tr.endFatal(wsmixer.DisconnectReason{Phase: wsmixer.PhaseDial, Fatal: true, Message: err.Error(), Cause: err})
		return
	}
	tr.dial()
}

// endFatal ends the transport with a fatal disconnect.
func (tr *fakeTransport) endFatal(reason wsmixer.DisconnectReason) {
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
}

// tokenRejected answers a rejected token before welcome (an upgrade 401 or
// a close 4011): with a provider and the refresh unspent, the provider is
// called again and the dial retried at once -- wsmixer.Client reports no
// disconnect for that first rejection, only the token-refresh redial;
// otherwise it is fatal. false: not handled here (a static token's 4011,
// whose verdict the fixture states).
func (tr *fakeTransport) tokenRejected(reason wsmixer.DisconnectReason, httpRefusal bool) bool {
	if tr.cfg.refreshable && !tr.refreshed {
		tr.refreshed = true
		// As wsmixer.Client (CLIENT-SDK.md Rejected token row): the first
		// rejection is a non-fatal disconnect, then the refresh redial.
		reason.Fatal = false
		tr.cfg.events.disconnect(reason)
		tr.cfg.events.retry(wsmixer.ReconnectInfo{Attempt: 1, Delay: 0, Cause: wsmixer.ReconnectTokenRefresh})
		tr.attemptDial()
		return true
	}
	if tr.cfg.refreshable || httpRefusal {
		reason.Fatal = true
		tr.endFatal(reason)
		return true
	}
	return false
}

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
			tr.attemptDial()
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

func (d *fakeDial) Done() <-chan struct{} {
	if d.ended {
		return closedChan()
	}
	return nil
}

func (d *fakeDial) SendApp(ctx context.Context, body any) error {
	if d.ended || d.abandoned || !d.welcomed {
		return errors.New("fake: connection gone")
	}
	b, _ := json.Marshal(body)
	if d.tr.cfg.group {
		d.tr.r.groupFrame(ctx, d, b)
		return nil
	}
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
	ExpectRaw   json.RawMessage   `json:"expect"`
	Expect      []TunnelStatus    `json:"-"`
	ExpectDials *int              `json:"expectDials"`
	// RunnerRequirement names a runner capability the fixture needs.
	RunnerRequirement string `json:"runnerRequirement"`
}

// runnerCapabilities are the tunnel/README.md runner requirements this
// runner meets: its fake client delivers flush_after ops after the
// disconnect notification.
var runnerCapabilities = map[string]bool{"post-disconnect-delivery": true}

// usesTokenProvider: a fixture with a token_provider event passes every
// config's token as a provider (tunnel/README.md).
func (f *tunnelFixture) usesTokenProvider() bool {
	for _, ev := range f.Events {
		if strings.Contains(string(ev), `"token_provider"`) {
			return true
		}
	}
	return false
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
	// An ended connection delivers only the flush_after ops received before its end.
	if (d.ended && !d.flushing) || d.abandoned || !d.welcomed {
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
		reason := wsmixer.DisconnectReason{Phase: wsmixer.PhaseDial, Message: "dial failed"}
		// An observable refused upgrade: the verdict WIRE.md §2.9 gives with
		// the fixtures' static token (tunnel/README.md), as wsmixer.Client
		// reaches it (TestClientStaticToken401Fatal; 403/404 in newDialError).
		if status, ok := p["httpStatus"].(float64); ok {
			reason.HTTPStatus = int(status)
			reason.Message = fmt.Sprintf("upgrade refused: HTTP %d", int(status))
			switch int(status) {
			case http.StatusUnauthorized:
				tr.tokenRejected(reason, true)
				return
			case http.StatusForbidden, http.StatusNotFound:
				reason.Fatal = true
				tr.endFatal(reason)
				return
			}
		}
		tr.cfg.events.disconnect(reason)
		tr.backoff(reason.Message)
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
		id, ok := strField(p, "id") // possibly one the agent does not know (yet)
		if !ok {
			id = d.serverID()
		}
		e := map[string]any{"op": "disable", "id": id}
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
		delay := fakeDrainDelay
		r.drainRetryAfter = 0
		if ms, ok := p["retry_after_ms"].(float64); ok {
			// wsmixer.Client waits the hint itself (no jitter on top).
			delay = time.Duration(ms) * time.Millisecond
			r.drainRetryAfter = delay
		}
		tr.scheduleRedial(wsmixer.ReconnectDrain, delay, 0)
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
		// As in wsmixer.Client, Done (d.ended) closes first; the disconnect
		// report, the flushed deliveries and the Done watcher's connEnd are
		// mutually unordered there. connEnd runs last here, after the flush
		// (defers run in reverse).
		defer tr.cfg.events.connEnd(d)
	}
	defer r.flushAfter(d, p)
	switch {
	case !d.welcomed && !tr.closed && reason.WSCode == 4011 && tr.tokenRejected(reason, false):
		// a provider's rejected token: refreshed (or, the second time, fatal)
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

// flushAfter delivers a disconnect's flush_after ops: received on d before
// it ended, delivered to the layer after the disconnect notification
// (tunnel/README.md, runnerRequirement post-disconnect-delivery).
func (r *tunnelRunner) flushAfter(d *fakeDial, p map[string]any) {
	list, _ := p["flush_after"].([]any)
	for _, item := range list {
		for name, payload := range item.(map[string]any) {
			m, _ := payload.(map[string]any)
			if m == nil {
				m = map[string]any{}
			}
			m["conn"] = float64(d.ordinal)
			d.flushing = true
			r.connEvent(name, m)
			d.flushing = false
		}
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
			want := time.Second // always 1000 in a single-service fixture
			if r.group {
				want = delay
			}
			return ri.Delay == want && strings.HasPrefix(ri.Reason, "register failed: OVERLOADED: ")
		case "drain":
			return ri.Attempt == 0 && ri.Reason == "drain" && ri.Delay <= delay && ri.Delay >= r.drainRetryAfter
		case "token-refresh":
			return ri == RetryInfo{Attempt: 1, Delay: 0, Reason: "token-refresh"}
		case "dial", "lost":
			return ri.Delay <= delay && ri.Reason != "drain" && ri.Reason != "token-refresh" && !strings.HasPrefix(ri.Reason, "register failed")
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
			c := r.mergedConfig(f.Config, p)
			if f.usesTokenProvider() && c.Token != "" {
				c.TokenSource, c.Token = r.providerFor(c.Token), ""
			}
			pending = append(pending, tun.configureAsync(c))
		case "token_provider":
			result, _ := p["result"].(string)
			r.providerResults = append(r.providerResults, result)
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
		_, group := f.Config["services"]
		if !group {
			if err := json.Unmarshal(f.ExpectRaw, &f.Expect); err != nil {
				t.Fatalf("%s: expect: %v", name, err)
			}
		}
		t.Run(name, func(t *testing.T) {
			if why, ok := tunnelFixtureSkips[name]; ok {
				t.Skip(why)
			}
			if req := f.RunnerRequirement; req != "" && !runnerCapabilities[req] {
				t.Skipf("runnerRequirement %q: this runner cannot meet it", req)
			}
			if group {
				runGroupFixture(t, &f)
				return
			}
			runTunnelFixture(t, &f)
		})
	}
}
