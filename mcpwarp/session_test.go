package mcpwarp

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"sync"
	"testing"
	"time"
)

// fakeClock drives the session's timers by hand.
type fakeClock struct {
	mu     sync.Mutex
	now    time.Duration
	timers []*fakeTimer
}

type fakeTimer struct {
	at      time.Duration
	f       func()
	stopped bool
	fired   bool
}

func (c *fakeClock) after(d time.Duration, f func()) func() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := &fakeTimer{at: c.now + d, f: f}
	c.timers = append(c.timers, t)
	return func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		was := !t.stopped && !t.fired
		t.stopped = true
		return was
	}
}

// advance moves the clock by d, firing due timers in order.
func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	end := c.now + d
	c.mu.Unlock()
	for {
		c.mu.Lock()
		sort.SliceStable(c.timers, func(i, j int) bool { return c.timers[i].at < c.timers[j].at })
		var next *fakeTimer
		for _, t := range c.timers {
			if !t.stopped && !t.fired && t.at <= end {
				next = t
				break
			}
		}
		if next == nil {
			c.now = end
			c.mu.Unlock()
			return
		}
		next.fired = true
		c.now = next.at
		c.mu.Unlock()
		next.f()
	}
}

// fakeConn records what the session sends on it.
type fakeConn struct {
	mu   sync.Mutex
	sent []string
	err  error
}

func (c *fakeConn) SendApp(_ context.Context, body any) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err != nil {
		return c.err
	}
	b, _ := json.Marshal(body)
	c.sent = append(c.sent, string(b))
	return nil
}

func (c *fakeConn) take() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := c.sent
	c.sent = nil
	return out
}

type sessionHarness struct {
	t      *testing.T
	s      *session
	clock  *fakeClock
	log    *eventLog
	fatals []*Error
}

func newSessionHarness(t *testing.T) *sessionHarness {
	h := &sessionHarness{t: t, clock: &fakeClock{}, log: &eventLog{}}
	h.s = newSession(sessionDeps{
		name:  "anki",
		kind:  KindHTTP,
		after: h.clock.after,
		emit:  h.log.add,
		fatal: func(e *Error) { h.fatals = append(h.fatals, e) },
	})
	return h
}

var (
	registerAnki   = string(EncodeRegister("anki", KindHTTP))
	unregisterAnki = string(EncodeUnregister("anki"))
)

func (h *sessionHarness) app(c appConn, body string) { h.s.onApp(c, []byte(body)) }

func (h *sessionHarness) wantSent(c *fakeConn, want ...string) {
	h.t.Helper()
	got := c.take()
	if len(got) != len(want) {
		h.t.Fatalf("sent %q, want %q", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			h.t.Fatalf("sent %q, want %q", got, want)
		}
	}
}

func (h *sessionHarness) retries(kind string) []retryInfo {
	var out []retryInfo
	for _, ev := range h.log.of(evRetry) {
		if ev.retryKind == kind {
			out = append(out, ev.retry)
		}
	}
	return out
}

const transientAnki = `{"mcpwarp":{"v":1,"op":"registered","errors":[{"name":"anki","code":"UNAVAILABLE","message":"registry unavailable"}]}}`
const registeredAnki = `{"mcpwarp":{"v":1,"op":"registered","services":[{"name":"anki","id":"srv_anki","url":"https://anki-tester.mcpwarp.test/mcp"}]}}`

// §3.2 Transient retry: min(1000 × 2^n, 30000) ms, n carried across
// reconnects, reset only by a successful registered.
func TestSessionTransientBackoffSchedule(t *testing.T) {
	h := newSessionHarness(t)
	c1 := &fakeConn{}
	h.s.onWelcome(c1)
	h.wantSent(c1, registerAnki)

	wantDelays := []time.Duration{1, 2, 4, 8, 16, 30, 30}
	for i, d := range wantDelays {
		h.app(c1, transientAnki)
		r := h.retries("register")
		if got := r[len(r)-1]; got.Attempt != i+1 || got.Delay != d*time.Second || got.Reason != "register failed: UNAVAILABLE: registry unavailable" {
			t.Fatalf("retry %d = %+v, want attempt %d delay %v", i, got, i+1, d*time.Second)
		}
		h.clock.advance(d*time.Second - time.Millisecond)
		h.wantSent(c1)
		h.clock.advance(time.Millisecond)
		h.wantSent(c1, registerAnki)
	}
	if n := len(h.log.of(evError)); n != len(wantDelays) {
		t.Fatalf("%d error events, want one per transient failure (%d)", n, len(wantDelays))
	}
	if len(h.fatals) != 0 {
		t.Fatalf("transient errors went fatal: %v", h.fatals)
	}
}

func TestSessionBackoffCarriesAcrossReconnectAndResetsOnSuccess(t *testing.T) {
	h := newSessionHarness(t)
	c1 := &fakeConn{}
	h.s.onWelcome(c1)
	h.wantSent(c1, registerAnki)
	h.app(c1, transientAnki) // 1 s
	h.clock.advance(time.Second)
	h.app(c1, transientAnki) // 2 s, pending

	// A disconnect cancels the pending retry; the next welcome registers anyway.
	h.s.onConnEnd(c1)
	if st := h.s.currentState(); st != stateConnecting {
		t.Fatalf("state %s after the connection ended, want connecting", st)
	}
	h.clock.advance(time.Minute)
	h.wantSent(c1, registerAnki) // only the 1 s retry's re-send

	c2 := &fakeConn{}
	h.s.onWelcome(c2)
	h.wantSent(c2, registerAnki)
	h.app(c2, transientAnki)
	if r := h.retries("register"); r[len(r)-1].Delay != 4*time.Second || r[len(r)-1].Attempt != 3 {
		t.Fatalf("after reconnect retry = %+v, want attempt 3 delay 4s (n carries across reconnects)", r[len(r)-1])
	}
	h.clock.advance(4 * time.Second)
	h.wantSent(c2, registerAnki)

	h.app(c2, registeredAnki)
	if st := h.s.currentState(); st != stateRegistered {
		t.Fatalf("state %s, want registered", st)
	}
	h.app(c2, transientAnki)
	if r := h.retries("register"); r[len(r)-1].Delay != time.Second || r[len(r)-1].Attempt != 1 {
		t.Fatalf("after success retry = %+v, want attempt 1 delay 1s", r[len(r)-1])
	}
}

// §2.6 OVERLOADED: with a register outstanding, re-send it once after
// 1000 ms if still connected; onRetry attempt counts consecutive re-sends
// and resets on success.
func TestSessionOverloadedResend(t *testing.T) {
	h := newSessionHarness(t)
	c := &fakeConn{}
	h.s.onWelcome(c)
	h.wantSent(c, registerAnki)

	overloaded := `{"mcpwarp":{"v":1,"op":"error","code":"OVERLOADED","message":"too many app messages in flight"}}`
	h.app(c, overloaded)
	h.app(c, overloaded) // a second one before the pause ends schedules nothing more
	h.clock.advance(999 * time.Millisecond)
	h.wantSent(c)
	h.clock.advance(time.Millisecond)
	h.wantSent(c, registerAnki)
	h.clock.advance(10 * time.Second)
	h.wantSent(c)

	h.app(c, overloaded)
	h.clock.advance(time.Second)
	h.wantSent(c, registerAnki)

	r := h.retries("overloaded")
	if len(r) != 2 || r[0].Attempt != 1 || r[1].Attempt != 2 || r[0].Delay != time.Second ||
		r[0].Reason != "register failed: OVERLOADED: too many app messages in flight" {
		t.Fatalf("overloaded retries = %+v, want attempts 1, 2 with delay 1s", r)
	}
	if n := len(h.log.of(evError)); n != 0 {
		t.Fatalf("OVERLOADED raised %d error events, want none", n)
	}

	h.app(c, registeredAnki)
	h.app(c, overloaded) // nothing outstanding: nothing to re-send
	h.clock.advance(time.Minute)
	h.wantSent(c)
	if r := h.retries("overloaded"); len(r) != 2 {
		t.Fatalf("an OVERLOADED with nothing outstanding fired onRetry: %+v", r)
	}

	// After a success the attempt counter starts again at 1.
	h.s.onConnEnd(c)
	c2 := &fakeConn{}
	h.s.onWelcome(c2)
	h.wantSent(c2, registerAnki)
	h.app(c2, overloaded)
	if r := h.retries("overloaded"); r[len(r)-1].Attempt != 1 {
		t.Fatalf("attempt after success = %d, want 1", r[len(r)-1].Attempt)
	}
	// A disconnect within the pause cancels the re-send.
	h.s.onConnEnd(c2)
	h.clock.advance(time.Minute)
	h.wantSent(c2)
}

func TestSessionOverloadedResendsUnregisterWhileClosing(t *testing.T) {
	h := newSessionHarness(t)
	c := &fakeConn{}
	h.s.onWelcome(c)
	h.app(c, registeredAnki)
	h.wantSent(c, registerAnki)

	done := make(chan struct{})
	go func() {
		defer close(done)
		h.s.beginClose(context.Background(), unregisterBudget)
	}()
	waitFor(t, func() bool { return len(peek(c)) == 1 })
	h.wantSent(c, unregisterAnki)
	h.app(c, `{"mcpwarp":{"v":1,"op":"error","code":"OVERLOADED"}}`)
	h.clock.advance(time.Second)
	h.wantSent(c, unregisterAnki)
	if r := h.retries("overloaded"); len(r) != 0 {
		t.Fatalf("an unregister re-send fired onRetry: %+v", r)
	}
	h.app(c, `{"mcpwarp":{"v":1,"op":"unregistered","services":[{"name":"anki","id":"srv_anki"}]}}`)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("beginClose did not end on unregistered")
	}
}

// A registered inside the OVERLOADED pause answers the register: no second one.
func TestSessionOverloadedResendCancelledBySuccess(t *testing.T) {
	h := newSessionHarness(t)
	c := &fakeConn{}
	h.s.onWelcome(c)
	h.wantSent(c, registerAnki)
	h.app(c, `{"mcpwarp":{"v":1,"op":"error","code":"OVERLOADED"}}`)
	h.clock.advance(500 * time.Millisecond)
	h.app(c, registeredAnki)
	h.clock.advance(time.Minute)
	h.wantSent(c)
}

// disable stops a pending transient retry and a pending OVERLOADED re-send.
func TestSessionTimersCancelledByDisable(t *testing.T) {
	for _, disable := range []string{
		`{"mcpwarp":{"v":1,"op":"disable","id":"srv_anki"}}`,
		`{"mcpwarp":{"v":1,"op":"registered","errors":[{"name":"anki","code":"SERVER_DISABLED","id":"srv_anki"}]}}`,
	} {
		h := newSessionHarness(t)
		c := &fakeConn{}
		h.s.onWelcome(c)
		h.wantSent(c, registerAnki)
		h.app(c, transientAnki) // 1 s retry pending
		h.app(c, disable)
		h.clock.advance(time.Minute)
		h.wantSent(c)

		h2 := newSessionHarness(t)
		c2 := &fakeConn{}
		h2.s.onWelcome(c2)
		h2.wantSent(c2, registerAnki)
		h2.app(c2, `{"mcpwarp":{"v":1,"op":"error","code":"OVERLOADED"}}`)
		h2.app(c2, disable)
		h2.clock.advance(time.Minute)
		h2.wantSent(c2)
		if st := h2.s.currentState(); st != stateDisabled {
			t.Fatalf("state %s, want disabled", st)
		}
	}
}

// blockingConn's SendApp blocks an unregister until its ctx ends, like a full
// control queue; anything else goes through.
type blockingConn struct{}

func (blockingConn) SendApp(ctx context.Context, body any) error {
	if b, _ := json.Marshal(body); string(b) != unregisterAnki {
		return nil
	}
	<-ctx.Done()
	return ctx.Err()
}

// §3.2: a blocked unregister send is bounded by close()'s ctx and by the
// 2000 ms budget.
func TestSessionBeginCloseBoundedWhileSendBlocks(t *testing.T) {
	h := newSessionHarness(t)
	h.s.onWelcome(blockingConn{})
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	h.s.beginClose(ctx, unregisterBudget)
	if el := time.Since(start); el > time.Second {
		t.Fatalf("beginClose took %v with a 100ms ctx", el)
	}

	h2 := newSessionHarness(t)
	h2.s.onWelcome(blockingConn{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		h2.s.beginClose(context.Background(), unregisterBudget)
	}()
	waitFor(t, func() bool { // the budget timer is armed
		h2.clock.mu.Lock()
		defer h2.clock.mu.Unlock()
		return len(h2.clock.timers) == 1
	})
	h2.clock.advance(unregisterBudget)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("beginClose outlived the 2000 ms budget while the send was blocked")
	}
}

func peek(c *fakeConn) []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.sent...)
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("timed out")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestSessionUnregisterBudgetAndFailedSend(t *testing.T) {
	h := newSessionHarness(t)
	c := &fakeConn{}
	h.s.onWelcome(c)
	h.wantSent(c, registerAnki)
	done := make(chan struct{})
	go func() {
		defer close(done)
		h.s.beginClose(context.Background(), unregisterBudget)
	}()
	waitFor(t, func() bool { return len(peek(c)) == 1 })
	h.clock.advance(1999 * time.Millisecond)
	select {
	case <-done:
		t.Fatal("beginClose ended before the 2000 ms budget")
	case <-time.After(20 * time.Millisecond):
	}
	h.clock.advance(time.Millisecond)
	<-done

	h2 := newSessionHarness(t)
	c2 := &fakeConn{}
	h2.s.onWelcome(c2)
	c2.err = errors.New("connection gone")
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		h2.s.beginClose(context.Background(), unregisterBudget)
	}()
	select {
	case <-finished:
	case <-time.After(2 * time.Second):
		t.Fatal("a failed unregister send did not end the wait at once")
	}
}

// §3.3 Sticky: after a matching conflict nothing registers again, and
// registered/disable/enable/conflict change nothing; close() sends no
// unregister.
func TestSessionConflictIsSticky(t *testing.T) {
	h := newSessionHarness(t)
	c := &fakeConn{}
	h.s.onWelcome(c)
	h.app(c, registeredAnki)
	h.wantSent(c, registerAnki)

	h.app(c, `{"mcpwarp":{"v":1,"op":"conflict","id":"srv_anki","name":"other"}}`) // matches by server id
	conflicts := h.log.of(evConflict)
	if len(conflicts) != 1 || conflicts[0].conflict != (Conflict{ServerID: "srv_anki", Name: "other", Reason: "taken_over"}) {
		t.Fatalf("conflict events = %+v", conflicts)
	}
	for _, body := range []string{
		registeredAnki,
		`{"mcpwarp":{"v":1,"op":"disable","id":"srv_anki"}}`,
		`{"mcpwarp":{"v":1,"op":"enable","id":"srv_anki","name":"anki"}}`,
		`{"mcpwarp":{"v":1,"op":"conflict","id":"srv_anki","name":"anki"}}`,
	} {
		h.app(c, body)
	}
	if h.s.onReplaced() {
		t.Fatal("drain{replaced} after a conflict changed something")
	}
	h.s.onConnEnd(c)
	c2 := &fakeConn{}
	h.s.onWelcome(c2)
	h.wantSent(c2)
	if st := h.s.currentState(); st != stateConflict {
		t.Fatalf("state %s, want conflict", st)
	}
	if n := len(h.log.of(evConflict)); n != 1 {
		t.Fatalf("%d conflict events, want 1", n)
	}
	h.s.beginClose(context.Background(), unregisterBudget)
	h.wantSent(c2)
}

func TestSessionConflictMatching(t *testing.T) {
	h := newSessionHarness(t)
	c := &fakeConn{}
	h.s.onWelcome(c)
	// No server id known yet: only the name counts.
	h.app(c, `{"mcpwarp":{"v":1,"op":"conflict","id":"srv_x","name":"other"}}`)
	if st := h.s.currentState(); st != stateRegistering {
		t.Fatalf("a non-matching conflict changed state to %s", st)
	}
	h.app(c, `{"mcpwarp":{"v":1,"op":"conflict","id":"srv_x","name":"anki"}}`)
	if st := h.s.currentState(); st != stateConflict {
		t.Fatalf("state %s, want conflict", st)
	}
}

// §3.3 Drain replaced: a take-over with reason "replaced"; ServerID is ""
// (the omitted serverId) when none is known.
func TestSessionReplaced(t *testing.T) {
	h := newSessionHarness(t)
	c := &fakeConn{}
	h.s.onWelcome(c)
	if !h.s.onReplaced() {
		t.Fatal("onReplaced = false")
	}
	if ev := h.log.of(evConflict); len(ev) != 1 || ev[0].conflict != (Conflict{Name: "anki", Reason: "replaced"}) {
		t.Fatalf("conflict events = %+v", ev)
	}
	h.app(c, `{"mcpwarp":{"v":1,"op":"conflict","id":"srv_anki","name":"anki"}}`)
	if n := len(h.log.of(evConflict)); n != 1 {
		t.Fatalf("a conflict after drain{replaced} emitted again (%d)", n)
	}

	h2 := newSessionHarness(t)
	c2 := &fakeConn{}
	h2.s.onWelcome(c2)
	h2.app(c2, registeredAnki)
	h2.s.onReplaced()
	if ev := h2.log.of(evConflict); len(ev) != 1 || ev[0].conflict.ServerID != "srv_anki" {
		t.Fatalf("conflict events = %+v, want serverId srv_anki", ev)
	}

	h3 := newSessionHarness(t)
	h3.s.onWelcome(&fakeConn{})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go h3.s.beginClose(ctx, unregisterBudget)
	waitFor(t, func() bool { return h3.s.currentState() == stateClosing })
	if h3.s.onReplaced() {
		t.Fatal("drain{replaced} while closing changed something")
	}
}

// §2.5: a refusal before the first registration is fatal, after it a
// non-fatal error; SERVER_DISABLED adopts the id.
func TestSessionRefusals(t *testing.T) {
	h := newSessionHarness(t)
	c := &fakeConn{}
	h.s.onWelcome(c)
	h.app(c, `{"mcpwarp":{"v":1,"op":"registered","errors":[{"name":"anki","code":"QUOTA_EXCEEDED","message":"quota exceeded"}]}}`)
	if len(h.fatals) != 1 || h.fatals[0].Code != "QUOTA_EXCEEDED" || !h.fatals[0].Fatal || h.fatals[0].Message != "quota exceeded" {
		t.Fatalf("fatals = %+v", h.fatals)
	}

	h2 := newSessionHarness(t)
	c2 := &fakeConn{}
	h2.s.onWelcome(c2)
	h2.app(c2, registeredAnki)
	h2.app(c2, `{"mcpwarp":{"v":1,"op":"registered","errors":[{"name":"anki","code":"RATE_LIMITED"}]}}`)
	if len(h2.fatals) != 0 {
		t.Fatalf("a refusal after a registration went fatal: %+v", h2.fatals)
	}
	if ev := h2.log.of(evError); len(ev) != 1 || ev[0].origin != originRegisterRefusal {
		t.Fatalf("error events = %+v", ev)
	}

	h3 := newSessionHarness(t)
	c3 := &fakeConn{}
	h3.s.onWelcome(c3)
	h3.app(c3, `{"mcpwarp":{"v":1,"op":"registered","services":[],"errors":[{"name":"anki","code":"SERVER_DISABLED","message":"server disabled","id":"srv_anki"}]}}`)
	if _, id, ok, _ := h3.s.snapshot(); !ok || id != "srv_anki" || h3.s.currentState() != stateDisabled {
		t.Fatalf("after SERVER_DISABLED: state %s id %q", h3.s.currentState(), id)
	}
	if ev := h3.log.of(evDisabled); len(ev) != 1 || ev[0].reason != "disabled" {
		t.Fatalf("disabled events = %+v", ev)
	}
	// enable re-registers.
	h3.app(c3, `{"mcpwarp":{"v":1,"op":"enable","id":"srv_anki","name":"anki"}}`)
	h3.wantSent(c3, registerAnki, registerAnki)

	h4 := newSessionHarness(t)
	c4 := &fakeConn{}
	h4.s.onWelcome(c4)
	h4.app(c4, `{"mcpwarp":{"v":1,"op":"error","code":"UNSUPPORTED_VERSION","message":"unsupported version 2"}}`)
	if len(h4.fatals) != 1 || h4.fatals[0].Code != "UNSUPPORTED_VERSION" {
		t.Fatalf("fatals = %+v", h4.fatals)
	}
	h4.app(c4, `{"mcpwarp":{"v":1,"op":"registered","services":[{"name":"anki","id":"x"}]}}`)
	if ev := h4.log.of(evError); len(ev) != 1 || ev[0].origin != originMalformed {
		t.Fatalf("a malformed envelope gave error events %+v", ev)
	}
}

// §2.7: once a later connection received welcome, the earlier one's
// messages and its end change nothing.
func TestSessionSupersededConnection(t *testing.T) {
	h := newSessionHarness(t)
	old, repl := &fakeConn{}, &fakeConn{}
	h.s.onWelcome(old)
	h.app(old, registeredAnki)
	h.s.onWelcome(repl)
	h.wantSent(repl, registerAnki)

	h.app(old, `{"mcpwarp":{"v":1,"op":"disable","id":"srv_anki"}}`)
	h.app(old, `{"mcpwarp":{"v":1,"op":"conflict","id":"srv_anki","name":"anki"}}`)
	h.app(old, `{"mcpwarp":{"v":1,"op":"enable","id":"srv_anki","name":"anki"}}`)
	h.s.onConnEnd(old)
	if st := h.s.currentState(); st != stateRegistering {
		t.Fatalf("state %s, want registering (the superseded connection changed it)", st)
	}
	h.wantSent(repl)
	if n := len(h.log.of(evDisabled)) + len(h.log.of(evConflict)) + len(h.log.of(evEnabled)); n != 0 {
		t.Fatalf("%d events from the superseded connection", n)
	}
}
