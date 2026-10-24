package mcpwarp

import (
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"
)

type groupHarness struct {
	t      *testing.T
	s      *groupSession
	clock  *fakeClock
	log    *eventLog
	fatals []*Error
}

func newGroupHarness(t *testing.T, names ...string) *groupHarness {
	h := &groupHarness{t: t, clock: &fakeClock{}, log: &eventLog{}}
	var inits []groupServiceInit
	for _, n := range names {
		inits = append(inits, groupServiceInit{name: n, kind: KindHTTP, app: &app{}})
	}
	h.s = newGroupSession(groupDeps{
		after: h.clock.after,
		emit:  h.log.add,
		fatal: func(e *Error) { h.fatals = append(h.fatals, e) },
	}, inits)
	return h
}

func (h *groupHarness) app(c appConn, body string) { h.s.onApp(c, []byte(body)) }

func registerOf(names ...string) string {
	var es []RegisterEntry
	for _, n := range names {
		es = append(es, RegisterEntry{Name: n, Kind: KindHTTP})
	}
	return string(EncodeRegisterList(es))
}

func registeredOK(names ...string) string {
	var parts []string
	for _, n := range names {
		parts = append(parts, `{"name":"`+n+`","id":"srv_`+n+`","url":"https://`+n+`-tester.mcpwarp.test/mcp"}`)
	}
	return `{"mcpwarp":{"v":1,"op":"registered","services":[` + strings.Join(parts, ",") + `]}}`
}

const overloadedOp = `{"mcpwarp":{"v":1,"op":"error","code":"OVERLOADED","message":"busy"}}`

func (h *groupHarness) wantSent(c *fakeConn, want ...string) {
	h.t.Helper()
	got := c.take()
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		h.t.Fatalf("sent %q, want %q", got, want)
	}
}

func (h *groupHarness) state(name string) ServiceState {
	return h.s.snapshotView()[name].State
}

func (h *groupHarness) retries(kind string) []retryInfo {
	var out []retryInfo
	for _, ev := range h.log.of(evRetry) {
		if ev.retryKind == kind {
			out = append(out, ev.retry)
		}
	}
	return out
}

// v0.7.1 ruling 1: the OVERLOADED backoff counts consecutive OVERLOADED
// only; an all-transient reply neither adds to it nor shares its counter.
func TestGroupOverloadedCounterSeparate(t *testing.T) {
	h := newGroupHarness(t, "alpha")
	c := &fakeConn{}
	h.s.onWelcome(c)
	h.wantSent(c, registerOf("alpha"))
	h.app(c, overloadedOp)
	h.clock.advance(time.Second)
	h.wantSent(c, registerOf("alpha"))
	h.app(c, overloadedOp)
	h.clock.advance(2 * time.Second)
	h.wantSent(c, registerOf("alpha"))
	// An all-transient reply resets the OVERLOADED streak and starts its own at 1 s.
	h.app(c, `{"mcpwarp":{"v":1,"op":"registered","errors":[{"name":"alpha","code":"INTERNAL","message":"x"}]}}`)
	h.clock.advance(time.Second)
	h.wantSent(c, registerOf("alpha"))
	h.app(c, overloadedOp)
	if r := h.retries("overloaded"); len(r) != 3 || r[0].Delay != time.Second || r[1].Delay != 2*time.Second || r[2].Delay != time.Second || r[2].Attempt != 1 {
		t.Fatalf("overloaded retries %+v, want 1 s, 2 s, then 1 s (attempt 1) after the transient reply", r)
	}
	if r := h.retries("register"); len(r) != 1 || r[0].Delay != time.Second || r[0].Attempt != 1 {
		t.Fatalf("register retries %+v, want one at 1 s, attempt 1", r)
	}
}

// Ruling 1: any non-OVERLOADED reply resets the OVERLOADED streak, a stray
// registered that answers no register of ours included.
func TestGroupStrayRegisteredResetsOverloaded(t *testing.T) {
	h := newGroupHarness(t, "alpha")
	c := &fakeConn{}
	h.s.onWelcome(c)
	h.app(c, registeredOK("alpha"))
	c.take()
	h.s.localDisable("alpha")
	h.wantSent(c, string(EncodeUnregisterList([]string{"alpha"})))
	h.app(c, overloadedOp)
	h.clock.advance(time.Second)
	h.wantSent(c, string(EncodeUnregisterList([]string{"alpha"})))
	h.app(c, registeredOK("other")) // stray: the outstanding frame is the unregister
	h.app(c, overloadedOp)
	if r := h.retries("overloaded"); len(r) != 2 || r[1].Delay != time.Second || r[1].Attempt != 1 {
		t.Fatalf("overloaded retries %+v, want the second back at 1 s, attempt 1", r)
	}
}

// v0.7.1 ruling 5: a reply that settles the outstanding frame cancels its
// pending re-send, so no third register follows a success.
func TestGroupSettlingCancelsResend(t *testing.T) {
	h := newGroupHarness(t, "alpha")
	c := &fakeConn{}
	h.s.onWelcome(c)
	h.wantSent(c, registerOf("alpha"))
	h.app(c, overloadedOp)
	h.app(c, registeredOK("alpha")) // the edge answered after all
	h.clock.advance(time.Minute)
	h.wantSent(c)
	if st := h.state("alpha"); st != ServiceActive {
		t.Fatalf("alpha %s, want active", st)
	}
}

// One frame in flight (§2.6): a subset add and a dashboard enable during an
// outstanding register queue behind it (ruling 3); during an OVERLOADED
// pause they fold into the one re-send (ruling 6a).
func TestGroupQueueAndFoldBehindInFlight(t *testing.T) {
	h := newGroupHarness(t, "alpha", "beta")
	c := &fakeConn{}
	h.s.onWelcome(c)
	h.wantSent(c, registerOf("alpha", "beta"))
	h.app(c, `{"mcpwarp":{"v":1,"op":"registered","services":[{"name":"alpha","id":"srv_alpha","url":"https://alpha-t.x/mcp"}],"errors":[{"name":"beta","code":"SERVER_DISABLED","id":"srv_beta"}]}}`)

	h.s.applyServices([]groupServiceInit{{name: "alpha", kind: KindHTTP}, {name: "beta", kind: KindHTTP}, {name: "gamma", kind: KindHTTP}})
	h.wantSent(c, registerOf("gamma"))
	h.app(c, `{"mcpwarp":{"v":1,"op":"enable","id":"srv_beta","name":"beta"}}`)
	h.wantSent(c) // queued behind gamma's register
	h.app(c, registeredOK("gamma"))
	h.wantSent(c, registerOf("beta"))
	h.app(c, registeredOK("beta"))

	// During an OVERLOADED pause a new name folds into the re-send.
	h.s.applyServices([]groupServiceInit{{name: "alpha", kind: KindHTTP}, {name: "beta", kind: KindHTTP}, {name: "gamma", kind: KindHTTP}, {name: "delta", kind: KindHTTP}})
	h.wantSent(c, registerOf("delta"))
	h.app(c, overloadedOp)
	h.s.applyServices([]groupServiceInit{{name: "alpha", kind: KindHTTP}, {name: "beta", kind: KindHTTP}, {name: "gamma", kind: KindHTTP}, {name: "delta", kind: KindHTTP}, {name: "eps", kind: KindHTTP}})
	h.wantSent(c)
	h.clock.advance(time.Second)
	h.wantSent(c, registerOf("delta", "eps"))
	h.app(c, registeredOK("delta", "eps"))
	h.clock.advance(time.Minute)
	h.wantSent(c)
	for _, n := range []string{"alpha", "beta", "gamma", "delta", "eps"} {
		if st := h.state(n); st != ServiceActive {
			t.Fatalf("%s %s, want active", n, st)
		}
	}
}

// close() may unregister while a register is outstanding (ruling 3); the
// wait ends on the reply to the unregister's own frame, a reply to the
// earlier register continues it (ruling 7, spec ruling of 2026-10-19).
func TestGroupCloseWhileRegisterOutstanding(t *testing.T) {
	h := newGroupHarness(t, "alpha")
	c := &fakeConn{}
	h.s.onWelcome(c)
	h.wantSent(c, registerOf("alpha"))
	done := make(chan struct{})
	h.s.beginCloseAsync(context.Background(), unregisterBudget, func() { close(done) })
	h.wantSent(c, string(EncodeUnregisterList([]string{"alpha"})))
	select {
	case <-done:
		t.Fatal("the close did not wait for a reply")
	default:
	}
	h.app(c, registeredOK("alpha")) // the register's reply comes first: the wait goes on
	h.wantSent(c)
	select {
	case <-done:
		t.Fatal("the reply to the earlier register ended the close's wait")
	default:
	}
	h.app(c, `{"mcpwarp":{"v":1,"op":"unregistered","services":[]}}`)
	select {
	case <-done:
	default:
		t.Fatal("the unregister's own reply did not end the close's wait")
	}

	// A register dropped by OVERLOADED during the close is abandoned: the
	// OVERLOADED answers it, nothing is re-sent, the close waits on.
	h3 := newGroupHarness(t, "alpha")
	c3 := &fakeConn{}
	h3.s.onWelcome(c3)
	h3.wantSent(c3, registerOf("alpha"))
	done3 := make(chan struct{})
	h3.s.beginCloseAsync(context.Background(), unregisterBudget, func() { close(done3) })
	h3.wantSent(c3, string(EncodeUnregisterList([]string{"alpha"})))
	h3.app(c3, overloadedOp)
	h3.clock.advance(time.Second) // past the re-send backoff, within the budget
	h3.wantSent(c3)
	select {
	case <-done3:
		t.Fatal("the OVERLOADED for the earlier register ended the close's wait")
	default:
	}
	h3.app(c3, `{"mcpwarp":{"v":1,"op":"unregistered","services":[]}}`)
	select {
	case <-done3:
	default:
		t.Fatal("the unregister's own reply did not end the close's wait")
	}

	// A whole-frame error op ends it too; OVERLOADED re-sends instead.
	h2 := newGroupHarness(t, "alpha")
	c2 := &fakeConn{}
	h2.s.onWelcome(c2)
	h2.app(c2, registeredOK("alpha"))
	c2.take()
	done2 := make(chan struct{})
	h2.s.beginCloseAsync(context.Background(), unregisterBudget, func() { close(done2) })
	h2.wantSent(c2, string(EncodeUnregisterList([]string{"alpha"})))
	h2.app(c2, overloadedOp)
	h2.clock.advance(time.Second)
	h2.wantSent(c2, string(EncodeUnregisterList([]string{"alpha"})))
	h2.app(c2, `{"mcpwarp":{"v":1,"op":"error","code":"BAD_REQUEST","message":"x"}}`)
	select {
	case <-done2:
	default:
		t.Fatal("a whole-frame error op did not end the close's wait")
	}
}

// §6A.8: a close unregisters in frames of at most 100, one in flight.
func TestGroupCloseFramesOfAHundred(t *testing.T) {
	var names []string
	for i := 0; i < 150; i++ {
		names = append(names, "s"+strings.Repeat("x", i%3)+string(rune('a'+i%26))+string(rune('a'+i/26)))
	}
	h := newGroupHarness(t, names...)
	c := &fakeConn{}
	h.s.onWelcome(c)
	h.wantSent(c, registerOf(names[:100]...))
	h.app(c, registeredOK(names[:100]...))
	h.wantSent(c, registerOf(names[100:]...))
	h.app(c, registeredOK(names[100:]...))
	done := make(chan struct{})
	h.s.beginCloseAsync(context.Background(), unregisterBudget, func() { close(done) })
	h.wantSent(c, string(EncodeUnregisterList(names[:100])))
	h.app(c, `{"mcpwarp":{"v":1,"op":"unregistered","services":[]}}`)
	h.wantSent(c, string(EncodeUnregisterList(names[100:])))
	h.app(c, `{"mcpwarp":{"v":1,"op":"unregistered","services":[]}}`)
	select {
	case <-done:
	default:
		t.Fatal("the close did not end after its last frame was answered")
	}
}

// A reply to the register outstanding when the close began answers that
// register, not the first close frame: with two close frames the second
// goes out only after the first's own answer (one in flight).
func TestGroupCloseFramesMatchTheirReplies(t *testing.T) {
	var names []string
	for i := 0; i < 150; i++ {
		names = append(names, "s"+string(rune('a'+i%26))+string(rune('a'+i/26)))
	}
	h := newGroupHarness(t, names...)
	c := &fakeConn{}
	h.s.onWelcome(c)
	h.wantSent(c, registerOf(names[:100]...))
	h.app(c, registeredOK(names[:100]...))
	h.wantSent(c, registerOf(names[100:]...)) // outstanding when the close begins
	done := make(chan struct{})
	h.s.beginCloseAsync(context.Background(), unregisterBudget, func() { close(done) })
	h.wantSent(c, string(EncodeUnregisterList(names[:100])))
	h.app(c, registeredOK(names[100:]...)) // answers the register
	h.wantSent(c)
	h.app(c, `{"mcpwarp":{"v":1,"op":"unregistered","services":[]}}`)
	h.wantSent(c, string(EncodeUnregisterList(names[100:])))
	select {
	case <-done:
		t.Fatal("the close ended before its last frame was answered")
	default:
	}
	h.app(c, `{"mcpwarp":{"v":1,"op":"unregistered","services":[]}}`)
	select {
	case <-done:
	default:
		t.Fatal("the close did not end after its last frame was answered")
	}
}

// §2.3 Empty reply / ruling 2: a registered listing none of the frame's
// names frees the slot and re-sends those names after the transient backoff.
func TestGroupEmptyReplyResends(t *testing.T) {
	h := newGroupHarness(t, "alpha")
	c := &fakeConn{}
	h.s.onWelcome(c)
	h.wantSent(c, registerOf("alpha"))
	h.app(c, `{"mcpwarp":{"v":1,"op":"registered"}}`)
	if r := h.retries("register"); len(r) != 1 || r[0].Reason != "register failed: INTERNAL: unlisted in reply" {
		t.Fatalf("register retries %+v", r)
	}
	h.clock.advance(time.Second)
	h.wantSent(c, registerOf("alpha"))
}

// §4.8 step 2 and the trust gate.
func TestServiceFromHost(t *testing.T) {
	for host, want := range map[string]string{
		"my-notes-tester.tunnel.mcpwarp.test":      "my-notes",
		"My-Notes-Tester.tunnel.mcpwarp.test:8443": "my-notes",
		"notes-ab.x":    "", // username too short
		"notes.x":       "", // no '-'
		"-tester.x":     "", // empty name
		"notes-tes-t.x": "", // username "t"
		"notes-tester":  "notes",
	} {
		got, ok := serviceFromHost(host)
		if (want == "") == ok || got != want {
			t.Errorf("serviceFromHost(%q) = %q, %v; want %q", host, got, ok, want)
		}
	}
	h := newGroupHarness(t, "a-svc", "b-svc")
	c := &fakeConn{}
	h.s.onWelcome(c)
	h.app(c, registeredOK("a-svc", "b-svc"))
	head := &requestHead{host: "a-svc-alice.x", headers: []headerField{{name: "x-mcpwarp-server-id", value: "srv_b-svc"}}}
	if _, name := h.s.route(head); name != "a-svc" {
		t.Fatalf("routed to %q with the trust gate off, want a-svc by Host", name)
	}
	h.s.deps.trustHeader = true
	if _, name := h.s.route(head); name != "b-svc" {
		t.Fatalf("routed to %q with the trust gate on, want b-svc by id", name)
	}
	head.headers[0].value = "srv_nope"
	if r, name := h.s.route(head); name != "" || r.unavailable != unavailableOther {
		t.Fatalf("an unknown id routed to %q (%+v), want 503 service unavailable, no Host fallback", name, r)
	}
}

// A dashboard disable (disable op or SERVER_DISABLED) shows Code DISABLED;
// a local disable shows Code "": a host tells them apart by Code.
func TestGroupDisabledCauseByCode(t *testing.T) {
	h := newGroupHarness(t, "alpha", "beta", "gamma")
	c := &fakeConn{}
	h.s.onWelcome(c)
	h.app(c, `{"mcpwarp":{"v":1,"op":"registered","services":[{"name":"alpha","id":"ia","url":"https://u/mcp"},{"name":"gamma","id":"ig","url":"https://u/mcp"}],"errors":[{"name":"beta","code":"SERVER_DISABLED","id":"ib"}]}}`)
	h.app(c, `{"mcpwarp":{"v":1,"op":"disable","id":"ia"}}`)
	h.s.localDisable("gamma")
	view := h.s.snapshotView()
	for name, want := range map[string]ServiceStatus{
		"alpha": {State: ServiceDisabled, ID: "ia", Code: "DISABLED", LastError: lastDisabled},
		"beta":  {State: ServiceDisabled, ID: "ib", Code: "DISABLED", LastError: lastDisabled},
		"gamma": {State: ServiceDisabled, ID: "ig"},
	} {
		if view[name] != want {
			t.Errorf("%s: %+v, want %+v", name, view[name], want)
		}
	}
}

// Only Enable(name) clears a take-over (§6A.3): a dashboard enable and a
// reconnect leave the name taken over and unregistered; Enable then
// registers that name alone.
func TestGroupOnlyEnableClearsTakeOver(t *testing.T) {
	h := newGroupHarness(t, "alpha", "beta")
	c := &fakeConn{}
	h.s.onWelcome(c)
	h.app(c, registeredOK("alpha", "beta"))
	c.take()
	h.app(c, `{"mcpwarp":{"v":1,"op":"conflict","id":"srv_beta","name":"beta","reason":"taken_over"}}`)
	h.app(c, `{"mcpwarp":{"v":1,"op":"enable","id":"srv_beta","name":"beta"}}`)
	h.wantSent(c)
	if st := h.state("beta"); st != ServiceTakenOver {
		t.Fatalf("beta %s after a dashboard enable, want taken_over", st)
	}
	h.s.onConnEnd(c)
	c2 := &fakeConn{}
	h.s.onWelcome(c2)
	h.wantSent(c2, registerOf("alpha"))
	h.app(c2, registeredOK("alpha"))
	if st := h.state("beta"); st != ServiceTakenOver {
		t.Fatalf("beta %s after a reconnect, want taken_over", st)
	}
	h.s.localEnable("beta")
	h.wantSent(c2, registerOf("beta"))
	if st := h.state("beta"); st != ServicePending {
		t.Fatalf("beta %s after Enable, want pending", st)
	}
}

func lastGroupEffect(h *groupHarness) groupEffect {
	evs := h.log.of(evGroup)
	if len(evs) == 0 {
		return effNone
	}
	return evs[len(evs)-1].groupEffect
}

// v0.7.1 (i)+(vii): during an OVERLOADED pause a removed name is left out of
// the re-send and, held before, unregistered right after the re-send is
// answered; a name carried only by the dropped register was never held, so
// it gets no unregister. Only registers fold in, dropped names first.
func TestGroupPauseFoldOrderAndOwedUnregister(t *testing.T) {
	h := newGroupHarness(t, "alpha", "beta")
	c := &fakeConn{}
	h.s.onWelcome(c)
	h.wantSent(c, registerOf("alpha", "beta"))
	h.app(c, registeredOK("alpha"+"", "beta"))
	all := func(names ...string) []groupServiceInit {
		var out []groupServiceInit
		for _, n := range names {
			out = append(out, groupServiceInit{name: n, kind: KindHTTP})
		}
		return out
	}
	h.s.applyServices(all("alpha", "beta", "gamma", "delta"))
	h.wantSent(c, registerOf("gamma", "delta"))
	h.app(c, overloadedOp)
	h.s.applyServices(all("alpha", "gamma", "delta", "eps")) // beta (held) and nothing else removed; eps added
	h.s.localDisable("delta")                                // carried only by the dropped register
	h.wantSent(c)
	h.clock.advance(time.Second)
	h.wantSent(c, registerOf("gamma", "eps"))
	h.app(c, registeredOK("gamma", "eps"))
	h.wantSent(c, string(EncodeUnregisterList([]string{"beta"})))
	h.app(c, `{"mcpwarp":{"v":1,"op":"unregistered","services":[{"name":"beta","id":"srv_beta"}]}}`)
	h.wantSent(c) // delta was never held: no unregister for it
}

// A name that owes an unregister from a disable or removal during an
// OVERLOADED pause and is enabled or added again in the same pause folds
// into the re-send, and its owed unregister is dropped: the edge never gets
// an unregister after the register for a name the group shows active.
func TestGroupPauseUndoDropsOwedUnregister(t *testing.T) {
	all := func(names ...string) []groupServiceInit {
		var out []groupServiceInit
		for _, n := range names {
			out = append(out, groupServiceInit{name: n, kind: KindHTTP})
		}
		return out
	}
	for _, tc := range []struct {
		name string
		undo func(h *groupHarness)
	}{
		{"disable+enable", func(h *groupHarness) { h.s.localDisable("alpha"); h.s.localEnable("alpha") }},
		{"remove+add", func(h *groupHarness) { h.s.applyServices(all("beta")); h.s.applyServices(all("beta", "alpha")) }},
	} {
		h := newGroupHarness(t, "alpha")
		c := &fakeConn{}
		h.s.onWelcome(c)
		h.app(c, registeredOK("alpha"))
		c.take()
		h.s.applyServices(all("alpha", "beta"))
		h.wantSent(c, registerOf("beta"))
		h.app(c, overloadedOp)
		tc.undo(h)
		h.wantSent(c)
		h.clock.advance(time.Second)
		h.wantSent(c, registerOf("beta", "alpha"))
		h.app(c, registeredOK("beta", "alpha"))
		h.clock.advance(time.Minute)
		h.wantSent(c) // no unregister{alpha} after the register
		v := h.s.snapshotView()
		if v["alpha"].State != ServiceActive || v["beta"].State != ServiceActive || !h.s.svcs["alpha"].holds {
			t.Fatalf("%s: services %+v, want alpha and beta active, alpha held", tc.name, v)
		}
	}
}

// v0.7.2 §2.6/§6A.7 (D-2026-10-19-05): a name of the dropped frame that is
// removed or locally disabled and then re-added or re-enabled within the
// pause is re-sent among the folded-in names in trigger order, after the
// dropped frame's remaining names, not in its original slot.
func TestGroupPauseReaddedDroppedNameFoldsLast(t *testing.T) {
	all := func(names ...string) []groupServiceInit {
		var out []groupServiceInit
		for _, n := range names {
			out = append(out, groupServiceInit{name: n, kind: KindHTTP})
		}
		return out
	}
	for _, tc := range []struct {
		name string
		undo func(h *groupHarness)
	}{
		{"remove+add", func(h *groupHarness) {
			h.s.applyServices(all("alpha", "gamma"))
			h.s.applyServices(all("alpha", "gamma", "delta", "beta"))
		}},
		{"disable+enable", func(h *groupHarness) {
			h.s.applyServices(all("alpha", "beta", "gamma", "delta"))
			h.s.localDisable("beta")
			h.s.localEnable("beta")
		}},
	} {
		h := newGroupHarness(t, "alpha")
		c := &fakeConn{}
		h.s.onWelcome(c)
		h.app(c, registeredOK("alpha"))
		c.take()
		h.s.applyServices(all("alpha", "beta", "gamma"))
		h.wantSent(c, registerOf("beta", "gamma"))
		h.app(c, overloadedOp)
		tc.undo(h)
		h.wantSent(c)
		h.clock.advance(time.Second)
		if got, want := c.take(), []string{registerOf("gamma", "delta", "beta")}; len(got) != 1 || got[0] != want[0] {
			t.Fatalf("%s: re-send %q, want %q", tc.name, got, want)
		}
		h.app(c, registeredOK("gamma", "delta", "beta"))
		h.clock.advance(time.Minute)
		h.wantSent(c)
		for _, n := range []string{"alpha", "beta", "gamma", "delta"} {
			if st := h.state(n); st != ServiceActive {
				t.Fatalf("%s: %s %s, want active", tc.name, n, st)
			}
		}
	}
}

// A Remove+Add within a pause keeps what the connection may hold: after a
// second OVERLOADED drops the re-send, removing the name again still owes
// its unregister, since the edge holds it from before the pause.
func TestGroupReaddInPauseKeepsHold(t *testing.T) {
	all := func(names ...string) []groupServiceInit {
		var out []groupServiceInit
		for _, n := range names {
			out = append(out, groupServiceInit{name: n, kind: KindHTTP})
		}
		return out
	}
	h := newGroupHarness(t, "alpha")
	c := &fakeConn{}
	h.s.onWelcome(c)
	h.app(c, registeredOK("alpha"))
	c.take()
	h.s.applyServices(all("alpha", "beta"))
	h.wantSent(c, registerOf("beta"))
	h.app(c, overloadedOp)
	h.s.applyServices(all("beta"))
	h.s.applyServices(all("beta", "alpha"))
	h.clock.advance(time.Second)
	h.wantSent(c, registerOf("beta", "alpha"))
	h.app(c, overloadedOp) // the re-send is dropped too
	h.s.applyServices(all("beta"))
	h.clock.advance(2 * time.Second)
	h.wantSent(c, registerOf("beta"))
	h.app(c, registeredOK("beta"))
	h.wantSent(c, string(EncodeUnregisterList([]string{"alpha"})))
}

// v0.7.1 (ii): a whole-frame error op answering an outstanding unregister
// settles it like an unregistered, with a warn log line.
func TestGroupErrorOpSettlesUnregister(t *testing.T) {
	h := newGroupHarness(t, "alpha", "beta")
	c := &fakeConn{}
	h.s.onWelcome(c)
	h.app(c, registeredOK("alpha", "beta"))
	c.take()
	h.s.applyServices([]groupServiceInit{{name: "beta", kind: KindHTTP}})
	h.wantSent(c, string(EncodeUnregisterList([]string{"alpha"})))
	h.app(c, `{"mcpwarp":{"v":1,"op":"error","code":"BAD_REQUEST","message":"x"}}`)
	if _, ok := h.s.snapshotView()["alpha"]; ok {
		t.Fatal("alpha still shown after its unregister was answered by an error op")
	}
	warned := false
	for _, ev := range h.log.of(evLog) {
		warned = warned || ev.logLevel == slog.LevelWarn && strings.Contains(ev.logMsg, "BAD_REQUEST")
	}
	if !warned {
		t.Fatal("no warn log line for the error op answering the unregister")
	}
}

// v0.7.1 (iii): after a reply mixing transient and non-transient entries
// the transient counter resets, so the transient names wait n=1 (1 s).
func TestGroupMixedReplyTransientCounterResets(t *testing.T) {
	h := newGroupHarness(t, "alpha", "beta")
	c := &fakeConn{}
	h.s.onWelcome(c)
	h.wantSent(c, registerOf("alpha", "beta"))
	h.app(c, `{"mcpwarp":{"v":1,"op":"registered","errors":[{"name":"alpha","code":"INTERNAL"},{"name":"beta","code":"INTERNAL"}]}}`)
	h.clock.advance(time.Second)
	h.wantSent(c, registerOf("alpha", "beta"))
	h.app(c, `{"mcpwarp":{"v":1,"op":"registered","services":[{"name":"alpha","id":"ia","url":"https://u/mcp"}],"errors":[{"name":"beta","code":"INTERNAL"}]}}`)
	r := h.retries("register")
	if len(r) != 2 || r[0].Delay != time.Second || r[1].Delay != time.Second || r[1].Attempt != 1 {
		t.Fatalf("register retries %+v, want 1 s then 1 s (attempt 1) after the mixed reply", r)
	}
}

// v0.7.1 (v): CONNECTION_LIMIT answers no frame (the register stays in
// flight); the connection's end frees the slot.
func TestGroupConnectionLimitAnswersNoFrame(t *testing.T) {
	h := newGroupHarness(t, "alpha")
	c := &fakeConn{}
	h.s.onWelcome(c)
	h.wantSent(c, registerOf("alpha"))
	h.app(c, `{"mcpwarp":{"v":1,"op":"error","code":"CONNECTION_LIMIT","message":"x"}}`)
	if h.s.out == nil {
		t.Fatal("CONNECTION_LIMIT freed the slot")
	}
	if st := h.state("alpha"); st != ServicePending {
		t.Fatalf("alpha %s, want pending", st)
	}
	h.s.onConnEnd(c)
	if h.s.out != nil {
		t.Fatal("the connection's end did not free the slot")
	}
}

// v0.7.1 (vi): a re-send with no names left is not sent and frees the
// slot; a connecting group is then G1's "nothing to send": online at once.
func TestGroupEmptyResendIsNothingToSend(t *testing.T) {
	h := newGroupHarness(t, "alpha")
	c := &fakeConn{}
	h.s.onWelcome(c)
	h.wantSent(c, registerOf("alpha"))
	h.app(c, overloadedOp)
	h.s.localDisable("alpha")
	h.clock.advance(time.Second)
	h.wantSent(c)
	if h.s.out != nil {
		t.Fatal("the slot is still reserved")
	}
	if eff := lastGroupEffect(h); eff != effOnline {
		t.Fatalf("effect %v, want online (G1 nothing to send)", eff)
	}
}

// v0.7.2 (viii): a scheduled transient re-send counts as queued, so a
// paused re-send that empties out while one is pending is not G1's
// "nothing to send": the group stays connecting/RETRYING until that
// re-send is answered.
func TestGroupEmptyResendWithTransientPendingStaysRetrying(t *testing.T) {
	h := newGroupHarness(t, "alpha")
	c := &fakeConn{}
	h.s.onWelcome(c)
	h.wantSent(c, registerOf("alpha"))
	transient := `{"mcpwarp":{"v":1,"op":"registered","errors":[{"name":"alpha","code":"INTERNAL","message":"x"}]}}`
	h.app(c, transient)
	h.clock.advance(time.Second)
	h.wantSent(c, registerOf("alpha"))
	h.app(c, transient) // alpha's re-send now scheduled in 2 s
	h.s.applyServices([]groupServiceInit{{name: "alpha", kind: KindHTTP}, {name: "gamma", kind: KindHTTP}})
	h.wantSent(c, registerOf("gamma"))
	h.app(c, overloadedOp)
	h.s.localDisable("gamma")
	h.clock.advance(time.Second) // the paused re-send empties out
	h.wantSent(c)
	if eff := lastGroupEffect(h); eff == effOnline {
		t.Fatal("online with alpha's transient re-send still scheduled")
	}
	h.clock.advance(time.Second)
	h.wantSent(c, registerOf("alpha"))
	h.app(c, registeredOK("alpha"))
	if eff := lastGroupEffect(h); eff != effOnline {
		t.Fatalf("effect %v after alpha's re-send was answered, want online", eff)
	}
}

// A transient re-send whose names were all locally disabled or removed
// before its timer fires sends nothing, frees itself and re-evaluates the
// group: with nothing else outstanding, queued or scheduled that is G1's
// "nothing to send" (online), not a group stuck at RETRYING.
func TestGroupEmptyTransientResendReevaluates(t *testing.T) {
	h := newGroupHarness(t, "alpha")
	c := &fakeConn{}
	h.s.onWelcome(c)
	h.wantSent(c, registerOf("alpha"))
	h.app(c, `{"mcpwarp":{"v":1,"op":"registered","errors":[{"name":"alpha","code":"INTERNAL","message":"x"}]}}`)
	if eff := lastGroupEffect(h); eff != effRetrying {
		t.Fatalf("effect %v after the transient reply, want retrying", eff)
	}
	h.s.localDisable("alpha")
	h.wantSent(c, string(EncodeUnregisterList([]string{"alpha"})))
	h.app(c, `{"mcpwarp":{"v":1,"op":"unregistered","services":[{"name":"alpha"}]}}`)
	h.clock.advance(time.Second)
	h.wantSent(c)
	if len(h.s.resends) != 0 {
		t.Fatal("the transient re-send was not freed")
	}
	if eff := lastGroupEffect(h); eff != effOnline {
		t.Fatalf("effect %v after the empty transient re-send, want online (G1)", eff)
	}
}

// A settled frame after which nothing is outstanding, queued or scheduled
// re-evaluates the group: here alpha's unregister is still outstanding when
// its empty transient re-send fires, and its later unregistered takes the
// group out of RETRYING (G1: online).
func TestGroupSettleReevaluatesWhenNothingLeft(t *testing.T) {
	h := newGroupHarness(t, "alpha")
	c := &fakeConn{}
	h.s.onWelcome(c)
	h.wantSent(c, registerOf("alpha"))
	h.app(c, `{"mcpwarp":{"v":1,"op":"registered","errors":[{"name":"alpha","code":"INTERNAL","message":"x"}]}}`)
	h.s.localDisable("alpha")
	h.wantSent(c, string(EncodeUnregisterList([]string{"alpha"})))
	h.clock.advance(time.Second) // the empty transient re-send fires with the unregister outstanding
	h.wantSent(c)
	if eff := lastGroupEffect(h); eff == effOnline {
		t.Fatal("online with the unregister still outstanding")
	}
	h.app(c, `{"mcpwarp":{"v":1,"op":"unregistered","services":[{"name":"alpha"}]}}`)
	if eff := lastGroupEffect(h); eff != effOnline {
		t.Fatalf("effect %v after the unregistered, want online (G1)", eff)
	}
}

// v0.7.1 (vii): UNSUPPORTED_VERSION answers the outstanding register but
// does not reject its names (G7 excludes it); it is then fatal (T24).
func TestGroupUnsupportedVersionNotRejected(t *testing.T) {
	h := newGroupHarness(t, "alpha")
	c := &fakeConn{}
	h.s.onWelcome(c)
	h.wantSent(c, registerOf("alpha"))
	h.app(c, `{"mcpwarp":{"v":1,"op":"error","code":"UNSUPPORTED_VERSION","message":"x"}}`)
	if st := h.state("alpha"); st == ServiceRejected {
		t.Fatal("UNSUPPORTED_VERSION rejected alpha")
	}
	if len(h.fatals) != 1 || h.fatals[0].Code != "UNSUPPORTED_VERSION" {
		t.Fatalf("fatals %+v, want UNSUPPORTED_VERSION", h.fatals)
	}
}

// §4.8 rows 3c-3e per service.
func TestGroupRoutePerServiceState(t *testing.T) {
	h := newGroupHarness(t, "a-svc", "b-svc", "c-svc", "d-svc")
	c := &fakeConn{}
	h.s.onWelcome(c)
	head := func(n string) *requestHead { return &requestHead{host: n + "-alice.x"} }
	if r, _ := h.s.route(head("a-svc")); r.unavailable != unavailableOther {
		t.Fatalf("a never-active pending service: %+v, want 503 service unavailable", r)
	}
	h.app(c, `{"mcpwarp":{"v":1,"op":"registered","services":[{"name":"a-svc","id":"ia","url":"https://u/mcp"},{"name":"b-svc","id":"ib","url":"https://u/mcp"}],"errors":[{"name":"c-svc","code":"SERVER_DISABLED","id":"ic"},{"name":"d-svc","code":"QUOTA_EXCEEDED"}]}}`)
	h.app(c, `{"mcpwarp":{"v":1,"op":"conflict","id":"ib","name":"b-svc"}}`)
	for host, want := range map[string]routeResult{
		"a-svc": {app: h.s.svcs["a-svc"].app},
		"b-svc": {reset: true},
		"c-svc": {unavailable: unavailableDisabled},
		"d-svc": {unavailable: unavailableOther},
	} {
		if got, _ := h.s.route(head(host)); got != want {
			t.Errorf("%s: %+v, want %+v", host, got, want)
		}
	}
	if conflicts := h.log.of(evServiceConflict); len(conflicts) != 1 || conflicts[0].serviceName != "b-svc" {
		t.Fatalf("per-name conflict events %+v, want one for b-svc", conflicts)
	}
}
