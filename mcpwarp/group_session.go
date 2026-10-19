package mcpwarp

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"
)

// groupEffect is what one processed input means for the group's
// connection-level status (§6A.6).
type groupEffect int

const (
	effNone     groupEffect = iota
	effOnline               // G1 (nothing to send), G2
	effRetrying             // G3
	effFatal                // G5
)

// usernameRE is the account username grammar of §4.8 step 2.
var usernameRE = regexp.MustCompile(`^[a-z0-9]{3,20}$`)

// splitFrames cuts a list into frames of at most MaxServicesPerMessage
// (§2.1 Batch, §6A.7 Batches), in order.
func splitFrames[T any](xs []T) [][]T {
	var out [][]T
	for len(xs) > MaxServicesPerMessage {
		out = append(out, xs[:MaxServicesPerMessage])
		xs = xs[MaxServicesPerMessage:]
	}
	if len(xs) > 0 {
		out = append(out, xs)
	}
	return out
}

// groupServiceInit is one configured service as the controller hands it
// to a group session.
type groupServiceInit struct {
	name          string
	kind          ServiceKind
	app           *app
	localDisabled bool
}

type svcOutcome int

const (
	outNone svcOutcome = iota
	outActive
	outServerDisabled
	outRejected
)

// gsvc is one service of a group connection and its §6A.5 marks.
type gsvc struct {
	name string
	kind ServiceKind
	app  *app

	id  string // the last server id learned for the name
	url string

	localDisabled bool
	takenOver     bool
	awaiting      bool // row 4: a register carrying it is outstanding or queued, a re-send is scheduled, or it has not gone out yet
	outcome       svcOutcome
	code, message string // outRejected
	dashDisabled  bool   // row 7
	everActive    bool   // active since the session started (§4.8 row 3e)
	mayHold       bool   // the live connection sent a register carrying it, and since neither an unregister nor a conflict
	holds         bool   // the live connection got a services[] entry for it since (what a conflict needs)
}

// gframe is one register/unregister frame.
type gframe struct {
	op     outstandingOp
	names  []string
	paused bool // dropped by OVERLOADED, its re-send scheduled; the slot stays reserved
	stop   func() bool
	// newHold: names this register made "may hold" (§6A.2); a drop by
	// OVERLOADED means it was never processed, so they are not held after all.
	newHold []string
}

type groupDeps struct {
	after       afterFunc
	emit        func(event) // never nil
	fatal       func(*Error)
	activeConn  func() appConn
	trustHeader bool
}

// groupSession is the multi-service registration state machine for one
// connection lifetime of a ServiceGroup (§2.7 multi-service table, §3.2,
// §3.3 Per name, §6A.5-§6A.8), the counterpart of session. It reports one
// evGroup per processed input that changes the services or the group's
// connection state.
type groupSession struct {
	deps groupDeps

	mu      sync.Mutex
	order   []string
	svcs    map[string]*gsvc
	removed map[string]*gsvc // removed, their unregister not answered yet (row 1)

	closing, closed bool
	takenOver       bool // G6: the whole connection
	anyActive       bool // some service was active since the session started (G5)

	conn      appConn
	connected bool
	learned   map[string]string // server id -> name, learned on this connection
	held      map[string]*DisableMsg

	out   *gframe
	queue []*gframe

	transientN  int // consecutive all-transient replies (§3.2), reset by a successful register
	overloadedN int // consecutive OVERLOADED (v0.7.1 ruling 1), reset by any other reply
	resends     map[int]func() bool
	resendSeq   int

	lastView map[string]ServiceStatus

	// The running close (beginCloseAsync), if any.
	closeFrames  [][]string
	closeConn    appConn
	closeCtx     context.Context
	closePreOp   outstandingOp // the frame in flight when the close began, its reply still due
	unregDone    func()
	unregCancel  context.CancelFunc
	unregStop    func() bool
	unregStopCtx func() bool
	closeResend  func() bool

	queueFns []func()
	flushing bool
}

func newGroupSession(deps groupDeps, services []groupServiceInit) *groupSession {
	if deps.after == nil {
		deps.after = realAfterFunc
	}
	if deps.emit == nil {
		deps.emit = func(event) {}
	}
	if deps.fatal == nil {
		deps.fatal = func(*Error) {}
	}
	s := &groupSession{deps: deps, svcs: map[string]*gsvc{}, removed: map[string]*gsvc{}, resends: map[int]func() bool{}}
	for _, in := range services {
		s.order = append(s.order, in.name)
		s.svcs[in.name] = &gsvc{name: in.name, kind: in.kind, app: in.app, localDisabled: in.localDisabled, awaiting: !in.localDisabled}
	}
	s.lastView = s.viewLocked()
	return s
}

func (s *groupSession) unlockAndFlush() {
	if s.flushing {
		s.mu.Unlock()
		return
	}
	s.flushing = true
	for len(s.queueFns) > 0 {
		q := s.queueFns
		s.queueFns = nil
		s.mu.Unlock()
		for _, f := range q {
			f()
		}
		s.mu.Lock()
	}
	s.flushing = false
	s.mu.Unlock()
}

func (s *groupSession) do(f func()) { s.queueFns = append(s.queueFns, f) }

func (s *groupSession) emitLocked(ev event) { s.do(func() { s.deps.emit(ev) }) }

func (s *groupSession) logLocked(level slog.Level, msg string) {
	s.emitLocked(event{kind: evLog, logLevel: level, logMsg: msg})
}

func (s *groupSession) endingLocked() bool { return s.closing || s.closed }

// acceptingLocked: edge ops apply except in group conflict and while the
// group shuts down (§6A.4 Acceptance).
func (s *groupSession) acceptingLocked() bool { return !s.endingLocked() && !s.takenOver }

func (s *groupSession) supersededLocked(c appConn) bool {
	if c == nil {
		return false
	}
	if c != s.conn {
		return true
	}
	if s.deps.activeConn != nil {
		if active := s.deps.activeConn(); active != nil && active != c {
			return true
		}
	}
	return false
}

// stateOf is §6A.5's precedence (row 1, removed, is the removed map).
func stateOf(v *gsvc) ServiceState {
	switch {
	case v.localDisabled:
		return ServiceDisabled
	case v.takenOver:
		return ServiceTakenOver
	case v.awaiting:
		return ServicePending
	case v.outcome == outServerDisabled:
		return ServiceDisabled
	case v.outcome == outRejected:
		return ServiceRejected
	case v.dashDisabled:
		return ServiceDisabled
	case v.outcome == outActive:
		return ServiceActive
	}
	return ServicePending
}

func statusOf(v *gsvc, removed bool) ServiceStatus {
	st := ServiceStatus{ID: v.id}
	if removed {
		st.State = ServiceUnregistered
		return st
	}
	st.State = stateOf(v)
	switch {
	case st.State == ServiceActive:
		st.URL = v.url
	case st.State == ServiceTakenOver:
		st.Code, st.LastError = string(CodeTakenOver), lastTakenOver
	case st.State == ServiceRejected:
		st.Code, st.LastError = v.code, rejectedText(v.code)
	case st.State == ServiceDisabled && !v.localDisabled:
		st.Code, st.LastError = string(CodeDisabled), lastDisabled
	}
	return st
}

// rejectedText is §6A.4's lastError for a rejected service.
func rejectedText(code string) string {
	switch RegisterErrorCode(code) {
	case RegisterUnauthenticated:
		return lastRejected
	case RegisterForbidden:
		return lastForbidden
	case RegisterInvalidName:
		return lastInvalidName
	case RegisterUsernameRequired:
		return lastUsername
	case RegisterQuotaExceeded:
		return lastQuota
	case RegisterConflict:
		return lastConflict
	}
	return lastInternal
}

func (s *groupSession) viewLocked() map[string]ServiceStatus {
	view := make(map[string]ServiceStatus, len(s.svcs)+len(s.removed))
	for name, v := range s.svcs {
		view[name] = statusOf(v, false)
	}
	for name, v := range s.removed {
		view[name] = statusOf(v, true)
	}
	return view
}

func sameView(a, b map[string]ServiceStatus) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if w, ok := b[k]; !ok || w != v {
			return false
		}
	}
	return true
}

// changedLocked reports the input's outcome: one evGroup when the services
// changed or the input has a connection-level effect.
func (s *groupSession) changedLocked(eff groupEffect) {
	view := s.viewLocked()
	if eff == effNone && sameView(view, s.lastView) {
		return
	}
	s.lastView = view
	s.emitLocked(event{kind: evGroup, groupServices: view, groupEffect: eff})
}

// snapshotView is the current services view.
func (s *groupSession) snapshotView() map[string]ServiceStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.viewLocked()
}

// eligibleLocked: a name a register may carry now.
func (s *groupSession) eligibleLocked(name string) bool {
	v, ok := s.svcs[name]
	return ok && !v.localDisabled && !v.takenOver && !s.takenOver
}

// --- connection events ------------------------------------------------------

func (s *groupSession) onWelcome(c appConn) {
	s.mu.Lock()
	defer s.unlockAndFlush()
	if s.endingLocked() {
		return
	}
	s.conn, s.connected = c, true
	s.resetConnLocked()
	if s.takenOver {
		return
	}
	var names []string
	for _, name := range s.order {
		if s.eligibleLocked(name) {
			names = append(names, name)
			s.svcs[name].awaiting = true
		}
	}
	if len(names) == 0 {
		s.changedLocked(effOnline) // G1: nothing to send
		return
	}
	s.logLocked(slog.LevelInfo, fmt.Sprintf("registering %d services", len(names)))
	for _, f := range splitFrames(names) {
		s.queue = append(s.queue, &gframe{op: opRegister, names: f})
	}
	s.pumpLocked()
	s.changedLocked(effNone)
}

// resetConnLocked forgets everything that belongs to one connection: its
// frames, timers, holds (v0.7.1 ruling 8: a replacement's welcome clears
// them too), learned ids and what it may hold.
func (s *groupSession) resetConnLocked() {
	s.stopFrameLocked()
	s.out, s.queue = nil, nil
	s.cancelResendsLocked()
	s.held, s.learned = nil, map[string]string{}
	for _, v := range s.svcs {
		v.mayHold, v.holds = false, false
	}
	for _, v := range s.removed {
		v.mayHold, v.holds = false, false
	}
}

func (s *groupSession) onConnEnd(c appConn) {
	s.mu.Lock()
	defer s.unlockAndFlush()
	if s.closeConn == c {
		s.releaseUnregLocked()
	}
	if s.supersededLocked(c) || s.endingLocked() {
		return
	}
	s.connected = false
	s.resetConnLocked()
	// G8: unregistered entries leave; every other service keeps its state.
	s.removed = map[string]*gsvc{}
	s.changedLocked(effNone)
}

func (s *groupSession) onApp(c appConn, body []byte) {
	s.mu.Lock()
	defer s.unlockAndFlush()
	if s.supersededLocked(c) {
		return
	}
	env, err := DecodeEnvelope(body)
	if err != nil {
		s.emitLocked(event{kind: evError, err: err, origin: originMalformed})
		return
	}
	ended := c != nil && connEnded(c)
	switch env.(type) {
	case *RegisteredMsg, *DisableMsg, *EnableMsg:
		if ended {
			return
		}
	}
	if s.closing && s.closeAnsweredLocked(env) {
		return
	}
	if !s.acceptingLocked() {
		return
	}
	switch m := env.(type) {
	case *RegisteredMsg:
		s.onRegisteredLocked(m)
	case *UnregisteredMsg:
		s.onUnregisteredLocked()
	case *DisableMsg:
		s.onDisableLocked(m)
	case *EnableMsg:
		s.onEnableLocked(m)
	case *ConflictMsg:
		s.onConflictLocked(m)
	case *ErrorMsg:
		s.onErrorLocked(m)
	}
}

// --- frames -----------------------------------------------------------------

// pumpLocked sends the next queued frame when none is outstanding (§2.6
// One in flight). A register drops the names that may no longer go out.
func (s *groupSession) pumpLocked() {
	for s.out == nil && s.connected && len(s.queue) > 0 && !s.endingLocked() {
		f := s.queue[0]
		s.queue = s.queue[1:]
		if f.op == opRegister {
			f.names = s.filterEligibleLocked(f.names)
			if len(f.names) == 0 {
				continue
			}
		}
		s.out = f
		s.sendFrameLocked(f)
	}
}

func (s *groupSession) filterEligibleLocked(names []string) []string {
	out := names[:0:0]
	for _, n := range names {
		if s.eligibleLocked(n) {
			out = append(out, n)
		}
	}
	return out
}

func (s *groupSession) sendFrameLocked(f *gframe) {
	var body json.RawMessage
	if f.op == opRegister {
		entries := make([]RegisterEntry, len(f.names))
		f.newHold = nil
		for i, n := range f.names {
			v := s.svcs[n]
			if !v.mayHold {
				f.newHold = append(f.newHold, n)
			}
			v.mayHold = true
			entries[i] = RegisterEntry{Name: n, Kind: v.kind}
		}
		body = EncodeRegisterList(entries)
	} else {
		for _, n := range f.names {
			if v := s.svcOrRemovedLocked(n); v != nil {
				v.mayHold, v.holds = false, false
			}
		}
		body = EncodeUnregisterList(f.names)
	}
	conn := s.conn
	// A failed send is not reported: the connection is gone, and its end drops the frame.
	s.do(func() { _ = sendApp(conn, body) })
}

func (s *groupSession) svcOrRemovedLocked(name string) *gsvc {
	if v, ok := s.svcs[name]; ok {
		return v
	}
	return s.removed[name]
}

// registerLocked queues a register for names. While the outstanding
// register is paused by OVERLOADED the names fold into its re-send (v0.7.1
// ruling 6a), so no second frame goes out.
func (s *groupSession) registerLocked(names []string) {
	for _, n := range names {
		if v, ok := s.svcs[n]; ok {
			v.awaiting = true
		}
	}
	if f := s.out; f != nil && f.paused && f.op == opRegister {
		var rest, inResend []string
		for _, n := range names {
			switch {
			case slices.Contains(f.names, n):
				inResend = append(inResend, n)
			case len(f.names) < MaxServicesPerMessage:
				f.names = append(f.names, n)
				inResend = append(inResend, n)
			default:
				rest = append(rest, n)
			}
		}
		names = rest
		// The re-send goes out before anything queued: an unregister owed
		// by these names from earlier in the pause would follow it and drop
		// them on the edge, so it is no longer owed.
		s.dropQueuedUnregistersLocked(inResend)
	}
	for _, f := range splitFrames(names) {
		s.queue = append(s.queue, &gframe{op: opRegister, names: f})
	}
	s.pumpLocked()
}

// dropQueuedUnregistersLocked takes names out of every queued unregister
// frame, dropping frames left empty.
func (s *groupSession) dropQueuedUnregistersLocked(names []string) {
	if len(names) == 0 {
		return
	}
	q := s.queue[:0]
	for _, f := range s.queue {
		if f.op == opUnregister {
			f.names = slices.DeleteFunc(f.names, func(n string) bool { return slices.Contains(names, n) })
			if len(f.names) == 0 {
				continue
			}
		}
		q = append(q, f)
	}
	s.queue = q
}

func (s *groupSession) unregisterLocked(names []string) {
	for _, f := range splitFrames(names) {
		s.queue = append(s.queue, &gframe{op: opUnregister, names: f})
	}
	s.pumpLocked()
}

// settleLocked ends the outstanding frame: its re-send timer is cancelled
// (v0.7.1 ruling 5) and the OVERLOADED streak reset (ruling 1).
func (s *groupSession) settleLocked() *gframe {
	f := s.out
	s.stopFrameLocked()
	s.out = nil
	s.overloadedN = 0
	return f
}

func (s *groupSession) stopFrameLocked() {
	if s.out != nil && s.out.stop != nil {
		s.out.stop()
		s.out.stop = nil
	}
}

func (s *groupSession) cancelResendsLocked() {
	for id, stop := range s.resends {
		stop()
		delete(s.resends, id)
	}
}

// --- edge ops ----------------------------------------------------------------

func (s *groupSession) onRegisteredLocked(m *RegisteredMsg) {
	if s.out == nil || s.out.op != opRegister {
		s.overloadedN = 0 // answers no register of ours, but ends the OVERLOADED streak (ruling 1)
		return
	}
	f := s.settleLocked()
	inFrame := map[string]bool{}
	for _, n := range f.names {
		inFrame[n] = true
	}
	applies := func(name string) *gsvc {
		if !inFrame[name] || !s.eligibleLocked(name) {
			return nil
		}
		return s.svcs[name]
	}
	listed := map[string]bool{}
	successful := false
	var transient []string
	var firstCode, firstMsg string
	for _, e := range m.Services {
		v := applies(e.Name)
		if v == nil || listed[e.Name] {
			continue
		}
		listed[e.Name] = true
		successful = true
		v.awaiting, v.outcome, v.dashDisabled, v.everActive, v.holds = false, outActive, false, true, true
		v.id, v.url = e.ID, e.URL
		s.anyActive = true
		s.learned[e.ID] = e.Name
		s.logLocked(slog.LevelInfo, fmt.Sprintf("service %q online at %s", e.Name, e.URL))
		if h := s.held[e.ID]; h != nil {
			delete(s.held, e.ID)
			v.dashDisabled = true
			s.logLocked(slog.LevelWarn, fmt.Sprintf("service %q disabled (%s)", e.Name, h.Reason))
		}
	}
	for _, e := range m.Errors {
		v := applies(e.Name)
		if v == nil || listed[e.Name] {
			continue
		}
		listed[e.Name] = true
		switch {
		case e.Code == RegisterServerDisabled:
			successful = true
			v.awaiting, v.outcome = false, outServerDisabled
			if e.ID != "" {
				v.id = e.ID
				s.learned[e.ID] = e.Name
			}
			s.logLocked(slog.LevelWarn, fmt.Sprintf("service %q disabled (disabled)", e.Name))
		case IsTransientRegisterError(e.Code):
			transient = append(transient, e.Name)
			if firstCode == "" {
				firstCode, firstMsg = string(e.Code), e.Message
			}
		default:
			successful = true
			v.awaiting, v.outcome, v.code, v.message = false, outRejected, string(e.Code), e.Message
			s.logLocked(slog.LevelWarn, fmt.Sprintf("service %q refused: %s: %s", e.Name, e.Code, e.Message))
		}
	}
	// A name of the frame the reply does not list is a transient failure (§6A.7 Correlation).
	for _, n := range f.names {
		if !listed[n] && s.eligibleLocked(n) {
			transient = append(transient, n)
		}
	}
	if successful {
		s.transientN = 0
	}
	eff := effNone
	if len(transient) > 0 {
		if firstCode == "" {
			firstCode, firstMsg = string(RegisterInternal), "unlisted in reply"
		}
		s.scheduleTransientLocked(transient, firstCode, firstMsg)
		eff = effRetrying
	}
	s.pumpLocked()
	s.changedLocked(s.settledEffectLocked(s.outcomeEffectLocked(eff)))
}

// outcomeEffectLocked is G5, else G2, else fallback (after a reply or G7).
func (s *groupSession) outcomeEffectLocked(fallback groupEffect) groupEffect {
	if s.noServiceRegisteredLocked() {
		return effFatal
	}
	for _, v := range s.svcs {
		if !v.localDisabled && !v.takenOver && !v.awaiting && (v.outcome != outNone || v.dashDisabled) {
			return effOnline
		}
	}
	return fallback
}

// nothingToSendEffectLocked: with nothing outstanding, queued or scheduled
// (v0.7.2 viii) the group is G1's "nothing to send" (online), or G5; else
// its status stays.
func (s *groupSession) nothingToSendEffectLocked() groupEffect {
	if s.out != nil || len(s.queue) > 0 || len(s.resends) > 0 {
		return effNone
	}
	if s.noServiceRegisteredLocked() {
		return effFatal
	}
	return effOnline
}

// noServiceRegisteredLocked is G5's condition.
func (s *groupSession) noServiceRegisteredLocked() bool {
	if s.anyActive || s.out != nil || len(s.queue) > 0 {
		return false
	}
	rejected := 0
	for _, v := range s.svcs {
		if v.localDisabled {
			continue
		}
		if stateOf(v) != ServiceRejected {
			return false
		}
		rejected++
	}
	return rejected > 0
}

// scheduleTransientLocked re-sends names after §3.2's backoff (Transient
// per name), on this connection only.
func (s *groupSession) scheduleTransientLocked(names []string, code, msg string) {
	s.transientN++
	delay := registerRetryDelay(s.transientN - 1)
	s.resendSeq++
	id, conn := s.resendSeq, s.conn
	s.resends[id] = s.deps.after(delay, func() {
		s.mu.Lock()
		defer s.unlockAndFlush()
		if _, ok := s.resends[id]; !ok {
			return
		}
		delete(s.resends, id)
		if !s.connected || s.conn != conn || !s.acceptingLocked() {
			return
		}
		var again []string
		for _, n := range names {
			if v, ok := s.svcs[n]; ok && v.awaiting && s.eligibleLocked(n) {
				again = append(again, n)
			}
		}
		if len(again) > 0 {
			s.registerLocked(again)
			return
		}
		// Every name left (disabled or removed): nothing is sent, and the
		// group is re-evaluated as after an empty OVERLOADED re-send.
		s.changedLocked(s.nothingToSendEffectLocked())
	})
	s.emitLocked(event{kind: evRetry, retryKind: "register", retry: retryInfo{
		Attempt: s.transientN, Delay: delay,
		Reason: fmt.Sprintf("%s: %s: %s", registerRetryLabel, code, msg),
	}})
}

func (s *groupSession) onUnregisteredLocked() {
	if s.out == nil || s.out.op != opUnregister {
		return
	}
	f := s.settleLocked()
	s.unregisterAnsweredLocked(f)
	s.pumpLocked()
	s.changedLocked(s.settledEffectLocked(effNone))
}

// settledEffectLocked is a settled frame's effect: eff, or when that is
// none and nothing is left outstanding, queued or scheduled, G1/G5, so a
// connecting group never stays RETRYING with nothing to send.
func (s *groupSession) settledEffectLocked(eff groupEffect) groupEffect {
	if eff != effNone {
		return eff
	}
	return s.nothingToSendEffectLocked()
}

// unregisterAnsweredLocked: the frame's names leave the held set whether
// listed or absent; removed ones leave the services.
func (s *groupSession) unregisterAnsweredLocked(f *gframe) {
	for _, n := range f.names {
		if v := s.svcOrRemovedLocked(n); v != nil {
			v.mayHold, v.holds = false, false
		}
		delete(s.removed, n)
	}
}

func (s *groupSession) onDisableLocked(m *DisableMsg) {
	name, ok := s.learned[m.ID]
	v := s.svcs[name]
	if !ok || v == nil {
		// An id this connection did not learn may be another service's: held per connection.
		if s.held == nil {
			s.held = map[string]*DisableMsg{}
		}
		held := *m
		s.held[m.ID] = &held
		return
	}
	v.dashDisabled = true
	s.logLocked(slog.LevelWarn, fmt.Sprintf("service %q disabled (%s)", name, m.Reason))
	s.changedLocked(effNone)
}

func (s *groupSession) onEnableLocked(m *EnableMsg) {
	name := ""
	if _, ok := s.svcs[m.Name]; ok {
		name = m.Name
	} else if n, ok := s.learned[m.ID]; ok {
		name = n
	} else {
		for _, n := range s.order {
			if s.svcs[n].id == m.ID && m.ID != "" {
				name = n
				break
			}
		}
	}
	v := s.svcs[name]
	if v == nil || v.localDisabled || v.takenOver {
		return
	}
	if m.ID != "" {
		v.id = m.ID
		s.learned[m.ID] = name
		delete(s.held, m.ID)
	}
	v.dashDisabled = false
	s.logLocked(slog.LevelInfo, fmt.Sprintf("service %q was enabled, registering again", name))
	if s.connected {
		s.registerLocked([]string{name})
	} else {
		v.awaiting = true
	}
	s.changedLocked(effNone)
}

func (s *groupSession) onConflictLocked(m *ConflictMsg) {
	var v *gsvc
	if c, ok := s.svcs[m.Name]; ok && c.holds {
		v = c
	} else if n, ok := s.learned[m.ID]; ok && s.svcs[n] != nil && s.svcs[n].holds {
		v = s.svcs[n]
	}
	if v == nil {
		return // a name this connection never held
	}
	v.takenOver, v.mayHold, v.holds, v.awaiting = true, false, false, false
	if v.id != "" {
		delete(s.held, v.id)
	}
	s.logLocked(slog.LevelWarn, fmt.Sprintf("another connection took over the name %q; not registering it again until enable() or configure()", v.name))
	s.emitLocked(event{kind: evServiceConflict, serviceName: v.name})
	s.changedLocked(effNone)
}

func (s *groupSession) onErrorLocked(m *ErrorMsg) {
	switch m.Code {
	case AppOverloaded:
		f := s.out
		if f == nil || f.paused {
			return
		}
		s.overloadedN++
		delay := registerRetryDelay(s.overloadedN - 1)
		f.paused = true
		if f.op == opRegister {
			// Dropped unprocessed: names only this register carried are not
			// held (v0.7.1 vii), so removing them owes no unregister.
			for _, n := range f.newHold {
				if v := s.svcOrRemovedLocked(n); v != nil && !v.holds {
					v.mayHold = false
				}
			}
			f.newHold = nil
		}
		conn := s.conn
		f.stop = s.deps.after(delay, func() {
			s.mu.Lock()
			defer s.unlockAndFlush()
			if s.out != f || !s.connected || s.conn != conn || !s.acceptingLocked() {
				return
			}
			f.paused, f.stop = false, nil
			if f.op == opRegister {
				f.names = s.filterEligibleLocked(f.names)
				if len(f.names) == 0 {
					// Not sent; the slot is freed (v0.7.1 vi). With nothing
					// outstanding, queued or scheduled (a transient re-send
					// counts, v0.7.2 viii) that is G1's "nothing to send",
					// unless G5; otherwise the status stays as it is.
					s.out = nil
					s.pumpLocked()
					s.changedLocked(s.nothingToSendEffectLocked())
					return
				}
			}
			s.sendFrameLocked(f)
		})
		s.emitLocked(event{kind: evRetry, retryKind: "overloaded", retry: retryInfo{
			Attempt: s.overloadedN, Delay: delay,
			Reason: fmt.Sprintf("%s: %s: %s", registerRetryLabel, m.Code, m.Message),
		}})
		s.changedLocked(effRetrying)
	case AppConnectionLimit:
		s.emitLocked(event{kind: evError, err: &Error{Code: string(m.Code), Message: m.Message}, origin: originApp})
	default:
		// v0.7.1 ruling 4: any other code answers the outstanding frame.
		f := s.out
		if f == nil {
			if m.Code == AppUnsupportedVersion {
				s.do(func() { s.deps.fatal(&Error{Code: string(m.Code), Message: m.Message, Fatal: true}) })
				return
			}
			s.emitLocked(event{kind: evError, err: &Error{Code: string(m.Code), Message: m.Message}, origin: originApp})
			return
		}
		s.settleLocked()
		eff := effNone
		switch {
		case m.Code == AppUnsupportedVersion:
			// It answers the frame but is no G7 refusal: T24 follows, names
			// keep their state (v0.7.1).
		case f.op == opRegister:
			for _, n := range f.names {
				if v := s.svcs[n]; v != nil && s.eligibleLocked(n) {
					v.awaiting, v.outcome, v.code, v.message = false, outRejected, string(m.Code), m.Message
				}
			}
			s.logLocked(slog.LevelWarn, fmt.Sprintf("register of %d services refused: %s: %s", len(f.names), m.Code, m.Message))
			eff = s.outcomeEffectLocked(effNone)
		default:
			// Settled like an unregistered (v0.7.1 ii).
			s.unregisterAnsweredLocked(f)
			s.logLocked(slog.LevelWarn, fmt.Sprintf("unregister of %d services answered by %s: %s", len(f.names), m.Code, m.Message))
		}
		if m.Code == AppUnsupportedVersion {
			if f.op == opUnregister {
				s.unregisterAnsweredLocked(f)
			}
			s.changedLocked(effNone)
			s.do(func() { s.deps.fatal(&Error{Code: string(m.Code), Message: m.Message, Fatal: true}) })
			return
		}
		s.pumpLocked()
		s.changedLocked(s.settledEffectLocked(eff))
	}
}

// onReplaced is G6: a drain{replaced} takes over every name.
func (s *groupSession) onReplaced() bool {
	s.mu.Lock()
	defer s.unlockAndFlush()
	if !s.acceptingLocked() {
		return false
	}
	s.takenOver = true
	for _, v := range s.svcs {
		v.takenOver, v.mayHold, v.awaiting = true, false, false
	}
	s.resetConnLocked()
	view := s.viewLocked()
	s.lastView = view
	s.emitLocked(event{kind: evConflict, conflict: Conflict{Reason: "replaced"}, groupServices: view})
	return true
}

// --- local operations (§6A.2, §6A.3) ------------------------------------------

// applyServices is a services-only change: the diff by name against the
// current set, in the new order.
func (s *groupSession) applyServices(list []groupServiceInit) {
	s.mu.Lock()
	defer s.unlockAndFlush()
	if s.endingLocked() {
		return
	}
	next := map[string]bool{}
	var order []string
	var register, unregister []string
	for _, in := range list {
		next[in.name] = true
		order = append(order, in.name)
		v, ok := s.svcs[in.name]
		if !ok {
			if r, wasRemoved := s.removed[in.name]; wasRemoved {
				// Its unregister has not gone out: the edge still holds it.
				delete(s.removed, in.name)
				v = &gsvc{name: in.name, id: r.id, mayHold: r.mayHold, holds: r.holds}
			} else {
				v = &gsvc{name: in.name}
			}
			v.kind, v.app, v.localDisabled = in.kind, in.app, in.localDisabled
			s.svcs[in.name] = v
			if !v.localDisabled {
				v.awaiting = true
				register = append(register, in.name)
			}
			continue
		}
		v.app = in.app
		if v.kind != in.kind {
			v.kind = in.kind
			if s.eligibleLocked(in.name) {
				v.awaiting = true
				register = append(register, in.name)
			}
		}
	}
	for _, name := range s.order {
		if next[name] {
			continue
		}
		v := s.svcs[name]
		delete(s.svcs, name)
		if v.mayHold && s.connected {
			s.removed[name] = v
			unregister = append(unregister, name)
		}
	}
	s.order = order
	if s.connected && !s.takenOver {
		if len(unregister) > 0 {
			s.unregisterLocked(unregister)
		}
		if len(register) > 0 {
			s.registerLocked(register)
		}
	}
	s.changedLocked(effNone)
}

// localDisable is disable(name) (§6A.3).
func (s *groupSession) localDisable(name string) {
	s.mu.Lock()
	defer s.unlockAndFlush()
	v, ok := s.svcs[name]
	if !ok || s.endingLocked() || v.localDisabled {
		return
	}
	v.localDisabled, v.awaiting = true, false
	if v.mayHold && s.connected {
		s.unregisterLocked([]string{name})
	}
	s.changedLocked(effNone)
}

// localEnable is enable(name) (§6A.3): clears a local disable, a
// taken_over and a rejected, and registers the name alone.
func (s *groupSession) localEnable(name string) {
	s.mu.Lock()
	defer s.unlockAndFlush()
	v, ok := s.svcs[name]
	if !ok || s.endingLocked() || s.takenOver {
		return
	}
	v.localDisabled, v.takenOver = false, false
	if v.outcome == outRejected {
		v.outcome, v.code, v.message = outNone, "", ""
	}
	v.awaiting = true
	if s.connected {
		s.registerLocked([]string{name})
	}
	s.changedLocked(effNone)
}

// --- close (§6A.8) ------------------------------------------------------------

// beginCloseAsync unregisters every name the connection may hold, in
// frames of at most 100, one in flight, within one budget (§3.2), then
// runs done. It may go out while a register is outstanding (v0.7.1
// ruling 3).
func (s *groupSession) beginCloseAsync(ctx context.Context, budget time.Duration, done func()) {
	s.mu.Lock()
	defer s.unlockAndFlush()
	if s.endingLocked() {
		s.do(done)
		return
	}
	s.closing = true
	// A frame still in flight (not dropped by OVERLOADED) is answered first.
	s.closePreOp = opNone
	if s.out != nil && !s.out.paused {
		s.closePreOp = s.out.op
	}
	s.stopFrameLocked()
	s.queue = nil
	s.cancelResendsLocked()
	var names []string
	if s.connected && !s.takenOver {
		for _, n := range s.order {
			if s.svcs[n].mayHold {
				names = append(names, n)
			}
		}
		for n, v := range s.removed {
			if v.mayHold {
				names = append(names, n)
			}
		}
	}
	if len(names) == 0 {
		s.do(done)
		return
	}
	sendCtx, cancel := context.WithCancel(ctx)
	s.unregDone, s.unregCancel, s.closeConn = done, cancel, s.conn
	s.closeCtx = context.WithValue(sendCtx, closeFrameKey{}, true)
	s.closeFrames = splitFrames(names)
	s.unregStop = s.deps.after(budget, s.releaseUnreg)
	s.unregStopCtx = context.AfterFunc(ctx, s.releaseUnreg)
	s.sendCloseFrameLocked()
}

func (s *groupSession) sendCloseFrameLocked() {
	ctx, conn, body := s.closeCtx, s.closeConn, EncodeUnregisterList(s.closeFrames[0])
	s.do(func() {
		if err := conn.SendApp(ctx, body); err != nil {
			s.releaseUnreg()
		}
	})
}

// closeAnsweredLocked matches a reply during the close to the frame it
// answers (v0.7.1 ruling 7): the first answers a frame still in flight when
// the close began; after that an unregistered or a whole-frame error op
// answers the current close frame, OVERLOADED re-sends it after the
// backoff, and a registered answers nothing of the close.
func (s *groupSession) closeAnsweredLocked(env Envelope) bool {
	if s.unregDone == nil {
		return false
	}
	if pre := s.closePreOp; pre != opNone {
		// The frame in flight when the close began is answered first: by
		// its own op's reply, or by an error op (which names no frame, so
		// frame order decides). The close keeps waiting for its own reply; a
		// register dropped by OVERLOADED here is abandoned, never re-sent.
		answersPre := false
		switch m := env.(type) {
		case *RegisteredMsg:
			answersPre = pre == opRegister
		case *UnregisteredMsg:
			answersPre = pre == opUnregister
		case *ErrorMsg:
			answersPre = m.Code != AppConnectionLimit
		}
		if answersPre {
			s.closePreOp = opNone
			return true
		}
	}
	switch m := env.(type) {
	case *RegisteredMsg:
		return true // answers no frame of the close
	case *UnregisteredMsg:
	case *ErrorMsg:
		switch m.Code {
		case AppConnectionLimit:
			return true
		case AppOverloaded:
			if s.closeResend == nil {
				s.overloadedN++
				s.closeResend = s.deps.after(registerRetryDelay(s.overloadedN-1), func() {
					s.mu.Lock()
					defer s.unlockAndFlush()
					s.closeResend = nil
					if s.unregDone != nil {
						s.sendCloseFrameLocked()
					}
				})
			}
			return true
		}
	default:
		return false
	}
	s.overloadedN = 0
	s.closeFrames = s.closeFrames[1:]
	if len(s.closeFrames) == 0 {
		s.releaseUnregLocked()
		return true
	}
	s.sendCloseFrameLocked()
	return true
}

func (s *groupSession) releaseUnregLocked() {
	if s.unregDone == nil {
		return
	}
	done := s.unregDone
	s.unregDone, s.closeConn, s.closeFrames = nil, nil, nil
	s.unregCancel()
	s.unregStop()
	s.unregStopCtx()
	if s.closeResend != nil {
		s.closeResend()
		s.closeResend = nil
	}
	s.do(done)
}

func (s *groupSession) releaseUnreg() {
	s.mu.Lock()
	s.releaseUnregLocked()
	s.unlockAndFlush()
}

func (s *groupSession) abortClose() { s.releaseUnreg() }

func (s *groupSession) markClosed() {
	s.mu.Lock()
	defer s.unlockAndFlush()
	s.closing, s.closed = true, true
	s.stopFrameLocked()
	s.cancelResendsLocked()
	s.releaseUnregLocked()
}

func (s *groupSession) closingNow() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closing && !s.closed
}

// --- routing (§4.8) -------------------------------------------------------------

// route selects the service for one stream (§4.8 Precedence with
// selection, rows 3a-3e) and returns its name ("" when none).
func (s *groupSession) route(head *requestHead) (routeResult, string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.endingLocked() {
		return routeResult{unavailable: unavailableClosing}, ""
	}
	v, removed := s.selectLocked(head)
	if v == nil {
		return routeResult{unavailable: unavailableOther}, ""
	}
	if removed {
		return routeResult{unavailable: unavailableOther}, v.name
	}
	switch st := stateOf(v); {
	case st == ServiceTakenOver || s.takenOver:
		return routeResult{reset: true}, v.name
	case st == ServiceDisabled && !v.localDisabled:
		return routeResult{unavailable: unavailableDisabled}, v.name
	case st == ServiceDisabled, st == ServiceRejected, st == ServicePending && !v.everActive:
		return routeResult{unavailable: unavailableOther}, v.name
	}
	return routeResult{app: v.app}, v.name
}

func (s *groupSession) selectLocked(head *requestHead) (*gsvc, bool) {
	if s.deps.trustHeader {
		for _, h := range head.headers {
			if !strings.EqualFold(h.name, ServerIDHeader) {
				continue
			}
			// The first one; no fallback to the Host label (§4.8 step 1).
			for _, name := range s.order {
				if v := s.svcs[name]; v.id != "" && v.id == h.value {
					return v, false
				}
			}
			for _, v := range s.removed {
				if v.id != "" && v.id == h.value {
					return v, true
				}
			}
			return nil, false
		}
	}
	name, ok := serviceFromHost(head.host)
	if !ok {
		return nil, false
	}
	if v, ok := s.svcs[name]; ok {
		return v, false
	}
	if v, ok := s.removed[name]; ok {
		return v, true
	}
	return nil, false
}

// serviceFromHost is §4.8 step 2: the Host's first label split at its last
// '-' into a service name and a username.
func serviceFromHost(host string) (string, bool) {
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	label := strings.ToLower(host)
	if i := strings.IndexByte(label, '.'); i >= 0 {
		label = label[:i]
	}
	i := strings.LastIndexByte(label, '-')
	if i < 0 {
		return "", false
	}
	name, user := label[:i], label[i+1:]
	if !IsValidServiceName(name) || !usernameRE.MatchString(user) {
		return "", false
	}
	return name, true
}

// ServerIDHeader and ServerNameHeader are the edge-added routing headers
// (§4.8). ServerIDHeader selects the service only with
// ServiceGroupOptions.TrustServerIDHeader; ServerNameHeader is never routed
// by.
const (
	ServerIDHeader   = "X-Mcpwarp-Server-Id"
	ServerNameHeader = "X-Mcpwarp-Server-Name"
)
