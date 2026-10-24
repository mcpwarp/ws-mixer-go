package mcpwarp

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/mcpwarp/ws-mixer-go/wsmixer"
)

// Registration is one successful register (§5.6 `registered`): the public
// URL, the server id, and whether this register created the service.
type Registration struct {
	URL      string
	ServerID string
	Created  bool
}

// Conflict describes a take-over (§3.3): a matching `conflict` op
// (Reason is its reason, default "taken_over"), or a ws-mixer drain with
// reason "replaced" (Reason "replaced"). ServerID is "" when no server id is
// known, the Go form of JS's omitted serverId.
type Conflict struct {
	ServerID string
	Name     string
	Reason   string
}

// exposeState is the agent state of §2.7/§5.5.
type exposeState string

const (
	stateConnecting  exposeState = "connecting"
	stateRegistering exposeState = "registering"
	stateRegistered  exposeState = "registered"
	stateDisabled    exposeState = "disabled"
	stateConflict    exposeState = "conflict"
	stateClosing     exposeState = "closing"
	stateClosed      exposeState = "closed"
)

const (
	registerRetryBase  = 1000 * time.Millisecond
	registerRetryCap   = 30 * time.Second
	unregisterBudget   = 2000 * time.Millisecond
	sendAppTimeout     = 5 * time.Second
	registerRetryLabel = "register failed"

	takenOverMessage = "service taken over by another connection"
)

// The /mcp 503 messages (§4.4 row 3).
const (
	unavailableDisabled = "service disabled"
	unavailableClosing  = "service closing"
	unavailableOther    = "service unavailable"
)

// appConn is the part of *wsmixer.Conn the session sends on. Identity
// (interface equality) is what tells one connection from another.
type appConn interface {
	SendApp(ctx context.Context, body any) error
	// Done is closed once the connection has ended (wsmixer.Conn.Done).
	Done() <-chan struct{}
}

func connEnded(c appConn) bool {
	select {
	case <-c.Done():
		return true
	default:
		return false
	}
}

// afterFunc schedules f after d and returns its stop function
// (time.AfterFunc's shape; a test seam for a fake clock).
type afterFunc func(d time.Duration, f func()) (stop func() bool)

func realAfterFunc(d time.Duration, f func()) func() bool { return time.AfterFunc(d, f).Stop }

type eventKind int

const (
	evState eventKind = iota
	evRegistered
	evDisabled
	evEnabled
	evConflict
	evError
	evRetry
	evDisconnect
	evClose
)

// errorOrigin says where a non-fatal error event came from; "" is a handler
// or response failure from the bridge.
type errorOrigin string

const (
	originMalformed         errorOrigin = "malformed"
	originRegisterTransient errorOrigin = "register_transient"
	originRegisterRefusal   errorOrigin = "register_refusal"
	originApp               errorOrigin = "app"
)

// retryInfo is one scheduled retry (§5.9 onRetry payload).
type retryInfo struct {
	Attempt int
	Delay   time.Duration
	Reason  string
}

// event is what the session and agent report to the SDK profile above them
// (expose()'s handle events, the tunnel controller's inputs).
type event struct {
	kind         eventKind
	state        exposeState              // evState
	registration Registration             // evRegistered
	serverID     string                   // evDisabled, evEnabled
	reason       string                   // evDisabled
	refused      bool                     // evDisabled: a SERVER_DISABLED register refusal, not a disable op
	conflict     Conflict                 // evConflict
	err          error                    // evError, evClose (nil after a plain close)
	origin       errorOrigin              // evError
	retry        retryInfo                // evRetry
	retryKind    string                   // evRetry: "register" or "overloaded"
	disconnect   wsmixer.DisconnectReason // evDisconnect
	heldDisable  bool                     // evRegistered: a held disable's evDisabled follows at once (§5.9)
}

type outstandingOp int

const (
	opNone outstandingOp = iota
	opRegister
	opUnregister
)

type sessionDeps struct {
	name       string
	kind       ServiceKind
	after      afterFunc
	emit       func(event) // never nil
	fatal      func(*Error)
	activeConn func() appConn // the client's current connection, or nil; may itself be nil
}

// session is the registration state machine for one exposed service (§2.7,
// §3.2, §3.3), independent of sockets and clocks: connections arrive through
// onWelcome/onConnEnd/onApp, timers through deps.after. It re-registers on
// every welcome until a take-over makes it sticky in `conflict`.
//
// Every method takes mu, queues its sends and events, and runs the queue in
// order with mu released (unlockAndFlush), so no callback or SendApp ever
// runs with mu held, and a callback may call back into the session.
type session struct {
	deps sessionDeps

	mu    sync.Mutex
	state exposeState

	url          string
	serverID     string
	hasServerID  bool
	registration *Registration
	everRegister bool
	takenOver    bool
	heldDisables map[string]*DisableMsg // this connection's disables for ids not known, by id

	conn             appConn // the latest welcomed connection
	connected        bool
	sentRegisterConn bool // a register went out on conn
	outstanding      outstandingOp

	transientFailures int
	overloadedResends int  // consecutive OVERLOADED, the re-send backoff's n (v0.7.1 ruling 1)
	registerQueued    bool // a register asked for while one was in flight goes out after its reply (ruling 3)
	closePreReply     bool // closing: the reply to the register in flight when the close began is still due
	retryStop         func() bool
	retryGen          int
	overloadStop      func() bool
	overloadGen       int

	// The running unregister wait (beginCloseAsync), if any.
	unregDone    func()
	unregConn    appConn
	unregCancel  context.CancelFunc
	unregStop    func() bool
	unregStopCtx func() bool

	queue    []func()
	flushing bool
}

func newSession(deps sessionDeps) *session {
	if deps.after == nil {
		deps.after = realAfterFunc
	}
	if deps.emit == nil {
		deps.emit = func(event) {}
	}
	if deps.fatal == nil {
		deps.fatal = func(*Error) {}
	}
	return &session{deps: deps, state: stateConnecting}
}

// unlockAndFlush runs everything queued, in order, with mu released, then
// releases mu. While one goroutine is running the queue, any other caller
// (including a queued callback re-entering the session) only appends to it;
// the running one picks that up before it stops.
func (s *session) unlockAndFlush() {
	if s.flushing {
		s.mu.Unlock()
		return
	}
	s.flushing = true
	for len(s.queue) > 0 {
		q := s.queue
		s.queue = nil
		s.mu.Unlock()
		for _, f := range q {
			f()
		}
		s.mu.Lock()
	}
	s.flushing = false
	s.mu.Unlock()
}

func (s *session) emitLocked(ev event) { s.queue = append(s.queue, func() { s.deps.emit(ev) }) }

func (s *session) setStateLocked(next exposeState) {
	if s.state == next {
		return
	}
	s.state = next
	s.emitLocked(event{kind: evState, state: next})
}

func (s *session) endingLocked() bool { return s.state == stateClosing || s.state == stateClosed }

// supersededLocked reports whether c is no longer the connection whose
// messages and end count (§2.7, §5.9): a later connection has received
// welcome, either as seen by the session or by the client. nil is a
// connection the agent has not bound yet, which is always the newest one.
func (s *session) supersededLocked(c appConn) bool {
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

// onWelcome: c received welcome and is the client's active connection.
func (s *session) onWelcome(c appConn) {
	s.mu.Lock()
	defer s.unlockAndFlush()
	if s.endingLocked() {
		return
	}
	s.conn = c
	s.connected = true
	s.sentRegisterConn = false
	s.outstanding, s.registerQueued = opNone, false
	// A held disable belongs to the connection it arrived on; a drain
	// replacement's welcome clears it too (v0.7.1 ruling 8).
	s.heldDisables = nil
	s.cancelTimersLocked()
	if s.takenOver {
		return
	}
	s.setStateLocked(stateRegistering)
	s.sendRegisterLocked()
}

// onConnEnd: c ended. Ignored once a later connection received welcome.
func (s *session) onConnEnd(c appConn) {
	s.mu.Lock()
	defer s.unlockAndFlush()
	if s.unregConn == c {
		s.releaseUnregLocked()
	}
	if s.supersededLocked(c) || s.endingLocked() {
		return
	}
	s.connected = false
	s.sentRegisterConn = false
	s.outstanding, s.registerQueued = opNone, false
	s.heldDisables = nil
	s.cancelTimersLocked()
	if !s.takenOver {
		s.setStateLocked(stateConnecting)
	}
}

// onApp handles one `app` body that arrived on c (nil: not yet bound).
func (s *session) onApp(c appConn, body []byte) {
	s.mu.Lock()
	defer s.unlockAndFlush()
	if s.supersededLocked(c) {
		return
	}
	env, err := DecodeEnvelope(body)
	if err != nil {
		if e, ok := err.(*Error); ok {
			e.ServiceName = s.deps.name
		}
		s.emitLocked(event{kind: evError, err: err, origin: originMalformed})
		return
	}
	// §2.7 Ended connection: registered, disable and enable delivered after
	// their connection ended (queued messages flushed after the close) are
	// ignored; conflict and error still apply. Decided by the connection's
	// own Done: onConnEnd runs on a Done watcher, unordered against the
	// disconnect report and those flushed deliveries.
	ended := c != nil && connEnded(c)
	switch env.(type) {
	case *RegisteredMsg, *DisableMsg, *EnableMsg:
		if ended {
			return
		}
	}
	switch m := env.(type) {
	case *RegisteredMsg:
		s.onRegisteredLocked(m)
		s.flushQueuedRegisterLocked()
	case *UnregisteredMsg:
		match := s.outstanding == opUnregister
		for _, svc := range m.Services {
			if svc.Name == s.deps.name {
				match = true
			}
		}
		if match {
			if s.outstanding == opUnregister {
				s.settleLocked()
			}
			s.releaseUnregLocked()
		}
	case *DisableMsg:
		if s.endingLocked() || s.takenOver {
			return
		}
		if s.hasServerID && m.ID == s.serverID {
			s.disableLocked(m)
			return
		}
		// A disable carries an id only, no name, and the edge fans it out to
		// every connection of the owner: an unknown id, before or after a
		// server id is known, may be another service's. Hold it until a
		// registered brings that id (v0.6.3 §2.7).
		held := *m
		if s.heldDisables == nil {
			s.heldDisables = map[string]*DisableMsg{}
		}
		s.heldDisables[m.ID] = &held
	case *EnableMsg:
		if s.endingLocked() || s.takenOver || m.Name != s.deps.name {
			return
		}
		s.serverID, s.hasServerID = m.ID, true
		s.heldDisables = nil // the enable came last
		s.emitLocked(event{kind: evEnabled, serverID: m.ID})
		s.setStateLocked(stateRegistering)
		if s.connected {
			s.registerLocked()
		}
	case *ConflictMsg:
		if s.endingLocked() || s.takenOver {
			return
		}
		if m.Name != s.deps.name && (!s.hasServerID || m.ID != s.serverID) {
			return
		}
		s.takeOverLocked(Conflict{ServerID: m.ID, Name: m.Name, Reason: m.Reason})
	case *ErrorMsg:
		s.onAppErrorLocked(m)
	}
}

// disableLocked applies a matching disable: state disabled, timers stopped.
func (s *session) disableLocked(m *DisableMsg) {
	s.cancelTimersLocked()
	s.setStateLocked(stateDisabled)
	s.emitLocked(event{kind: evDisabled, serverID: m.ID, reason: m.Reason})
}

// onReplaced handles a ws-mixer drain with reason "replaced" (§3.3): a
// take-over. False when it changes nothing (closing, or already taken over).
func (s *session) onReplaced() bool {
	s.mu.Lock()
	defer s.unlockAndFlush()
	if s.endingLocked() || s.takenOver {
		return false
	}
	c := Conflict{Name: s.deps.name, Reason: "replaced"}
	if s.hasServerID {
		c.ServerID = s.serverID
	}
	s.takeOverLocked(c)
	return true
}

func (s *session) onRegisteredLocked(m *RegisteredMsg) {
	if s.state == stateClosing {
		// It answers the register in flight when the close began, if any;
		// never the unregister: the close keeps waiting for its own reply.
		s.closePreReply = false
		return
	}
	if s.endingLocked() || s.takenOver {
		return
	}
	ours := false
	for _, svc := range m.Services {
		ours = ours || svc.Name == s.deps.name
	}
	for _, e := range m.Errors {
		ours = ours || e.Name == s.deps.name
	}
	paused := s.outstanding == opRegister && s.overloadStop != nil
	if !ours {
		// §2.3 Empty reply: it answers the outstanding register as an
		// all-transient reply, re-sent after the backoff; no handle state
		// changes. During an OVERLOADED pause the dropped frame is not on the
		// wire, so it answers nothing (spec v0.8.0 §2.6).
		if s.outstanding == opRegister && !paused {
			s.settleLocked()
			s.scheduleRegisterRetryLocked(string(RegisterInternal), "unlisted in reply")
		}
		return
	}
	// A registered naming the service answers the outstanding register, an
	// OVERLOADED-paused one included (the re-queued frame counts as answered:
	// settled, re-send cancelled, OVERLOADED counter reset). With nothing
	// outstanding it is still applied (§2.7) but leaves the OVERLOADED
	// counter alone; the transient counter follows §3.2 either way.
	answers := s.outstanding == opRegister
	for _, svc := range m.Services {
		if svc.Name != s.deps.name {
			continue
		}
		if answers {
			s.settleLocked()
		}
		s.transientFailures = 0
		s.cancelTimersLocked()
		s.url = svc.URL
		s.serverID, s.hasServerID = svc.ID, true
		s.everRegister = true
		held := s.heldDisables[svc.ID]
		delete(s.heldDisables, svc.ID)
		reg := Registration{URL: svc.URL, ServerID: svc.ID, Created: svc.Created}
		s.registration = &reg
		s.setStateLocked(stateRegistered)
		s.emitLocked(event{kind: evRegistered, registration: reg, heldDisable: held != nil})
		if held != nil {
			// §5.9 Held disable: registered, then the held disable, back to
			// back; the tunnel collapses the pair into one disabled snapshot
			// (T50, tunnel/42_disable_unknown_id_held).
			s.disableLocked(held)
		}
		return
	}
	for _, e := range m.Errors {
		if e.Name != s.deps.name {
			continue
		}
		if answers {
			s.settleLocked()
		}
		if !IsTransientRegisterError(e.Code) {
			s.transientFailures = 0 // a non-transient entry is a successful reply (§3.2)
		}
		switch {
		case e.Code == RegisterServerDisabled:
			if e.ID != "" {
				s.serverID, s.hasServerID = e.ID, true
			}
			s.cancelTimersLocked()
			s.setStateLocked(stateDisabled)
			s.emitLocked(event{kind: evDisabled, serverID: s.serverID, reason: "disabled", refused: true})
		case IsTransientRegisterError(e.Code):
			s.emitLocked(event{kind: evError, err: &Error{Code: string(e.Code), Message: e.Message, ServiceName: s.deps.name}, origin: originRegisterTransient})
			s.scheduleRegisterRetryLocked(string(e.Code), e.Message)
		case !s.everRegister:
			s.fatalLocked(&Error{Code: string(e.Code), Message: e.Message, Fatal: true, ServiceName: s.deps.name})
		default:
			s.emitLocked(event{kind: evError, err: &Error{Code: string(e.Code), Message: e.Message, ServiceName: s.deps.name}, origin: originRegisterRefusal})
		}
		return
	}
}

// scheduleRegisterRetryLocked re-sends the register after §3.2's
// transient backoff, on this connection only.
func (s *session) scheduleRegisterRetryLocked(code, msg string) {
	delay := registerRetryDelay(s.transientFailures)
	s.transientFailures++
	s.emitLocked(event{kind: evRetry, retryKind: "register", retry: retryInfo{
		Attempt: s.transientFailures, Delay: delay,
		Reason: fmt.Sprintf("%s: %s: %s", registerRetryLabel, code, msg),
	}})
	s.clearRetryLocked()
	s.retryGen++
	gen, conn := s.retryGen, s.conn
	s.retryStop = s.deps.after(delay, func() {
		s.mu.Lock()
		defer s.unlockAndFlush()
		if gen != s.retryGen {
			return
		}
		s.retryStop = nil
		if !s.endingLocked() && !s.takenOver && s.state != stateDisabled && s.connected && s.conn == conn {
			s.registerLocked()
		}
	})
}

// settleLocked ends the outstanding frame: its OVERLOADED re-send is
// cancelled (v0.7.1 ruling 5) and the OVERLOADED streak reset (ruling 1).
func (s *session) settleLocked() {
	s.outstanding = opNone
	s.overloadedResends = 0
	if s.overloadStop != nil {
		s.overloadStop()
		s.overloadStop = nil
	}
	s.overloadGen++
}

// registerLocked sends a register, one in flight (§2.6): while the
// outstanding register waits out an OVERLOADED pause it folds into that
// re-send (ruling 6a); while one is in flight it goes out after its reply
// (ruling 3).
func (s *session) registerLocked() {
	switch {
	case s.outstanding == opRegister && s.overloadStop != nil:
	case s.outstanding != opNone:
		s.registerQueued = true
	default:
		s.sendRegisterLocked()
	}
}

func (s *session) flushQueuedRegisterLocked() {
	if !s.registerQueued || s.outstanding != opNone {
		return
	}
	s.registerQueued = false
	if s.connected && !s.endingLocked() && !s.takenOver && s.retryStop == nil {
		s.sendRegisterLocked()
	}
}

// registerRetryDelay is §3.2's min(1000 × 2^n, 30000) ms.
func registerRetryDelay(n int) time.Duration {
	d := registerRetryBase
	for i := 0; i < n && d < registerRetryCap; i++ {
		d *= 2
	}
	if d > registerRetryCap {
		d = registerRetryCap
	}
	return d
}

func (s *session) onAppErrorLocked(m *ErrorMsg) {
	switch m.Code {
	case AppOverloaded:
		if s.closePreReply {
			// It drops the register in flight when the close began: that
			// register is abandoned, never re-sent; the close waits on.
			s.closePreReply = false
			return
		}
		op := s.outstanding
		if op == opNone || s.overloadStop != nil {
			return
		}
		// The frame was dropped; its slot stays reserved for the re-send
		// after min(1000 × 2^(n−1), 30000) ms, n counting consecutive
		// OVERLOADED only (v0.7.1 rulings 1, 6a).
		s.overloadedResends++
		delay := registerRetryDelay(s.overloadedResends - 1)
		s.overloadGen++
		gen, conn := s.overloadGen, s.conn
		s.overloadStop = s.deps.after(delay, func() {
			s.mu.Lock()
			defer s.unlockAndFlush()
			if gen != s.overloadGen {
				return
			}
			s.overloadStop = nil
			if !s.connected || s.conn != conn {
				return
			}
			// Only if that op is still unanswered: a reply inside the pause ends it.
			if s.outstanding != op {
				return
			}
			switch {
			case op == opRegister && !s.endingLocked() && !s.takenOver && s.state != stateDisabled:
				s.registerQueued = false // folded into this re-send
				s.sendRegisterLocked()
			case op == opUnregister && s.state == stateClosing:
				name := s.deps.name
				s.queue = append(s.queue, func() { _ = sendApp(conn, EncodeUnregister(name)) })
			}
		})
		if op == opRegister && !s.endingLocked() {
			s.emitLocked(event{kind: evRetry, retryKind: "overloaded", retry: retryInfo{
				Attempt: s.overloadedResends, Delay: delay,
				Reason: fmt.Sprintf("%s: %s: %s", registerRetryLabel, m.Code, m.Message),
			}})
		}
	case AppConnectionLimit:
		// Names no frame; the 4014 close follows.
		s.emitLocked(event{kind: evError, err: &Error{Code: string(m.Code), Message: m.Message, ServiceName: s.deps.name}, origin: originApp})
	default:
		// v0.7.1 ruling 4: any other code answers the outstanding frame,
		// a close's unregister included (ruling 7).
		switch op := s.outstanding; {
		case s.closePreReply:
			s.closePreReply = false // it answers the register in flight when the close began
		case op != opNone && s.overloadStop != nil:
			// During an OVERLOADED pause it answers nothing, a close's
			// unregister included: the dropped frame is not on the wire; counter,
			// re-send and the close's wait stay (spec v0.8.0 §2.6).
		case op != opNone:
			s.settleLocked()
			if op == opUnregister {
				s.releaseUnregLocked()
			}
		}
		if m.Code == AppUnsupportedVersion {
			s.fatalLocked(&Error{Code: string(m.Code), Message: m.Message, Fatal: true, ServiceName: s.deps.name})
			return
		}
		s.emitLocked(event{kind: evError, err: &Error{Code: string(m.Code), Message: m.Message, ServiceName: s.deps.name}, origin: originApp})
		s.flushQueuedRegisterLocked()
	}
}

// fatalLocked queues deps.fatal: the agent closes the client and ends.
func (s *session) fatalLocked(err *Error) {
	s.queue = append(s.queue, func() { s.deps.fatal(err) })
}

func (s *session) takeOverLocked(c Conflict) {
	s.takenOver = true
	s.outstanding = opNone
	// The edge no longer holds the name for this connection: nothing to unregister.
	s.sentRegisterConn = false
	s.cancelTimersLocked()
	s.setStateLocked(stateConflict)
	s.emitLocked(event{kind: evConflict, conflict: c})
}

func (s *session) sendRegisterLocked() {
	s.outstanding = opRegister
	s.sentRegisterConn = true
	conn, body := s.conn, EncodeRegister(s.deps.name, s.deps.kind)
	// A failed send is not reported: the connection is gone and the next welcome registers anyway.
	s.queue = append(s.queue, func() { _ = sendApp(conn, body) })
}

func sendApp(c appConn, body any) error {
	if c == nil {
		return fmt.Errorf("mcpwarp: no connection")
	}
	ctx, cancel := context.WithTimeout(context.Background(), sendAppTimeout)
	defer cancel()
	return c.SendApp(ctx, body)
}

func (s *session) clearRetryLocked() {
	if s.retryStop != nil {
		s.retryStop()
		s.retryStop = nil
	}
	s.retryGen++
}

func (s *session) cancelTimersLocked() {
	s.clearRetryLocked()
	if s.overloadStop != nil {
		s.overloadStop()
		s.overloadStop = nil
	}
	s.overloadGen++
}

// releaseUnregLocked ends the unregister wait, if one is running: the send
// is cancelled, the budget timer stopped, the close continuation queued.
func (s *session) releaseUnregLocked() {
	if s.unregDone == nil {
		return
	}
	done := s.unregDone
	s.unregDone, s.unregConn = nil, nil
	s.unregCancel()
	s.unregStop()
	s.unregStopCtx()
	s.queue = append(s.queue, done)
}

func (s *session) releaseUnreg() {
	s.mu.Lock()
	s.releaseUnregLocked()
	s.unlockAndFlush()
}

// beginCloseAsync is §3.2's Unregister row: state closing, timers
// cancelled; if a register went out on the live connection, send unregister
// and wait for `unregistered`, the budget, a failed send, the connection's
// end, ctx or abortClose, whichever comes first; then run done. ctx also
// bounds the send itself, so a full control queue cannot outlast it. done
// always runs exactly once (at once when already closing or closed).
func (s *session) beginCloseAsync(ctx context.Context, budget time.Duration, done func()) {
	s.mu.Lock()
	if s.endingLocked() {
		s.queue = append(s.queue, done)
		s.unlockAndFlush()
		return
	}
	wasRegistered := s.connected && s.sentRegisterConn
	conn := s.conn
	// A register still in flight (not dropped by OVERLOADED) gets the next reply.
	s.closePreReply = s.outstanding == opRegister && s.overloadStop == nil
	s.setStateLocked(stateClosing)
	s.cancelTimersLocked()
	if !wasRegistered {
		s.queue = append(s.queue, done)
		s.unlockAndFlush()
		return
	}
	sendCtx, cancel := context.WithCancel(ctx)
	s.unregDone, s.unregCancel, s.unregConn = done, cancel, conn
	s.outstanding = opUnregister
	s.unregStop = s.deps.after(budget, s.releaseUnreg)
	s.unregStopCtx = context.AfterFunc(ctx, s.releaseUnreg)
	name := s.deps.name
	s.queue = append(s.queue, func() {
		if err := conn.SendApp(sendCtx, EncodeUnregister(name)); err != nil {
			s.releaseUnreg()
		}
	})
	s.unlockAndFlush()
}

// abortClose ends a running unregister wait now.
func (s *session) abortClose() { s.releaseUnreg() }

// beginClose is beginCloseAsync, returning once the wait is over.
func (s *session) beginClose(ctx context.Context, budget time.Duration) {
	ch := make(chan struct{})
	s.beginCloseAsync(ctx, budget, func() { close(ch) })
	<-ch
}

// markClosed ends the session for good.
func (s *session) markClosed() {
	s.mu.Lock()
	defer s.unlockAndFlush()
	s.cancelTimersLocked()
	s.releaseUnregLocked()
	s.setStateLocked(stateClosed)
}

func (s *session) currentState() exposeState {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state
}

// snapshot returns the handle-visible values (§5.5): url, server id (ok
// false when none is known) and the latest registration.
func (s *session) snapshot() (url, serverID string, hasServerID bool, reg *Registration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.url, s.serverID, s.hasServerID, s.registration
}

// availability is what /mcp gets in the current state (§4.4 rows 0 and 3):
// conflict → RESET; a non-empty unavailable message → 503 with it.
func (s *session) availability() (conflict bool, unavailable string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch s.state {
	case stateConflict:
		return true, ""
	case stateDisabled:
		return false, unavailableDisabled
	case stateClosing, stateClosed:
		return false, unavailableClosing
	case stateConnecting, stateRegistering, stateRegistered:
		return false, ""
	default:
		return false, unavailableOther
	}
}
