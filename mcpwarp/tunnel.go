package mcpwarp

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/mcpwarp/ws-mixer-go/wsmixer"
)

// TunnelState is a TunnelStatus state (§6.5).
type TunnelState string

const (
	StateOff        TunnelState = "off"
	StateConnecting TunnelState = "connecting"
	StateOnline     TunnelState = "online"
	StateDisabled   TunnelState = "disabled"
	StateConflict   TunnelState = "conflict"
	StateError      TunnelState = "error"
)

// StatusCode is a TunnelStatus code (§6.5, 13 values).
type StatusCode string

const (
	CodeUnauthorized       StatusCode = "UNAUTHORIZED"
	CodeTakenOver          StatusCode = "TAKEN_OVER"
	CodeDisconnected       StatusCode = "DISCONNECTED"
	CodeRetrying           StatusCode = "RETRYING"
	CodeDisabled           StatusCode = "DISABLED"
	CodeInvalidConfig      StatusCode = "INVALID_CONFIG"
	CodeInvalidName        StatusCode = "INVALID_NAME"
	CodeUsernameRequired   StatusCode = "USERNAME_REQUIRED"
	CodeQuotaExceeded      StatusCode = "QUOTA_EXCEEDED"
	CodeConflict           StatusCode = "CONFLICT"
	CodeUnsupportedVersion StatusCode = "UNSUPPORTED_VERSION"
	CodeDisconnectedFatal  StatusCode = "DISCONNECTED_FATAL"
	CodeInternal           StatusCode = "INTERNAL"
)

// TunnelStatus is the controller's one status (§6.4). An empty field is
// absent: URL is set only while online, Code and LastError together.
type TunnelStatus struct {
	State     TunnelState
	URL       string
	Code      StatusCode
	LastError string
}

// TunnelOptions configures NewTunnel (§6.1). Integration, Agent and the
// Handler/Target choice are validated by NewTunnel; the rest when a
// connection starts.
type TunnelOptions struct {
	Integration         string
	Agent               Agent
	WebSocket           *http.Client
	MaxRequestBodyBytes int64
	Reconnect           ReconnectOptions
	// OnRetry is called for retries of the current connection only.
	OnRetry       func(RetryInfo)
	Handler       http.Handler
	Target        string
	TargetTimeout time.Duration
	// Logger receives §6.9's lines (never the token); nil is silent.
	Logger *slog.Logger

	// Test seams: the clock, the transport under each agent, and where the
	// apply pump runs (a goroutine by default).
	after        afterFunc
	newTransport func(transportConfig) transport
	schedule     func(func())
}

// TunnelConfig is what may change at runtime (§6.2). Not Enabled, or a
// Token that trims to "", is off whatever the rest holds. Token is sent
// trimmed. WebSocket overrides TunnelOptions.WebSocket; it is compared by
// identity.
type TunnelConfig struct {
	Enabled   bool
	Token     string
	Name      string
	URL       string
	Kind      ServiceKind
	WebSocket *http.Client
}

// Tunnel is the long-lived controller of §6: configure it any number of
// times, read one status.
type Tunnel struct {
	opts   TunnelOptions
	target *url.URL

	mu        sync.Mutex
	status    TunnelStatus
	listeners map[uint64]func(TunnelStatus)
	nextID    uint64

	desired      *TunnelConfig // the latest config; nil after stop, a conflict, an error
	pendingApply bool          // the pump has an apply to run
	applied      *TunnelConfig // applied, waiting for the previous connections to close
	gen          int
	active       *tunnelGen
	draining     int // connections still closing
	idle         chan struct{}

	disposed     bool
	disposeDone  chan struct{}
	disposeTimer func() bool

	queue      []func()
	flushing   bool
	callbacks  []func()
	delivering bool
}

type tunnelGen struct {
	id  int
	a   *agent
	cfg *TunnelConfig
}

// Constant lastError strings (§6.6).
const (
	lastRetryDial     = "Can't reach mcpwarp; retrying."
	lastRetryRegister = "mcpwarp couldn't register the service; retrying."
	lastLost          = "Connection lost; retrying."
	lastTakenOver     = "Another connection took over this name."
	lastDisabled      = "This service is disabled in the mcpwarp dashboard."
	lastRejected      = "The token was rejected."
	lastForbidden     = "The token is not allowed to publish this service."
	lastInvalidName   = "The service name is invalid: use lowercase letters, digits and dashes (not at the start or end), at most 30 characters."
	lastInvalidConfig = "The tunnel settings are invalid."
	lastUsername      = "Choose a username in your mcpwarp account first."
	lastQuota         = "Your mcpwarp account has no services left in its quota."
	lastConflict      = "The name is in use by another server."
	lastUnsupported   = "This SDK version is no longer supported; please update."
	lastFatal         = "The connection to mcpwarp failed."
	lastInternal      = "Something went wrong; the log has the details."
)

const defaultDisposeTimeout = 5 * time.Second

// NewTunnel creates a stopped ("off") tunnel. It fails for an invalid
// Integration, Agent or Handler/Target choice (errors.Is ErrInvalidOptions);
// problems with a config surface as state "error" instead.
func NewTunnel(opts TunnelOptions) (*Tunnel, error) {
	target, err := validateAdapter(opts.Handler, opts.Target, opts.TargetTimeout)
	if err != nil {
		return nil, err
	}
	if err := validateIntegration(opts.Integration); err != nil {
		return nil, err
	}
	if err := validateAgent(opts.Agent); err != nil {
		return nil, err
	}
	if opts.after == nil {
		opts.after = realAfterFunc
	}
	if opts.schedule == nil {
		opts.schedule = func(f func()) { go f() }
	}
	t := &Tunnel{opts: opts, target: target, status: TunnelStatus{State: StateOff}, listeners: map[uint64]func(TunnelStatus){}}
	t.idle = make(chan struct{})
	return t, nil
}

// unlockAndFlush runs the queued notifications, log lines and agent calls
// in order with mu released (re-entrant like the session's).
//
// User callbacks -- status listeners, OnRetry, the Logger -- are a second
// queue, delivered in order only once no flush is running on any frame and
// with mu released: a callback that calls Configure, Stop or Dispose then
// flushes that call's work itself instead of queueing it behind the
// callback it is running in.
func (t *Tunnel) unlockAndFlush() {
	if t.flushing {
		t.mu.Unlock()
		return
	}
	t.runQueueLocked()
	if t.delivering {
		t.mu.Unlock()
		return
	}
	t.delivering = true
	for len(t.callbacks) > 0 {
		cbs := t.callbacks
		t.callbacks = nil
		t.mu.Unlock()
		for _, f := range cbs {
			f()
		}
		t.mu.Lock()
		if !t.flushing {
			t.runQueueLocked()
		}
	}
	t.delivering = false
	t.mu.Unlock()
}

// runQueueLocked runs the work queue to empty, mu released while each item runs.
func (t *Tunnel) runQueueLocked() {
	t.flushing = true
	for len(t.queue) > 0 {
		q := t.queue
		t.queue = nil
		t.mu.Unlock()
		for _, f := range q {
			f()
		}
		t.mu.Lock()
	}
	t.flushing = false
}

func (t *Tunnel) do(f func()) { t.queue = append(t.queue, f) }

// callback queues a user callback (see unlockAndFlush).
func (t *Tunnel) callback(f func()) { t.callbacks = append(t.callbacks, f) }

func (t *Tunnel) logLocked(level slog.Level, msg string) {
	if l := t.opts.Logger; l != nil {
		t.callback(func() {
			defer func() { _ = recover() }()
			l.Log(context.Background(), level, msg)
		})
	}
}

// Status returns a copy of the current status.
func (t *Tunnel) Status() TunnelStatus {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.status
}

// Subscribe calls l with the current status, then on every change, and
// returns the function that unsubscribes it. After Dispose, l gets one
// {off} and is not kept. The first call goes through the same single
// deliverer as every change, queued at registration: l sees each status
// once, in order, never on two goroutines at once. With no delivery in
// progress it runs before Subscribe returns; otherwise (another goroutine
// delivering, or Subscribe called from inside a callback) the deliverer
// runs it right after its current callback, still before any later status.
func (t *Tunnel) Subscribe(l func(TunnelStatus)) (unsubscribe func()) {
	t.mu.Lock()
	s := t.status
	disposed := t.disposed
	var id uint64
	if !disposed {
		t.nextID++
		id = t.nextID
		t.listeners[id] = l
	}
	t.callback(func() { t.call(l, s) })
	t.unlockAndFlush()
	if disposed {
		return func() {}
	}
	return func() {
		t.mu.Lock()
		delete(t.listeners, id)
		t.mu.Unlock()
	}
}

func (t *Tunnel) call(l func(TunnelStatus), s TunnelStatus) {
	defer func() {
		if p := recover(); p != nil {
			t.mu.Lock()
			t.logLocked(slog.LevelError, fmt.Sprintf("status listener threw: %v", p))
			t.unlockAndFlush()
		}
	}()
	l(s)
}

// setStatusLocked delivers next to every listener unless it equals the
// current status (§6.4 dedup).
func (t *Tunnel) setStatusLocked(next TunnelStatus) {
	if next == t.status {
		return
	}
	t.status = next
	ls := make([]func(TunnelStatus), 0, len(t.listeners))
	for _, l := range t.sortedListeners() {
		ls = append(ls, l)
	}
	t.callback(func() {
		for _, l := range ls {
			t.call(l, next)
		}
	})
}

func (t *Tunnel) sortedListeners() []func(TunnelStatus) {
	out := make([]func(TunnelStatus), 0, len(t.listeners))
	for id := uint64(1); id <= t.nextID; id++ {
		if l, ok := t.listeners[id]; ok {
			out = append(out, l)
		}
	}
	return out
}

func problemStatus(state TunnelState, code StatusCode, lastError string) TunnelStatus {
	return TunnelStatus{State: state, Code: code, LastError: lastError}
}

// Configure applies config (§6.2). It never fails for a bad config (that is
// state "error"); it returns ctx's error only when ctx ended first. It
// returns once the previous connection has stopped and the new one has
// started dialing, not when it is online. Calls apply in order and the last
// one wins; a config equal to the current one (trimmed token, effective
// WebSocket by identity) changes nothing.
func (t *Tunnel) Configure(ctx context.Context, config TunnelConfig) error {
	return wait(ctx, t.configureAsync(config))
}

// Stop stops the connection and goes off; a later Configure starts again,
// even with the same config. It returns once the connection has closed.
func (t *Tunnel) Stop(ctx context.Context) error {
	return wait(ctx, t.stopAsync())
}

// Dispose is final (§6.3): status off delivered, listeners dropped, the
// connection stopped; it waits for that close until ctx's deadline (5 s
// when ctx has none), then logs that it gave up and returns while the close
// goes on in the background. Every call returns once the first one has.
func (t *Tunnel) Dispose(ctx context.Context) error {
	timeout := defaultDisposeTimeout
	if dl, ok := ctx.Deadline(); ok {
		timeout = time.Until(dl)
	}
	done := t.disposeAsync(timeout)
	select {
	case <-done:
	case <-ctx.Done():
	}
	return nil
}

func wait(ctx context.Context, ch <-chan struct{}) error {
	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func closedChan() chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}

func willStart(c *TunnelConfig) bool { return c.Enabled && strings.TrimSpace(c.Token) != "" }

func (t *Tunnel) effectiveWebSocket(c *TunnelConfig) *http.Client {
	if c.WebSocket != nil {
		return c.WebSocket
	}
	return t.opts.WebSocket
}

func (t *Tunnel) sameConfig(a *TunnelConfig, b TunnelConfig) bool {
	return a != nil && a.Enabled == b.Enabled && strings.TrimSpace(a.Token) == strings.TrimSpace(b.Token) &&
		a.Name == b.Name && a.URL == b.URL && a.Kind == b.Kind && t.effectiveWebSocket(a) == t.effectiveWebSocket(&b)
}

// idleLocked: no apply pending, none waiting, nothing closing.
func (t *Tunnel) idleLocked() bool { return !t.pendingApply && t.applied == nil && t.draining == 0 }

func (t *Tunnel) waiterLocked() <-chan struct{} {
	if t.idleLocked() {
		return closedChan()
	}
	return t.idle
}

func (t *Tunnel) checkIdleLocked() {
	if !t.idleLocked() {
		return
	}
	close(t.idle)
	t.idle = make(chan struct{})
	if t.disposeDone != nil && t.disposeTimer != nil {
		t.disposeTimer()
		t.disposeTimer = nil
		close(t.disposeDone)
	}
}

func (t *Tunnel) configureAsync(config TunnelConfig) <-chan struct{} {
	t.mu.Lock()
	defer t.unlockAndFlush()
	if t.disposed {
		return closedChan()
	}
	if t.sameConfig(t.desired, config) {
		return t.waiterLocked()
	}
	c := config
	t.desired = &c
	if !t.pendingApply {
		t.pendingApply = true
		t.do(func() { t.opts.schedule(t.pump) })
	}
	return t.idle
}

// pump runs the latest pending apply: the current connection is stopped,
// the status set, and the config started once every earlier connection has
// closed. Superseded configs never get here, so they never dial.
func (t *Tunnel) pump() {
	t.mu.Lock()
	defer t.unlockAndFlush()
	if !t.pendingApply {
		return
	}
	t.pendingApply = false
	cfg := t.desired
	if t.disposed || cfg == nil {
		t.checkIdleLocked()
		return
	}
	t.detachLocked()
	if willStart(cfg) {
		t.setStatusLocked(TunnelStatus{State: StateConnecting})
	} else {
		t.setStatusLocked(TunnelStatus{State: StateOff})
	}
	t.applied = cfg
	t.maybeStartLocked()
}

func (t *Tunnel) maybeStartLocked() {
	if t.applied == nil || t.draining > 0 {
		t.checkIdleLocked()
		return
	}
	cfg := t.applied
	t.applied = nil
	if !t.disposed && cfg == t.desired {
		if willStart(cfg) {
			t.startLocked(cfg)
		} else {
			t.logLocked(slog.LevelInfo, "off")
		}
	}
	t.checkIdleLocked()
}

func (t *Tunnel) stopAsync() <-chan struct{} {
	t.mu.Lock()
	defer t.unlockAndFlush()
	if t.disposed {
		return closedChan()
	}
	t.desired, t.pendingApply, t.applied = nil, false, nil
	t.detachLocked()
	t.setStatusLocked(TunnelStatus{State: StateOff})
	t.checkIdleLocked()
	return t.waiterLocked()
}

func (t *Tunnel) disposeAsync(timeout time.Duration) <-chan struct{} {
	t.mu.Lock()
	defer t.unlockAndFlush()
	if t.disposeDone != nil {
		return t.disposeDone
	}
	t.disposed = true
	t.desired, t.pendingApply, t.applied = nil, false, nil
	t.detachLocked()
	t.setStatusLocked(TunnelStatus{State: StateOff})
	t.listeners = map[uint64]func(TunnelStatus){}
	t.disposeDone = make(chan struct{})
	if t.idleLocked() {
		close(t.disposeDone)
		return t.disposeDone
	}
	done := t.disposeDone
	if timeout < 0 {
		timeout = 0 // a deadline already past waits for nothing
	}
	ms := strconv.FormatInt(timeout.Milliseconds(), 10)
	t.disposeTimer = t.opts.after(timeout, func() {
		t.mu.Lock()
		defer t.unlockAndFlush()
		if t.disposeTimer == nil {
			return
		}
		t.disposeTimer = nil
		t.logLocked(slog.LevelWarn, "dispose() gave up waiting after "+ms+" ms")
		close(done)
	})
	return done
}

// detachLocked ends the current generation synchronously: its events are
// stale from now on, and its connection closes in the background, counted
// in draining.
func (t *Tunnel) detachLocked() {
	t.gen++
	g := t.active
	t.active = nil
	if g == nil {
		return
	}
	t.logLocked(slog.LevelInfo, "stopping")
	t.draining++
	t.do(func() {
		g.a.closeAsync(context.Background(), func() {
			t.mu.Lock()
			defer t.unlockAndFlush()
			t.draining--
			t.maybeStartLocked()
		})
	})
}

func (t *Tunnel) startLocked(cfg *TunnelConfig) {
	t.gen++
	gen := t.gen
	t.setStatusLocked(TunnelStatus{State: StateConnecting})
	via := ""
	if cfg.URL != "" {
		via = " via " + cfg.URL
	}
	t.logLocked(slog.LevelInfo, fmt.Sprintf("connecting as %q%s", cfg.Name, via))
	if err := validateService(cfg.Name, cfg.Kind, true, t.opts.Agent, t.opts.Reconnect, cfg.URL); err != nil {
		t.failLocked(cfg, err)
		return
	}
	a := newAgent(agentConfig{
		url:           cfg.URL,
		token:         wsmixer.StaticToken(strings.TrimSpace(cfg.Token)),
		name:          cfg.Name,
		kind:          cfg.Kind,
		integration:   t.opts.Integration,
		runtime:       t.opts.Agent.Runtime,
		os:            t.opts.Agent.OS,
		httpClient:    t.effectiveWebSocket(cfg),
		reconnect:     t.opts.Reconnect.wsmixer(),
		maxBodyBytes:  t.opts.MaxRequestBodyBytes,
		handler:       t.opts.Handler,
		target:        t.target,
		targetTimeout: t.opts.TargetTimeout,
		emit:          func(ev event) { t.onAgentEvent(gen, cfg, ev) },
		after:         t.opts.after,
		newTransport:  t.opts.newTransport,
	})
	t.active = &tunnelGen{id: gen, a: a, cfg: cfg}
	t.do(func() {
		t.mu.Lock()
		live := t.gen == gen
		t.mu.Unlock()
		if live {
			a.start()
		}
	})
}

// onAgentEvent is §6.7 for one event of generation gen; events of an ended
// generation change nothing (§6.8).
func (t *Tunnel) onAgentEvent(gen int, cfg *TunnelConfig, ev event) {
	t.mu.Lock()
	defer t.unlockAndFlush()
	if gen != t.gen {
		return
	}
	cur := t.status
	switch ev.kind {
	case evRegistered:
		t.logLocked(slog.LevelInfo, "online at "+ev.registration.URL)
		t.setStatusLocked(TunnelStatus{State: StateOnline, URL: ev.registration.URL})
	case evDisabled:
		if ev.refused {
			t.logLocked(slog.LevelWarn, "the service is disabled in the mcpwarp dashboard")
		} else {
			t.logLocked(slog.LevelWarn, "the service was disabled ("+ev.reason+")")
		}
		t.setStatusLocked(problemStatus(StateDisabled, CodeDisabled, lastDisabled))
	case evEnabled:
		t.logLocked(slog.LevelInfo, "the service was enabled, registering again")
		t.setStatusLocked(TunnelStatus{State: StateConnecting})
	case evConflict:
		t.conflictedLocked(cfg)
	case evError:
		// Any register refusal is terminal: after an earlier registration nothing would retry it.
		if ev.origin == originRegisterRefusal {
			t.failLocked(cfg, ev.err)
			return
		}
		t.logLocked(slog.LevelWarn, technical(ev.err))
		if (ev.origin == originApp || ev.origin == originMalformed) && cur.State == StateConnecting {
			t.setStatusLocked(problemStatus(StateConnecting, CodeInternal, lastInternal))
		}
	case evRetry:
		r := ev.retry
		t.logLocked(slog.LevelWarn, fmt.Sprintf("retry #%d in %d ms: %s", r.Attempt, r.Delay.Milliseconds(), r.Reason))
		if on := t.opts.OnRetry; on != nil {
			t.callback(func() {
				defer func() { _ = recover() }()
				on(RetryInfo{Attempt: r.Attempt, Delay: r.Delay, Reason: r.Reason})
			})
		}
		// While online or disabled this is a drain replacement's register; the old connection still serves.
		if (ev.retryKind == "register" || ev.retryKind == "overloaded") && cur.State == StateConnecting {
			t.setStatusLocked(problemStatus(StateConnecting, CodeRetrying, lastRetryRegister))
		}
	case evDisconnect:
		r := ev.disconnect
		if r.Fatal {
			return // the close event that follows carries the outcome
		}
		if r.Superseded {
			t.logLocked(slog.LevelInfo, "the superseded connection ended: "+r.Message)
			return
		}
		t.logLocked(slog.LevelWarn, "disconnected: "+r.Message)
		switch {
		case r.Phase == wsmixer.PhaseConnected:
			t.setStatusLocked(problemStatus(StateConnecting, CodeDisconnected, lastLost))
		case cur.State == StateConnecting && cur.Code != CodeDisconnected:
			// A failed dial; after a lost connection the failed redials keep saying so.
			t.setStatusLocked(problemStatus(StateConnecting, CodeRetrying, lastRetryDial))
		}
	case evClose:
		err := ev.err
		if err == nil {
			err = errors.New("the connection closed")
		}
		t.failLocked(cfg, err)
	}
}

func (t *Tunnel) conflictedLocked(cfg *TunnelConfig) {
	t.logLocked(slog.LevelWarn, fmt.Sprintf("another connection took over the name %q; not reconnecting until configure() is called again", cfg.Name))
	t.forgetLocked(cfg)
	t.detachLocked()
	t.setStatusLocked(problemStatus(StateConflict, CodeTakenOver, lastTakenOver))
}

// failLocked is a fatal failure of the current generation, or a validation
// error at start (T7, T8).
func (t *Tunnel) failLocked(cfg *TunnelConfig, err error) {
	var e *Error
	if errors.As(err, &e) && e.Code == ErrCodeTakenOver {
		t.conflictedLocked(cfg)
		return
	}
	st := describeProblem(err)
	var oe *optionsError
	switch {
	case errors.As(err, &oe):
		st = problemStatus(StateError, CodeInvalidConfig, lastInvalidConfig)
		if oe.badName {
			st = problemStatus(StateError, CodeInvalidName, lastInvalidName)
		}
		t.logLocked(slog.LevelError, string(st.Code)+": "+errorMessage(err))
	case e != nil:
		t.logLocked(slog.LevelError, technical(err))
	default:
		t.logLocked(slog.LevelError, string(st.Code)+": "+errorMessage(err))
	}
	t.forgetLocked(cfg)
	t.detachLocked()
	t.setStatusLocked(st)
}

func (t *Tunnel) forgetLocked(cfg *TunnelConfig) {
	if t.desired == cfg {
		t.desired = nil
	}
}

// describeProblem is what a fatal failure means for the user (§6.5, §6.6).
func describeProblem(err error) TunnelStatus {
	var e *Error
	if errors.As(err, &e) {
		switch e.Code {
		case ErrCodeUnauthorized, string(RegisterUnauthenticated):
			return problemStatus(StateError, CodeUnauthorized, lastRejected)
		case string(RegisterForbidden):
			return problemStatus(StateError, CodeUnauthorized, lastForbidden)
		case ErrCodeTakenOver:
			return problemStatus(StateConflict, CodeTakenOver, lastTakenOver)
		case string(RegisterInvalidName):
			return problemStatus(StateError, CodeInvalidName, lastInvalidName)
		case string(RegisterUsernameRequired):
			return problemStatus(StateError, CodeUsernameRequired, lastUsername)
		case string(RegisterQuotaExceeded):
			return problemStatus(StateError, CodeQuotaExceeded, lastQuota)
		case string(RegisterConflict):
			return problemStatus(StateError, CodeConflict, lastConflict)
		case string(AppUnsupportedVersion):
			return problemStatus(StateError, CodeUnsupportedVersion, lastUnsupported)
		case ErrCodeDisconnectedFatal:
			return problemStatus(StateError, CodeDisconnectedFatal, lastFatal)
		}
	}
	return problemStatus(StateError, CodeInternal, lastInternal)
}
