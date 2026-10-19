package mcpwarp

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/mcpwarp/ws-mixer-go/wsmixer"
)

// ServiceGroupOptions configures NewServiceGroup (§6A.1): TunnelOptions
// minus Handler, Target and TargetTimeout, which are per service, plus
// TrustServerIDHeader.
type ServiceGroupOptions struct {
	Integration         string
	Agent               Agent
	WebSocket           *http.Client
	MaxRequestBodyBytes int64
	Reconnect           ReconnectOptions
	// OnRetry is called for retries of the current connection only.
	OnRetry func(RetryInfo)
	// TrustServerIDHeader routes a request by its X-Mcpwarp-Server-Id
	// header (§4.8 step 1). Turn it on only behind an edge that sets the
	// header and strips client copies; otherwise any client could steer a
	// request to another service of the connection. Off, the Host label
	// alone selects the service.
	TrustServerIDHeader bool
	// Logger receives §6.9's and §6A.10's lines (never the token); nil is silent.
	Logger *slog.Logger
	// Metrics is the wsmixer.Options.Metrics of every connection; nil is none.
	Metrics wsmixer.Metrics

	// Test seams, as TunnelOptions', plus the target-mode HTTP transport.
	after        afterFunc
	newTransport func(transportConfig) transport
	schedule     func(func())
	transport    http.RoundTripper
}

// ServiceConfig is one service of a group (§6A.2): exactly one of Handler
// or Target, as for Expose.
type ServiceConfig struct {
	Name          string
	Kind          ServiceKind
	Handler       http.Handler
	Target        string
	TargetTimeout time.Duration
}

// ServiceGroupConfig is what may change at runtime (§6A.2). Enabled,
// Token/TokenSource, URL and WebSocket are the connection's, as in
// TunnelConfig; a change to them restarts it. A change to Services alone is
// applied on the live connection, by name, without a redial.
type ServiceGroupConfig struct {
	Enabled     bool
	Token       string
	TokenSource TokenSource
	Services    []ServiceConfig
	URL         string
	WebSocket   *http.Client
}

// ServiceGroup is the multi-service controller of §6A: one connection
// publishing a set of services.
type ServiceGroup struct {
	opts ServiceGroupOptions

	mu        sync.Mutex
	status    ServiceGroupStatus
	listeners map[uint64]func(ServiceGroupStatus)
	nextID    uint64

	desired       *ServiceGroupConfig
	localDisabled map[string]bool // disable(name), until enable(name), removal, stop, dispose or a fatal error
	pendingApply  bool
	applied       *ServiceGroupConfig
	gen           int
	active        *groupGen
	draining      int
	idle          chan struct{}

	disposed     bool
	disposeDone  chan struct{}
	disposeTimer func() bool

	queue      []func()
	flushing   bool
	callbacks  []func()
	delivering bool
}

type groupGen struct {
	id  int
	a   *agent
	cfg *ServiceGroupConfig
}

// NewServiceGroup creates a stopped ("off") group. It fails for an invalid
// Integration or Agent (errors.Is ErrInvalidOptions); problems with a
// config surface as state "error" instead.
func NewServiceGroup(opts ServiceGroupOptions) (*ServiceGroup, error) {
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
	g := &ServiceGroup{
		opts:          opts,
		status:        ServiceGroupStatus{State: StateOff, Services: map[string]ServiceStatus{}},
		listeners:     map[uint64]func(ServiceGroupStatus){},
		localDisabled: map[string]bool{},
		idle:          make(chan struct{}),
	}
	return g, nil
}

// --- queues (as Tunnel's) -------------------------------------------------

func (g *ServiceGroup) unlockAndFlush() {
	if g.flushing {
		g.mu.Unlock()
		return
	}
	g.runQueueLocked()
	if g.delivering {
		g.mu.Unlock()
		return
	}
	g.delivering = true
	for len(g.callbacks) > 0 {
		cbs := g.callbacks
		g.callbacks = nil
		g.mu.Unlock()
		for _, f := range cbs {
			f()
		}
		g.mu.Lock()
		if !g.flushing {
			g.runQueueLocked()
		}
	}
	g.delivering = false
	g.mu.Unlock()
}

func (g *ServiceGroup) runQueueLocked() {
	g.flushing = true
	for len(g.queue) > 0 {
		q := g.queue
		g.queue = nil
		g.mu.Unlock()
		for _, f := range q {
			f()
		}
		g.mu.Lock()
	}
	g.flushing = false
}

func (g *ServiceGroup) do(f func()) { g.queue = append(g.queue, f) }

func (g *ServiceGroup) callback(f func()) { g.callbacks = append(g.callbacks, f) }

func (g *ServiceGroup) logLocked(level slog.Level, msg string) {
	if l := g.opts.Logger; l != nil {
		g.callback(func() {
			defer func() { _ = recover() }()
			l.Log(context.Background(), level, msg)
		})
	}
}

// --- status ------------------------------------------------------------------

func cloneGroupStatus(s ServiceGroupStatus) ServiceGroupStatus {
	svcs := make(map[string]ServiceStatus, len(s.Services))
	for k, v := range s.Services {
		svcs[k] = v
	}
	s.Services = svcs
	return s
}

// Client is the running connection's wsmixer.Client, or nil: before the
// first Configure that starts one, while off, stopped, restarting or in a
// fatal/conflict state, and after Dispose. Reconnects happen inside one
// Client, so it stays the same across them; a restart (a change of Enabled,
// the token, URL or WebSocket) replaces it. The current connection's
// welcome.session is Client().Conn().Session().
func (g *ServiceGroup) Client() *wsmixer.Client {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.active == nil {
		return nil
	}
	return g.active.a.client()
}

// Status returns a copy of the current status.
func (g *ServiceGroup) Status() ServiceGroupStatus {
	g.mu.Lock()
	defer g.mu.Unlock()
	return cloneGroupStatus(g.status)
}

// Subscribe calls l with the current status, then on every change, and
// returns the function that unsubscribes it; delivery is as Tunnel.Subscribe.
// Each call gets its own copy of Services.
func (g *ServiceGroup) Subscribe(l func(ServiceGroupStatus)) (unsubscribe func()) {
	g.mu.Lock()
	s := g.status
	disposed := g.disposed
	var id uint64
	if !disposed {
		g.nextID++
		id = g.nextID
		g.listeners[id] = l
	}
	g.callback(func() { g.call(l, s) })
	g.unlockAndFlush()
	if disposed {
		return func() {}
	}
	return func() {
		g.mu.Lock()
		delete(g.listeners, id)
		g.mu.Unlock()
	}
}

func (g *ServiceGroup) call(l func(ServiceGroupStatus), s ServiceGroupStatus) {
	defer func() {
		if p := recover(); p != nil {
			g.mu.Lock()
			g.logLocked(slog.LevelError, fmt.Sprintf("status listener threw: %v", p))
			g.unlockAndFlush()
		}
	}()
	l(cloneGroupStatus(s))
}

func sameGroupStatus(a, b ServiceGroupStatus) bool {
	return a.State == b.State && a.Code == b.Code && a.LastError == b.LastError && sameView(a.Services, b.Services)
}

// setStatusLocked delivers next unless it equals the current status (§6.4
// dedup, services included).
func (g *ServiceGroup) setStatusLocked(next ServiceGroupStatus) {
	if next.Services == nil {
		next.Services = map[string]ServiceStatus{}
	}
	if sameGroupStatus(next, g.status) {
		return
	}
	g.status = next
	var ls []func(ServiceGroupStatus)
	for id := uint64(1); id <= g.nextID; id++ {
		if l, ok := g.listeners[id]; ok {
			ls = append(ls, l)
		}
	}
	g.callback(func() {
		for _, l := range ls {
			g.call(l, next)
		}
	})
}

// connStatusLocked is st with the current services.
func (g *ServiceGroup) connStatusLocked(st TunnelStatus) ServiceGroupStatus {
	return ServiceGroupStatus{State: st.State, Code: st.Code, LastError: st.LastError, Services: g.status.Services}
}

// --- API ------------------------------------------------------------------------

// Configure applies config (§6A.2). It never fails for a bad config (that is
// state "error"); it returns ctx's error only when ctx ended first. A change
// of Enabled, the token, URL or the effective WebSocket restarts the
// connection, as Tunnel.Configure; a change of Services alone is diffed by
// name on the live connection and returns once its frames are queued.
func (g *ServiceGroup) Configure(ctx context.Context, config ServiceGroupConfig) error {
	return wait(ctx, g.configureAsync(config))
}

// Stop stops the connection and goes off, unregistering every name it may
// hold (§6A.8). It returns once the connection has closed.
func (g *ServiceGroup) Stop(ctx context.Context) error { return wait(ctx, g.stopAsync()) }

// Dispose is final, as Tunnel.Dispose.
func (g *ServiceGroup) Dispose(ctx context.Context) error {
	timeout := defaultDisposeTimeout
	if dl, ok := ctx.Deadline(); ok {
		timeout = time.Until(dl)
	}
	done := g.disposeAsync(timeout)
	select {
	case <-done:
	case <-ctx.Done():
	}
	return nil
}

// AddService is Configure with the desired config's services plus svc,
// replacing an entry of the same name (§6A.3). Without a desired config it
// does nothing.
func (g *ServiceGroup) AddService(ctx context.Context, svc ServiceConfig) error {
	return wait(ctx, g.editAsync(func(c *ServiceGroupConfig) bool {
		out := make([]ServiceConfig, 0, len(c.Services)+1)
		replaced := false
		for _, s := range c.Services {
			if s.Name == svc.Name {
				out = append(out, svc)
				replaced = true
			} else {
				out = append(out, s)
			}
		}
		if !replaced {
			out = append(out, svc)
		}
		c.Services = out
		return true
	}))
}

// RemoveService is Configure with that entry removed (§6A.3); removing the
// last one goes off.
func (g *ServiceGroup) RemoveService(ctx context.Context, name string) error {
	return wait(ctx, g.editAsync(func(c *ServiceGroupConfig) bool {
		out := make([]ServiceConfig, 0, len(c.Services))
		for _, s := range c.Services {
			if s.Name != name {
				out = append(out, s)
			}
		}
		if len(out) == len(c.Services) {
			return false
		}
		c.Services = out
		return true
	}))
}

// Disable is the local disable of §6A.3: the name stays configured, shows
// disabled with no code, is unregistered if the connection may hold it and
// left out of every re-register, until Enable(name), its removal, Stop,
// Dispose or a fatal error.
func (g *ServiceGroup) Disable(ctx context.Context, name string) error {
	g.mu.Lock()
	defer g.unlockAndFlush()
	if g.disposed || g.desired == nil || !configured(g.desired, name) {
		return nil
	}
	g.localDisabled[name] = true
	if a := g.liveAgentLocked(); a != nil {
		g.do(func() { a.group.localDisable(name) })
	}
	return nil
}

// Enable clears a local disable, a taken_over and a rejected for name and
// registers it alone (§6A.3); for an active or pending name it registers it
// again.
func (g *ServiceGroup) Enable(ctx context.Context, name string) error {
	g.mu.Lock()
	defer g.unlockAndFlush()
	if g.disposed || g.desired == nil || !configured(g.desired, name) {
		return nil
	}
	delete(g.localDisabled, name)
	if a := g.liveAgentLocked(); a != nil {
		g.do(func() { a.group.localEnable(name) })
	}
	return nil
}

func configured(c *ServiceGroupConfig, name string) bool {
	for _, s := range c.Services {
		if s.Name == name {
			return true
		}
	}
	return false
}

// liveAgentLocked is the running generation's agent when it runs the
// desired config with no apply pending.
func (g *ServiceGroup) liveAgentLocked() *agent {
	if g.active == nil || g.active.cfg != g.desired || g.pendingApply || g.applied != nil {
		return nil
	}
	return g.active.a
}

// editAsync reads the desired config, edits it and applies the result under
// one lock, so concurrent edits never start from the same stale copy.
func (g *ServiceGroup) editAsync(edit func(*ServiceGroupConfig) bool) <-chan struct{} {
	g.mu.Lock()
	defer g.unlockAndFlush()
	if g.disposed || g.desired == nil {
		return closedChan()
	}
	c := *g.desired
	c.Services = append([]ServiceConfig(nil), c.Services...)
	if !edit(&c) {
		return closedChan()
	}
	return g.configureLocked(c)
}

// --- applying configs (as Tunnel's pump) ------------------------------------------

func groupWillStart(c *ServiceGroupConfig) bool {
	return c.Enabled && len(c.Services) > 0 && (c.TokenSource != nil || strings.TrimSpace(c.Token) != "")
}

func (g *ServiceGroup) effectiveWebSocket(c *ServiceGroupConfig) *http.Client {
	if c.WebSocket != nil {
		return c.WebSocket
	}
	return g.opts.WebSocket
}

// sameConn compares what the connection itself depends on.
func (g *ServiceGroup) sameConn(a *ServiceGroupConfig, b *ServiceGroupConfig) bool {
	ta := TunnelConfig{Token: a.Token, TokenSource: a.TokenSource}
	tb := TunnelConfig{Token: b.Token, TokenSource: b.TokenSource}
	return a.Enabled == b.Enabled && sameToken(&ta, &tb) && a.URL == b.URL && g.effectiveWebSocket(a) == g.effectiveWebSocket(b)
}

// sameHandler is == on handlers without its panic (a HandlerFunc is a func:
// never equal, so a reconfigure swaps it, which changes nothing visible).
func sameHandler(a, b http.Handler) bool {
	if a == nil || b == nil {
		return a == b
	}
	if !reflect.ValueOf(a).Comparable() || !reflect.ValueOf(b).Comparable() {
		return false
	}
	return a == b
}

func sameServices(a, b []ServiceConfig) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		x, y := a[i], b[i]
		if x.Name != y.Name || x.Kind != y.Kind || x.Target != y.Target || x.TargetTimeout != y.TargetTimeout || !sameHandler(x.Handler, y.Handler) {
			return false
		}
	}
	return true
}

func (g *ServiceGroup) idleLocked() bool {
	return !g.pendingApply && g.applied == nil && g.draining == 0
}

func (g *ServiceGroup) waiterLocked() <-chan struct{} {
	if g.idleLocked() {
		return closedChan()
	}
	return g.idle
}

func (g *ServiceGroup) checkIdleLocked() {
	if !g.idleLocked() {
		return
	}
	close(g.idle)
	g.idle = make(chan struct{})
	if g.disposeDone != nil && g.disposeTimer != nil {
		g.disposeTimer()
		g.disposeTimer = nil
		close(g.disposeDone)
	}
}

func (g *ServiceGroup) configureAsync(config ServiceGroupConfig) <-chan struct{} {
	g.mu.Lock()
	defer g.unlockAndFlush()
	return g.configureLocked(config)
}

func (g *ServiceGroup) configureLocked(config ServiceGroupConfig) <-chan struct{} {
	if g.disposed {
		return closedChan()
	}
	c := config
	c.Services = append([]ServiceConfig(nil), config.Services...)
	if d := g.desired; d != nil && g.sameConn(d, &c) && sameServices(d.Services, c.Services) {
		return g.waiterLocked()
	}
	// A services-only change on the running connection: diffed, no redial.
	if a := g.liveAgentLocked(); a != nil && g.sameConn(g.desired, &c) && groupWillStart(&c) {
		g.dropLocalDisablesLocked(&c)
		g.desired = &c
		g.active.cfg = &c
		err := g.validateServices(&c)
		var inits []groupServiceInit
		if err == nil {
			inits, err = g.serviceInits(&c)
		}
		if err != nil {
			g.failLocked(&c, err)
			return g.waiterLocked()
		}
		g.do(func() { a.group.applyServices(inits) })
		return closedChan()
	}
	g.desired = &c
	g.dropLocalDisablesLocked(&c)
	if !g.pendingApply {
		g.pendingApply = true
		g.do(func() { g.opts.schedule(g.pump) })
	}
	return g.idle
}

// dropLocalDisablesLocked forgets the local disables of removed names.
func (g *ServiceGroup) dropLocalDisablesLocked(c *ServiceGroupConfig) {
	for name := range g.localDisabled {
		if !configured(c, name) {
			delete(g.localDisabled, name)
		}
	}
}

func (g *ServiceGroup) initialStatus(c *ServiceGroupConfig) ServiceGroupStatus {
	st := ServiceGroupStatus{State: StateConnecting, Services: map[string]ServiceStatus{}}
	for _, s := range c.Services {
		if g.localDisabled[s.Name] {
			st.Services[s.Name] = ServiceStatus{State: ServiceDisabled}
		} else {
			st.Services[s.Name] = ServiceStatus{State: ServicePending}
		}
	}
	return st
}

func (g *ServiceGroup) pump() {
	g.mu.Lock()
	defer g.unlockAndFlush()
	if !g.pendingApply {
		return
	}
	g.pendingApply = false
	cfg := g.desired
	if g.disposed || cfg == nil {
		g.checkIdleLocked()
		return
	}
	g.detachLocked()
	if groupWillStart(cfg) {
		g.setStatusLocked(g.initialStatus(cfg))
	} else {
		g.setStatusLocked(ServiceGroupStatus{State: StateOff})
	}
	g.applied = cfg
	g.maybeStartLocked()
}

func (g *ServiceGroup) maybeStartLocked() {
	if g.applied == nil || g.draining > 0 {
		g.checkIdleLocked()
		return
	}
	cfg := g.applied
	g.applied = nil
	if !g.disposed && cfg == g.desired {
		if groupWillStart(cfg) {
			g.startLocked(cfg)
		} else {
			g.logLocked(slog.LevelInfo, "off")
		}
	}
	g.checkIdleLocked()
}

func (g *ServiceGroup) stopAsync() <-chan struct{} {
	g.mu.Lock()
	defer g.unlockAndFlush()
	if g.disposed {
		return closedChan()
	}
	g.desired, g.pendingApply, g.applied = nil, false, nil
	g.localDisabled = map[string]bool{}
	g.detachLocked()
	g.setStatusLocked(ServiceGroupStatus{State: StateOff})
	g.checkIdleLocked()
	return g.waiterLocked()
}

func (g *ServiceGroup) disposeAsync(timeout time.Duration) <-chan struct{} {
	g.mu.Lock()
	defer g.unlockAndFlush()
	if g.disposeDone != nil {
		return g.disposeDone
	}
	g.disposed = true
	g.desired, g.pendingApply, g.applied = nil, false, nil
	g.localDisabled = map[string]bool{}
	g.detachLocked()
	g.setStatusLocked(ServiceGroupStatus{State: StateOff})
	g.listeners = map[uint64]func(ServiceGroupStatus){}
	g.disposeDone = make(chan struct{})
	if g.idleLocked() {
		close(g.disposeDone)
		return g.disposeDone
	}
	done := g.disposeDone
	if timeout < 0 {
		timeout = 0
	}
	ms := strconv.FormatInt(timeout.Milliseconds(), 10)
	g.disposeTimer = g.opts.after(timeout, func() {
		g.mu.Lock()
		defer g.unlockAndFlush()
		if g.disposeTimer == nil {
			return
		}
		g.disposeTimer = nil
		g.logLocked(slog.LevelWarn, "dispose() gave up waiting after "+ms+" ms")
		close(done)
	})
	return done
}

func (g *ServiceGroup) detachLocked() {
	g.gen++
	cur := g.active
	g.active = nil
	if cur == nil {
		return
	}
	g.logLocked(slog.LevelInfo, "stopping")
	g.draining++
	g.do(func() {
		cur.a.closeAsync(context.Background(), func() {
			g.mu.Lock()
			defer g.unlockAndFlush()
			g.draining--
			g.maybeStartLocked()
		})
	})
}

// validateServices is §6A.2's check 7 and the per-entry checks at start
// (§6.7 T7, T8): a duplicate name, a bad name, or an entry failing §5.2
// rows 1-4 or 8.
func (g *ServiceGroup) validateServices(c *ServiceGroupConfig) error {
	seen := map[string]bool{}
	for _, s := range c.Services {
		if seen[s.Name] {
			return invalid("mcpwarp: duplicate service name")
		}
		seen[s.Name] = true
	}
	for _, s := range c.Services {
		if !IsValidServiceName(s.Name) {
			return &optionsError{msg: "mcpwarp: invalid service `name`: lowercase letters, digits and inner dashes, at most 30 characters", badName: true}
		}
		if _, err := validateAdapter(s.Handler, s.Target, s.TargetTimeout); err != nil {
			return err
		}
		if s.Kind != "" && s.Kind != KindHTTP && s.Kind != KindStdio {
			return invalid("mcpwarp: `kind` must be \"http\" or \"stdio\"")
		}
	}
	return nil
}

func (g *ServiceGroup) serviceInits(c *ServiceGroupConfig) ([]groupServiceInit, error) {
	inits := make([]groupServiceInit, 0, len(c.Services))
	for _, s := range c.Services {
		var target *url.URL
		if s.Target != "" {
			u, err := validateAdapter(nil, s.Target, s.TargetTimeout)
			if err != nil {
				return nil, err
			}
			target = u
		}
		kind := s.Kind
		if kind == "" {
			kind = KindHTTP
		}
		a := newApp(s.Handler, target, s.TargetTimeout, g.opts.transport)
		a.name = s.Name
		inits = append(inits, groupServiceInit{
			name:          s.Name,
			kind:          kind,
			app:           a,
			localDisabled: g.localDisabled[s.Name],
		})
	}
	return inits, nil
}

func (g *ServiceGroup) startLocked(cfg *ServiceGroupConfig) {
	g.gen++
	gen := g.gen
	via := ""
	if cfg.URL != "" {
		via = " via " + cfg.URL
	}
	g.logLocked(slog.LevelInfo, fmt.Sprintf("connecting with %d services%s", len(cfg.Services), via))
	err := g.validateServices(cfg)
	if err == nil {
		hasToken := cfg.TokenSource != nil || strings.TrimSpace(cfg.Token) != ""
		err = validateService(cfg.Services[0].Name, "", hasToken, g.opts.Agent, g.opts.Reconnect, cfg.URL)
	}
	var inits []groupServiceInit
	if err == nil {
		inits, err = g.serviceInits(cfg)
	}
	if err != nil {
		g.failLocked(cfg, err)
		return
	}
	token := wsmixer.StaticToken(strings.TrimSpace(cfg.Token))
	if cfg.TokenSource != nil {
		token = cfg.TokenSource.Token
	}
	a := newAgent(agentConfig{
		url:          cfg.URL,
		token:        token,
		refreshable:  cfg.TokenSource != nil,
		name:         cfg.Services[0].Name,
		integration:  g.opts.Integration,
		runtime:      g.opts.Agent.Runtime,
		os:           g.opts.Agent.OS,
		httpClient:   g.effectiveWebSocket(cfg),
		reconnect:    g.opts.Reconnect.wsmixer(),
		options:      wsmixer.Options{Metrics: g.opts.Metrics},
		maxBodyBytes: g.opts.MaxRequestBodyBytes,
		emit:         func(ev event) { g.onAgentEvent(gen, ev) },
		after:        g.opts.after,
		newTransport: g.opts.newTransport,
		group:        &groupAgentConfig{services: inits, trustHeader: g.opts.TrustServerIDHeader},
	})
	g.active = &groupGen{id: gen, a: a, cfg: cfg}
	g.do(func() {
		g.mu.Lock()
		live := g.gen == gen
		g.mu.Unlock()
		if live {
			a.start()
		}
	})
}

// onAgentEvent is §6A.6 for one event of generation gen: the connection
// rows of §6.7 as the tunnel's, G1-G8 from the group session.
func (g *ServiceGroup) onAgentEvent(gen int, ev event) {
	g.mu.Lock()
	defer g.unlockAndFlush()
	if gen != g.gen || g.active == nil {
		return
	}
	cfg := g.active.cfg
	cur := g.status
	switch ev.kind {
	case evGroup:
		next := cur
		next.Services = ev.groupServices
		switch ev.groupEffect {
		case effOnline:
			if cur.State == StateConnecting {
				next.State, next.Code, next.LastError = StateOnline, "", ""
			}
		case effRetrying:
			if cur.State == StateConnecting {
				next.Code, next.LastError = CodeRetrying, lastRetryRegister
			}
		case effFatal:
			g.logLocked(slog.LevelError, "INTERNAL: no service could be registered")
			g.forgetLocked(cfg)
			g.detachLocked()
			g.setStatusLocked(ServiceGroupStatus{State: StateError, Code: CodeInternal, LastError: lastNoServiceRegistered, Services: ev.groupServices})
			return
		}
		g.setStatusLocked(next)
	case evLog:
		g.logLocked(ev.logLevel, ev.logMsg)
	case evConflict:
		g.logLocked(slog.LevelWarn, "the connection was taken over; not reconnecting until configure() is called again")
		g.forgetLocked(cfg)
		g.detachLocked()
		svcs := ev.groupServices
		if svcs == nil {
			svcs = cur.Services
		}
		g.setStatusLocked(ServiceGroupStatus{State: StateConflict, Code: CodeTakenOver, LastError: lastTakenOver, Services: svcs})
	case evError:
		g.logLocked(slog.LevelWarn, technical(ev.err))
		if (ev.origin == originApp || ev.origin == originMalformed) && cur.State == StateConnecting {
			g.setStatusLocked(g.connStatusLocked(problemStatus(StateConnecting, CodeInternal, lastInternal)))
		}
	case evRetry:
		r := ev.retry
		g.logLocked(slog.LevelWarn, fmt.Sprintf("retry #%d in %d ms: %s", r.Attempt, r.Delay.Milliseconds(), r.Reason))
		if on := g.opts.OnRetry; on != nil {
			g.callback(func() {
				defer func() { _ = recover() }()
				on(RetryInfo{Attempt: r.Attempt, Delay: r.Delay, Reason: r.Reason})
			})
		}
	case evDisconnect:
		r := ev.disconnect
		if r.Fatal {
			return
		}
		if r.Superseded {
			g.logLocked(slog.LevelInfo, "the superseded connection ended: "+r.Message)
			return
		}
		g.logLocked(slog.LevelWarn, "disconnected: "+r.Message)
		switch {
		case r.Phase == wsmixer.PhaseConnected:
			g.setStatusLocked(g.connStatusLocked(problemStatus(StateConnecting, CodeDisconnected, lastLost)))
		case cur.State == StateConnecting && cur.Code != CodeDisconnected:
			g.setStatusLocked(g.connStatusLocked(problemStatus(StateConnecting, CodeRetrying, lastRetryDial)))
		}
	case evClose:
		err := ev.err
		if err == nil {
			err = errors.New("the connection closed")
		}
		g.failLocked(cfg, err)
	}
}

// failLocked is a fatal failure of the current generation, or a validation
// error (at start, or of a services-only change). A validation error shows
// no services; a fatal failure keeps them (as G5 does).
func (g *ServiceGroup) failLocked(cfg *ServiceGroupConfig, err error) {
	st := describeProblem(err)
	var oe *optionsError
	var e *Error
	keep := g.status.Services
	switch {
	case errors.As(err, &oe):
		st = problemStatus(StateError, CodeInvalidConfig, lastInvalidConfig)
		if oe.badName {
			st = problemStatus(StateError, CodeInvalidName, lastInvalidName)
		}
		keep = nil
		g.logLocked(slog.LevelError, string(st.Code)+": "+errorMessage(err))
	case errors.As(err, &e):
		g.logLocked(slog.LevelError, technical(err))
	default:
		g.logLocked(slog.LevelError, string(st.Code)+": "+errorMessage(err))
	}
	g.forgetLocked(cfg)
	g.localDisabled = map[string]bool{}
	g.detachLocked()
	g.setStatusLocked(ServiceGroupStatus{State: st.State, Code: st.Code, LastError: st.LastError, Services: keep})
}

func (g *ServiceGroup) forgetLocked(cfg *ServiceGroupConfig) {
	if g.desired == cfg {
		g.desired = nil
	}
}
