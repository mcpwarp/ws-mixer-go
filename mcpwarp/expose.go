package mcpwarp

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/mcpwarp/ws-mixer-go/wsmixer"
)

// ExposeState is a Handle's state (§5.5).
type ExposeState string

const (
	ExposeConnecting  ExposeState = "connecting"
	ExposeRegistering ExposeState = "registering"
	ExposeRegistered  ExposeState = "registered"
	ExposeDisabled    ExposeState = "disabled"
	ExposeConflict    ExposeState = "conflict"
	ExposeClosing     ExposeState = "closing"
	ExposeClosed      ExposeState = "closed"
)

// EventKind names a Handle event (§5.6).
type EventKind int

const (
	// EventRegistered: a register succeeded (every welcome, after enable). Event.Registration.
	EventRegistered EventKind = iota + 1
	// EventDisabled: disable, or a SERVER_DISABLED refusal. Event.ServerID, Event.Reason.
	EventDisabled
	// EventEnabled: a matching enable; a register follows. Event.ServerID.
	EventEnabled
	// EventConflict: a take-over after Expose returned. Event.Conflict; the state is conflict for good.
	EventConflict
	// EventError: a non-fatal error. Event.Err.
	EventError
	// EventDisconnect: a transport disconnect. Event.Disconnect.
	EventDisconnect
	// EventClose: the handle ended, last event. Event.Err is nil after Close, the fatal error otherwise.
	EventClose
)

var eventNames = [...]string{"", "registered", "disabled", "enabled", "conflict", "error", "disconnect", "close"}

func (k EventKind) String() string {
	if k > 0 && int(k) < len(eventNames) {
		return eventNames[k]
	}
	return fmt.Sprintf("EventKind(%d)", int(k))
}

// Event is one Handle event; which fields are set depends on Kind.
type Event struct {
	Kind         EventKind
	Registration Registration
	ServerID     string
	Reason       string
	Conflict     Conflict
	Err          error
	Disconnect   wsmixer.DisconnectReason
}

// Handle is one exposed service (§5.5), returned by Expose once it is
// registered or disabled.
type Handle struct {
	a       *agent
	onRetry func(RetryInfo)

	mu       sync.Mutex
	settled  bool
	settle   chan error
	queue    []Event
	ch       chan Event
	notify   chan struct{}
	ended    bool // EventClose queued
	stopCtx  func() bool
	closeErr error
}

// Expose publishes one local MCP server at its public mcpwarp URL (§5): it
// validates opts (§5.2), dials, registers, and returns once the service is
// registered or disabled from the dashboard (§5.4). Errors are a validation
// error (errors.Is ErrInvalidOptions), a fatal *Error (a register refusal,
// UNSUPPORTED_VERSION, UNAUTHORIZED, TAKEN_OVER, DISCONNECTED_FATAL), or,
// when ctx ends first, an error wrapping context.Cause(ctx). Dial failures
// and transient register errors are retried, not returned.
//
// ctx is §5.8's signal for the whole exposure: once Expose has returned,
// cancelling it closes the handle (same as Close).
func Expose(ctx context.Context, opts ExposeOptions) (*Handle, error) {
	target, err := validateExpose(&opts)
	if err != nil {
		return nil, err
	}
	if ctx.Err() != nil {
		return nil, abortError(ctx)
	}
	token := opts.TokenProvider
	if token == nil {
		token = wsmixer.StaticToken(opts.Token)
	}
	h := &Handle{onRetry: opts.OnRetry, settle: make(chan error, 1), notify: make(chan struct{}, 1)}
	h.a = newAgent(agentConfig{
		url:           opts.URL,
		token:         token,
		name:          opts.Name,
		kind:          opts.Kind,
		integration:   opts.Integration,
		runtime:       opts.Agent.Runtime,
		os:            opts.Agent.OS,
		httpClient:    opts.WebSocket,
		reconnect:     opts.Reconnect.wsmixer(),
		options:       wsmixer.Options{Logger: opts.Logger},
		maxBodyBytes:  opts.MaxRequestBodyBytes,
		handler:       opts.Handler,
		target:        target,
		targetTimeout: opts.TargetTimeout,
		emit:          h.onEvent,
	})
	return h.run(ctx)
}

func (h *Handle) run(ctx context.Context) (*Handle, error) {
	h.a.start()
	select {
	case err := <-h.settle:
		if err != nil {
			return nil, err
		}
	case <-ctx.Done():
		h.mu.Lock()
		won := !h.settled
		h.settled = true
		h.mu.Unlock()
		if won {
			h.a.closeAsync(context.Background(), nil)
			return nil, abortError(ctx)
		}
		if err := <-h.settle; err != nil {
			return nil, err
		}
	}
	h.mu.Lock()
	h.stopCtx = context.AfterFunc(ctx, func() { _ = h.Close(context.Background()) })
	h.mu.Unlock()
	return h, nil
}

func abortError(ctx context.Context) error {
	return fmt.Errorf("mcpwarp: expose() aborted before the service was registered: %w", context.Cause(ctx))
}

// settleLocked resolves Expose once; false when it already was.
func (h *Handle) settleLocked(err error) bool {
	if h.settled {
		return false
	}
	h.settled = true
	h.settle <- err
	return true
}

// onEvent is the agent's event sink (§5.4 resolution, §5.6 events).
func (h *Handle) onEvent(ev event) {
	if ev.kind == evRetry {
		h.retry(ev.retry)
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.settled {
		switch ev.kind {
		case evConflict:
			// Taken over before it registered: the handle could never serve.
			h.settleLocked(&Error{Code: ErrCodeTakenOver, Fatal: true, ServiceName: h.a.cfg.name,
				Message: fmt.Sprintf("mcpwarp: service %q was taken over by another connection before it registered", h.a.cfg.name)})
			go h.a.closeAsync(context.Background(), nil)
			return
		case evClose:
			err := ev.err
			if err == nil {
				err = &Error{Code: ErrCodeClosed, Message: "mcpwarp: closed before the service was registered", Fatal: true, ServiceName: h.a.cfg.name}
			}
			h.settleLocked(err)
			return
		case evRegistered, evDisabled:
			h.settleLocked(nil)
		}
	}
	if h.ended {
		return
	}
	switch ev.kind {
	case evRegistered:
		h.pushLocked(Event{Kind: EventRegistered, Registration: ev.registration})
	case evDisabled:
		h.pushLocked(Event{Kind: EventDisabled, ServerID: ev.serverID, Reason: ev.reason})
	case evEnabled:
		h.pushLocked(Event{Kind: EventEnabled, ServerID: ev.serverID})
	case evConflict:
		h.pushLocked(Event{Kind: EventConflict, Conflict: ev.conflict})
	case evError:
		h.pushLocked(Event{Kind: EventError, Err: ev.err})
	case evDisconnect:
		h.pushLocked(Event{Kind: EventDisconnect, Disconnect: ev.disconnect})
	case evClose:
		h.pushLocked(Event{Kind: EventClose, Err: ev.err})
		h.ended = true
		if h.stopCtx != nil {
			h.stopCtx()
		}
	}
}

// retry forwards a retry to OnRetry; a panic there is an EventError.
func (h *Handle) retry(r retryInfo) {
	if h.onRetry == nil {
		return
	}
	defer func() {
		if p := recover(); p != nil {
			h.mu.Lock()
			if !h.ended {
				h.pushLocked(Event{Kind: EventError, Err: fmt.Errorf("mcpwarp: OnRetry panicked: %v", p)})
			}
			h.mu.Unlock()
		}
	}()
	h.onRetry(RetryInfo{Attempt: r.Attempt, Delay: r.Delay, Reason: r.Reason})
}

func (h *Handle) pushLocked(ev Event) {
	h.queue = append(h.queue, ev)
	select {
	case h.notify <- struct{}{}:
	default:
	}
}

// Events returns the handle's event channel. Events are queued from the
// start (the first EventRegistered fires before Expose returns) and
// delivered in order, without blocking the handle; the channel is closed
// after EventClose. Every call returns the same channel.
func (h *Handle) Events() <-chan Event {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.ch == nil {
		h.ch = make(chan Event)
		go h.pump()
	}
	return h.ch
}

func (h *Handle) pump() {
	for {
		h.mu.Lock()
		if len(h.queue) == 0 {
			ended := h.ended
			h.mu.Unlock()
			if ended {
				close(h.ch)
				return
			}
			<-h.notify
			continue
		}
		ev := h.queue[0]
		h.queue = h.queue[1:]
		h.mu.Unlock()
		h.ch <- ev
	}
}

// URL is the latest registered public URL; "" only when Expose returned
// disabled and the service has not registered since.
func (h *Handle) URL() string {
	u, _, _, _ := h.a.session.snapshot()
	return u
}

// ServerID is the latest server id ("" when none is known).
func (h *Handle) ServerID() string {
	_, id, _, _ := h.a.session.snapshot()
	return id
}

// Registration is the latest successful register; false when there was none.
func (h *Handle) Registration() (Registration, bool) {
	_, _, _, reg := h.a.session.snapshot()
	if reg == nil {
		return Registration{}, false
	}
	return *reg, true
}

// Path is the only request path forwarded: "/mcp".
func (h *Handle) Path() string { return mcpPath }

// State is the handle's state (§5.5).
func (h *Handle) State() ExposeState { return ExposeState(h.a.session.currentState()) }

// Client is the underlying ws-mixer client (escape hatch).
func (h *Handle) Client() *wsmixer.Client { return h.a.client() }

// Close is §5.7: unregister on the live connection (waiting for
// `unregistered` up to 2 s), then a graceful client close; EventClose
// follows. Idempotent. It returns ctx's error only when ctx ended before the
// close did (the close then completes in the background).
func (h *Handle) Close(ctx context.Context) error {
	return h.a.close(ctx)
}

// technical formats err for the tunnel's log: "CODE: message" for an
// *Error, the message otherwise, without the "mcpwarp: " prefix.
func technical(err error) string {
	var e *Error
	if errors.As(err, &e) {
		if e.Message == "" {
			return e.Code
		}
		return e.Code + ": " + errorMessage(e)
	}
	return errorMessage(err)
}

func errorMessage(err error) string {
	return strings.TrimPrefix(err.Error(), "mcpwarp: ")
}
