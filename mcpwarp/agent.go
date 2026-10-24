package mcpwarp

import (
	"context"
	"net/http"
	"net/url"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/mcpwarp/ws-mixer-go/wsmixer"
)

const (
	sdkName  = "ws-mixer-go"
	clientID = "sdk-go" // hello.meta.client, the Go counterpart of JS's "sdk-js"
)

// agentConfig is everything one exposed service needs. Expose and the tunnel
// controller validate their options and fill this in.
type agentConfig struct {
	url         string // "" → DefaultURL
	token       wsmixer.TokenProvider
	refreshable bool // token is a caller's provider (a rejected token gets one refresh), not a static token
	name        string
	kind        ServiceKind // "" → KindHTTP
	integration string      // sent lowercased
	runtime     string      // hello.agent.runtime override; "" → detected
	os          string      // hello.agent.os override; "" → detected
	httpClient  *http.Client

	options   wsmixer.Options
	reconnect wsmixer.ReconnectOptions

	maxBodyBytes  int64 // 0 → 4 MiB
	handler       http.Handler
	target        *url.URL
	targetTimeout time.Duration     // 0 → 30 s
	transport     http.RoundTripper // target mode; nil → http.DefaultTransport

	emit func(event) // may be nil

	after        afterFunc                       // test seam; nil → time.AfterFunc
	headDeadline time.Duration                   // test seam; 0 → 10 s
	newTransport func(transportConfig) transport // test seam; nil → wsmixer.Client
}

// helloMeta is hello.meta (§3.1).
type helloMeta struct {
	Client      string `json:"client"`
	Version     string `json:"version"`
	OS          string `json:"os"`
	Integration string `json:"integration"`
}

// agent is one exposed service: a transport (wsmixer.Client) carrying the
// identity of §3.1, the registration session, and the request bridge.
// Expose and the tunnel controller build on it: start it, watch its events,
// close it. Nothing in it blocks on its own: every wait is a timer or a
// transport callback, so a fake transport and clock drive it synchronously.
type agent struct {
	cfg     agentConfig
	session *session
	bridge  *bridge
	tr      transport

	streamsMu sync.Mutex
	streams   map[*wsmixer.Stream]struct{} // being served

	heldMu          sync.Mutex
	holdDisconnects bool // a drain{replaced} take-over waits for its conflict event
	held            []wsmixer.DisconnectReason

	startOnce  sync.Once
	closeMu    sync.Mutex
	closing    bool
	closeDones []func()
	closed     bool
	finished   bool
	done       chan struct{}
	err        error
}

func newAgent(cfg agentConfig) *agent {
	if cfg.url == "" {
		cfg.url = DefaultURL
	}
	if cfg.kind == "" {
		cfg.kind = KindHTTP
	}
	if cfg.after == nil {
		cfg.after = realAfterFunc
	}
	rt, osName := detectRuntime()
	if cfg.runtime != "" {
		rt = cfg.runtime
	}
	if cfg.os != "" {
		osName = cfg.os
	}

	a := &agent{cfg: cfg, done: make(chan struct{}), streams: map[*wsmixer.Stream]struct{}{}}
	a.session = newSession(sessionDeps{
		name:       cfg.name,
		kind:       cfg.kind,
		after:      cfg.after,
		emit:       a.emit,
		fatal:      a.onSessionFatal,
		activeConn: func() appConn { return a.tr.active() },
	})
	a.bridge = newBridge(bridge{
		availability:  a.session.availability,
		maxBodyBytes:  cfg.maxBodyBytes,
		handler:       cfg.handler,
		target:        cfg.target,
		targetTimeout: cfg.targetTimeout,
		onError:       func(err error) { a.emit(event{kind: evError, err: err}) },
		headDeadline:  cfg.headDeadline,
	}, cfg.transport)

	newTransport := cfg.newTransport
	if newTransport == nil {
		newTransport = newWSTransport
	}
	a.tr = newTransport(transportConfig{
		url:         cfg.url,
		token:       cfg.token,
		refreshable: cfg.refreshable,
		integration: strings.ToLower(cfg.integration),
		runtime:     rt,
		os:          osName,
		options:     cfg.options,
		reconnect:   cfg.reconnect,
		httpClient:  cfg.httpClient,
		name:        cfg.name,
		events: transportEvents{
			connect:    a.session.onWelcome,
			connEnd:    a.session.onConnEnd,
			app:        a.session.onApp,
			drain:      a.onDrain,
			disconnect: a.onDisconnect,
			retry:      a.onReconnect,
			stream:     a.onStream,
		},
	})
	return a
}

// start begins dialing; idempotent.
func (a *agent) start() { a.startOnce.Do(a.tr.start) }

func (a *agent) client() *wsmixer.Client { return a.tr.client() }

// onStream serves each stream on its own goroutine: streams never wait on
// each other, and the shared delivery goroutine never waits on a request.
func (a *agent) onStream(st *wsmixer.Stream) {
	a.streamsMu.Lock()
	a.streams[st] = struct{}{}
	a.streamsMu.Unlock()
	go func() {
		defer func() {
			a.streamsMu.Lock()
			delete(a.streams, st)
			a.streamsMu.Unlock()
		}()
		a.bridge.serve(st, st.Conn().Done())
	}()
}

func (a *agent) onDisconnect(r wsmixer.DisconnectReason) {
	a.heldMu.Lock()
	if a.holdDisconnects {
		a.held = append(a.held, r)
		a.heldMu.Unlock()
		return
	}
	a.heldMu.Unlock()
	a.reportDisconnect(r)
}

func (a *agent) reportDisconnect(r wsmixer.DisconnectReason) {
	a.emit(event{kind: evDisconnect, disconnect: r})
	if r.Fatal {
		code := ErrCodeDisconnectedFatal
		if (r.HasErrorCode && r.ErrorCode == wsmixer.UnauthorizedCode) || r.HTTPStatus == http.StatusUnauthorized {
			code = ErrCodeUnauthorized
		}
		a.finish(&Error{Code: code, Message: r.Message, Fatal: true, ServiceName: a.cfg.name, Cause: r})
	}
}

// onReconnect reports a transport redial as a retry (§5.9): a drain's
// replacement dial is attempt 0, the token-refresh redial attempt 1.
func (a *agent) onReconnect(info wsmixer.ReconnectInfo) {
	kind := "reconnect"
	switch info.Cause {
	case wsmixer.ReconnectDrain:
		kind, info.Attempt = "drain", 0
	case wsmixer.ReconnectTokenRefresh:
		kind, info.Attempt, info.Delay = "token-refresh", 1, 0
	}
	a.emit(event{kind: evRetry, retryKind: kind, retry: retryInfo{Attempt: info.Attempt, Delay: info.Delay, Reason: info.Cause}})
}

// onDrain: a drain with reason "replaced" is a take-over, never a hand-over
// (§3.3). The transport is closed before this returns, which is what keeps
// it from dialing the replacement it would otherwise schedule next.
func (a *agent) onDrain(reason string) {
	if reason != "replaced" {
		return
	}
	// The close below must happen now (no replacement dial may start), but
	// the session may report the conflict later, from another goroutine's
	// flush: the close's disconnect reports wait for the conflict event.
	a.heldMu.Lock()
	a.holdDisconnects = true
	a.heldMu.Unlock()
	tookOver := a.session.onReplaced()
	if !tookOver {
		a.releaseDisconnects()
	}
	if tookOver || a.session.currentState() == stateClosing {
		a.closeTakenOver()
	}
}

// releaseDisconnects reports the disconnects held since a drain{replaced}.
func (a *agent) releaseDisconnects() {
	a.heldMu.Lock()
	a.holdDisconnects = false
	held := a.held
	a.held = nil
	a.heldMu.Unlock()
	for _, r := range held {
		a.reportDisconnect(r)
	}
}

// closeTakenOver is §6.8's Take-over close: every stream being served is
// RESET with CANCEL, then the transport closes at once with error{NO_ERROR}
// + close 1000 (Client.CloseImmediately): no drain frame, no grace, no
// unregister, nothing redialed.
func (a *agent) closeTakenOver() {
	a.streamsMu.Lock()
	cut := make([]*wsmixer.Stream, 0, len(a.streams))
	for st := range a.streams {
		cut = append(cut, st)
	}
	a.streamsMu.Unlock()
	for _, st := range cut {
		_ = st.Reset(wsmixer.CancelCode, takenOverMessage)
	}
	a.tr.close(true, nil)
}

// emit reports ev; a take-over also closes the transport, right after the
// conflict is reported, so the close's own disconnect follows it.
func (a *agent) emit(ev event) {
	if a.cfg.emit != nil {
		a.cfg.emit(ev)
	}
	if ev.kind == evConflict {
		a.releaseDisconnects()
		a.closeTakenOver()
	}
}

func (a *agent) onSessionFatal(err *Error) {
	a.finish(err)
	a.tr.close(false, nil)
}

// closeAsync is §3.2's Unregister row, then the transport's graceful close
// (drain{client_requested}, ≤ 5 s grace, close 1000); done runs once both
// are over. ctx bounds the unregister wait. Idempotent: later calls only
// add their done.
func (a *agent) closeAsync(ctx context.Context, done func()) {
	a.closeMu.Lock()
	if a.closed {
		a.closeMu.Unlock()
		if done != nil {
			done()
		}
		return
	}
	if done != nil {
		a.closeDones = append(a.closeDones, done)
	}
	if a.closing {
		a.closeMu.Unlock()
		return
	}
	a.closing = true
	a.closeMu.Unlock()
	a.session.beginCloseAsync(ctx, unregisterBudget, func() {
		a.tr.close(false, func() {
			a.finish(nil)
			a.closeMu.Lock()
			a.closed = true
			dones := a.closeDones
			a.closeDones = nil
			a.closeMu.Unlock()
			for _, d := range dones {
				d()
			}
		})
	})
}

// close is closeAsync, returning once it is over or ctx is done (the close
// then goes on in the background).
func (a *agent) close(ctx context.Context) error {
	ch := make(chan struct{})
	a.closeAsync(ctx, func() { close(ch) })
	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		a.session.abortClose()
		return ctx.Err()
	}
}

// finish ends the agent once: err is nil after a close, the fatal error
// otherwise.
func (a *agent) finish(err error) {
	a.closeMu.Lock()
	if a.finished {
		a.closeMu.Unlock()
		return
	}
	a.finished = true
	a.err = err
	a.closeMu.Unlock()
	a.session.markClosed()
	close(a.done)
	// Outside any lock: the listener may close this agent in turn.
	a.emit(event{kind: evClose, err: err})
}

// detectRuntime is hello.agent.runtime/os for Go (§8 Agent identity):
// "go/<version>" and "<GOOS>/<GOARCH>", each at most 64 characters.
func detectRuntime() (rt, osName string) {
	return clampAgentField("go/" + strings.TrimPrefix(runtime.Version(), "go")), clampAgentField(runtime.GOOS + "/" + runtime.GOARCH)
}

func clampAgentField(s string) string {
	if len(s) > 64 {
		return s[:64]
	}
	return s
}
