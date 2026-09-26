package wsmixer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
)

// defaultSDKVersion is reported as Agent.SDKVersion in the hello message
// when the caller doesn't set one. Single source for this package.
const defaultSDKVersion = "0.6.0"

// ClientOptions configures Dial: one dial attempt, one handshake, no
// reconnect/backoff of its own. It is the low-level building block Client
// (client_reconnect.go) is built on top of -- most callers should use Client
// instead, which adds the full WIRE.md section 2.9 reconnect state machine on
// top of exactly this same Dial.
type ClientOptions struct {
	Options

	Token        string
	Agent        AgentInfo
	Meta         any
	Capabilities []string
	HTTPHeader   http.Header

	// OnStream is called whenever the server opens a new stream.
	OnStream func(*Stream)
	// OnApp is called for every incoming `app` message.
	OnApp func(body json.RawMessage)
	// OnDrain is called when the server sends `drain`. The Go client does not
	// reconnect on its own; the caller decides what to do.
	OnDrain func(*DrainMsg)
}

// postUpgradeError marks a clientHandshake failure as having happened after
// websocket.Dial's own upgrade already succeeded (the 101) -- everything
// from here to welcome is phase "handshake" (client_reconnect.go's
// phaseFor), regardless of what kind of error it turns out to be
// underneath: a transport death/EOF with no close frame carries no more
// information than a plain wrapped error, but it still happened strictly
// after the 101, not during the dial that precedes it. Unwrap-able so
// errors.As for a *ConnError/websocket.CloseError/*DialError still sees
// through it exactly as before this existed.
type postUpgradeError struct{ err error }

func (e *postUpgradeError) Error() string { return e.err.Error() }
func (e *postUpgradeError) Unwrap() error { return e.err }

// Dial connects to a ws-mixer.v1 server, performs the hello/welcome
// handshake, and returns a ready-to-use *Conn. ctx bounds both the dial
// itself and, immediately after, the welcome wait inside clientHandshake --
// it must allow for opts.HelloTimeout on top of however long the dial may
// take, or the welcome timeout can never deliver its graceful 4001: a ctx
// deadline that fires first aborts the transport outright, with no close
// frame at all (see clientHandshake's own doc for why).
func Dial(ctx context.Context, url string, opts ClientOptions) (*Conn, error) {
	opts.Options.SetDefaults()

	header := opts.HTTPHeader.Clone()
	if header == nil {
		header = http.Header{}
	}
	header.Set("Authorization", "Bearer "+opts.Token)

	ws, resp, err := websocket.Dial(ctx, url, &websocket.DialOptions{
		Subprotocols:    []string{Subprotocol},
		HTTPHeader:      header,
		CompressionMode: websocket.CompressionDisabled,
	})
	if err != nil {
		return nil, fmt.Errorf("wsmixer: dial: %w", newDialError(err, resp))
	}
	// Mirrors Listener.ServeHTTP's `defer ws.CloseNow()`: guarantee the socket
	// is released on every exit path from here down, success included (the
	// success path disarms it once c.Run() owns the connection's lifecycle).
	succeeded := false
	defer func() {
		if !succeeded {
			_ = ws.CloseNow()
		}
	}()

	if ws.Subprotocol() != Subprotocol {
		return nil, &DialError{
			Err:      fmt.Errorf("wsmixer: server did not echo the %s subprotocol; failing fatally, do not retry", Subprotocol),
			Fatal:    true,
			Mismatch: true,
		}
	}
	ws.SetReadLimit(opts.ReadLimit)

	c := newConn(ws, RoleClient, opts.Options)
	c.hp.Store(&handlers{onStream: opts.OnStream, onApp: opts.OnApp, onDrain: opts.OnDrain})

	if err := clientHandshake(ctx, c, opts); err != nil {
		if ce, ok := err.(*ConnError); ok {
			c.fail(ce)
			<-c.closed
		}
		if errors.Is(ctx.Err(), context.Canceled) {
			// The caller is abandoning the dial (Client.Close/CloseWith
			// during the handshake wait cancels this same ctx via
			// dialAndHandshake's closeCh watcher, or a plain Dial caller
			// explicitly cancelling their own ctx) -- not a handshake
			// failure to report as one, and nothing here to blame the peer
			// for. Leave err unwrapped: phase dial, exactly as before
			// postUpgradeError existed.
			//
			// Deliberately NOT ctx.Err() != nil (review item 6, probed): a
			// context.DeadlineExceeded is a timeout, not an application
			// cancellation -- dialAndHandshake's one ctx covers
			// ConnectTimeout+HelloTimeout in sum, so a dial that eats into
			// most of that combined budget before the 101 even lands can
			// make the shared ctx's own deadline expire during the welcome
			// wait that follows, before the HelloTimeout AfterFunc above
			// ever gets a chance to fire its own graceful 4001 -- that is a
			// genuine post-101 failure (phase handshake), not the caller
			// abandoning anything. errors.Is unwraps through fmt.Errorf's
			// %w chain, so a plain Dial caller's own context.DeadlineExceeded
			// still tests true through postUpgradeError's own Unwrap either
			// way -- only the phase classification changes here, not
			// whether that sentinel is still detectable.
			return nil, err
		}
		return nil, &postUpgradeError{err: err}
	}

	c.opts.Metrics.ConnectionOpened(c.session, "client")
	c.Run()
	succeeded = true
	go func() {
		<-c.closed
		c.opts.Metrics.ConnectionClosed(c.session, c.CloseCode(), c.ErrCodeName())
	}()
	return c, nil
}

// clientHandshake sends hello and waits for welcome, in that order.
// c.opts.HelloTimeout bounds the welcome wait, using a time.AfterFunc-driven
// fail() rather than a context deadline on the Read itself -- see the
// comment above the timer below for why: a context deadline passed straight
// to Read makes coder/websocket tear the whole connection down with no
// close frame at all the instant it fires, before this side ever gets a
// chance to send its own graceful error{}+close.
func clientHandshake(ctx context.Context, c *Conn, opts ClientOptions) error {
	hello := &HelloMsg{
		T: "hello", V: 1, Token: opts.Token, Agent: opts.Agent,
		Window: c.opts.Window, MaxStreams: c.opts.MaxStreams, Capabilities: opts.Capabilities,
	}
	if hello.Agent.SDK == "" {
		hello.Agent.SDK = "ws-mixer-go"
		hello.Agent.SDKVersion = defaultSDKVersion
	}
	if opts.Meta != nil {
		b, err := json.Marshal(opts.Meta)
		if err != nil {
			return fmt.Errorf("wsmixer: encoding hello.meta: %w", err)
		}
		hello.Meta = b
	}
	hb, err := json.Marshal(hello)
	if err != nil {
		return fmt.Errorf("wsmixer: encoding hello: %w", err)
	}

	wctx, wcancel := context.WithTimeout(ctx, 5*time.Second)
	defer wcancel()
	if err := c.ws.Write(wctx, websocket.MessageBinary, EncodeData(0, hb)); err != nil {
		return fmt.Errorf("wsmixer: sending hello: %w", err)
	}

	// A context deadline passed straight to Read would make coder/websocket
	// tear the whole connection down with no close frame at all once it
	// fires (context.AfterFunc-driven c.close() -- conn.go's
	// setupReadTimeout in coder/websocket -- the same caveat
	// runServerHandshake's own hello timeout notes, and OVERVIEW.md section
	// 3.1 for Ping): fabricating a *ConnError afterward would be pointless,
	// since fail()'s own graceful error{}+close would find nothing left to
	// write to. Timing the wait out with an explicit fail() from a
	// time.AfterFunc instead -- mirroring runServerHandshake's own
	// hello-timeout handling exactly, including the claimed
	// CompareAndSwap arbitrating the timer against a Read that returns at
	// nearly the same instant -- delivers the real error{PROTOCOL_ERROR}
	// and WS close 4001 to the peer before anything is torn down, and
	// closing is what unblocks the Read below.
	welcomeErr := newConnErrorf(ProtocolErrorCode, "no welcome within %dms of hello", c.opts.HelloTimeout.Milliseconds())
	var claimed atomic.Bool
	timer := time.AfterFunc(c.opts.HelloTimeout, func() {
		if !claimed.CompareAndSwap(false, true) {
			return // the read below already claimed the outcome
		}
		c.fail(welcomeErr)
	})
	defer timer.Stop()

	_, data, rerr := c.ws.Read(ctx)
	if rerr != nil {
		if !claimed.CompareAndSwap(false, true) {
			// The timer already claimed this failure and is running (or has
			// finished running) fail(welcomeErr) itself: report the same
			// *ConnError so Dial's own fail(ce) call for it is a no-op
			// (closeOnce), not a second error{} frame -- exactly
			// runServerHandshake's "AcceptConn's later c.fail(ce) ... is a
			// no-op" case.
			return welcomeErr
		}
		// The timer had not fired: this is not the welcome timeout. A close
		// frame arriving here (the peer upgraded, then closed before ever
		// sending welcome -- e.g. rejected during auth, no ws-mixer error{}
		// sent) gets its own message, kept %w-wrapped so
		// classifyFailureErr's errors.As(websocket.CloseError) still sees
		// through it.
		var ce websocket.CloseError
		if errors.As(rerr, &ce) {
			return fmt.Errorf("wsmixer: peer closed with code %d before welcome completed the handshake: %w", ce.Code, rerr)
		}
		// Anything else not caught above splits two ways, both still plain
		// wrapped errors (no synthesized close -- Dial's own postUpgradeError
		// wrapping, or the lack of it, is what actually decides the phase
		// now, not the message text here):
		if errors.Is(ctx.Err(), context.Canceled) {
			// A caller-cancelled ctx -- the caller is abandoning the dial
			// (Client.Close/CloseWith during this very wait, or a plain
			// Dial caller explicitly cancelling their own ctx), not
			// something to blame the peer for. Dial leaves this one
			// unwrapped (phase dial), so the message doesn't need to
			// pretend a welcome timeout happened either. Deliberately NOT
			// ctx.Err() != nil: see Dial's own identical check for why a
			// DeadlineExceeded must NOT take this branch (review item 6).
			return fmt.Errorf("wsmixer: hello/welcome wait ended: %w", rerr)
		}
		// ctx is either still live, or its deadline (not a cancellation)
		// expired: a genuine transport death (TCP reset, a half-open
		// connection finally dying, plain EOF) strictly after the 101,
		// before welcome -- not the HelloTimeout deadline (claimed above
		// would have caught that) and not a caller-driven cancellation.
		// Naming it "no welcome within Nms" would claim a timeout that
		// never actually happened; Dial's postUpgradeError wrapping is what
		// now reports this as phase handshake (D-2026-09-20 change 3), so
		// the message just says what was actually observed.
		return fmt.Errorf("wsmixer: connection lost before welcome completed the handshake: %w", rerr)
	}
	// timer.Stop() returning false means the deadline already fired and its
	// callback has started running (time.AfterFunc guarantees the callback
	// never runs at all when Stop returns true): it may already be writing
	// error{} or closing the socket, so there is no safe way to continue
	// toward welcome. The CompareAndSwap closes the remaining instant where
	// Stop() and the callback observe the deadline at the same time.
	if !timer.Stop() || !claimed.CompareAndSwap(false, true) {
		return welcomeErr
	}
	frame, ferr := DecodeFrame(data)
	if ferr != nil {
		if ce, ok := ferr.(*ConnError); ok {
			return ce
		}
		return newConnErrorf(ProtocolErrorCode, "malformed frame before welcome")
	}
	if frame.StreamID != 0 || frame.Type != FrameData {
		return newConnErrorf(ProtocolErrorCode, "frame received before welcome completed the handshake")
	}
	msg, perr := ParseControl(frame.Payload)
	if perr != nil {
		return perr.(*ConnError)
	}
	switch m := msg.(type) {
	case *ErrorMsg:
		return &ConnError{Code: ErrorCode(m.Code), Message: m.Message}
	case *WelcomeMsg:
		return c.applyWelcome(hello.Window, m)
	default:
		return newConnErrorf(ProtocolErrorCode, "first stream-0 message from the server must be welcome")
	}
}

// applyWelcome validates an incoming welcome and populates the connection's
// negotiated parameters. helloWindow is the receive window this side
// advertised in its own hello.
func (c *Conn) applyWelcome(helloWindow int64, welcome *WelcomeMsg) error {
	if welcome.V != 1 {
		return &ConnError{Code: UnsupportedCode, Message: fmt.Sprintf("welcome.v=%d does not match ws-mixer.v1", welcome.V)}
	}
	if !c.opts.allowSubfloorTiming {
		if welcome.PingInterval < pingIntervalMin {
			return newConnErrorf(ProtocolErrorCode, "welcome.ping_interval %d is below the %dms floor", welcome.PingInterval, pingIntervalMin)
		}
		if welcome.PingTimeout < 2*welcome.PingInterval {
			return newConnErrorf(ProtocolErrorCode, "welcome.ping_timeout (%d) must be at least 2x ping_interval (%d)", welcome.PingTimeout, welcome.PingInterval)
		}
	}
	c.session = welcome.Session
	c.peerWindow = welcome.Window
	c.ourWindow = helloWindow
	c.maxStreams = welcome.MaxStreams
	// declaredMaxStreams is the ceiling the server actually told us about
	// (and so is the ceiling it will honor when deciding whether to send an
	// OPEN); record it before any further, purely-local lowering below, so a
	// self-inflicted refusal can be told apart from the server exceeding what
	// it itself declared (see repeatRefusedOpen).
	c.declaredMaxStreams = welcome.MaxStreams
	if c.opts.MaxStreams > 0 && c.opts.MaxStreams < c.maxStreams {
		c.maxStreams = c.opts.MaxStreams
	}
	c.pingInterval = time.Duration(welcome.PingInterval) * time.Millisecond
	c.pingTimeout = time.Duration(welcome.PingTimeout) * time.Millisecond
	c.welcomeMeta = welcome.Meta
	c.welcomeMsg = welcome
	c.finishHandshake()
	return nil
}
