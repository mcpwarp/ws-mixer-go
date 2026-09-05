package wsmixer

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/coder/websocket"
)

// defaultSDKVersion is reported as Agent.SDKVersion in the hello message
// when the caller doesn't set one. Single source for this package.
const defaultSDKVersion = "0.4.0"

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

// Dial connects to a ws-mixer.v1 server, performs the hello/welcome
// handshake, and returns a ready-to-use *Conn.
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
		return nil, err
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

	hctx, hcancel := context.WithTimeout(ctx, c.opts.HelloTimeout)
	defer hcancel()
	_, data, rerr := c.ws.Read(hctx)
	if rerr != nil {
		return fmt.Errorf("wsmixer: no welcome within %dms of hello: %w", c.opts.HelloTimeout.Milliseconds(), rerr)
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
