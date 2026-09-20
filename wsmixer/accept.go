package wsmixer

import (
	"context"
	"crypto/rand"
	"encoding/base32"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
)

// Subprotocol is the WebSocket subprotocol both sides must negotiate
// (OVERVIEW.md section 2.1).
const Subprotocol = "ws-mixer.v1"

// Hello is what the Authenticate hook sees: everything hello carried, plus
// the raw HTTP request that produced the upgrade.
type Hello struct {
	Token        string
	Agent        AgentInfo
	Meta         json.RawMessage
	Capabilities []string
	Request      *http.Request
}

// WelcomeMeta is the Authenticate hook's answer: application data returned to
// the client inside welcome.meta.
type WelcomeMeta struct {
	Meta any
}

// Unauthorized builds the error Authenticate should return for a bad or
// expired token: error{UNAUTHORIZED}, WS close 4011.
func Unauthorized(format string, args ...any) error {
	return &ConnError{Code: UnauthorizedCode, Message: fmt.Sprintf(format, args...)}
}

// Unsupported builds the error Authenticate should return for a version or
// capability mismatch: error{UNSUPPORTED}, WS close 4010.
func Unsupported(format string, args ...any) error {
	return &ConnError{Code: UnsupportedCode, Message: fmt.Sprintf(format, args...)}
}

// AcceptOptions configures AcceptConn.
type AcceptOptions struct {
	Options

	// ServerInfo is echoed back to the client in welcome.server.
	ServerInfo *ServerInfo

	// Authenticate is called once per connection, after hello parses and
	// before welcome is sent. Returning an error fails the handshake with
	// error{UNAUTHORIZED} (use Unauthorized/Unsupported to pick the code) and
	// closes with no welcome ever sent.
	Authenticate func(ctx context.Context, h *Hello) (WelcomeMeta, error)

	// Request is the HTTP request that produced the upgrade, forwarded to
	// Authenticate as Hello.Request. nil for a harness with no HTTP layer.
	Request *http.Request

	// SessionID overrides how the welcome.session id is generated. nil uses
	// crypto/rand base32, as today.
	SessionID func() string
}

// AcceptConn runs the post-upgrade server-side wire handshake on an already
// upgraded socket: hello-with-timeout, validation, Authenticate, welcome.
// bearer is the token the caller already extracted from the Authorization
// header (the caller owns the pre-upgrade checks). A zero-value AcceptOptions
// is usable as-is: AcceptConn applies Options.SetDefaults() itself. On
// failure it performs the error{}+close teardown itself and waits for it, so
// the caller only reports; the returned *Conn is still valid to inspect
// (Err/CloseCode/ErrCodeName/Done), it is simply already closed. On success
// the returned *Conn is handshaken but NOT running: register handlers, then
// call Run.
func AcceptConn(ctx context.Context, ws WSConn, bearer string, opts AcceptOptions) (*Conn, error) {
	opts.Options.SetDefaults()
	c := newConn(ws, RoleServer, opts.Options)
	if err := runServerHandshake(ctx, c, bearer, opts); err != nil {
		ce, ok := err.(*ConnError)
		if !ok {
			ce = newConnErrorf(InternalErrorCode, "%v", err)
		}
		c.fail(ce)
		<-c.closed
		return c, err
	}
	return c, nil
}

// runServerHandshake runs the post-upgrade half of the server-side handshake
// (OVERVIEW.md sections 2.1/2.7): read hello with a timeout, validate it,
// call Authenticate, and send welcome — or return an error on failure, which
// AcceptConn turns into the error{}+close teardown. It operates purely
// through c.ws (the WSConn interface both *websocket.Conn and the sequence
// test harness's fake transport satisfy), so this is the one implementation
// of the exchange.
func runServerHandshake(ctx context.Context, c *Conn, bearer string, opts AcceptOptions) error {
	// A context deadline passed straight to Read would make coder/websocket
	// tear the whole connection down with no close frame at all once it
	// fires (context cancellation on Read closes the connection outright, per
	// its documented behavior — the same caveat OVERVIEW.md section 3.1 notes
	// for Ping). Timing this out with an explicit Close instead delivers the
	// real error{PROTOCOL_ERROR}/4001 to the peer, and closing is what
	// unblocks the Read below.
	helloErr := newConnErrorf(ProtocolErrorCode, "no hello within %dms of connection accept", opts.HelloTimeout.Milliseconds())
	// claimed arbitrates between the timeout callback below and the
	// post-Read success path immediately after: whichever side wins the
	// CompareAndSwap owns the handshake outcome, so a timer that fires
	// concurrently with (or just after) a successful Read can never write
	// error{}/close the socket while this goroutine is also proceeding
	// toward welcome, or vice versa.
	var claimed atomic.Bool
	timer := time.AfterFunc(opts.HelloTimeout, func() {
		if !claimed.CompareAndSwap(false, true) {
			return // the post-Read path already claimed the handshake
		}
		// fail writes error{} (via writeControlNow, since Run() has not
		// started the writer loop yet) and closes the socket and c.closed
		// under closeOnce, so AcceptConn's later c.fail(ce) for this same
		// helloErr is a no-op instead of a second error{} frame.
		c.fail(helloErr)
	})
	defer timer.Stop()

	_, data, err := c.ws.Read(ctx)
	if err != nil {
		return helloErr
	}
	// timer.Stop() returning false means the deadline already fired and its
	// callback has started running (time.AfterFunc guarantees the callback
	// never runs at all when Stop returns true): it may already be writing
	// error{} or closing the socket, so there is no safe way to continue
	// toward welcome. The CompareAndSwap closes the remaining instant where
	// Stop() and the callback observe the deadline at the same time.
	if !timer.Stop() || !claimed.CompareAndSwap(false, true) {
		return helloErr
	}
	frame, ferr := DecodeFrame(data)
	if ferr != nil {
		if ce, ok := ferr.(*ConnError); ok {
			return ce
		}
		return newConnErrorf(ProtocolErrorCode, "malformed frame before hello")
	}
	if frame.StreamID != 0 || frame.Type != FrameData {
		return newConnErrorf(ProtocolErrorCode, "frame received before hello completed the handshake")
	}
	msg, perr := ParseControl(frame.Payload)
	if perr != nil {
		return perr.(*ConnError)
	}
	hello, ok := msg.(*HelloMsg)
	if !ok {
		return newConnErrorf(ProtocolErrorCode, "first stream-0 message must be hello")
	}
	if bearer == "" || hello.Token != bearer {
		return &ConnError{Code: UnauthorizedCode, Message: "hello.token does not match the Authorization bearer token"}
	}

	c.helloMeta = hello.Meta
	c.peerAgent = hello.Agent
	c.peerWindow = hello.Window
	clientMaxStreams := hello.MaxStreams

	welcomeMeta := WelcomeMeta{}
	if opts.Authenticate != nil {
		wm, aerr := opts.Authenticate(ctx, &Hello{
			Token: hello.Token, Agent: hello.Agent, Meta: hello.Meta,
			Capabilities: hello.Capabilities, Request: opts.Request,
		})
		if aerr != nil {
			if ce, ok := aerr.(*ConnError); ok {
				return ce
			}
			return &ConnError{Code: UnauthorizedCode, Message: aerr.Error()}
		}
		welcomeMeta = wm
	}

	if opts.SessionID != nil {
		c.session = opts.SessionID()
	} else {
		c.session = newSessionID()
	}
	c.ourWindow = opts.Window
	c.maxStreams = min64(clientMaxStreams, opts.MaxStreams)
	if c.maxStreams <= 0 {
		c.maxStreams = opts.MaxStreams
	}
	c.declaredMaxStreams = c.maxStreams
	c.pingInterval = opts.PingInterval
	c.pingTimeout = opts.PingTimeout

	welcome := &WelcomeMsg{
		T: "welcome", V: 1, Session: c.session,
		Window: c.ourWindow, MaxStreams: c.maxStreams,
		PingInterval: c.pingInterval.Milliseconds(),
		PingTimeout:  c.pingTimeout.Milliseconds(),
		Server:       opts.ServerInfo,
	}
	if welcomeMeta.Meta != nil {
		b, merr := json.Marshal(welcomeMeta.Meta)
		if merr != nil {
			return newConnErrorf(InternalErrorCode, "encoding welcome.meta: %v", merr)
		}
		welcome.Meta = b
	}
	b, merr := json.Marshal(welcome)
	if merr != nil {
		return newConnErrorf(InternalErrorCode, "encoding welcome: %v", merr)
	}
	wctx, wcancel := context.WithTimeout(ctx, 5*time.Second)
	defer wcancel()
	if err := c.ws.Write(wctx, websocket.MessageBinary, EncodeData(0, b)); err != nil {
		return newConnErrorf(InternalErrorCode, "sending welcome: %v", err)
	}
	c.finishHandshake()
	return nil
}

func min64(a, b int64) int64 {
	if a <= 0 {
		return b
	}
	if b <= 0 {
		return a
	}
	if a < b {
		return a
	}
	return b
}

func newSessionID() string {
	var buf [16]byte
	_, _ = rand.Read(buf[:])
	return strings.ToLower(base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(buf[:]))
}

// CloseCode returns the WebSocket close code derived from Err() --
// 4000+error_code, or 1000 when there is no *ConnError. For an error code
// that maps outside the legal WS close-code range (errors.go, WIRE.md
// section 2.8), this is the code error{} actually carried, not the clamped
// code the WS close frame itself used (wsCloseCode, conn.go) -- both peers
// derive this same semantic code from error{}, so it is the right number to
// report even when the close frame on the wire said something else.
func (c *Conn) CloseCode() int {
	if ce, ok := c.Err().(*ConnError); ok {
		return ce.CloseCode()
	}
	return 1000
}

// ErrCodeName returns the wire error code name the connection ended with, or
// "" if there is none.
func (c *Conn) ErrCodeName() string {
	if ce, ok := c.Err().(*ConnError); ok {
		return ce.Code.String()
	}
	return ""
}
