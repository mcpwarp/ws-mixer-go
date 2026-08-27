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

// ServerOptions configures a Listener.
type ServerOptions struct {
	Options

	// Authenticate is called once per connection, after hello parses and
	// before welcome is sent. Returning an error fails the handshake with
	// error{UNAUTHORIZED} (use Unauthorized/Unsupported to pick the code) and
	// closes with no welcome ever sent.
	Authenticate func(ctx context.Context, h *Hello) (WelcomeMeta, error)

	// AuthenticateRequest is an optional, cheaper hook that runs before the
	// WebSocket upgrade even happens (OVERVIEW.md section 2.1: "Header-level
	// rejection (HTTP 401 with no 101) is a separate, cheaper hook on the
	// upgrade itself"). It sees the raw HTTP request; returning an error
	// rejects the upgrade with HTTP 401 and a JSON body, before any 101.
	// Unlike Authenticate, this never sees hello.token or hello.meta.
	AuthenticateRequest func(r *http.Request) error

	// OnConn is called once the handshake completes successfully, before any
	// stream traffic is processed, and before the connection's
	// read/write/ping/watchdog goroutines start. The natural place to
	// register OnStream/OnApp/OnDrain, though those setters are safe to call
	// at any later point too -- they take effect for events delivered after
	// the call returns.
	OnConn func(*Conn)

	// ServerInfo is echoed back to the client in welcome.server.
	ServerInfo *ServerInfo
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

// Listener wraps ws-mixer's upgrade handler. It implements http.Handler, so
// it can be mounted directly with net/http.
type Listener struct {
	opts ServerOptions
}

// NewListener builds a Listener from ServerOptions.
func NewListener(opts ServerOptions) *Listener {
	opts.Options.setDefaults()
	if opts.OnConn == nil {
		opts.OnConn = func(*Conn) {}
	}
	return &Listener{opts: opts}
}

// ServeHTTP upgrades the request to a ws-mixer.v1 WebSocket connection,
// performs the handshake, and (on success) hands the resulting *Conn to
// OnConn. It blocks until the connection closes, matching net/http's handler
// contract.
//
// Per OVERVIEW.md section 2.1, both the subprotocol offer and the
// Authorization header are checked *before* the upgrade: a missing
// subprotocol is HTTP 400, missing/malformed auth is HTTP 401, and neither
// ever produces a 101-then-close.
func (l *Listener) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !offersSubprotocol(r, Subprotocol) {
		l.opts.Logger.Warn("client did not offer ws-mixer.v1", "sec_websocket_protocol", r.Header.Get("Sec-WebSocket-Protocol"))
		writeUpgradeError(w, http.StatusBadRequest, "unsupported_subprotocol", fmt.Sprintf("client must offer the %q subprotocol", Subprotocol))
		return
	}

	bearer, ok := parseBearerToken(r.Header.Get("Authorization"))
	if !ok {
		l.opts.Logger.Warn("missing or malformed Authorization header")
		writeUpgradeError(w, http.StatusUnauthorized, "unauthorized", "Authorization: Bearer <token> is required")
		return
	}

	if l.opts.AuthenticateRequest != nil {
		if err := l.opts.AuthenticateRequest(r); err != nil {
			l.opts.Logger.Warn("AuthenticateRequest rejected the upgrade", "error", err)
			writeUpgradeError(w, http.StatusUnauthorized, "unauthorized", err.Error())
			return
		}
	}

	ws, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		Subprotocols:    []string{Subprotocol},
		CompressionMode: websocket.CompressionDisabled,
	})
	if err != nil {
		l.opts.Logger.Warn("websocket accept failed", "error", err)
		return
	}
	defer ws.CloseNow()

	if ws.Subprotocol() != Subprotocol {
		// Belt-and-braces: offersSubprotocol already checked the offer, and
		// Accept was given exactly Subprotocol, so this should never trigger.
		l.opts.Logger.Warn("client did not negotiate ws-mixer.v1")
		_ = ws.Close(websocket.StatusProtocolError, "subprotocol not negotiated")
		return
	}
	ws.SetReadLimit(l.opts.ReadLimit)

	c := newConn(ws, RoleServer, l.opts.Options)
	hopts := serverHandshakeOptions{
		Options:      l.opts.Options,
		ServerInfo:   l.opts.ServerInfo,
		Authenticate: l.opts.Authenticate,
		Request:      r,
	}
	if err := performServerHandshake(r.Context(), c, bearer, hopts); err != nil {
		l.opts.Metrics.HandshakeFailed("server")
		if ce, ok := err.(*ConnError); ok {
			c.fail(ce)
		}
		<-c.closed
		return
	}

	l.opts.Metrics.ConnectionOpened(c.session, "server")
	// OnConn before run(): it is where OnStream/OnApp/OnDrain get registered,
	// and that must happen before the reader goroutine starts touching them.
	l.opts.OnConn(c)
	c.run()
	<-c.closed
	l.opts.Metrics.ConnectionClosed(c.session, closeCodeOf(c), errCodeOf(c))
}

// offersSubprotocol reports whether r's Sec-WebSocket-Protocol header(s) list
// want. r.Header.Values handles both the single comma-separated-list form and
// (non-conformant but seen in the wild) repeated headers.
func offersSubprotocol(r *http.Request, want string) bool {
	for _, line := range r.Header.Values("Sec-WebSocket-Protocol") {
		for _, p := range strings.Split(line, ",") {
			if strings.TrimSpace(p) == want {
				return true
			}
		}
	}
	return false
}

// parseBearerToken extracts the token from "Authorization: Bearer <token>",
// case-insensitively on the scheme, rejecting a missing header, a missing
// scheme (a bare token), or an empty token.
func parseBearerToken(header string) (token string, ok bool) {
	const prefix = "bearer "
	if len(header) <= len(prefix) || !strings.EqualFold(header[:len(prefix)], prefix) {
		return "", false
	}
	token = strings.TrimSpace(header[len(prefix):])
	if token == "" {
		return "", false
	}
	return token, true
}

// writeUpgradeError writes a pre-upgrade rejection: no 101 was ever sent.
func writeUpgradeError(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": code, "message": message})
}

// serverHandshakeOptions carries what performServerHandshake needs beyond the
// *Conn and its already-established transport: the bearer token extracted
// from the pre-upgrade Authorization header (or, for a harness with no HTTP
// layer, whatever token the test wants to have "already matched" the header
// check), the application's Authenticate hook, and the ServerOptions fields
// the exchange itself reads (HelloTimeout, Window, MaxStreams, PingInterval,
// PingTimeout, ServerInfo).
type serverHandshakeOptions struct {
	Options
	ServerInfo   *ServerInfo
	Authenticate func(ctx context.Context, h *Hello) (WelcomeMeta, error)
	Request      *http.Request // nil when there is no real HTTP request (e.g. a test harness)
}

// performServerHandshake runs the post-upgrade half of the server-side
// handshake (OVERVIEW.md sections 2.1/2.7): read hello with a timeout,
// validate it, call Authenticate, and send welcome — or error+close on
// failure. It operates purely through c.ws (the wsConn interface both
// *websocket.Conn and the sequence test harness's fake transport satisfy),
// so this is the one implementation of the exchange: Listener.ServeHTTP
// calls it after Accept, and the sequence test harness calls it the same way
// over its fake transport.
func performServerHandshake(ctx context.Context, c *Conn, bearer string, hopts serverHandshakeOptions) error {
	// A context deadline passed straight to Read would make coder/websocket
	// tear the whole connection down with no close frame at all once it
	// fires (context cancellation on Read closes the connection outright, per
	// its documented behavior — the same caveat OVERVIEW.md section 3.1 notes
	// for Ping). Timing this out with an explicit Close instead delivers the
	// real error{PROTOCOL_ERROR}/4001 to the peer, and closing is what
	// unblocks the Read below.
	helloErr := newConnErrorf(ProtocolErrorCode, "no hello within %dms of connection accept", hopts.HelloTimeout.Milliseconds())
	// claimed arbitrates between the timeout callback below and the
	// post-Read success path immediately after: whichever side wins the
	// CompareAndSwap owns the handshake outcome, so a timer that fires
	// concurrently with (or just after) a successful Read can never write
	// error{}/close the socket while this goroutine is also proceeding
	// toward welcome, or vice versa.
	var claimed atomic.Bool
	timer := time.AfterFunc(hopts.HelloTimeout, func() {
		if !claimed.CompareAndSwap(false, true) {
			return // the post-Read path already claimed the handshake
		}
		// fail writes error{} (via writeControlNow, since run() has not
		// started the writer loop yet) and closes the socket and c.closed
		// under closeOnce, so ServeHTTP's later c.fail(ce) for this same
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
	if hopts.Authenticate != nil {
		wm, aerr := hopts.Authenticate(ctx, &Hello{
			Token: hello.Token, Agent: hello.Agent, Meta: hello.Meta,
			Capabilities: hello.Capabilities, Request: hopts.Request,
		})
		if aerr != nil {
			if ce, ok := aerr.(*ConnError); ok {
				return ce
			}
			return &ConnError{Code: UnauthorizedCode, Message: aerr.Error()}
		}
		welcomeMeta = wm
	}

	c.session = newSessionID()
	c.ourWindow = hopts.Window
	c.maxStreams = min64(clientMaxStreams, hopts.MaxStreams)
	if c.maxStreams <= 0 {
		c.maxStreams = hopts.MaxStreams
	}
	c.declaredMaxStreams = c.maxStreams
	c.pingInterval = hopts.PingInterval
	c.pingTimeout = hopts.PingTimeout

	welcome := &WelcomeMsg{
		T: "welcome", V: 1, Session: c.session,
		Window: c.ourWindow, MaxStreams: c.maxStreams,
		PingInterval: c.pingInterval.Milliseconds(),
		PingTimeout:  c.pingTimeout.Milliseconds(),
		Server:       hopts.ServerInfo,
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

func closeCodeOf(c *Conn) int {
	if ce, ok := c.Err().(*ConnError); ok {
		return ce.CloseCode()
	}
	return 1000
}

func errCodeOf(c *Conn) string {
	if ce, ok := c.Err().(*ConnError); ok {
		return ce.Code.String()
	}
	return ""
}
