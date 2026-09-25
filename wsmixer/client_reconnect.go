package wsmixer

// Client is the reconnecting, high-level ws-mixer.v1 client CLIENT-SDK.md
// requires: it owns the WIRE.md section 2.9 reconnect state machine on top
// of the plain, single-attempt Dial (client.go), which keeps working
// unchanged underneath it. Mirrors ws-mixer-js's MixerClient
// (src/client.ts) behaviorally -- full-jitter backoff,
// drain/4012/4013/4009 special-cased, a fatal set that never retries --
// translated into Go's goroutine/channel idiom rather than JS's
// async/await + EventEmitter one. One deliberate divergence, added
// D-2026-09-20: the backoff attempt counter (and the once-only budgets
// alongside it) resets only once a connection has stayed up
// ReconnectOptions.StableAfter past its own welcome (armStability), not at
// welcome itself -- WIRE.md section 2.9's `stable` state -- so a server
// that welcomes and immediately closes can't make the client redial once a
// second forever.
//
// State machine (single source of truth for "what Client is doing right
// now"): idle -> dialing -> connected -> backoff -> dialing -> ... ending in
// closed, exactly once, via goFatal/reportAndSchedule's exhaustion branch/
// Close(). A `drain` from the peer starts a second, parallel dialing attempt
// (retiringConn) without ever moving the state machine through backoff --
// WIRE.md section 2.9: "new connection immediately and in parallel, before
// the old one closes."

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
)

// TokenProvider returns a fresh bearer token for one dial attempt. Client
// invokes it on every dial -- including every reconnect -- and never caches
// the result (CLIENT-SDK.md's "token provider" rule). A provider that
// returns an error is fatal: never retried (except the one HTTP-401/4011
// refresh below), and the error is surfaced verbatim on
// DisconnectReason.Cause -- unless the error wraps ErrTokenUnavailable, in
// which case it is treated like an ordinary failed dial instead (see that
// sentinel's own doc comment).
type TokenProvider func(ctx context.Context) (string, error)

// ErrTokenUnavailable is the sentinel a TokenProvider's error wraps (e.g.
// fmt.Errorf("%w: auth server unreachable", wsmixer.ErrTokenUnavailable), or
// a custom error type whose Is method matches it) to mark a failure to
// OBTAIN a token as temporary -- the token itself isn't the problem, the
// provider just couldn't reach it right now (the network is briefly down, or
// the auth server is unreachable while refreshing an expired token) --
// rather than fatal. Client then treats the attempt like a failed dial
// instead of ending the client: phase "dial", non-fatal, normal backoff, no
// immediate retry (classifyFailureErr) -- it still counts as a failed
// attempt (attempt increments, MaxAttempts still applies) and still spends
// the one-time 401/4011 refresh-retry budget when hit on that call
// (dialAndHandshake), exactly like any other failure there.
//
// Detection is errors.Is only (CLIENT-SDK.md's "Provider failure" row): a
// provider's error either wraps this sentinel or it doesn't. Client never
// infers "temporary" from an error's shape -- no Retryable()/Temporary()
// duck-typing -- since an accidental match on an otherwise-fatal provider
// error would silently turn it into an endless retry loop instead of ever
// surfacing to the caller. An unmarked provider error stays fatal exactly as
// before.
var ErrTokenUnavailable = errors.New("wsmixer: token temporarily unavailable")

// StaticToken returns a TokenProvider for the common case of a token that
// never changes.
func StaticToken(token string) TokenProvider {
	return (&staticTokenProvider{token: token}).provide
}

// staticTokenProvider backs StaticToken; its method value is what
// isRealProvider recognizes (nit 2).
type staticTokenProvider struct{ token string }

func (s *staticTokenProvider) provide(context.Context) (string, error) { return s.token, nil }

// isRealProvider reports whether tp might return a different token on a
// second call -- a caller-supplied TokenProvider (e.g. one that refreshes
// short-lived credentials) as opposed to StaticToken's constant wrapper.
// Mirrors the JS SDK's `isProvider` gate (`typeof this.opts.token ===
// "function"`, true only when the caller actually passed a provider
// function rather than a plain string): Go's TokenProvider is always a
// func, so there is no such type-level distinction to make, but every
// StaticToken-built provider is a method value of the same underlying
// method regardless of receiver, so reflect.Value.Pointer() (the entry
// point of the underlying code, not the receiver) recognizes it. Used to
// skip the one-time HTTP-401 refresh-retry: retrying with the exact same
// static token can never fix an auth failure -- dialAndHandshake goes fatal
// at once for that case instead (docs/DECISIONS.md 2026-09-20), rather than
// retrying the refresh a real TokenProvider gets a shot at.
func isRealProvider(tp TokenProvider) bool {
	return reflect.ValueOf(tp).Pointer() != reflect.ValueOf(StaticToken("")).Pointer()
}

// DisconnectPhase is where in the connection lifecycle a disconnect
// happened, per CLIENT-SDK.md's disconnect reason shape.
type DisconnectPhase string

const (
	PhaseDial      DisconnectPhase = "dial"
	PhaseHandshake DisconnectPhase = "handshake"
	PhaseConnected DisconnectPhase = "connected"
)

// DisconnectReason describes one disconnect, recoverable or fatal, reported
// to Client's OnDisconnect for every single one of them (CLIENT-SDK.md's
// disconnect reason shape).
type DisconnectReason struct {
	Phase DisconnectPhase
	// WSCode is the WebSocket close code, when a close occurred; 0 if none
	// (e.g. a dial failure that never reached a socket). When derived from a
	// *ConnError (HasErrorCode true), this is the semantic 4000+error_code
	// value (see Conn.CloseCode()'s doc), which for an error code that maps
	// outside the legal WS close-code range can differ from the close frame
	// actually observed on the wire (wsCloseCode, conn.go); when this side
	// instead only ever saw a raw close frame with no preceding error{}, it
	// is exactly what was on the wire.
	WSCode int
	// ErrorCode/HasErrorCode/ErrorName are set whenever this disconnect
	// carries a ws-mixer error code, from any of three sources
	// (CLIENT-SDK.md's "Disconnect reason shape" row):
	//  1. a peer's wire error{} message preceded the close -- ErrorCode is
	//     exactly that message's code;
	//  2. a close frame carrying a WS close code in ws-mixer's own reserved
	//     range (4001-4999) was observed with no preceding error{} ("bare"),
	//     in which case ErrorCode is derived mechanically as
	//     ErrorCode(wsCode-4000) (deriveBareCloseErrorCode, WIRE.md section
	//     2.8's ws_close = 4000 + error_code rule, which the derivation
	//     simply runs in reverse -- never wrong, so this is never guessed);
	//  3. a ws-mixer error the SDK itself raises LOCALLY, with no close
	//     frame from the peer involved at all: a missing/mismatched
	//     subprotocol echo on the upgrade (classifyFailureErr's *DialError
	//     branch, UNSUPPORTED, no close frame or HTTP status either), the
	//     welcome timeout (client.go's own locally generated
	//     PROTOCOL_ERROR/4001), and any other local fail() protocol
	//     violation of an already-connected conn. Go specifics: both of
	//     those last two happen to reach classifyFailureErr/
	//     buildConnectedDisconnectReason as a *ConnError -- this side's own
	//     fail() produces exactly the same type a peer's error{} does -- so
	//     they are handled by the SAME code as source 1 above, not a
	//     separate branch; they are still source 3 by the spec's own
	//     definition (no close frame from the PEER preceded them), the code
	//     path is just shared.
	// ErrorName is that code's String() in every case (unknown ->
	// "INTERNAL_ERROR", as always). Never synthesized for anything that
	// isn't a ws-mixer code: an HTTP upgrade rejection (401/403/404/429/5xx)
	// leaves this unset -- HTTPStatus carries that instead -- and so does an
	// abnormal closure, no close frame at all, or an ordinary non-ws-mixer
	// WS close code (1000, 1001, 1006, 1007, 1009, 1011, ...).
	ErrorCode    ErrorCode
	HasErrorCode bool
	ErrorName    string
	// HTTPStatus is set when the upgrade itself failed at the HTTP layer
	// (401/403/404/429/5xx/...).
	HTTPStatus int
	Fatal      bool
	Message    string
	// CloseReason is the peer's WebSocket close-frame reason string,
	// verbatim, whenever this side actually observed a close frame --
	// independent of whether a ws-mixer *ConnError (HasErrorCode) also
	// preceded it. "" when no close frame was observed (an abnormal
	// closure, or a dial failure that never reached a socket) or the peer
	// sent an empty reason. Also "" when the peer sent a stream-0 error{}
	// message and then closed: the handshake and connected-phase read paths
	// both stop reading as soon as they've parsed that error{}, so the
	// close frame behind it is never actually observed.
	CloseReason string
	// Cause is set only when a token provider threw/rejected: its error,
	// verbatim.
	Cause error
}

// Error lets a DisconnectReason be used directly as an error (e.g. returned
// from Client.Connect on a fatal first-connect failure).
func (r DisconnectReason) Error() string { return r.Message }

// ReconnectOptions configures Client's WIRE.md section 2.9 reconnect state
// machine: full-jitter backoff (delay = random(0, min(cap, base*2^attempt))),
// the attempt counter (and every once-only reconnect budget alongside it)
// reset only once a connection has stayed up StableAfter past its own
// welcome -- not at welcome itself (see StableAfter's own doc comment).
type ReconnectOptions struct {
	Base time.Duration // default 1s
	Cap  time.Duration // default 60s
	// ConnectTimeout bounds the dial itself (WIRE.md section 2.9's "connect
	// timeout 10s"), not the welcome wait that follows it -- that gets
	// Options.HelloTimeout (WIRE.md section 2.10 step 2's separate,
	// sequential 10s budget) on top, since dialAndHandshake's single
	// per-attempt ctx has to cover both dialOnce's websocket.Dial and,
	// inside it, clientHandshake's welcome read (see dialAndHandshake's own
	// comment for why they can't race on one ctx sized for only one of
	// them). Default 10s. A timed-out handshake against a peer that no
	// longer reads can also take up to ~5s more for coder/websocket's own
	// close handshake before the attempt actually returns, so a single
	// attempt's worst case is roughly ConnectTimeout + HelloTimeout + 5s.
	ConnectTimeout time.Duration
	// MaxAttempts bounds consecutive reconnect attempts that never reach a
	// stable connection (StableAfter). It is not simply "reconnects since
	// the client started": a connection that stays up StableAfter re-arms
	// this budget from zero (armStability), so a healthy client that drops
	// and recovers occasionally is never at risk of exhausting it -- only a
	// server that keeps failing every attempt before any of them can ever
	// stabilize does. Zero (the Go zero value, so reconnect is ON by
	// default with no configuration at all) means unlimited.
	MaxAttempts int
	// Disabled turns reconnect off entirely (nit 7: this used to be
	// overloaded onto a negative MaxAttempts; an explicit bool reads better
	// at every call site and doesn't collide with a caller's own "-1 means
	// something else" convention). WIRE.md section 2.9's `drain` row still
	// applies (in-flight streams finish normally until the server's own
	// deadline), but the eventual close is reported as one fatal disconnect
	// instead of triggering a redial.
	Disabled bool

	// StableAfter is how long a connection has to stay up, past welcome,
	// before it counts as genuinely stable: the backoff attempt counter, and
	// the once-only budgets below it (keepaliveRetryUsed, refreshRetryUsed --
	// see armStability), are re-armed only once a connection has stayed up
	// this long after its own welcome, not at welcome itself. Without this,
	// a server that welcomes a connection and then immediately closes it
	// resets the counter every single cycle and the client redials about
	// once a second forever instead of ever backing off (spec WIRE.md
	// section 2.9's `stable` state). Zero (the Go zero value) means the 10s
	// default, same as every other field here -- there is no way to opt out
	// of a stability window entirely, only to make it effectively
	// instantaneous with a tiny nonzero value (e.g. time.Nanosecond), which
	// approximates the old reset-at-welcome behavior. A drain-triggered
	// parallel reconnect (handleServerDrain) does not itself increment
	// attempt, so a server that drains faster than StableAfter, repeatedly,
	// never lets a connection stabilize either -- attempt simply stays
	// frozen wherever it last was (not climbing, since a drain reconnect
	// isn't a failure), until either a connection finally does stay up this
	// long, or an unrelated ordinary failure starts climbing it from there.
	// Default 10s.
	StableAfter time.Duration

	// now/after/randFloat64 are unexported test-only seams: a white-box
	// table test in this package can inject a fake clock and a deterministic
	// "random" source, but nothing outside the package can (or needs to).
	// Left nil, Client uses time.Now/time.After/math/rand.Float64.
	now         func() time.Time
	after       func(d time.Duration) <-chan time.Time
	randFloat64 func() float64
}

func (r *ReconnectOptions) setDefaults() {
	if r.Base <= 0 {
		r.Base = 1000 * time.Millisecond
	}
	if r.Cap <= 0 {
		r.Cap = 60 * time.Second
	}
	if r.ConnectTimeout <= 0 {
		r.ConnectTimeout = 10 * time.Second
	}
	if r.StableAfter <= 0 {
		r.StableAfter = 10 * time.Second
	}
	if r.now == nil {
		r.now = time.Now
	}
	if r.after == nil {
		r.after = time.After
	}
	if r.randFloat64 == nil {
		r.randFloat64 = rand.Float64
	}
}

// jitter returns random(0, max).
func (r *ReconnectOptions) jitter(max time.Duration) time.Duration {
	if max <= 0 {
		return 0
	}
	return time.Duration(r.randFloat64() * float64(max))
}

// fullJitter returns AWS "full jitter": random(0, min(cap, base*2^attempt)).
func (r *ReconnectOptions) fullJitter(attempt int) time.Duration {
	exp := float64(r.Base) * math.Pow(2, float64(attempt))
	if exp > float64(r.Cap) {
		exp = float64(r.Cap)
	}
	return time.Duration(r.randFloat64() * exp)
}

// providerError marks a TokenProvider failure, its underlying error
// surfaced verbatim on DisconnectReason.Cause either way (CLIENT-SDK.md's
// "Provider failure" rule): fatal, no retry -- unless cause wraps
// ErrTokenUnavailable (that sentinel's own doc comment), in which case it is
// treated like an ordinary failed dial instead, with normal backoff.
type providerError struct{ cause error }

func (e *providerError) Error() string { return e.cause.Error() }
func (e *providerError) Unwrap() error { return e.cause }

// fatalOverride forces a wrapped dial/handshake failure to be treated as
// fatal regardless of what it would otherwise classify as -- used for the
// second failure of the one-time 401/4011 refresh-retry (CLIENT-SDK.md: "a
// second HTTP 401 is fatal").
type fatalOverride struct{ err error }

func (e *fatalOverride) Error() string { return e.err.Error() }
func (e *fatalOverride) Unwrap() error { return e.err }

func markFatal(err error) error { return &fatalOverride{err: err} }

// isUnauthorized reports whether err is an HTTP 401 dial failure, a
// UNAUTHORIZED (4011) handshake ConnError, or a bare pre-welcome
// websocket.CloseError{Code: 4011} (a peer that closed 4011 without ever
// sending error{} -- still UNAUTHORIZED by code) -- the failures eligible
// for CLIENT-SDK.md's one-time immediate refresh-retry.
func isUnauthorized(err error) bool {
	var de *DialError
	if errors.As(err, &de) && de.HTTPStatus == http.StatusUnauthorized {
		return true
	}
	var ce *ConnError
	if errors.As(err, &ce) && ce.Code == UnauthorizedCode {
		return true
	}
	var wsce websocket.CloseError
	if errors.As(err, &wsce) && wsce.Code == 4011 {
		return true
	}
	return false
}

// failureInfo is everything classifyFailureErr extracts from a dial/
// handshake failure, used both to decide fatality/retry and to build the
// final DisconnectReason.
type failureInfo struct {
	phase         DisconnectPhase
	httpStatus    int
	wsCode        int
	errCode       ErrorCode
	hasErrorCode  bool
	fatal         bool
	retryAfter    time.Duration
	hasRetryAfter bool
	message       string
	closeReason   string
	cause         error
}

func (fi failureInfo) toReason() DisconnectReason {
	name := ""
	if fi.hasErrorCode {
		name = fi.errCode.String()
	}
	return DisconnectReason{
		Phase:        fi.phase,
		WSCode:       fi.wsCode,
		ErrorCode:    fi.errCode,
		HasErrorCode: fi.hasErrorCode,
		ErrorName:    name,
		HTTPStatus:   fi.httpStatus,
		Message:      fi.message,
		CloseReason:  fi.closeReason,
		Cause:        fi.cause,
	}
}

// phaseFor classifies which phase a dial/handshake failure belongs to: a
// *ConnError only ever comes from the post-101 hello/welcome exchange
// (client.go's clientHandshake), and so does a websocket.CloseError (the
// peer closed the socket, with or without a preceding ws-mixer error{},
// before welcome). A *postUpgradeError marks every OTHER clientHandshake
// failure the same way -- a transport death/EOF/unreadable frame that
// happened strictly after websocket.Dial's own upgrade already succeeded
// (the 101) is phase handshake too, by construction (where the failure
// happened), not by guessing from the error's type -- everything else (a
// *DialError, a plain pre-101 dial error, a *providerError) is phase
// "dial". Checked first and unconditionally: a *providerError wraps
// whatever the caller's own TokenProvider returned, which could
// coincidentally be or wrap a *ConnError or a websocket.CloseError of its
// own -- that must never be misread as this side's own handshake phase.
func phaseFor(err error) DisconnectPhase {
	var pe *providerError
	if errors.As(err, &pe) {
		return PhaseDial
	}
	var ce *ConnError
	if errors.As(err, &ce) {
		return PhaseHandshake
	}
	var wsce websocket.CloseError
	if errors.As(err, &wsce) {
		return PhaseHandshake
	}
	var pue *postUpgradeError
	if errors.As(err, &pue) {
		return PhaseHandshake
	}
	return PhaseDial
}

// deriveBareCloseErrorCode maps a bare WS close code -- one observed with no
// preceding ws-mixer error{} message -- back to its semantic ErrorCode, per
// the mechanical ws_close = 4000 + error_code rule (WIRE.md section 2.8):
// that arithmetic is never wrong, so a bare close still carries a usable
// ErrorCode/ErrorName instead of leaving every caller to redo it themselves
// (the JS SDK already does this same derivation). Only ws-mixer's own
// reserved close-code range, 4001-4999, counts -- 4000 (NoError) is
// deliberately excluded, since this package's own Conn.Close always sends
// error{NoError,...} ahead of a graceful close, so a genuinely bare 4000
// would itself be a peer bug this derivation has no basis to paper over. An
// ordinary WS close outside that range (1000, 1001, 1006 abnormal closure,
// 1007, 1009, 1011, ...), or no close observed at all (wsCode 0), is never a
// ws-mixer error code and yields has=false. Shared by classifyFailureErr's
// (handshake-phase) bare-websocket.CloseError case and
// buildConnectedDisconnectReason's (connected-phase) non-*ConnError case, so
// both derive it identically -- implements ws-mixer-spec's D-2026-09-20-09
// (docs/DECISIONS.md).
func deriveBareCloseErrorCode(wsCode int) (code ErrorCode, has bool) {
	if wsCode < 4001 || wsCode > 4999 {
		return 0, false
	}
	return ErrorCode(wsCode - 4000), true
}

// classifyFailureErr inspects a dialAndHandshake failure (via errors.As, so
// it sees through fatalOverride/providerError/DialError wrapping) and
// extracts everything needed for both the retry decision and the final
// DisconnectReason.
func classifyFailureErr(err error) failureInfo {
	// Checked first and unconditionally, before phaseFor or the wsCode
	// extraction below: a *providerError wraps whatever the caller's own
	// TokenProvider returned, which could coincidentally be or wrap a
	// *ConnError or a websocket.CloseError of its own -- none of that is
	// this side's own handshake phase/close code, so it must never reach
	// the extraction below. This also means a *fatalOverride is never
	// consulted for a *providerError (dialAndHandshake never wraps one in
	// markFatal anyway -- see ErrTokenUnavailable's own doc comment) --
	// this branch always returns before the fatalOverride check further
	// down ever runs.
	var pe *providerError
	if errors.As(err, &pe) {
		if errors.Is(pe.cause, ErrTokenUnavailable) {
			// Marked temporary (CLIENT-SDK.md's "Provider failure" row):
			// treated exactly like a failed dial -- non-fatal, normal
			// backoff, no immediate retry -- with the provider's error
			// still surfaced verbatim on Cause.
			return failureInfo{phase: PhaseDial, fatal: false, cause: pe.cause, message: pe.cause.Error()}
		}
		return failureInfo{phase: PhaseDial, fatal: true, cause: pe.cause, message: pe.cause.Error()}
	}

	phase := phaseFor(err)
	fi := failureInfo{phase: phase, message: err.Error()}

	// A websocket.CloseError observed anywhere in the chain (clientHandshake's
	// bare-close case, client.go) carries the peer's actual close code/reason
	// -- extract both up front so every branch below inherits them unless it
	// has a more specific wsCode of its own (the *ConnError branch does).
	var wsce websocket.CloseError
	if errors.As(err, &wsce) {
		fi.wsCode = int(wsce.Code)
		fi.closeReason = wsce.Reason
	}

	forced := false
	var fo *fatalOverride
	if errors.As(err, &fo) {
		forced = true
	}

	var de *DialError
	if errors.As(err, &de) {
		fi.httpStatus = de.HTTPStatus
		fi.fatal = de.Fatal || forced
		fi.retryAfter = de.RetryAfter
		fi.hasRetryAfter = de.HasRetryAfter
		if de.Mismatch {
			// The third errorCode source (CLIENT-SDK.md's "Disconnect
			// reason shape" row, D-2026-09-20-09): not every ErrorCode
			// comes off the wire -- a missing/mismatched subprotocol echo
			// is a ws-mixer error the SDK itself raised locally, with no
			// close frame and no HTTP status behind it, but UNSUPPORTED is
			// exactly what it means (the same code welcome.v mismatch maps
			// to). Report-only: fatality already comes from de.Fatal above,
			// set unconditionally for Mismatch regardless of this. The JS
			// SDK reports the same code for this case.
			fi.errCode = UnsupportedCode
			fi.hasErrorCode = true
		}
		return fi
	}

	var ce *ConnError
	if errors.As(err, &ce) {
		fi.hasErrorCode = true
		fi.errCode = ce.Code
		fi.wsCode = ce.CloseCode()
		fi.message = ce.Message
		fi.fatal = forced || ce.Code == UnsupportedCode
		return fi
	}

	// Neither *DialError nor *ConnError matched: this is either an ordinary
	// non-ws-mixer dial failure, or the bare-CloseError case above (a peer
	// that closed 4010/4011 without ever sending error{} -- an SDK bug on
	// its part, but WIRE.md section 2.9's fatal set is keyed on the close
	// code, not on whether error{} happened to precede it). 4011 has no
	// fatal-by-itself rule here: it joins isUnauthorized's one-time
	// refresh-retry instead, and only a forced second failure is fatal,
	// exactly like the *ConnError{UnauthorizedCode} case.
	fi.fatal = forced || fi.wsCode == 4010
	// Report-only: a bare close still gets a derived ErrorCode/ErrorName
	// (deriveBareCloseErrorCode) -- this never changes fi.fatal/fi.phase/
	// fi.wsCode, all already decided above, only what the final
	// DisconnectReason carries alongside them.
	if ec, has := deriveBareCloseErrorCode(fi.wsCode); has {
		fi.errCode = ec
		fi.hasErrorCode = true
	}
	return fi
}

// ClientConfig configures Client.
type ClientConfig struct {
	Options
	Agent        AgentInfo
	Meta         any
	Capabilities []string
	HTTPHeader   http.Header

	Reconnect ReconnectOptions

	// OnConnect fires once per successful welcome -- the first connection
	// and every reconnect after it -- with the live *Conn and the server's
	// welcome. Re-register any per-connection application state here (e.g.
	// re-announce services): WIRE.md section 2.9 says a reconnect is a brand
	// new connection with no resumption, so nothing from the previous *Conn
	// carries over.
	OnConnect func(c *Conn, welcome *WelcomeMsg)
	// OnDisconnect fires for every disconnect, recoverable or fatal
	// (CLIENT-SDK.md's disconnect reason shape) -- unconditionally, so a
	// caller never has to guess whether something silently dropped a retry
	// loop.
	OnDisconnect func(DisconnectReason)
	// OnStream/OnApp/OnDrain are wired onto every underlying *Conn
	// automatically, on every (re)connect -- the caller does not need to
	// re-attach them from OnConnect (S5: doing so anyway from OnConnect is
	// harmless, not undefined -- Conn.OnStream/OnApp/OnDrain simply replace
	// the just-auto-wired handler on that one *Conn, exactly like calling
	// them on any other *Conn; the *next* reconnect gets a brand new *Conn
	// wired fresh from these fields again, so nothing carries over and
	// nothing here is silently doubled up). Each runs on that *Conn's own
	// deliveryLoop goroutine (conn.go), which Client's wg does not track: a
	// callback for an event that had already arrived before Client.Close/
	// CloseWith ended the underlying conn can still fire shortly after
	// Close/CloseWith itself returns (deliveryLoop's doc comment).
	OnStream func(*Stream)
	OnApp    func(json.RawMessage)
	OnDrain  func(*DrainMsg)
}

type clientState int

const (
	clientIdle clientState = iota
	clientDialing
	clientBackoff
	clientConnected
	// clientDisconnected is the truthful, if transient, state between "the
	// active conn just ended" and "the next thing (a new dial, backoff, or
	// permanent closure) has actually started" -- nit 4: without it, State()
	// kept reporting "connected" (with Conn() already nil) for however long
	// it took the goroutine that decides what happens next to actually run.
	clientDisconnected
	clientClosed
)

// Client is the reconnecting ws-mixer.v1 client. Build one with NewClient,
// start it with Connect, and end it with Close.
type Client struct {
	url   string
	token TokenProvider
	opts  ClientConfig
	rc    ReconnectOptions

	mu           sync.Mutex
	state        clientState
	conn         *Conn
	retiringConn *Conn
	// gracefulConn is the one live conn Close() has claimed to drain/close
	// itself (see the ownership invariant above Close, and watchConn's
	// closeCh escape below). Left nil the rest of the time.
	gracefulConn            *Conn
	attempt                 int
	everConnected           bool
	closing                 bool
	drainReconnectScheduled bool
	drainedNoReconnect      bool
	keepaliveRetryUsed      bool
	// refreshRetryUsed is dialAndHandshake's one-time 401/4011
	// refresh-retry budget: a once-only flag like keepaliveRetryUsed,
	// re-armed only at stability (armStability), not per dial attempt and
	// not on welcome alone (docs/DECISIONS.md 2026-09-20).
	refreshRetryUsed bool
	cumulative       Stats

	// firstResultCh/firstResultErr/firstResultOnce implement Connect's
	// idempotency (nit 6): firstResultCh is closed exactly once, by
	// finishFirstResult, after firstResultErr is written -- so every
	// concurrent (or later) Connect call observes the same result via a
	// channel receive that can broadcast to any number of receivers,
	// instead of the single value a 1-buffered chan error can only ever
	// hand to one of them (a second concurrent caller used to block
	// forever).
	firstResultCh   chan struct{}
	firstResultErr  error
	firstResultOnce sync.Once

	closeCh     chan struct{}
	closeChOnce sync.Once
	startOnce   sync.Once
	wg          sync.WaitGroup

	// cbMu/cbCond/cbQueue/cbClosed back the queue for every OnConnect/
	// OnDisconnect invocation, drained in order by the one dedicated
	// callbackLoop goroutine below -- deliberately never added to wg.
	// Close() calls wg.Wait(), and the most natural thing a user writes is
	// calling Close() from inside OnConnect or OnDisconnect (blocker 2); if
	// those callbacks ran directly on an attemptLoop/watchConn goroutine
	// that IS in wg, that call would deadlock wg.Wait() against its own,
	// not-yet-Done() goroutine. Running callbacks on a separate, untracked
	// goroutine instead sidesteps that self-wait by construction -- Go has
	// no clean way to ask "is the calling goroutine one wg is waiting on",
	// so this avoids ever needing to ask.
	//
	// This is a slice + sync.Cond, not a fixed-size chan func(): a bounded
	// channel only bounds the deadlock nit 2 flagged, it doesn't eliminate
	// it -- enough OnDisconnect/OnConnect reports queued up (more than the
	// channel's capacity) while one of them is itself blocked calling
	// Close() would still wedge every wg-tracked goroutine trying to send
	// the overflow, which wg.Wait() then waits on forever. An unbounded
	// queue behind a mutex has no capacity to exhaust, so enqueueCb below
	// can never block its caller, structurally, regardless of queue depth.
	cbMu     sync.Mutex
	cbCond   *sync.Cond
	cbQueue  []func()
	cbClosed bool
}

// NewClient builds a Client for url, authenticating every dial with token.
// It does not connect until Connect is called. token must not be nil (nit
// 6): every dial calls it, so a nil provider would panic on first use
// anyway, just later and less clearly -- fail fast here instead.
func NewClient(url string, token TokenProvider, opts ClientConfig) *Client {
	if token == nil {
		panic("wsmixer: NewClient: token is nil (every dial attempt calls it; pass a TokenProvider or StaticToken(...))")
	}
	opts.Options.SetDefaults()
	opts.Reconnect.setDefaults()
	cl := &Client{
		url:           url,
		token:         token,
		opts:          opts,
		rc:            opts.Reconnect,
		firstResultCh: make(chan struct{}),
		closeCh:       make(chan struct{}),
	}
	cl.cbCond = sync.NewCond(&cl.cbMu)
	go cl.callbackLoop()
	return cl
}

// callbackLoop is the single goroutine that ever invokes OnConnect/
// OnDisconnect (see the cbMu field comment). It drains cbQueue in order,
// blocking on cbCond whenever the queue is empty, until closeCbCh marks it
// closed AND the queue has been fully drained -- so every already-enqueued
// callback still runs even if it was queued right before Close.
func (cl *Client) callbackLoop() {
	for {
		cl.cbMu.Lock()
		for len(cl.cbQueue) == 0 && !cl.cbClosed {
			cl.cbCond.Wait()
		}
		if len(cl.cbQueue) == 0 {
			cl.cbMu.Unlock()
			return
		}
		fn := cl.cbQueue[0]
		cl.cbQueue = cl.cbQueue[1:]
		cl.cbMu.Unlock()
		fn()
	}
}

// enqueueCb adds fn to cbQueue and wakes callbackLoop. A no-op once
// closeCbCh has already run (nothing should still be reporting after that,
// but this stays defensive rather than panicking on a nil-checked send).
func (cl *Client) enqueueCb(fn func()) {
	cl.cbMu.Lock()
	if cl.cbClosed {
		cl.cbMu.Unlock()
		return
	}
	cl.cbQueue = append(cl.cbQueue, fn)
	cl.cbMu.Unlock()
	cl.cbCond.Signal()
}

// closeCbCh marks the callback queue closed: callbackLoop exits once it has
// drained whatever was already queued. Idempotent. Safe to call from
// multiple goroutines racing to end the client (F1: a fatal disconnect and a
// concurrent user Close() can both reach here; only the ordering of who
// calls cl.wg.Wait() first differs, not which of them is "allowed" to close
// the queue).
func (cl *Client) closeCbCh() {
	cl.cbMu.Lock()
	if cl.cbClosed {
		cl.cbMu.Unlock()
		return
	}
	cl.cbClosed = true
	cl.cbMu.Unlock()
	cl.cbCond.Broadcast()
}

// scheduleCbChClose closes the callback queue once every wg-tracked
// goroutine has finished, from a goroutine of its own: F1's fatal paths
// (goFatal, onAttemptFailed, reportAndSchedule) run ON a goroutine wg is
// itself tracking, so they cannot call cl.wg.Wait() inline without
// deadlocking against themselves. Without this, a fatal termination the
// caller never explicitly Close()s -- or whose Close() call only ever takes
// Close's early-return branch, see there -- leaked callbackLoop forever:
// there was previously no other path that ever closed the queue.
func (cl *Client) scheduleCbChClose() {
	go func() {
		cl.wg.Wait()
		cl.closeCbCh()
	}()
}

// Connect starts the reconnect loop (idempotent: a second, even concurrent,
// call observes the same first result -- nit 6) and blocks until the first
// welcome lands, or the connection attempt(s) end fatally/exhausted, or ctx
// is done first (the background loop keeps running regardless of ctx).
func (cl *Client) Connect(ctx context.Context) error {
	cl.startOnce.Do(func() {
		// tryTrack, not a bare wg.Add: this runs on the caller's own
		// goroutine, which cl.wg is not already tracking, so an unguarded
		// Add here could race a concurrent Close()'s wg.Wait() (see
		// tryTrack's doc comment). If it returns false, the client is
		// already closing -- Close's own finishFirstResult call (guaranteed
		// on every path that sets closing) resolves firstResultCh below
		// without any attemptLoop ever needing to run.
		if cl.tryTrack() {
			go func() {
				defer cl.wg.Done()
				cl.attemptLoop(0, "initial")
			}()
		}
	})
	select {
	case <-cl.firstResultCh:
		return cl.firstResultErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Conn returns the currently active connection, or nil while dialing/backing
// off/closed.
func (cl *Client) Conn() *Conn {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	return cl.conn
}

// State reports the reconnect state machine's current position.
func (cl *Client) State() string {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	switch cl.state {
	case clientDialing:
		return "dialing"
	case clientBackoff:
		return "backoff"
	case clientConnected:
		return "connected"
	case clientDisconnected:
		return "disconnected"
	case clientClosed:
		return "closed"
	default:
		return "idle"
	}
}

// Stats returns the "ignore and count" counters (CLIENT-SDK.md), cumulative
// across every connection this Client has ever had, including the currently
// active one.
func (cl *Client) Stats() Stats {
	cl.mu.Lock()
	s := cl.cumulative
	conn := cl.conn
	cl.mu.Unlock()
	if conn != nil {
		s.add(conn.Stats())
	}
	return s
}

// --- conn ownership invariant --------------------------------------------
//
// Every live *Conn this Client ever creates has exactly one closer:
//
//   - By default, that's watchConn's own closeCh escape (below): once
//     stopBackoff has fired (Close, or any fatal path), a conn that
//     watchConn is still watching and that nothing else has claimed gets
//     force-closed there.
//   - Close() can claim ONE conn instead -- the currently active cl.conn it
//     is about to Drain()/Close() gracefully -- by recording it as
//     cl.gracefulConn under cl.mu before releasing the lock. watchConn's
//     escape never force-closes the claimed conn; Close's own graceful
//     teardown owns it exclusively.
//   - A fatal path (goFatal, onAttemptFailed's fatal branch,
//     reportAndSchedule's exhaustion/disabled branch) closes whatever it
//     owns itself, under cl.mu, as part of going fatal: it takes cl.conn/
//     cl.retiringConn, nils both fields -- nil-ing cl.conn alone is what
//     makes watchConn's own eventual wasActive check come back false and
//     so not also report the same disconnect; nil-ing retiringConn is
//     separate ownership bookkeeping -- then closes them asynchronously.
//   - onAttemptSucceeded publishes a freshly dialed conn to cl.conn only
//     under the same cl.mu it re-checks cl.closing with. A conn that lands
//     after Close (or a fatal path) has already started is never published
//     at all -- it is closed and discarded directly, exactly like
//     attemptLoop's own closingNow check handles the same race a few lines
//     earlier in the same dial.
//
// One accepted asymmetry, not a bug: when a fatal dial failure lands while a
// still-live predecessor conn exists (the drain-reconnect window above,
// where cl.conn and cl.retiringConn point at the very same live conn until
// the new dial actually lands), closeLiveConns closes that predecessor
// silently and its termination is subsumed into the single fatal report for
// the failed attempt rather than getting an OnDisconnect of its own --
// deliberate: a fatal termination collapses to exactly one report (the
// fatal reason subsumes the predecessor's), mirroring the JS reference
// SDK's own goFatal path, which swallows the retiring conn's report the
// same way; the two differ only in how the close happens -- Go closes it
// asynchronously here via closeLiveConns because Conn.Close blocks on the
// close-frame round trip, while JS's fail() closes it synchronously. This
// differs from nit 3's
// drain-race-won case below (onAttemptSucceeded), which is not fatal and so
// still owes CLIENT-SDK.md's unconditional one-report-per-disconnect
// promise -- there, the superseded predecessor gets its own report even
// though its watchConn call also goes silent. Either way, the predecessor's
// own watchConn goes silent (wasActive false) purely because cl.conn no
// longer points at it -- nil-ing cl.retiringConn plays no part in that.
//
// The failure mode this prevents: a conn that is live, but that nothing
// currently tracks as cl.conn/cl.retiringConn/cl.gracefulConn, is an
// orphan. Its watchConn goroutine parks on <-conn.Done() forever (or, once
// closeCh fires, on <-conn.Done() right after the old orphaned-conn check
// used to skip it), which parks wg.Wait() forever, which wedges Close(). Any
// new fatal or Close path added here MUST end with every live conn either
// closed by it directly or still traceable to cl.conn/cl.retiringConn/
// cl.gracefulConn for someone else (watchConn's escape, ultimately) to close.

// closeStart is Close/CloseWith's shared entry: it makes exactly the state
// transitions Close itself used to make inline -- cl.closing=true,
// cl.state=clientClosed, cl.drainedNoReconnect cleared, claiming conn as
// cl.gracefulConn (B2/ownership invariant above Close: watchConn's closeCh
// escape must not also force-close it out from under whatever the caller is
// about to do to it itself) -- then stops the backoff loop and resolves
// Connect's first result, exactly as before. ok is false if something else
// (a concurrent Close/CloseWith, or a fatal path that set cl.closing
// directly) already started closing; the caller must fall back to
// closeAlreadyClosing instead of touching conn/retiring itself, since
// whichever path got here first already owns them (see the ownership
// invariant). retiring is deliberately not claimed here, matching Close's
// original comment on why that's fine: it gets a bare forced NoError close,
// not whatever the caller is doing to conn, so it's harmless for the
// closeCh escape to also force-close it if it gets there first (Conn.Close
// is idempotent).
func (cl *Client) closeStart() (conn, retiring *Conn, ok bool) {
	cl.mu.Lock()
	if cl.closing {
		cl.mu.Unlock()
		return nil, nil, false
	}
	cl.closing = true
	cl.state = clientClosed
	cl.drainedNoReconnect = false
	conn = cl.conn
	retiring = cl.retiringConn
	cl.retiringConn = nil
	cl.gracefulConn = conn
	cl.mu.Unlock()

	cl.stopBackoff()
	cl.finishFirstResult(errors.New("wsmixer: closed before the first connection completed"))
	return conn, retiring, true
}

// closeAlreadyClosing is Close/CloseWith's shared "someone else already
// started shutting down" fallback (F1): still reap resources here rather
// than returning immediately -- wg.Wait() bounds on whichever path called
// stopBackoff, which every closeStart-derived path does.
func (cl *Client) closeAlreadyClosing() {
	cl.wg.Wait()
	cl.closeCbCh()
}

// Close performs a graceful client shutdown (WIRE.md section 2.10 rule 14):
// stops reconnecting for good, sends drain{reason:"client_requested"} on the
// active connection, waits up to 5s for in-flight streams to finish, then
// closes with NO_ERROR/1000 instead of the server-drain sequence's GOING_AWAY.
// Safe to call at any time, including before the first connection completes
// (in which case Connect, if still waiting, returns an error) or more than
// once (idempotent, including racing a concurrent CloseWith -- closeStart's
// cl.closing check picks exactly one winner) -- including when something
// else (a fatal disconnect, MaxAttempts exhaustion, Disabled) already ended
// the client first: Close is always resource-final regardless of who
// initiated closing, reaping every wg-tracked goroutine and the callback
// queue before it returns (F1). The very last OnDisconnect can still run
// after Close returns, though: draining the callback queue happens on
// callbackLoop's own goroutine, which Close does not wait on (see the cbMu
// field comment) -- only on the wg-tracked goroutines that feed it. Also
// safe to call synchronously from inside OnConnect/OnDisconnect themselves
// (blocker 2): those callbacks run on callbackLoop, a goroutine deliberately
// never added to cl.wg (see the cbMu field comment), so Close's own
// cl.wg.Wait() never waits on the very goroutine calling it.
//
// Separately, ClientConfig.OnStream/OnApp/OnDrain can also still run shortly
// after Close returns: each runs on its underlying *Conn's own deliveryLoop
// goroutine (conn.go), which -- like callbackLoop above -- cl.wg does not
// track either, and which still delivers an event that had already arrived
// before this Close call ended that conn (deliveryLoop's doc comment).
//
// For an application closing the connection for its own reasons (not a
// graceful shutdown), use CloseWith instead.
func (cl *Client) Close(ctx context.Context) error {
	conn, retiring, ok := cl.closeStart()
	if !ok {
		// Someone else already started shutting down (Close, CloseWith, or a
		// fatal path that set cl.closing directly: goFatal, onAttemptFailed,
		// reportAndSchedule).
		cl.closeAlreadyClosing()
		return nil
	}

	// retiring != conn (review item 5): during a drain hand-over's window,
	// cl.conn and cl.retiringConn can point at the very same live conn (see
	// the ownership invariant above Close) -- closeStart hands both back
	// as-is. Without this guard, this goroutine's bare NoError close below
	// would race the graceful conn.Drain/conn.Close a few lines down on the
	// same conn's closeOnce, and losing that race would send NO_ERROR/1000
	// on the wire instead of the graceful close this method is actually
	// trying to perform. closeLiveConns (the fatal-path equivalent) already
	// guards the same way.
	if retiring != nil && retiring != conn {
		go func() { _ = retiring.Close(uint32(NoError), "client closing") }()
	}
	var err error
	if conn != nil {
		if conn.isDraining() {
			// Blocker 3: conn is already draining -- either the peer's own
			// drain, or an earlier Drain() call -- so conn.Drain would just
			// no-op (drain.go: "if c.draining { return nil }"): it would
			// never send drain{client_requested}, never RESET survivors,
			// and never close the socket, leaving wg.Wait() below parked on
			// watchConn's <-conn.Done() until the peer's own drain deadline
			// (or forever, if the peer never enforces one). The in-flight
			// streams are already being wound down by whichever drain
			// sequence is already running; skip straight to closing the
			// socket so a graceful Close() still terminates promptly.
			err = conn.Close(uint32(NoError), "client closing")
		} else {
			err = conn.Drain(ctx, "client_requested", DrainOptions{
				Deadline: 5 * time.Second, HasCloseCode: true, CloseCode: NoError,
			})
			// Drain stays truthful and can now return connClosedErr's
			// io.ErrUnexpectedEOF (conn.go) when the peer's transport dies
			// abnormally -- no close frame -- while this side is still
			// waiting out the drain (Drain's own `case <-c.closed: return
			// c.connClosedErr()`). That is an ORDINARY outcome for Close,
			// though, not a failure of it: Close's contract is that the
			// client ends up closed, not that the peer completed the close
			// handshake -- a peer that simply drops the connection mid-drain
			// is exactly as "closed" as one that finishes it gracefully.
			// Filter it here, at the one call site whose result Close
			// actually returns to its own caller.
			if errors.Is(err, io.ErrUnexpectedEOF) {
				err = nil
			}
		}
	}
	cl.wg.Wait()
	// Safe here: every goroutine that could ever enqueue a callback is
	// tracked by wg, and wg.Wait() just returned (see the cbMu field
	// comment and callbackLoop's doc comment). closeCbCh is idempotent, so
	// this races harmlessly with scheduleCbChClose's own call to it if some
	// other, concurrent shutdown path got there first.
	cl.closeCbCh()
	return err
}

// CloseWith closes the client immediately with a ws-mixer application error
// code and message (CLIENT-SDK.md's "Application close" row): unlike Close,
// there is no drain{client_requested}/grace period -- the live connection,
// if any, is closed at once with error{code,message} then WS close
// 4000+code (Conn.Close's own three steps; codes > 999, including every
// application code >= 0x1000_0000, are clamped on the wire to
// InternalErrorCode's 4002, exactly as for any Conn.Close -- see its doc).
// For an application closing a connection for its own reasons ws-mixer
// itself does not interpret, use ApplicationCloseCode.
//
// Takes an ErrorCode, not the uint32 Conn.Close does: CloseWith's whole
// point is being the Client-level counterpart to Conn.Close for exactly the
// ApplicationCloseCode use case ApplicationCloseCode's own doc comment
// already describes, so a call site reads naturally as
// `cl.CloseWith(ctx, wsmixer.ApplicationCloseCode, "bye")` rather than
// forcing a uint32 conversion at the call site for the common case; CloseWith
// converts internally where Conn.Close's own signature requires it.
//
// Like Close, this marks the client closing FIRST (closeStart, shared with
// Close -- see its own doc comment for exactly which state transitions that
// makes), so no reconnect is ever scheduled after this call, and it is
// idempotent and safe to race against a concurrent Close/CloseWith (exactly
// one of them wins closeStart's cl.closing check; every other caller falls
// back to closeAlreadyClosing and still blocks until the winner has
// finished). Ends any retiring conn (during a drain hand-over) and any
// in-flight dial the same way Close does (closeStart/stopBackoff) -- except
// the retiring conn itself is closed with CloseWith's own code/message too,
// not Close's NO_ERROR, matching the JS SDK's close({code,message})
// (cross-SDK alignment: JS closes every conn it still owns with the
// caller's code, not just the active one). Reports exactly one non-fatal
// DisconnectReason, with code's ErrorCode/ErrorName/WSCode, the same way
// Close does: not synthesized here directly, but via watchConn observing
// the conn.Close call below end the connection, exactly the same mechanism
// that produces Close's own one report -- see watchConn's closingNow
// branch. When there is no live connection yet (still dialing/backing off,
// or CloseWith is called before the first connection ever completed), there
// is nothing for watchConn to observe and so nothing to report, exactly
// like Close in that same situation.
//
// ctx is accepted for signature symmetry with Close and honoured the same
// way Close honours it for its own immediate-conn.Close branch (conn
// already draining): not at all -- Conn.Close has no ctx parameter of its
// own to forward one to, and is already internally bounded (a ~2s flush
// deadline pre-Run, a 50ms best-effort flush window post-Run).
//
// Also safe to call synchronously from inside OnConnect/OnDisconnect
// themselves, exactly like Close (blocker 2, see Close's own doc comment):
// those callbacks run on callbackLoop, a goroutine deliberately never added
// to cl.wg, so CloseWith's own cl.wg.Wait() (shared with Close via
// closeStart) never waits on the very goroutine calling it.
//
// Like Close, ClientConfig.OnStream/OnApp/OnDrain can still run shortly
// after CloseWith returns: each runs on its underlying *Conn's own
// deliveryLoop goroutine, untracked by cl.wg, which still delivers an event
// that had already arrived before this call ended that conn (see Close's
// own doc comment and deliveryLoop's, conn.go).
func (cl *Client) CloseWith(ctx context.Context, code ErrorCode, message string) error {
	conn, retiring, ok := cl.closeStart()
	if !ok {
		cl.closeAlreadyClosing()
		return nil
	}

	// retiring != conn: see Close's own identical guard/comment above --
	// the same drain-hand-over window applies here, and losing this race
	// would send NO_ERROR/1000 instead of the application code/message this
	// call is specifically trying to send. The retiring conn itself gets
	// the SAME code/message CloseWith was called with, not NoError -- cross-
	// SDK alignment with the JS SDK's close({code,message}), which closes
	// every conn it still owns with the caller's own code, not just the
	// active one. Plain Close keeps NO_ERROR for both (its own graceful
	// shutdown is never conditional on which conn happens to be retiring).
	if retiring != nil && retiring != conn {
		go func() { _ = retiring.Close(uint32(code), message) }()
	}
	var err error
	if conn != nil {
		err = conn.Close(uint32(code), message)
	}
	cl.wg.Wait()
	cl.closeCbCh()
	return err
}

// tryTrack registers one more goroutine with cl.wg, guarded by the same
// cl.mu used elsewhere to synchronize with cl.closing. Every Wait call is
// preceded (happens-before) by a cl.mu critical section in which cl.closing
// is already true -- either the Wait caller's own critical section (Close's
// main branch, which sets closing, or its early-return branch, which
// merely observes closing already true), or its spawner's critical section
// (goFatal, onAttemptFailed, reportAndSchedule, each of which sets closing
// under cl.mu and then calls scheduleCbChClose), with the go statement's
// own happens-before carrying that true reading forward: scheduleCbChClose's
// spawned goroutine never touches cl.mu itself but still calls wg.Wait()
// only after closing is already true. closing is monotone: it
// only ever moves false->true, always under cl.mu. Because cl.mu totally
// orders tryTrack's critical section against every closing-true critical
// section, setting or observing, exactly two outcomes are possible for
// tryTrack, never a third:
//   - tryTrack observes closing==false: by monotonicity, its critical
//     section precedes every closing-true critical section in cl.mu's
//     total order, so it calls wg.Add(1) and returns true, and that Add
//     happens-before cl.mu's unlock here, which happens-before every such
//     later critical section's lock, which happens-before that critical
//     section's own Wait call (program order, or the go statement's
//     happens-before for scheduleCbChClose). Add-before-Wait: safe.
//   - tryTrack observes closing==true: it returns false without ever
//     calling wg.Add. No Add call exists to race any Wait at all.
//
// Either way, wg.Add can never run concurrently with a wg.Wait() call on a
// counter that could be zero (sync.WaitGroup's documented misuse case).
// Callers reachable from a goroutine cl.wg is not already tracking (Connect,
// handleServerDrain -- both run on a goroutine with no outstanding,
// not-yet-Done wg count of its own) must go through tryTrack instead of
// calling cl.wg.Add directly, and must not proceed (spawn nothing, assume
// nothing) when it returns false. A wg.Add called from within a goroutine
// wg is already tracking (onAttemptSucceeded, reportAndSchedule) does not
// need this: that goroutine's own outstanding Add guarantees the counter is
// >=1 at the time, so it can never be the Add-when-zero case Wait cares
// about, no matter how cl.closing is racing.
func (cl *Client) tryTrack() bool {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	if cl.closing {
		return false
	}
	cl.wg.Add(1)
	return true
}

func (cl *Client) stopBackoff() { cl.closeChOnce.Do(func() { close(cl.closeCh) }) }

func (cl *Client) finishFirstResult(err error) {
	cl.firstResultOnce.Do(func() {
		cl.firstResultErr = err
		close(cl.firstResultCh)
	})
}

// report enqueues one OnDisconnect invocation on callbackLoop (see the cbMu
// field comment) rather than calling it inline: the caller may be an
// attemptLoop/watchConn goroutine tracked by wg, and OnDisconnect calling
// Close() synchronously from here would deadlock wg.Wait() (blocker 2).
func (cl *Client) report(reason DisconnectReason) {
	if cl.opts.OnDisconnect == nil {
		return
	}
	cl.enqueueCb(func() { cl.opts.OnDisconnect(reason) })
}

// --- the attempt loop --------------------------------------------------

// attemptLoop performs one connection attempt, after waiting out delay
// (interruptibly -- Close() unblocks it early). It is the single place
// attempts are scheduled: both the sequential retry path and drain's
// parallel reconnect call into it, each on its own goroutine, so an attempt
// launched from a drain never blocks whichever goroutine is watching the
// connection being drained.
func (cl *Client) attemptLoop(delay time.Duration, cause string) {
	if !cl.waitBackoff(delay) {
		return
	}

	cl.mu.Lock()
	if cl.closing {
		cl.mu.Unlock()
		return
	}
	cl.state = clientDialing
	cl.mu.Unlock()

	conn, welcome, err := cl.dialAndHandshake()

	cl.mu.Lock()
	closingNow := cl.closing
	cl.mu.Unlock()
	if closingNow {
		if conn != nil {
			go func() { _ = conn.Close(uint32(NoError), "client closing") }()
		}
		return
	}

	if err != nil {
		cl.onAttemptFailed(err)
		return
	}
	if probeWidenWindow != nil {
		probeWidenWindow()
	}
	cl.onAttemptSucceeded(conn, welcome)
}

// probeWidenWindow is a test-only seam (nil in production, like the clock
// seams on ReconnectOptions) that widens the window between the closingNow
// check above and onAttemptSucceeded's own cl.closing re-check, letting a
// regression test make the Close()-vs-landing-dial race (B2) deterministic
// instead of relying on scheduler luck.
var probeWidenWindow func()

func (cl *Client) waitBackoff(delay time.Duration) bool {
	if delay <= 0 {
		return true
	}
	cl.mu.Lock()
	if cl.closing {
		cl.mu.Unlock()
		return false
	}
	cl.state = clientBackoff
	cl.mu.Unlock()
	select {
	case <-cl.rc.after(delay):
		return true
	case <-cl.closeCh:
		return false
	}
}

// dialAndHandshake resolves the token provider and dials once, applying
// CLIENT-SDK.md's "401 on upgrade" rule: an HTTP 401 (or a handshake
// UNAUTHORIZED/4011) gets exactly one immediate refresh-retry -- the
// provider is called again and the dial retried right away, no backoff. A
// second REJECTION of that kind (isUnauthorized again), within this one
// dialAndHandshake call, is always forced fatal, exactly as before -- but a
// non-rejection failure of the retry's own dial (a DNS blip, TCP reset,
// HTTP 503, ...) is not a second "no" from the auth server, so it takes the
// ordinary recoverable path with normal backoff instead (CLIENT-SDK.md says
// "a second rejection is fatal", not "any second failure is fatal"); the
// budget stays spent either way, since it was claimed unconditionally
// before this retry ever dialed. What changed (docs/DECISIONS.md
// 2026-09-20): "one" is now a once-only budget across dialAndHandshake
// calls, not per-call -- cl.refreshRetryUsed, re-armed only at stability
// (armStability), the same as keepaliveRetryUsed. A rejection that arrives
// while the budget is already spent (a previous cycle used its one
// refresh-retry and the resulting connection never reached StableAfter
// before failing again) goes straight to fatal without ever calling the
// provider a second time -- otherwise a server that welcomes, then closes
// again before stability, then rejects the redial makes every single cycle
// mint a fresh auth-service hit. A provider error, on the first call or the
// retry's, is surfaced verbatim and fatal unless it wraps ErrTokenUnavailable
// (that sentinel's own doc comment), in which case it takes the ordinary
// recoverable path instead -- either way the refresh-retry budget itself
// stays spent once claimed, exactly like a non-rejection failure of the
// retry's own dial above.
func (cl *Client) dialAndHandshake() (*Conn, *WelcomeMsg, error) {
	// One ctx covers both dialOnce's websocket.Dial (the actual TCP+upgrade)
	// and, inside it, clientHandshake's welcome wait -- client.go's
	// clientHandshake reads with this same ctx, undecorated, precisely so
	// coder/websocket's own context-expiry-tears-down-the-transport
	// behavior (see client.go's clientHandshake doc) never fires ahead of
	// the HelloTimeout-driven time.AfterFunc that's supposed to deliver a
	// graceful 4001. WIRE.md section 2.9's ConnectTimeout and section
	// 2.10's welcome-wait timeout are two separate, sequential budgets (the
	// dial completes, *then* the hello wait starts) -- so the one ctx this
	// whole attempt gets has to cover both in sum, or the two race and
	// ConnectTimeout firing first silently steals the handshake-phase 4001
	// every time it happens to win. cl.opts.HelloTimeout, not
	// c.opts.HelloTimeout on some not-yet-existing Conn: this is the
	// already-SetDefaults()'d value NewClient stored on cl.opts, the same
	// one dialOnce's ClientOptions.Options (and so eventually c.opts) is
	// built from a few lines down.
	ctx, cancel := context.WithTimeout(context.Background(), cl.rc.ConnectTimeout+cl.opts.HelloTimeout)
	defer cancel()
	// Tie this attempt's timeout to closeCh too (blocker/S1): otherwise
	// Close() has to wait out the full ConnectTimeout before wg.Wait() can
	// return, even though it already decided to shut down. `done` bounds
	// the watcher goroutine to this call instead of leaking one per attempt.
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-cl.closeCh:
			cancel()
		case <-done:
		}
	}()

	tok, err := cl.token(ctx)
	if err != nil {
		return nil, nil, &providerError{cause: err}
	}
	conn, welcome, err := cl.dialOnce(ctx, tok)
	if err == nil {
		return conn, welcome, nil
	}
	if !isUnauthorized(err) {
		return nil, nil, err
	}
	if !isRealProvider(cl.token) {
		// A rejected StaticToken is fatal at once (reverses nit 2's earlier
		// call, docs/DECISIONS.md 2026-09-20): cl.token is StaticToken's own
		// wrapper, so calling it again can only ever hand back the exact
		// same (already-rejected) token -- the refresh-retry below would be
		// pointless. Unlike a real TokenProvider, though, there is also no
		// point letting this ride the normal recoverable path: the same
		// token can never start working, so retrying it forever with
		// backoff only hammers the auth service and hides the problem from
		// the caller. Force fatal immediately instead, via the same
		// fatalOverride/forced mechanism the real-provider path's second
		// failure already uses.
		return nil, nil, markFatal(err)
	}

	// The refresh-retry itself is a once-only budget (cl.refreshRetryUsed),
	// re-armed only at stability -- see this function's own doc comment.
	// Claim it now, before ever calling the provider again: if a previous,
	// not-yet-stable cycle already spent it, this rejection is fatal at
	// once, exactly like the StaticToken case just above (for the same
	// reason -- calling the provider again here would just mint another
	// avoidable auth-service hit for a client that keeps failing to
	// stabilize).
	cl.mu.Lock()
	budgetAlreadySpent := cl.refreshRetryUsed
	cl.refreshRetryUsed = true
	cl.mu.Unlock()
	if budgetAlreadySpent {
		return nil, nil, markFatal(err)
	}

	tok2, err2 := cl.token(ctx)
	if err2 != nil {
		return nil, nil, &providerError{cause: err2}
	}
	conn2, welcome2, err2 := cl.dialOnce(ctx, tok2)
	if err2 == nil {
		return conn2, welcome2, nil
	}
	if !isUnauthorized(err2) {
		// The refresh-retry's own dial failed, but not with another
		// rejection -- a DNS blip, TCP reset, or HTTP 503 landing on this
		// particular redial is an ordinary transient failure, not a second
		// "no" from the auth server, and CLIENT-SDK.md's fatal rule is
		// specifically "a second rejection is fatal", not "any second
		// failure of any kind is fatal". The budget still stays spent
		// (refreshRetryUsed was already claimed above, unconditionally,
		// before this dial ever ran) -- only a genuine second rejection, or
		// this cycle reaching StableAfter, changes that -- but this
		// particular failure takes the normal recoverable path with normal
		// backoff instead of ending the client permanently over what may
		// be nothing more than a dropped packet.
		return nil, nil, err2
	}
	return nil, nil, markFatal(err2)
}

func (cl *Client) dialOnce(ctx context.Context, token string) (*Conn, *WelcomeMsg, error) {
	// OnDrain is bound to the specific *Conn this dial produces, once it
	// exists (blocker/S3): handleServerDrain needs to tell "the conn this
	// drain arrived on" apart from "whatever cl.conn happens to be by the
	// time it gets to run" to avoid racing a second parallel reconnect on
	// top of one already in flight. boundConn is set immediately after
	// Dial returns, well before the deliveryLoop it started could ever
	// reach a drain frame (that requires a full read+decode+dispatch round
	// trip, many more instructions than Dial's own "start loops, return"
	// tail) -- so this is not a real race in practice, just a formality the
	// nil check below is defensive against.
	var boundConn atomic.Pointer[Conn]
	conn, err := Dial(ctx, cl.url, ClientOptions{
		Options:      cl.opts.Options,
		Token:        token,
		Agent:        cl.agentInfo(),
		Meta:         cl.opts.Meta,
		Capabilities: cl.opts.Capabilities,
		HTTPHeader:   cl.opts.HTTPHeader,
		OnStream:     cl.opts.OnStream,
		OnApp:        cl.opts.OnApp,
		OnDrain: func(d *DrainMsg) {
			if c := boundConn.Load(); c != nil {
				cl.handleServerDrain(c, d)
			}
		},
	})
	if err != nil {
		return nil, nil, err
	}
	boundConn.Store(conn)
	return conn, conn.Welcome(), nil
}

func (cl *Client) agentInfo() AgentInfo {
	a := cl.opts.Agent
	if a.SDK == "" {
		a.SDK = "ws-mixer-go"
		a.SDKVersion = defaultSDKVersion
	}
	return a
}

// supersededMessage is both the message a drain race's winning replacement
// closes the old conn with, and the DisconnectReason.Message reported for it
// (nit 3) -- kept as one constant so the two never drift apart.
const supersededMessage = "superseded by a new connection"

func (cl *Client) onAttemptSucceeded(conn *Conn, welcome *WelcomeMsg) {
	cl.mu.Lock()
	if cl.closing {
		// B2: Close() (or a fatal path) started, and possibly already
		// finished its own snapshot of cl.conn/cl.retiringConn, in the
		// window between attemptLoop's closingNow check and this lock --
		// probeWidenWindow above exists to make exactly this window
		// deterministic for a regression test. Publishing cl.conn now would
		// hand watchConn's closeCh escape a conn nothing claims (see the
		// ownership invariant above Close): discard it here instead, the
		// same way attemptLoop's own closingNow check discards a landing
		// conn a few lines earlier in the same dial -- no OnConnect, no
		// report, since this conn was never actually "up" from the caller's
		// point of view.
		cl.mu.Unlock()
		go func() { _ = conn.Close(uint32(NoError), "client closing") }()
		return
	}
	// attempt and keepaliveRetryUsed are deliberately NOT reset here anymore
	// (D-2026-09-20, StableAfter): armStability below re-arms them only once
	// this conn has stayed up StableAfter past this same welcome, not at
	// welcome itself -- see its own doc comment for why.
	cl.everConnected = true
	cl.state = clientConnected
	cl.conn = conn
	retiring := cl.retiringConn
	cl.retiringConn = nil
	wonDrainRace := retiring != nil && retiring != conn
	if wonDrainRace {
		// Blocker 1: a drain-triggered parallel reconnect just won the race
		// against the old connection actually closing. drainReconnectScheduled
		// exists so the *old* conn's own watchConn call can tell "I'm ending
		// because a reconnect is already in flight for me" apart from "I'm
		// ending for some unrelated reason" -- but by the time old's
		// watchConn actually runs (old.Close() below, or old's own natural
		// end), cl.conn already points at this new conn, so watchConn's
		// `wasActive := cl.conn == conn` is false for old and it early-outs
		// before ever touching the flag (see watchConn's !wasActive branch).
		// Left set, the flag stays true forever and silently swallows the
		// very next unrelated disconnect of THIS (now active) conn -- it
		// looks like an in-flight drain reconnect that was never actually
		// scheduled, so watchConn reports and does nothing instead of
		// reconnecting. Clear it here, matching the JS SDK's connectOnce
		// (client.ts, the comment this bug is named after): this is the one
		// and only place that needs to, since every other path that sets it
		// true (handleServerDrain) is already matched by watchConn's own
		// clear in the race-free case (old ends before new lands).
		cl.drainReconnectScheduled = false
	}
	cl.mu.Unlock()

	if wonDrainRace {
		// The old connection doesn't need to wait for the server's own
		// drain deadline any more. Its in-flight streams weren't wrong, the
		// *connection* they were riding on was replaced -- NO_ERROR, not a
		// stream-scoped cancel (the streams' own CANCEL, if any, is the
		// server's job at its drain deadline).
		go func() { _ = retiring.Close(uint32(NoError), supersededMessage) }()
		// Nit 3: CLIENT-SDK.md's disconnect reason shape is unconditional --
		// every disconnect is reported, exactly once. When the drain race's
		// *loser* ends first (old closes naturally before the replacement
		// lands), watchConn's own drainSched branch already reports it. When
		// the race is won here instead (the replacement lands first), old's
		// watchConn call will find cl.conn already pointing at the new conn
		// by the time old.Close() above lands (wasActive false) and stays
		// silent by design (see watchConn's !wasActive branch) -- so without
		// this, the winning case silently got no report at all, unlike the
		// losing case. Report it here instead, once, mirroring the
		// semantics of the JS SDK's own retiring-conn teardown (client.ts
		// wireConn's `old.fail(WsMixerError(NO_ERROR, "superseded by a new
		// connection"), ...)`), even though JS's own close handler happens
		// to swallow this particular report too (its `!wasActive` return
		// fires first there as well) -- CLIENT-SDK.md's unconditional
		// promise wins over matching that particular gap.
		cl.report(DisconnectReason{
			Phase:        PhaseConnected,
			WSCode:       NoError.CloseCode(),
			ErrorCode:    NoError,
			HasErrorCode: true,
			ErrorName:    NoError.String(),
			Fatal:        false,
			Message:      supersededMessage,
		})
	}

	if cl.opts.OnConnect != nil {
		cl.enqueueCb(func() { cl.opts.OnConnect(conn, welcome) })
	}

	cl.wg.Add(1)
	go func() {
		defer cl.wg.Done()
		cl.watchConn(conn)
	}()

	cl.wg.Add(1)
	go func() {
		defer cl.wg.Done()
		cl.armStability(conn)
	}()

	cl.finishFirstResult(nil)
}

// armStability re-arms the backoff attempt counter and every once-only
// reconnect budget -- keepaliveRetryUsed (WIRE.md section 2.9's 4013 row,
// "one immediate attempt, then normal backoff") and refreshRetryUsed
// (dialAndHandshake's one-time 401/4011 refresh-retry) -- once, and only
// once, conn has stayed up cl.rc.StableAfter past its own welcome -- not at
// welcome itself (onAttemptSucceeded, above). A server that welcomes a
// connection and then immediately closes it must not be able to reset any
// of them: without this, such a server makes the client redial about once
// a second forever instead of ever backing off, and mints a fresh
// auth-service hit on every single cycle if its rejections are 401/4011
// (spec WIRE.md section 2.9's `stable` state).
//
// Uses the same cl.rc.after seam the backoff delay itself uses (not a bare
// time.AfterFunc/time.Timer), so a white-box test can hold this
// deterministic exactly like every other clock-driven wait in this file.
// Bounded to conn's own lifetime, with no leak: conn.Done() ends this
// goroutine the instant conn itself ends (an ordinary reconnect, a drain
// replacement, ...) instead of it idling out the rest of StableAfter for
// nothing, and closeCh (Close, or any fatal path) unblocks it immediately
// too, same as every other wg-tracked goroutine in this file.
func (cl *Client) armStability(conn *Conn) {
	select {
	case <-cl.rc.after(cl.rc.StableAfter):
	case <-conn.Done():
		return
	case <-cl.closeCh:
		return
	}
	cl.mu.Lock()
	if cl.conn == conn {
		// Still the active connection StableAfter later: genuinely stable.
		// A conn that has since ended (an ordinary reconnect, a drain
		// replacement, ...) leaves cl.conn pointing elsewhere by the time
		// this fires, and this is deliberately a no-op then -- exactly the
		// case this whole mechanism exists to not reset for.
		cl.attempt = 0
		cl.keepaliveRetryUsed = false
		cl.refreshRetryUsed = false
	}
	cl.mu.Unlock()
}

// closeLiveConns is the fatal-path half of the ownership invariant above
// Close: goFatal/onAttemptFailed's fatal branch/reportAndSchedule's
// exhaustion-or-disabled branch call this, after nil-ing cl.conn/
// cl.retiringConn under cl.mu, to actually close whatever they just took
// ownership of. Both may be nil (the common case: the failing attempt had no
// live predecessor conn), and both may be the same *Conn (B1: a drain's
// parallel reconnect attempt fails fatally while the old conn it's replacing
// is still cl.conn AND cl.retiringConn at once) -- closed asynchronously,
// like every other conn.Close call in this file, since Close blocks on the
// close-frame round trip and none of these fatal paths should block on it.
func closeLiveConns(conn, retiring *Conn) {
	if conn != nil {
		go func() { _ = conn.Close(uint32(NoError), "client closing") }()
	}
	if retiring != nil && retiring != conn {
		go func() { _ = retiring.Close(uint32(NoError), "client closing") }()
	}
}

func (cl *Client) onAttemptFailed(err error) {
	fi := classifyFailureErr(err)
	reason := fi.toReason()

	if fi.fatal {
		cl.mu.Lock()
		if cl.closing {
			cl.mu.Unlock()
			// N3: the client is already ending for some other reason (a
			// concurrent Close(), or another fatal path that raced ahead of
			// this one) -- CLIENT-SDK.md's disconnect-reason promise is
			// unconditional, so this failure still gets its own report; it
			// just isn't the one driving the client into its closed state
			// (something else already is, and already owns closing any live
			// conn -- see the ownership invariant above Close).
			reason.Fatal = true
			cl.report(reason)
			return
		}
		cl.state = clientClosed
		cl.closing = true
		conn := cl.conn
		retiring := cl.retiringConn
		cl.conn = nil
		cl.retiringConn = nil
		cl.mu.Unlock()
		reason.Fatal = true
		cl.report(reason)
		cl.finishFirstResult(reason)
		cl.stopBackoff()
		closeLiveConns(conn, retiring) // B1: don't orphan a still-live predecessor conn
		cl.scheduleCbChClose()         // F1: this ends the client without a Close() call
		return
	}

	if fi.hasRetryAfter {
		cl.reportAndSchedule(reason, func(int) time.Duration { return fi.retryAfter })
		return
	}
	cl.reportAndSchedule(reason, func(next int) time.Duration { return cl.rc.fullJitter(next) })
}

// reportAndSchedule is the single point where a recoverable disconnect
// either gets its one DisconnectReason report and a scheduled retry, or --
// if MaxAttempts is already exhausted, or reconnect is Disabled entirely --
// one merged fatal report instead. The two outcomes are mutually exclusive:
// exactly one report per disconnect.
func (cl *Client) reportAndSchedule(reason DisconnectReason, computeDelay func(nextAttempt int) time.Duration) {
	cl.mu.Lock()
	if cl.closing {
		cl.mu.Unlock()
		// N3: same unconditional-report rule as onAttemptFailed's fatal
		// branch above -- the client is already ending for some other
		// reason, so only the scheduling is skipped here, never the report.
		reason.Fatal = false
		cl.report(reason)
		return
	}
	attempt := cl.attempt
	maxAttempts := cl.rc.MaxAttempts
	disabled := cl.rc.Disabled
	exhausted := maxAttempts > 0 && attempt >= maxAttempts
	if disabled || exhausted {
		cl.state = clientClosed
		cl.closing = true
		conn := cl.conn
		retiring := cl.retiringConn
		cl.conn = nil
		cl.retiringConn = nil
		cl.mu.Unlock()
		reason.Fatal = true
		if exhausted {
			reason.Message = fmt.Sprintf("max consecutive reconnects without a stable connection (%d) exhausted: %s", maxAttempts, reason.Message)
		} else {
			reason.Message = "reconnect disabled: " + reason.Message
		}
		cl.report(reason)
		cl.finishFirstResult(reason)
		cl.stopBackoff()
		closeLiveConns(conn, retiring) // B1: don't orphan a still-live predecessor conn
		cl.scheduleCbChClose()         // F1: this ends the client without a Close() call
		return
	}
	cl.attempt++
	// nit 1: increment before computing the delay, not after -- this
	// deliberately follows the JS SDK's connectOnce (`this.attempt++`
	// happens before scheduling), not a literal reading of WIRE.md section
	// 2.9's fullJitter(attempt) formula, which alone doesn't say which side
	// of the increment "attempt" means. Parity with the reference SDK wins.
	next := cl.attempt
	cl.mu.Unlock()

	reason.Fatal = false
	cl.report(reason)
	delay := computeDelay(next)
	cl.wg.Add(1)
	go func() {
		defer cl.wg.Done()
		cl.attemptLoop(delay, "retry")
	}()
}

// goFatal ends the client permanently for a disconnect that is fatal from
// the moment it's observed (no exhaustion check applies): WIRE.md section
// 2.9's 4010/4011 fatal set hit post-connect, or the drain-with-reconnect-
// disabled case.
func (cl *Client) goFatal(reason DisconnectReason) {
	cl.mu.Lock()
	if cl.closing {
		cl.mu.Unlock()
		return
	}
	cl.state = clientClosed
	cl.closing = true
	conn := cl.conn
	retiring := cl.retiringConn
	cl.conn = nil
	cl.retiringConn = nil
	cl.mu.Unlock()
	reason.Fatal = true
	cl.report(reason)
	cl.finishFirstResult(reason)
	cl.stopBackoff()
	closeLiveConns(conn, retiring) // B1: don't orphan a still-live predecessor conn
	cl.scheduleCbChClose()         // F1: this ends the client without a Close() call
}

// --- drain handling ------------------------------------------------------

// handleServerDrain is wired as every dialed Conn's OnDrain. It always
// invokes the caller's own OnDrain first, then -- unless the client is
// closing, reconnect is Disabled, or conn is no longer the active connection
// -- immediately starts a new connection in parallel (WIRE.md section 2.9:
// "new connection immediately and in parallel, before the old one closes").
//
// conn is bound per-dial (dialOnce), not read from cl.conn, specifically so
// this can tell "the conn this drain actually arrived on" apart from
// "whatever cl.conn happens to be by the time this function gets to run"
// (blocker/S3): the caller's own OnDrain runs first and can take a while,
// during which cl.conn can move on -- a second, unrelated drain landing on
// what was already a retiring conn, or Close() tearing everything down --
// and blindly trusting cl.conn at that point (as if this were still "the"
// active connection) risks starting a second parallel reconnect on top of
// one already in flight, leaving two live conns. Mirrors the JS SDK's
// wireConn, which closes over its own `conn` rather than reading `this.conn`
// for the same reason.
func (cl *Client) handleServerDrain(conn *Conn, d *DrainMsg) {
	if cl.opts.OnDrain != nil {
		cl.opts.OnDrain(d)
	}

	cl.mu.Lock()
	select {
	case <-conn.Done():
		// A flushed drain from conn.go's post-close delivery flush: conn is
		// already dead, so watchConn (or whatever already reacted to its
		// close) has this disconnect covered. Starting a reconnect from here
		// too would set drainReconnectScheduled after the fact and make
		// watchConn's drainSched branch report a fatal close as a silent,
		// non-fatal one instead -- a flushed drain must never reconnect for
		// a conn that is no longer live.
		cl.mu.Unlock()
		return
	default:
	}
	if cl.closing || cl.state == clientClosed {
		cl.mu.Unlock()
		return
	}
	if cl.conn != conn {
		// Stale: cl.conn already moved on while OnDrain was running. Acting
		// on it now (setting retiringConn/drainReconnectScheduled, or
		// starting a parallel reconnect) would race whatever already
		// happened. conn is not necessarily already tracked as the conn
		// being retired either, so make sure it still gets closed here
		// instead of leaking it.
		alreadyRetiring := cl.retiringConn == conn
		cl.mu.Unlock()
		if !alreadyRetiring {
			go func() { _ = conn.Close(uint32(NoError), "superseded before its drain reconnect could start") }()
		}
		return
	}
	if cl.rc.Disabled {
		// Reconnect disabled: WIRE.md section 2.9 still lets in-flight
		// streams finish normally until the server's own deadline, which
		// then closes with 4012 -- let this conn run to that close instead
		// of tearing it down here; watchConn reports that close as the one
		// fatal "drained; reconnect disabled" disconnect.
		cl.drainedNoReconnect = true
		cl.mu.Unlock()
		return
	}
	if cl.drainReconnectScheduled && cl.retiringConn == conn {
		// F2: a second drain frame for the conn already being replaced.
		// WIRE.md doesn't forbid a server from sending drain twice, so this
		// must be tolerated rather than assumed impossible: starting a
		// second parallel attemptLoop here is exactly how a live conn gets
		// orphaned. Both racing attemptLoops dial and succeed independently
		// (onAttemptSucceeded runs for each), but only the first one to
		// finish still finds cl.retiringConn == conn (its own dial) -- the
		// second's onAttemptSucceeded sees cl.retiringConn already nil
		// (cleared by the first) and, thinking it lost no race at all,
		// never closes anything: its freshly dialed conn just becomes the
		// new cl.conn, silently discarding the first replacement, whose own
		// watchConn now waits on a conn nobody will ever close (see that
		// escape hatch below). One reconnect per drained conn is enough;
		// ignore the repeat.
		cl.mu.Unlock()
		return
	}
	cl.drainReconnectScheduled = true
	cl.retiringConn = conn
	cl.mu.Unlock()

	// WIRE.md section 2.9's drain field table: "retry_after_ms: reconnect
	// hint. Absent = reconnect immediately with jitter". Honouring it when
	// present is a deliberate deviation from the JS reference SDK (which
	// ignores it entirely, see watchConn's 4009 case below) toward the spec
	// -- see docs/DECISIONS.md.
	delay := cl.rc.jitter(2 * time.Second)
	if d.RetryAfterMS != nil {
		delay = time.Duration(*d.RetryAfterMS) * time.Millisecond
	}
	// tryTrack, not a bare wg.Add: this runs on the conn's own
	// delivery-loop goroutine, which cl.wg is not already tracking, so an
	// unguarded Add here could race a concurrent Close()'s wg.Wait() (see
	// tryTrack's doc comment). If it returns false, the client is already
	// closing and no reconnect is wanted -- bail without spawning. conn
	// itself is not orphaned: it is exactly the cl.conn/cl.retiringConn this
	// function just set a few lines up (both point at the same live conn in
	// this window -- see the ownership invariant above Close), so Close (or
	// whichever fatal path won the race to set closing) already has it as
	// either its gracefulConn or its retiring snapshot and will close it
	// itself.
	if !cl.tryTrack() {
		return
	}
	go func() {
		defer cl.wg.Done()
		cl.attemptLoop(delay, "drain")
	}()
}

// --- watching an established connection for its eventual disconnect ------

// watchConn blocks until conn ends, then classifies why and either reports
// a fatal disconnect (ending the client) or schedules the next reconnect
// attempt per WIRE.md section 2.9's table -- unless a drain already handled
// this exact disconnect (drainReconnectScheduled) or the client is already
// shutting down for an unrelated reason, in which case it still reports
// exactly once but schedules nothing.
func (cl *Client) watchConn(conn *Conn) {
	select {
	case <-conn.Done():
	case <-cl.closeCh:
		// F2: a stray conn -- most concretely, one of a pair of racing
		// drain-triggered replacements that lost a race handleServerDrain's
		// own duplicate-frame guard now prevents in the first place, but
		// this is deliberately unconditional, not tied to that one cause --
		// has no other way to ever end once nothing still references it as
		// cl.conn/cl.retiringConn: nobody else is ever going to close it.
		// Without this escape, Close()'s wg.Wait() would block on this
		// goroutine forever (or until a real peer's own keepalive timeout,
		// whichever comes first).
		//
		// Only force-close it here if Close() hasn't claimed it, though:
		// closeCh also fires the instant an ordinary Close() call starts on
		// the active conn, well before Close's own graceful conn.Drain()/
		// conn.Close() calls actually reach the socket -- racing ahead of
		// those with a bare NO_ERROR close here would skip the
		// drain{client_requested} handshake and the up-to-5s wait for
		// in-flight streams WIRE.md section 2.10 rule 14 requires. See the
		// ownership invariant above Close: Close() claims exactly the one
		// conn it is gracefully draining/closing itself (cl.gracefulConn);
		// every other live conn -- including the retiring one, which Close
		// force-closes directly anyway, and any conn a fatal path failed to
		// close itself -- has no other claimant and gets force-closed here.
		cl.mu.Lock()
		claimed := cl.gracefulConn == conn
		cl.mu.Unlock()
		if !claimed {
			_ = conn.Close(uint32(NoError), "client closing")
		}
		<-conn.Done()
	}

	cl.mu.Lock()
	wasActive := cl.conn == conn
	if wasActive {
		cl.conn = nil
		// nit 4: truthfully report "disconnected" rather than leave state at
		// its last value (still "connected") for however long it takes
		// whatever runs next -- attemptLoop's own dialing/backoff, or
		// goFatal/reportAndSchedule's closed -- to actually get scheduled.
		// Guarded on cl.state != clientClosed (review item 4): every fatal
		// path (goFatal, onAttemptFailed's fatal branch, reportAndSchedule's
		// exhaustion/disabled branch) already nils cl.conn in the same
		// critical section it sets clientClosed in, so wasActive is already
		// false by the time any of those run first -- this guard is a no-op
		// for them. closeStart (Close/CloseWith) is the one path that sets
		// clientClosed WITHOUT nil-ing cl.conn (the caller still needs it,
		// to close/drain itself): without this guard, THIS watchConn call,
		// triggered by that very close, would overwrite clientClosed with
		// clientDisconnected right before returning -- a client that was
		// connected when Close()/CloseWith() was called always observed
		// State()=="disconnected" instead of "closed" once they returned.
		if cl.state != clientClosed {
			cl.state = clientDisconnected
		}
	}
	if cl.retiringConn == conn {
		cl.retiringConn = nil
	}
	drainSched := false
	drainedNoReconnect := false
	if wasActive {
		drainSched = cl.drainReconnectScheduled
		cl.drainReconnectScheduled = false
		drainedNoReconnect = cl.drainedNoReconnect
		cl.drainedNoReconnect = false
	}
	closingNow := cl.closing
	cl.cumulative.add(conn.Stats())
	cl.mu.Unlock()

	if !wasActive {
		// A superseded conn (drain's replacement already landed) or one
		// fail()'d directly by Close()/onAttemptSucceeded's teardown above:
		// either way, whoever detached it from cl.conn already knows why,
		// and scheduling a second reconnect here would race the one already
		// in flight.
		return
	}

	reason := cl.buildConnectedDisconnectReason(conn)

	if closingNow {
		reason.Fatal = false
		cl.report(reason)
		return
	}
	if drainedNoReconnect {
		reason.Message = "drained; reconnect disabled"
		cl.goFatal(reason)
		return
	}
	if drainSched {
		// Already reconnecting in parallel from handleServerDrain.
		reason.Fatal = false
		cl.report(reason)
		return
	}

	wsCode := cl.effectiveWSCode(conn)
	switch wsCode {
	case 4010, 4011:
		cl.goFatal(reason)
	case 4012, 1001:
		// close 4012, or a plain WS 1001 (nit 3: OVERVIEW.md section 2.8
		// treats a non-ws-mixer 1001 as equivalent to 4012, matching the JS
		// SDK's ABNORMAL_CLOSURE_WS_CODE branch), with no preceding drain
		// (drainSched already handled the with-drain case above): reconnect
		// immediately, jitter(0,2000)ms.
		cl.reportAndSchedule(reason, func(int) time.Duration { return cl.rc.jitter(2 * time.Second) })
	case 4013:
		cl.mu.Lock()
		used := cl.keepaliveRetryUsed
		cl.keepaliveRetryUsed = true
		cl.mu.Unlock()
		if !used {
			cl.reportAndSchedule(reason, func(int) time.Duration { return 0 })
		} else {
			// The budget stays spent: only armStability un-spends it, once
			// this cycle's connection (if it ever lands) stays up
			// StableAfter. Un-spending it here too, on every recurring
			// 4013, used to make it self-reset every other cycle regardless
			// of stability -- masked at v0.4.1 by welcome itself also
			// resetting it, but a real bug now that welcome alone no longer
			// does (docs/DECISIONS.md 2026-09-20): a server flapping 4013
			// forever got an immediate retry on every odd cycle, never
			// climbing backoff past fullJitter(2).
			cl.reportAndSchedule(reason, func(next int) time.Duration { return cl.rc.fullJitter(next) })
		}
	case 4009, 4014:
		// WIRE.md section 2.9: "start at cap" for both -- 4009 (drain at
		// max_streams) and 4014 (APPLICATION_CLOSE) are both deliberate
		// post-welcome refusals, not failures: the app accepted the
		// connection, then refused it on purpose (e.g. a per-account
		// connection cap), so the right response is to back off hard
		// immediately rather than ever probe it again at the bottom of the
		// ladder. (Before StableAfter existed, this doubled as a workaround
		// for onAttemptSucceeded resetting attempt to 0 on every welcome --
		// with the counter now only re-armed once a connection has actually
		// stayed up StableAfter, that workaround reasoning no longer
		// applies, but the explicit "refused on purpose" rule stands on its
		// own regardless.) scheduleReconnectAtCap starts the backoff where a
		// real failure ladder would already be, instead of redialing once a
		// second forever. retry_after_ms is a field of the drain message,
		// not error{} -- and any drain the client observes already triggers
		// its own immediate parallel reconnect above (the drainSched
		// branch, which now honours retry_after_ms itself -- see
		// handleServerDrain), which takes priority over this close-code
		// classification for that same connection. Mirrors the JS SDK's
		// scheduleReconnectAtCap, which does not look for a hint here
		// either.
		cl.reportAndSchedule(reason, func(int) time.Duration { return cl.rc.jitter(cl.rc.Cap) })
	default:
		// Everything else: 4001/4003/4004 (an SDK bug -- still just normal
		// backoff, but always loudly reported via OnDisconnect, which is
		// never gated on anything), 1006/TCP reset/DNS failure/HTTP 5xx-
		// equivalent (no ws-mixer error observed at all), and any other
		// close.
		cl.reportAndSchedule(reason, func(next int) time.Duration { return cl.rc.fullJitter(next) })
	}
}

// effectiveWSCode picks the close code Client's reconnect table switches on:
// the ws-mixer *ConnError's mapped code if one is present, else the raw
// WebSocket close code this side actually observed (conn.PeerCloseCode()),
// else 0 for an abnormal closure with no code at all (WIRE.md section 2.9's
// "1006/TCP reset/DNS failure" bucket).
func (cl *Client) effectiveWSCode(conn *Conn) int {
	if _, ok := conn.Err().(*ConnError); ok {
		return conn.CloseCode()
	}
	if raw := conn.PeerCloseCode(); raw != -1 {
		return raw
	}
	return 0
}

func (cl *Client) buildConnectedDisconnectReason(conn *Conn) DisconnectReason {
	r := DisconnectReason{Phase: PhaseConnected}
	// CloseReason is independent of whether a ws-mixer *ConnError preceded
	// the close (see the field comment on DisconnectReason): set it before
	// the *ConnError branch's early return, not just in the fallback below.
	r.CloseReason = conn.PeerCloseReason()
	if ce, ok := conn.Err().(*ConnError); ok {
		r.WSCode = ce.CloseCode()
		r.ErrorCode = ce.Code
		r.HasErrorCode = true
		r.ErrorName = ce.Code.String()
		r.Message = ce.Message
		return r
	}
	if raw := conn.PeerCloseCode(); raw != -1 {
		r.WSCode = raw
		// Report-only, same derivation classifyFailureErr's bare-close case
		// uses (deriveBareCloseErrorCode): a bare close still gets a usable
		// ErrorCode/ErrorName. This never changes which switch case
		// watchConn takes -- that reads wsCode via effectiveWSCode, not
		// these fields -- only what this DisconnectReason itself carries.
		if ec, has := deriveBareCloseErrorCode(raw); has {
			r.ErrorCode = ec
			r.HasErrorCode = true
			r.ErrorName = ec.String()
		}
	}
	if err := conn.Err(); err != nil {
		r.Message = err.Error()
	} else {
		r.Message = "connection closed"
	}
	return r
}
