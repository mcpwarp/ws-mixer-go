package mcpwarp

import (
	"errors"
	"log/slog"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/mcpwarp/ws-mixer-go/wsmixer"
)

// Agent overrides what the SDK detects for hello.agent.runtime/os, the
// User-Agent header and hello.meta.os. Each set field must be 1-64 printable
// ASCII characters.
type Agent struct {
	Runtime string
	OS      string
}

// ReconnectOptions is the ws-mixer client's reconnect policy (WIRE.md §2.9).
// Zero fields take the defaults (Base 1 s, Cap 60 s, ConnectTimeout 10 s,
// StableAfter 10 s, MaxAttempts unlimited); negative values are invalid.
// MaxAttempts also bounds the attempts before the first registration.
type ReconnectOptions struct {
	Base           time.Duration
	Cap            time.Duration
	ConnectTimeout time.Duration
	MaxAttempts    int
	StableAfter    time.Duration
}

// RetryInfo is one scheduled retry (§5.9): Reason is the transport's
// disconnect message for a redial, "drain" for a drain's replacement dial
// (Attempt 0), "token-refresh" for the one immediate redial after a
// rejected token (Attempt 1, Delay 0), "register failed: <CODE>: <message>"
// for a register re-send.
type RetryInfo struct {
	Attempt int
	Delay   time.Duration
	Reason  string
}

// ExposeOptions configures Expose (§5.1). Exactly one of Handler and Target
// is required.
type ExposeOptions struct {
	// URL is the edge; "" is DefaultURL.
	URL string
	// Token is a static token (PAT or connect-audience JWT). TokenProvider,
	// when set, is used instead and called on every dial.
	Token         string
	TokenProvider wsmixer.TokenProvider
	// Name is the public service name (IsValidServiceName).
	Name string
	// Kind is KindHTTP when empty.
	Kind ServiceKind
	// Integration is the integration's registration UUID; sent lowercased.
	Integration string
	// WebSocket, if set, performs the WebSocket upgrade of every dial (its
	// Transport carries the dialer, proxy and TLS configuration).
	WebSocket *http.Client
	Agent     Agent
	Reconnect ReconnectOptions
	// MaxRequestBodyBytes is the request body cap; 0 is 4 MiB.
	MaxRequestBodyBytes int64
	// OnRetry is called before every scheduled reconnect and register retry.
	// A panic in it is reported as an EventError.
	OnRetry func(RetryInfo)
	Handler http.Handler
	// Target is a local server URL (http or https) requests are proxied to.
	Target string
	// TargetTimeout bounds the wait for the target's response headers;
	// 0 is 30 s. Only with Target.
	TargetTimeout time.Duration
	// Logger receives the ws-mixer client's own log lines
	// (wsmixer.Options.Logger); nil is slog.Default().
	Logger *slog.Logger
}

// ErrInvalidOptions is matched (errors.Is) by every validation error of
// Expose, NewTunnel and Tunnel.Dispose. The error's message is the §5.2 one.
var ErrInvalidOptions = errors.New("mcpwarp: invalid options")

type optionsError struct {
	msg     string
	badName bool // §5.2 row 5: the tunnel reports INVALID_NAME, not INVALID_CONFIG
}

func (e *optionsError) Error() string        { return e.msg }
func (e *optionsError) Is(target error) bool { return target == ErrInvalidOptions }

func invalid(msg string) error { return &optionsError{msg: msg} }

const maxTimerMs = 2147483647

var agentFieldRE = regexp.MustCompile(`^[\x20-\x7e]{1,64}$`)

// validateAdapter is §5.2 rows 1-4: the handler/target choice.
func validateAdapter(handler http.Handler, target string, targetTimeout time.Duration) (*url.URL, error) {
	if (handler == nil) == (target == "") {
		return nil, invalid("mcpwarp: pass exactly one of `handler` or `target`")
	}
	if handler != nil {
		if targetTimeout != 0 {
			return nil, invalid("mcpwarp: `targetTimeoutMs` applies only to `target`")
		}
		return nil, nil
	}
	u, err := url.Parse(target)
	if err != nil || !u.IsAbs() || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return nil, invalid("mcpwarp: `target` is not a valid URL")
	}
	if targetTimeout < 0 || targetTimeout > maxTimerMs*time.Millisecond {
		ms := strconv.FormatFloat(float64(targetTimeout)/float64(time.Millisecond), 'f', -1, 64)
		return nil, invalid("mcpwarp: targetTimeoutMs must be a positive number of at most 2147483647 ms, got " + ms)
	}
	return u, nil
}

// validateIntegration is §5.2 rows 6-7.
func validateIntegration(integration string) error {
	if integration == "" {
		return invalid("mcpwarp: `integration` (your integration's registration UUID) is required")
	}
	if !integrationIDRE.MatchString(strings.ToLower(integration)) {
		return invalid("mcpwarp: `integration` must be an RFC 4122 UUID in text form; the server silently ignores anything else")
	}
	return nil
}

// validateAgent is §5.2 row 10.
func validateAgent(a Agent) error {
	if a.Runtime != "" && !agentFieldRE.MatchString(a.Runtime) {
		return invalid("mcpwarp: `agent.runtime` must be 1-64 printable ASCII characters")
	}
	if a.OS != "" && !agentFieldRE.MatchString(a.OS) {
		return invalid("mcpwarp: `agent.os` must be 1-64 printable ASCII characters")
	}
	return nil
}

// validateReconnect is §5.2 row 11.
func validateReconnect(r ReconnectOptions) error {
	switch {
	case r.Base < 0:
		return invalid("mcpwarp: `reconnect.base` must be a finite number >= 0")
	case r.Cap < 0:
		return invalid("mcpwarp: `reconnect.cap` must be a finite number >= 0")
	case r.ConnectTimeout < 0:
		return invalid("mcpwarp: `reconnect.connectTimeout` must be a positive finite number")
	case r.MaxAttempts < 0:
		return invalid("mcpwarp: `reconnect.maxAttempts` must be a number >= 0 (0 for no limit)")
	case r.StableAfter < 0:
		return invalid("mcpwarp: `reconnect.stableAfter` must be a finite number >= 0")
	}
	return nil
}

// validateURL checks the edge URL: "" or an absolute ws/wss URL. Not a §5.2
// row: a URL the dialer can never use is reported here rather than as an
// endless retry loop.
func validateURL(u string) error {
	if u == "" {
		return nil
	}
	p, err := url.Parse(u)
	if err != nil || p.Host == "" || (p.Scheme != "ws" && p.Scheme != "wss") {
		return invalid("mcpwarp: `url` must be a ws:// or wss:// URL")
	}
	return nil
}

// validateService is §5.2 rows 5, 8 and 9-11 for one service: name, kind,
// token, agent, reconnect, then the URL. Integration and the adapter come
// first (rows 1-4, 6-7) and are checked by the caller.
func validateService(name string, kind ServiceKind, hasToken bool, agent Agent, r ReconnectOptions, edgeURL string) error {
	if !IsValidServiceName(name) {
		return &optionsError{msg: "mcpwarp: invalid service `name`: lowercase letters, digits and inner dashes, at most 30 characters", badName: true}
	}
	if kind != "" && kind != KindHTTP && kind != KindStdio {
		return invalid("mcpwarp: `kind` must be \"http\" or \"stdio\"")
	}
	if !hasToken {
		return invalid("mcpwarp: `token` is required")
	}
	if err := validateAgent(agent); err != nil {
		return err
	}
	if err := validateReconnect(r); err != nil {
		return err
	}
	return validateURL(edgeURL)
}

// validateExpose is §5.2 in order. Row 5 (name) comes after the adapter rows
// and before integration, as in the JS reference: adapter (1-4), name (5),
// integration (6-7), kind (8), token (9), agent (10), reconnect (11).
func validateExpose(o *ExposeOptions) (*url.URL, error) {
	target, err := validateAdapter(o.Handler, o.Target, o.TargetTimeout)
	if err != nil {
		return nil, err
	}
	if !IsValidServiceName(o.Name) {
		return nil, validateService(o.Name, "", false, Agent{}, ReconnectOptions{}, "")
	}
	if err := validateIntegration(o.Integration); err != nil {
		return nil, err
	}
	hasToken := o.TokenProvider != nil || o.Token != ""
	return target, validateService(o.Name, o.Kind, hasToken, o.Agent, o.Reconnect, o.URL)
}

func (r ReconnectOptions) wsmixer() wsmixer.ReconnectOptions {
	return wsmixer.ReconnectOptions{
		Base:           r.Base,
		Cap:            r.Cap,
		ConnectTimeout: r.ConnectTimeout,
		MaxAttempts:    r.MaxAttempts,
		StableAfter:    r.StableAfter,
	}
}
