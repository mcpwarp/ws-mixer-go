package wsmixer

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// The seven v1 control message discriminators (OVERVIEW.md section 2.7).
const (
	tHello   = "hello"
	tWelcome = "welcome"
	tPing    = "ping"
	tPong    = "pong"
	tDrain   = "drain"
	tError   = "error"
	tApp     = "app"
)

// Field bounds from OVERVIEW.md section 2.7's field tables.
const (
	maxTokenLen      = 4096
	maxAgentFieldLen = 128
	windowMin        = 16384
	windowMax        = (1 << 31) - 1
	maxStreamsMin    = 1
	maxStreamsMax    = 100000
	maxSessionLen    = 64
	pingIntervalMin  = 5000
	maxErrorMessage  = 1024
	maxDrainMessage  = 256
	maxIDValue       = (int64(1) << 53) - 1 // 2^53-1, ping/pong id range
	maxStreamIDValue = (1 << 31) - 1        // 31-bit stream ids
)

var capabilityRE = regexp.MustCompile(`^[a-z0-9_.-]{1,64}$`)

// AgentInfo identifies the SDK on the sending end of `hello`.
type AgentInfo struct {
	SDK        string `json:"sdk"`
	SDKVersion string `json:"sdk_version"`
	Runtime    string `json:"runtime,omitempty"`
	OS         string `json:"os,omitempty"`
}

// ServerInfo is the server's answer to AgentInfo, carried in `welcome`.
type ServerInfo struct {
	Name    string `json:"name"`
	Version string `json:"version"`
	Node    string `json:"node"`
}

// HelloMsg is `t:"hello"`, client -> server, the first frame on stream 0.
type HelloMsg struct {
	T            string          `json:"t"`
	V            int64           `json:"v"`
	Token        string          `json:"token"`
	Agent        AgentInfo       `json:"agent"`
	Window       int64           `json:"window,omitempty"`
	MaxStreams   int64           `json:"max_streams,omitempty"`
	Capabilities []string        `json:"capabilities,omitempty"`
	Meta         json.RawMessage `json:"meta,omitempty"`
}

// WelcomeMsg is `t:"welcome"`, server -> client, the reply to `hello`.
type WelcomeMsg struct {
	T            string          `json:"t"`
	V            int64           `json:"v"`
	Session      string          `json:"session"`
	Window       int64           `json:"window"`
	MaxStreams   int64           `json:"max_streams"`
	PingInterval int64           `json:"ping_interval"`
	PingTimeout  int64           `json:"ping_timeout"`
	Server       *ServerInfo     `json:"server,omitempty"`
	Capabilities []string        `json:"capabilities,omitempty"`
	Meta         json.RawMessage `json:"meta,omitempty"`
}

// PingMsg is `t:"ping"`, sent by both sides on their own independent schedule.
type PingMsg struct {
	T  string `json:"t"`
	ID int64  `json:"id"`
	TS int64  `json:"ts,omitempty"`
}

// PongMsg is `t:"pong"`, sent immediately in reply to a PingMsg.
type PongMsg struct {
	T  string `json:"t"`
	ID int64  `json:"id"`
	TS int64  `json:"ts,omitempty"`
}

// DrainMsg is `t:"drain"`.
type DrainMsg struct {
	T            string `json:"t"`
	Reason       string `json:"reason"`
	LastStreamID uint32 `json:"last_stream_id"`
	DeadlineMS   *int64 `json:"deadline_ms,omitempty"`
	RetryAfterMS *int64 `json:"retry_after_ms,omitempty"`
	Message      string `json:"message,omitempty"`
}

// ErrorMsg is `t:"error"`: connection-fatal, always the last message on the
// wire before the WebSocket close.
type ErrorMsg struct {
	T            string  `json:"t"`
	Code         uint32  `json:"code"`
	Message      string  `json:"message"`
	StreamID     *uint32 `json:"stream_id,omitempty"`
	LastStreamID *uint32 `json:"last_stream_id,omitempty"`
}

// AppMsg is `t:"app"`: an opaque, application-defined message.
type AppMsg struct {
	T    string          `json:"t"`
	Body json.RawMessage `json:"body"`
}

// ParseControl validates and decodes one stream-0 DATA payload. It implements
// OVERVIEW.md section 2.7's "envelope validation and forward compatibility"
// table plus every message type's per-field checks by hand (decision 9 in
// OVERVIEW.md section 6: never a JSON Schema validator on the hot path).
//
// The returned value is one of *HelloMsg, *WelcomeMsg, *PingMsg, *PongMsg,
// *DrainMsg, *ErrorMsg or *AppMsg. Every validation failure is a *ConnError
// with ProtocolErrorCode (or EnhanceYourCalm for the oversize case), since a
// malformed control message always desynchronizes the handshake or the
// connection state machine.
func ParseControl(raw []byte) (any, error) {
	if len(raw) > MaxStreamZeroPayload {
		return nil, newConnErrorf(EnhanceYourCalm, "stream 0 payload of %d bytes exceeds the %d byte control-channel limit", len(raw), MaxStreamZeroPayload)
	}

	var top map[string]json.RawMessage
	if err := json.Unmarshal(raw, &top); err != nil {
		// Distinguish "not even JSON" from "valid JSON, but not an object" only
		// on this (rare, already-failing) path, so the common case is a single
		// unmarshal.
		if !json.Valid(raw) {
			return nil, newConnErrorf(ProtocolErrorCode, "malformed JSON on stream 0: %v", err)
		}
		var probe any
		_ = json.Unmarshal(raw, &probe)
		return nil, newConnErrorf(ProtocolErrorCode, "control message must be a JSON object, got %s", jsonKind(probe))
	}

	tRaw, ok := top["t"]
	if !ok {
		return nil, newConnErrorf(ProtocolErrorCode, "control message missing required field \"t\"")
	}
	t, err := fieldString(tRaw, "t")
	if err != nil {
		return nil, newConnErrorf(ProtocolErrorCode, "%v", err)
	}

	switch t {
	case tHello:
		return parseHello(top)
	case tWelcome:
		return parseWelcome(top)
	case tPing:
		return parsePing(top)
	case tPong:
		return parsePong(top)
	case tDrain:
		return parseDrain(top)
	case tError:
		return parseError(top)
	case tApp:
		return parseApp(top)
	default:
		return nil, newConnErrorf(ProtocolErrorCode, "unknown control message type %q", t)
	}
}

func jsonKind(v any) string {
	switch v.(type) {
	case nil:
		return "null"
	case bool:
		return "boolean"
	case float64, json.Number:
		return "number"
	case string:
		return "string"
	case []any:
		return "array"
	default:
		return fmt.Sprintf("%T", v)
	}
}

// --- field extraction helpers -------------------------------------------------

func fieldString(raw json.RawMessage, name string) (string, error) {
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return "", fmt.Errorf("field %q must be a string", name)
	}
	return s, nil
}

func fieldInt(raw json.RawMessage, name string) (int64, error) {
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return 0, fmt.Errorf("field %q must be an integer", name)
	}
	if _, ok := v.(float64); !ok {
		return 0, fmt.Errorf("field %q must be an integer", name)
	}
	// Reject floats: OVERVIEW.md section 2.7 "No floats anywhere in the
	// control channel". Re-render the raw bytes and check they parse cleanly
	// as a base-10 integer literal, rather than trusting float64 (which loses
	// precision above 2^53 and silently accepts "1.0").
	s := strings.TrimSpace(string(raw))
	if strings.ContainsAny(s, ".eE") {
		return 0, fmt.Errorf("field %q must be an integer, not a float", name)
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("field %q must be an integer", name)
	}
	return n, nil
}

func fieldObject(raw json.RawMessage, name string) (map[string]json.RawMessage, error) {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, fmt.Errorf("field %q must be an object", name)
	}
	return m, nil
}

func fieldStringArray(raw json.RawMessage, name string) ([]string, error) {
	var arr []json.RawMessage
	if err := json.Unmarshal(raw, &arr); err != nil {
		return nil, fmt.Errorf("field %q must be an array of strings", name)
	}
	out := make([]string, 0, len(arr))
	for _, item := range arr {
		s, err := fieldString(item, name)
		if err != nil {
			return nil, fmt.Errorf("field %q must be an array of strings", name)
		}
		out = append(out, s)
	}
	return out, nil
}

func requireField(top map[string]json.RawMessage, name string) (json.RawMessage, error) {
	raw, ok := top[name]
	if !ok {
		return nil, fmt.Errorf("missing required field %q", name)
	}
	return raw, nil
}

func validateCapabilities(caps []string) error {
	for _, c := range caps {
		if !capabilityRE.MatchString(c) {
			return fmt.Errorf("capability %q does not match ^[a-z0-9_.-]{1,64}$", c)
		}
	}
	return nil
}

// --- shared plumbing -------------------------------------------------------

func requireString(top map[string]json.RawMessage, name string) (string, error) {
	raw, err := requireField(top, name)
	if err != nil {
		return "", err
	}
	return fieldString(raw, name)
}

func requireInt(top map[string]json.RawMessage, name string) (int64, error) {
	raw, err := requireField(top, name)
	if err != nil {
		return 0, err
	}
	return fieldInt(raw, name)
}

// connErr wraps a plain field-validation error (produced by the helpers above,
// which know nothing about wsmixer's error codes) into a *ConnError.
func connErr(err error) *ConnError {
	return &ConnError{Code: ProtocolErrorCode, Message: err.Error()}
}
