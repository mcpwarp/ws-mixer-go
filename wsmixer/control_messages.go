package wsmixer

// This file holds the per-message-type validators dispatched by
// ParseControl in control.go, one function per t in the v1 seven
// (WIRE.md §2.7's field tables).

import (
	"encoding/json"
	"fmt"
	"unicode/utf8"
)

// --- hello ---------------------------------------------------------------

func parseHello(top map[string]json.RawMessage) (*HelloMsg, error) {
	m := &HelloMsg{T: tHello}

	vRaw, err := requireField(top, "v")
	if err != nil {
		return nil, connErr(err)
	}
	if m.V, err = fieldInt(vRaw, "v"); err != nil {
		return nil, connErr(err)
	}
	if m.V != 1 {
		return nil, newConnErrorf(UnsupportedCode, "hello.v=%d does not match ws-mixer.v1", m.V)
	}

	tokRaw, err := requireField(top, "token")
	if err != nil {
		return nil, connErr(err)
	}
	if m.Token, err = fieldString(tokRaw, "token"); err != nil {
		return nil, connErr(err)
	}
	if l := len(m.Token); l < 1 || l > maxTokenLen {
		return nil, newConnErrorf(ProtocolErrorCode, "hello.token length %d out of range 1..%d", l, maxTokenLen)
	}

	agentRaw, err := requireField(top, "agent")
	if err != nil {
		return nil, connErr(err)
	}
	agentObj, err := fieldObject(agentRaw, "agent")
	if err != nil {
		return nil, connErr(err)
	}
	if m.Agent, err = parseAgentInfo(agentObj); err != nil {
		return nil, connErr(err)
	}

	if raw, ok := top["window"]; ok {
		if m.Window, err = fieldInt(raw, "window"); err != nil {
			return nil, connErr(err)
		}
		if m.Window < windowMin || m.Window > windowMax {
			return nil, newConnErrorf(ProtocolErrorCode, "hello.window %d out of range %d..%d", m.Window, windowMin, windowMax)
		}
	} else {
		m.Window = 262144
	}

	if raw, ok := top["max_streams"]; ok {
		if m.MaxStreams, err = fieldInt(raw, "max_streams"); err != nil {
			return nil, connErr(err)
		}
		if m.MaxStreams < maxStreamsMin || m.MaxStreams > maxStreamsMax {
			return nil, newConnErrorf(ProtocolErrorCode, "hello.max_streams %d out of range %d..%d", m.MaxStreams, maxStreamsMin, maxStreamsMax)
		}
	} else {
		m.MaxStreams = 64
	}

	if raw, ok := top["capabilities"]; ok {
		if m.Capabilities, err = fieldStringArray(raw, "capabilities"); err != nil {
			return nil, connErr(err)
		}
		if err := validateCapabilities(m.Capabilities); err != nil {
			return nil, newConnErrorf(ProtocolErrorCode, "hello.capabilities: %v", err)
		}
	}

	if raw, ok := top["meta"]; ok {
		m.Meta = json.RawMessage(raw)
	}

	return m, nil
}

func parseAgentInfo(obj map[string]json.RawMessage) (AgentInfo, error) {
	var a AgentInfo
	sdkRaw, err := requireField(obj, "sdk")
	if err != nil {
		return a, fmt.Errorf("agent.%v", err)
	}
	if a.SDK, err = fieldString(sdkRaw, "agent.sdk"); err != nil {
		return a, err
	}
	svRaw, err := requireField(obj, "sdk_version")
	if err != nil {
		return a, fmt.Errorf("agent.%v", err)
	}
	if a.SDKVersion, err = fieldString(svRaw, "agent.sdk_version"); err != nil {
		return a, err
	}
	if utf8.RuneCountInString(a.SDK) > maxAgentFieldLen || utf8.RuneCountInString(a.SDKVersion) > maxAgentFieldLen {
		return a, fmt.Errorf("agent.sdk / agent.sdk_version must be <= %d characters", maxAgentFieldLen)
	}
	if raw, ok := obj["runtime"]; ok {
		if a.Runtime, err = fieldString(raw, "agent.runtime"); err != nil {
			return a, err
		}
	}
	if raw, ok := obj["os"]; ok {
		if a.OS, err = fieldString(raw, "agent.os"); err != nil {
			return a, err
		}
	}
	return a, nil
}

// --- welcome ---------------------------------------------------------------

func parseWelcome(top map[string]json.RawMessage) (*WelcomeMsg, error) {
	m := &WelcomeMsg{T: tWelcome}
	var err error

	if m.V, err = requireInt(top, "v"); err != nil {
		return nil, connErr(err)
	}
	if m.V != 1 {
		return nil, newConnErrorf(UnsupportedCode, "welcome.v=%d does not match ws-mixer.v1", m.V)
	}
	if m.Session, err = requireString(top, "session"); err != nil {
		return nil, connErr(err)
	}
	if len(m.Session) > maxSessionLen {
		return nil, newConnErrorf(ProtocolErrorCode, "welcome.session length %d exceeds %d", len(m.Session), maxSessionLen)
	}
	if m.Window, err = requireInt(top, "window"); err != nil {
		return nil, connErr(err)
	}
	if m.Window < windowMin || m.Window > windowMax {
		return nil, newConnErrorf(ProtocolErrorCode, "welcome.window %d out of range %d..%d", m.Window, windowMin, windowMax)
	}
	if m.MaxStreams, err = requireInt(top, "max_streams"); err != nil {
		return nil, connErr(err)
	}
	if m.MaxStreams < maxStreamsMin || m.MaxStreams > maxStreamsMax {
		return nil, newConnErrorf(ProtocolErrorCode, "welcome.max_streams %d out of range %d..%d", m.MaxStreams, maxStreamsMin, maxStreamsMax)
	}
	// The ping_interval >= 5000ms / ping_timeout >= 2x ping_interval floor
	// (WIRE.md §2.7) is enforced by Conn.applyWelcome
	// (client.go), not here: this parser is a stateless wire decoder with no
	// notion of the conformance-only allowSubfloorTiming escape hatch, and
	// applyWelcome is the one place that can gate the check on it.
	if m.PingInterval, err = requireInt(top, "ping_interval"); err != nil {
		return nil, connErr(err)
	}
	if m.PingTimeout, err = requireInt(top, "ping_timeout"); err != nil {
		return nil, connErr(err)
	}

	if raw, ok := top["server"]; ok {
		obj, err := fieldObject(raw, "server")
		if err != nil {
			return nil, connErr(err)
		}
		si := &ServerInfo{}
		if v, ok := obj["name"]; ok {
			if si.Name, err = fieldString(v, "server.name"); err != nil {
				return nil, connErr(err)
			}
		}
		if v, ok := obj["version"]; ok {
			if si.Version, err = fieldString(v, "server.version"); err != nil {
				return nil, connErr(err)
			}
		}
		if v, ok := obj["node"]; ok {
			if si.Node, err = fieldString(v, "server.node"); err != nil {
				return nil, connErr(err)
			}
		}
		m.Server = si
	}
	if raw, ok := top["capabilities"]; ok {
		if m.Capabilities, err = fieldStringArray(raw, "capabilities"); err != nil {
			return nil, connErr(err)
		}
		if err := validateCapabilities(m.Capabilities); err != nil {
			return nil, newConnErrorf(ProtocolErrorCode, "welcome.capabilities: %v", err)
		}
	}
	if raw, ok := top["meta"]; ok {
		m.Meta = json.RawMessage(raw)
	}
	return m, nil
}

// --- ping / pong -------------------------------------------------------------

func parsePing(top map[string]json.RawMessage) (*PingMsg, error) { return parsePingPong(top, tPing) }
func parsePong(top map[string]json.RawMessage) (*PongMsg, error) {
	p, err := parsePingPong(top, tPong)
	if err != nil {
		return nil, err
	}
	return (*PongMsg)(p), nil
}

func parsePingPong(top map[string]json.RawMessage, t string) (*PingMsg, error) {
	m := &PingMsg{T: t}
	idRaw, err := requireField(top, "id")
	if err != nil {
		return nil, connErr(err)
	}
	if m.ID, err = fieldInt(idRaw, "id"); err != nil {
		return nil, connErr(err)
	}
	if m.ID < 0 || m.ID > maxIDValue {
		return nil, newConnErrorf(ProtocolErrorCode, "%s.id %d out of range 0..2^53-1", t, m.ID)
	}
	if raw, ok := top["ts"]; ok {
		if m.TS, err = fieldInt(raw, "ts"); err != nil {
			return nil, connErr(err)
		}
	}
	return m, nil
}

// --- drain ---------------------------------------------------------------

func parseDrain(top map[string]json.RawMessage) (*DrainMsg, error) {
	m := &DrainMsg{T: tDrain}
	var err error
	if m.Reason, err = requireString(top, "reason"); err != nil {
		return nil, connErr(err)
	}
	lsidRaw, err := requireField(top, "last_stream_id")
	if err != nil {
		return nil, connErr(err)
	}
	lsid, err := fieldInt(lsidRaw, "last_stream_id")
	if err != nil {
		return nil, connErr(err)
	}
	if lsid < 0 || lsid > maxStreamIDValue {
		return nil, newConnErrorf(ProtocolErrorCode, "drain.last_stream_id %d out of range 0..2^31-1", lsid)
	}
	m.LastStreamID = uint32(lsid)

	if raw, ok := top["deadline_ms"]; ok {
		v, err := fieldInt(raw, "deadline_ms")
		if err != nil {
			return nil, connErr(err)
		}
		m.DeadlineMS = &v
	}
	if raw, ok := top["retry_after_ms"]; ok {
		v, err := fieldInt(raw, "retry_after_ms")
		if err != nil {
			return nil, connErr(err)
		}
		m.RetryAfterMS = &v
	}
	if raw, ok := top["message"]; ok {
		if m.Message, err = fieldString(raw, "message"); err != nil {
			return nil, connErr(err)
		}
		if len(m.Message) > maxDrainMessage {
			return nil, newConnErrorf(ProtocolErrorCode, "drain.message length %d exceeds %d", len(m.Message), maxDrainMessage)
		}
	}
	return m, nil
}

// KnownDrainReason reports whether reason is one of the documented values.
// An unrecognized reason is tolerated at runtime (WIRE.md §2.7):
// callers should treat it as "maintenance" and count it, not reject it.
func KnownDrainReason(reason string) bool {
	switch reason {
	case "rollout", "overload", "id_exhausted", "replaced", "maintenance", "client_requested":
		return true
	default:
		return false
	}
}

// --- error ---------------------------------------------------------------

func parseError(top map[string]json.RawMessage) (*ErrorMsg, error) {
	m := &ErrorMsg{T: tError}
	codeRaw, err := requireField(top, "code")
	if err != nil {
		return nil, connErr(err)
	}
	code, err := fieldInt(codeRaw, "code")
	if err != nil {
		return nil, connErr(err)
	}
	if code < 0 || code > 0xffffffff {
		return nil, newConnErrorf(ProtocolErrorCode, "error.code %d out of range", code)
	}
	m.Code = uint32(code)

	if m.Message, err = requireString(top, "message"); err != nil {
		return nil, connErr(err)
	}
	if len(m.Message) > maxErrorMessage {
		return nil, newConnErrorf(ProtocolErrorCode, "error.message length %d exceeds %d", len(m.Message), maxErrorMessage)
	}

	if raw, ok := top["stream_id"]; ok {
		v, err := fieldInt(raw, "stream_id")
		if err != nil {
			return nil, connErr(err)
		}
		if v < 0 || v > maxStreamIDValue {
			return nil, newConnErrorf(ProtocolErrorCode, "error.stream_id %d out of range 0..2^31-1", v)
		}
		sid := uint32(v)
		m.StreamID = &sid
	}
	if raw, ok := top["last_stream_id"]; ok {
		v, err := fieldInt(raw, "last_stream_id")
		if err != nil {
			return nil, connErr(err)
		}
		if v < 0 || v > maxStreamIDValue {
			return nil, newConnErrorf(ProtocolErrorCode, "error.last_stream_id %d out of range 0..2^31-1", v)
		}
		lsid := uint32(v)
		m.LastStreamID = &lsid
	}
	return m, nil
}

// --- app ---------------------------------------------------------------

func parseApp(top map[string]json.RawMessage) (*AppMsg, error) {
	m := &AppMsg{T: tApp}
	bodyRaw, err := requireField(top, "body")
	if err != nil {
		return nil, connErr(err)
	}
	var probe any
	if err := json.Unmarshal(bodyRaw, &probe); err != nil {
		return nil, newConnErrorf(ProtocolErrorCode, "app.body must be a JSON object: %v", err)
	}
	if _, ok := probe.(map[string]any); !ok {
		return nil, newConnErrorf(ProtocolErrorCode, "app.body must be a JSON object, got %s", jsonKind(probe))
	}
	m.Body = json.RawMessage(bodyRaw)
	return m, nil
}
