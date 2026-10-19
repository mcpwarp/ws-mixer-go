package mcpwarp

import (
	"bytes"
	"encoding/json"
	"regexp"
)

// envelopeVersion is the only `v` this layer speaks (MCPWARP_V).
const envelopeVersion = 1

// DefaultURL is the edge an agent dials when no URL is configured. The edge
// routes by Host; the path is ignored.
const DefaultURL = "wss://connect.mcpwarp.io"

// ServiceNameMax is the longest service name the edge accepts, in characters.
const ServiceNameMax = 30

// ServiceNameRE is the service-name syntax: lowercase letters, digits and
// inner dashes. Names are also limited to ServiceNameMax characters.
var ServiceNameRE = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]*[a-z0-9])?$`)

// IsValidServiceName reports whether name is a service name the edge
// accepts: at most ServiceNameMax characters, matching ServiceNameRE.
func IsValidServiceName(name string) bool {
	return len(name) <= ServiceNameMax && ServiceNameRE.MatchString(name)
}

// integrationIDRE is what the edge accepts as hello.meta.integration,
// tested after lowercasing (§2.4 INTEGRATION_ID_RE).
var integrationIDRE = regexp.MustCompile(`^(?:[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}|00000000-0000-0000-0000-000000000000|ffffffff-ffff-ffff-ffff-ffffffffffff)$`)

// ServiceKind is a register's services[].kind.
type ServiceKind string

const (
	KindHTTP  ServiceKind = "http"
	KindStdio ServiceKind = "stdio"
)

// RegisterErrorCode is a registered.errors[].code value (§2.5). The edge may
// send codes outside this list; they are carried as-is and treated as a
// refusal.
type RegisterErrorCode string

const (
	RegisterInvalidName      RegisterErrorCode = "INVALID_NAME"
	RegisterUsernameRequired RegisterErrorCode = "USERNAME_REQUIRED"
	RegisterQuotaExceeded    RegisterErrorCode = "QUOTA_EXCEEDED"
	RegisterConflict         RegisterErrorCode = "CONFLICT"
	RegisterServerDisabled   RegisterErrorCode = "SERVER_DISABLED"
	RegisterInternal         RegisterErrorCode = "INTERNAL"
	RegisterNoResponders     RegisterErrorCode = "NO_RESPONDERS"
	RegisterTimeout          RegisterErrorCode = "TIMEOUT"
	RegisterAborted          RegisterErrorCode = "ABORTED"
	RegisterConnectionLost   RegisterErrorCode = "CONNECTION_LOST"
	RegisterMalformedReply   RegisterErrorCode = "MALFORMED_REPLY"
	RegisterBadRequest       RegisterErrorCode = "BAD_REQUEST"
	RegisterUnauthenticated  RegisterErrorCode = "UNAUTHENTICATED"
	RegisterForbidden        RegisterErrorCode = "FORBIDDEN"
	RegisterNotImplemented   RegisterErrorCode = "NOT_IMPLEMENTED"
	RegisterUnavailable      RegisterErrorCode = "UNAVAILABLE"
	RegisterServiceError     RegisterErrorCode = "SERVICE_ERROR"
)

// IsTransientRegisterError reports whether code is retried on the same
// connection with backoff instead of being a refusal (TRANSIENT_REGISTER_ERRORS).
func IsTransientRegisterError(code RegisterErrorCode) bool {
	switch code {
	case RegisterInternal, RegisterNoResponders, RegisterTimeout, RegisterAborted,
		RegisterConnectionLost, RegisterMalformedReply, RegisterUnavailable, RegisterServiceError:
		return true
	}
	return false
}

// AppErrorCode is an `op:"error"` code value (§2.6). Unknown codes are
// carried as-is and are a non-fatal error.
type AppErrorCode string

const (
	AppBadRequest         AppErrorCode = "BAD_REQUEST"
	AppUnsupportedVersion AppErrorCode = "UNSUPPORTED_VERSION"
	AppUnknownOp          AppErrorCode = "UNKNOWN_OP"
	AppTooManyServices    AppErrorCode = "TOO_MANY_SERVICES"
	AppOverloaded         AppErrorCode = "OVERLOADED"
	AppConnectionLimit    AppErrorCode = "CONNECTION_LIMIT"
)

// Envelope is one decoded inbound envelope: *RegisteredMsg,
// *UnregisteredMsg, *DisableMsg, *EnableMsg, *ConflictMsg, *ErrorMsg or
// *UnknownMsg.
type Envelope interface{ isEnvelope() }

// RegisteredService is one registered.services[] entry.
type RegisteredService struct {
	Name    string
	ID      string
	URL     string
	Created bool
}

// ServiceError is one registered.errors[] entry. ID is "" when absent; the
// edge sets it on SERVER_DISABLED.
type ServiceError struct {
	Name    string
	Code    RegisterErrorCode
	Message string
	ID      string
}

// UnregisteredService is one unregistered.services[] entry.
type UnregisteredService struct {
	Name string
	ID   string
}

// RegisteredMsg is `op:"registered"`, the reply to register.
type RegisteredMsg struct {
	Services []RegisteredService
	Errors   []ServiceError
}

// UnregisteredMsg is `op:"unregistered"`, the reply to unregister.
type UnregisteredMsg struct {
	Services []UnregisteredService
}

// DisableMsg is `op:"disable"`: the service was disabled in the dashboard.
type DisableMsg struct {
	ID     string
	Reason string
}

// EnableMsg is `op:"enable"`: the service was re-enabled.
type EnableMsg struct {
	ID   string
	Name string
}

// ConflictMsg is `op:"conflict"`: another connection took the name over.
type ConflictMsg struct {
	ID     string
	Name   string
	Reason string
}

// ErrorMsg is `op:"error"`, an envelope-level failure.
type ErrorMsg struct {
	Code    AppErrorCode
	Message string
}

// UnknownMsg is an envelope this agent ignores: `v` other than 1, or an op
// it does not know (including the agent→edge ops register/unregister). V and
// Op are the raw JSON values, nil when absent.
type UnknownMsg struct {
	V  json.RawMessage
	Op json.RawMessage
}

func (*RegisteredMsg) isEnvelope()   {}
func (*UnregisteredMsg) isEnvelope() {}
func (*DisableMsg) isEnvelope()      {}
func (*EnableMsg) isEnvelope()       {}
func (*ConflictMsg) isEnvelope()     {}
func (*ErrorMsg) isEnvelope()        {}
func (*UnknownMsg) isEnvelope()      {}

type registerEnvelope struct {
	V        int               `json:"v"`
	Op       string            `json:"op"`
	Services []registerService `json:"services"`
}

type registerService struct {
	Name string      `json:"name"`
	Kind ServiceKind `json:"kind"`
}

type unregisterEnvelope struct {
	V        int                 `json:"v"`
	Op       string              `json:"op"`
	Services []unregisterService `json:"services"`
}

type unregisterService struct {
	Name string `json:"name"`
}

// EncodeRegister returns the `app` body announcing one service:
// {"mcpwarp":{"v":1,"op":"register","services":[{"name":<name>,"kind":<kind>}]}},
// byte-exact per §2.2. It does not validate name or kind.
func EncodeRegister(name string, kind ServiceKind) json.RawMessage {
	return encodeBody(registerEnvelope{V: envelopeVersion, Op: "register", Services: []registerService{{Name: name, Kind: kind}}})
}

// EncodeUnregister returns the `app` body withdrawing one service:
// {"mcpwarp":{"v":1,"op":"unregister","services":[{"name":<name>}]}},
// byte-exact per §2.2.
func EncodeUnregister(name string) json.RawMessage {
	return encodeBody(unregisterEnvelope{V: envelopeVersion, Op: "unregister", Services: []unregisterService{{Name: name}}})
}

// MaxServicesPerMessage is the most entries one register/unregister may
// carry (§2.1 Batch); the edge answers a larger register with the
// whole-frame error{TOO_MANY_SERVICES}.
const MaxServicesPerMessage = 100

// RegisterEntry is one services[] entry of a register.
type RegisterEntry struct {
	Name string
	Kind ServiceKind
}

// EncodeRegisterList returns the `app` body announcing entries in order, in
// §2.2's canonical key order. It does not validate the entries, their count
// (1..MaxServicesPerMessage) or their uniqueness.
func EncodeRegisterList(entries []RegisterEntry) json.RawMessage {
	svcs := make([]registerService, len(entries))
	for i, e := range entries {
		svcs[i] = registerService{Name: e.Name, Kind: e.Kind}
	}
	return encodeBody(registerEnvelope{V: envelopeVersion, Op: "register", Services: svcs})
}

// EncodeUnregisterList returns the `app` body withdrawing names in order. It
// does not validate the names or their count.
func EncodeUnregisterList(names []string) json.RawMessage {
	svcs := make([]unregisterService, len(names))
	for i, n := range names {
		svcs[i] = unregisterService{Name: n}
	}
	return encodeBody(unregisterEnvelope{V: envelopeVersion, Op: "unregister", Services: svcs})
}

func encodeBody(env any) json.RawMessage {
	return json.RawMessage(marshalNoEscape(struct {
		Mcpwarp any `json:"mcpwarp"`
	}{env}))
}

// marshalNoEscape is json.Marshal without HTML escaping, matching
// JSON.stringify for the strings this layer puts on the wire.
func marshalNoEscape(v any) []byte {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		panic("mcpwarp: encoding " + err.Error())
	}
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n"))
}

// DecodeEnvelope decodes an `app` message body. It returns (nil, nil) when
// the body has no object-valued `mcpwarp` key (not this layer's message),
// an *UnknownMsg when `v` is not 1 or the op is unknown, and an *Error with
// Code ErrCodeMalformedEnvelope when a known op breaks the §2.8 decode rules.
// Validation covers every list entry, including entries for other service
// names, before any entry is matched by name. The first defect is reported,
// checking fields in §2.3's row order (a services[] entry: name, id, url,
// created).
func DecodeEnvelope(body json.RawMessage) (Envelope, error) {
	top, ok := asObject(body)
	if !ok {
		return nil, nil
	}
	env, ok := asObject(top["mcpwarp"])
	if !ok {
		return nil, nil
	}
	unknown := &UnknownMsg{V: env["v"], Op: env["op"]}
	if !isVersion1(env["v"]) {
		return unknown, nil
	}
	var op string
	if raw, ok := env["op"]; !ok || jsonKind(raw) != kindString || json.Unmarshal(raw, &op) != nil {
		return unknown, nil
	}
	switch op {
	case "registered":
		return decodeRegistered(env)
	case "unregistered":
		svcs, err := objectList(env, "services", "unregistered")
		if err != nil {
			return nil, err
		}
		out := &UnregisteredMsg{Services: make([]UnregisteredService, 0, len(svcs))}
		for _, s := range svcs {
			name, err := reqString(s, "name", "unregistered.services[]")
			if err != nil {
				return nil, err
			}
			id, err := optString(s, "id", "unregistered.services[]", "")
			if err != nil {
				return nil, err
			}
			out.Services = append(out.Services, UnregisteredService{Name: name, ID: id})
		}
		return out, nil
	case "disable":
		id, err := reqString(env, "id", "disable")
		if err != nil {
			return nil, err
		}
		reason, err := optString(env, "reason", "disable", "disabled")
		if err != nil {
			return nil, err
		}
		return &DisableMsg{ID: id, Reason: reason}, nil
	case "enable":
		id, err := reqString(env, "id", "enable")
		if err != nil {
			return nil, err
		}
		name, err := reqString(env, "name", "enable")
		if err != nil {
			return nil, err
		}
		return &EnableMsg{ID: id, Name: name}, nil
	case "conflict":
		id, err := reqString(env, "id", "conflict")
		if err != nil {
			return nil, err
		}
		name, err := reqString(env, "name", "conflict")
		if err != nil {
			return nil, err
		}
		reason, err := optString(env, "reason", "conflict", "taken_over")
		if err != nil {
			return nil, err
		}
		return &ConflictMsg{ID: id, Name: name, Reason: reason}, nil
	case "error":
		code, err := reqString(env, "code", "error")
		if err != nil {
			return nil, err
		}
		msg, err := optString(env, "message", "error", "")
		if err != nil {
			return nil, err
		}
		return &ErrorMsg{Code: AppErrorCode(code), Message: msg}, nil
	default:
		return unknown, nil
	}
}

func decodeRegistered(env map[string]json.RawMessage) (Envelope, error) {
	svcs, err := objectList(env, "services", "registered")
	if err != nil {
		return nil, err
	}
	out := &RegisteredMsg{Services: make([]RegisteredService, 0, len(svcs))}
	for _, s := range svcs {
		name, err := reqString(s, "name", "registered.services[]")
		if err != nil {
			return nil, err
		}
		id, err := reqString(s, "id", "registered.services[]")
		if err != nil {
			return nil, err
		}
		url, ok := stringValue(s["url"])
		if !ok || url == "" {
			return nil, malformed("registered.services[].url must be a non-empty string")
		}
		created := false
		if raw, ok := s["created"]; ok && jsonKind(raw) != kindNull {
			if jsonKind(raw) != kindBool || json.Unmarshal(raw, &created) != nil {
				return nil, malformed("registered.services[].created must be a boolean")
			}
		}
		out.Services = append(out.Services, RegisteredService{Name: name, ID: id, URL: url, Created: created})
	}
	errs, err := objectList(env, "errors", "registered")
	if err != nil {
		return nil, err
	}
	out.Errors = make([]ServiceError, 0, len(errs))
	for _, e := range errs {
		name, err := reqString(e, "name", "registered.errors[]")
		if err != nil {
			return nil, err
		}
		code, err := reqString(e, "code", "registered.errors[]")
		if err != nil {
			return nil, err
		}
		msg, err := optString(e, "message", "registered.errors[]", "")
		if err != nil {
			return nil, err
		}
		id, err := optString(e, "id", "registered.errors[]", "")
		if err != nil {
			return nil, err
		}
		out.Errors = append(out.Errors, ServiceError{Name: name, Code: RegisterErrorCode(code), Message: msg, ID: id})
	}
	return out, nil
}

func malformed(msg string) *Error { return &Error{Code: ErrCodeMalformedEnvelope, Message: msg} }

func reqString(o map[string]json.RawMessage, key, where string) (string, error) {
	s, ok := stringValue(o[key])
	if !ok {
		return "", malformed(where + "." + key + " must be a string")
	}
	return s, nil
}

func optString(o map[string]json.RawMessage, key, where, fallback string) (string, error) {
	raw, present := o[key]
	if !present {
		return fallback, nil
	}
	s, ok := stringValue(raw)
	if !ok {
		return "", malformed(where + "." + key + " must be a string")
	}
	return s, nil
}

// objectList is §2.8 rule 6: absent or null → empty; otherwise an array whose
// every element is an object (not an array, not null).
func objectList(o map[string]json.RawMessage, key, where string) ([]map[string]json.RawMessage, error) {
	raw, present := o[key]
	if !present || jsonKind(raw) == kindNull {
		return nil, nil
	}
	bad := malformed(where + "." + key + " must be an array of objects")
	var items []json.RawMessage
	if jsonKind(raw) != kindArray || json.Unmarshal(raw, &items) != nil {
		return nil, bad
	}
	out := make([]map[string]json.RawMessage, 0, len(items))
	for _, it := range items {
		obj, ok := asObject(it)
		if !ok {
			return nil, bad
		}
		out = append(out, obj)
	}
	return out, nil
}

func stringValue(raw json.RawMessage) (string, bool) {
	if raw == nil || jsonKind(raw) != kindString {
		return "", false
	}
	var s string
	if json.Unmarshal(raw, &s) != nil {
		return "", false
	}
	return s, true
}

func asObject(raw json.RawMessage) (map[string]json.RawMessage, bool) {
	if raw == nil || jsonKind(raw) != kindObject {
		return nil, false
	}
	var m map[string]json.RawMessage
	if json.Unmarshal(raw, &m) != nil {
		return nil, false
	}
	return m, true
}

// isVersion1 is `v === 1`: a JSON number equal to 1 (so 1.0 counts, "1" does not).
func isVersion1(raw json.RawMessage) bool {
	if raw == nil || jsonKind(raw) != kindNumber {
		return false
	}
	var f float64
	return json.Unmarshal(raw, &f) == nil && f == envelopeVersion
}

type jsonValueKind int

const (
	kindInvalid jsonValueKind = iota
	kindNull
	kindBool
	kindNumber
	kindString
	kindArray
	kindObject
)

func jsonKind(raw json.RawMessage) jsonValueKind {
	b := bytes.TrimLeft(raw, " \t\r\n")
	if len(b) == 0 {
		return kindInvalid
	}
	switch b[0] {
	case 'n':
		return kindNull
	case 't', 'f':
		return kindBool
	case '"':
		return kindString
	case '[':
		return kindArray
	case '{':
		return kindObject
	default:
		return kindNumber
	}
}
