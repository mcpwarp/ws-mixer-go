package mcpwarp

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// spec/fixtures/mcpwarp/envelope/{valid,invalid}/*.json (MCPWARP.md §10.2),
// from the agent's side: an edge→agent op decodes with the §2.3 defaults,
// fails with MALFORMED_ENVELOPE (message_contains is a substring of the
// error message), or decodes as unknown (UNSUPPORTED_VERSION / UNKNOWN_OP);
// a valid register/unregister equals the encoder's output byte for byte.
// Agent→edge cases whose verdict is the edge's are checked as far as the
// agent can (its own name rule, the decoder ignoring them); SCHEMA_* hints
// for agent→edge ops are skipped. schema_valid is not re-checked here: there
// is no JSON Schema validator in this module; the spec repo's
// check-fixtures.mjs holds that verdict.

type envelopeFixture struct {
	Description string          `json:"description"`
	Op          string          `json:"op"`
	Message     json.RawMessage `json:"message"`
	SchemaValid bool            `json:"schema_valid"`
	WireValid   bool            `json:"wire_valid"`
	Expect      *struct {
		ErrorCode       string `json:"error_code"`
		MessageContains string `json:"message_contains"`
	} `json:"expect"`
}

func loadEnvelopeFixtures(t *testing.T, kind string) map[string]envelopeFixture {
	t.Helper()
	dir := filepath.Join(mcpwarpFixtureDir(t, "envelope"), kind)
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]envelopeFixture{}
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		var f envelopeFixture
		if err := json.Unmarshal(raw, &f); err != nil {
			t.Fatalf("%s: %v", e.Name(), err)
		}
		out[e.Name()] = f
	}
	if len(out) == 0 {
		t.Fatalf("no fixtures in %s", dir)
	}
	return out
}

func sortedNames(m map[string]envelopeFixture) []string {
	names := make([]string, 0, len(m))
	for n := range m {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

func isAgentToEdge(op string) bool { return op == "register" || op == "unregister" }

func decodeFixtureMessage(msg json.RawMessage) (Envelope, error) {
	return DecodeEnvelope(json.RawMessage(`{"mcpwarp":` + string(msg) + `}`))
}

// expectedEnvelope is what §2.3's defaults make of a well-formed edge→agent message.
func expectedEnvelope(t *testing.T, msg json.RawMessage) Envelope {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(msg, &m); err != nil {
		t.Fatal(err)
	}
	str := func(o map[string]any, k, def string) string {
		if v, ok := o[k].(string); ok {
			return v
		}
		return def
	}
	list := func(k string) []map[string]any {
		arr, _ := m[k].([]any)
		out := []map[string]any{}
		for _, it := range arr {
			out = append(out, it.(map[string]any))
		}
		return out
	}
	switch m["op"] {
	case "registered":
		r := &RegisteredMsg{Services: []RegisteredService{}, Errors: []ServiceError{}}
		for _, s := range list("services") {
			created, _ := s["created"].(bool)
			r.Services = append(r.Services, RegisteredService{Name: str(s, "name", ""), ID: str(s, "id", ""), URL: str(s, "url", ""), Created: created})
		}
		for _, e := range list("errors") {
			r.Errors = append(r.Errors, ServiceError{Name: str(e, "name", ""), Code: RegisterErrorCode(str(e, "code", "")), Message: str(e, "message", ""), ID: str(e, "id", "")})
		}
		return r
	case "unregistered":
		u := &UnregisteredMsg{Services: []UnregisteredService{}}
		for _, s := range list("services") {
			u.Services = append(u.Services, UnregisteredService{Name: str(s, "name", ""), ID: str(s, "id", "")})
		}
		return u
	case "disable":
		return &DisableMsg{ID: str(m, "id", ""), Reason: str(m, "reason", "disabled")}
	case "enable":
		return &EnableMsg{ID: str(m, "id", ""), Name: str(m, "name", "")}
	case "conflict":
		return &ConflictMsg{ID: str(m, "id", ""), Name: str(m, "name", ""), Reason: str(m, "reason", "taken_over")}
	case "error":
		return &ErrorMsg{Code: AppErrorCode(str(m, "code", "")), Message: str(m, "message", "")}
	}
	t.Fatalf("no expected decode for op %v", m["op"])
	return nil
}

func compactJSON(t *testing.T, raw json.RawMessage) string {
	t.Helper()
	var b bytes.Buffer
	if err := json.Compact(&b, raw); err != nil {
		t.Fatal(err)
	}
	return b.String()
}

type fixtureService struct {
	Name string      `json:"name"`
	Kind ServiceKind `json:"kind"`
}

// encodeFixture encodes svcs with the list encoders, checking that a single
// entry encodes the same through EncodeRegister/EncodeUnregister.
func encodeFixture(t *testing.T, op string, svcs []fixtureService) string {
	t.Helper()
	if op == "register" {
		entries := make([]RegisterEntry, len(svcs))
		for i, s := range svcs {
			entries[i] = RegisterEntry{Name: s.Name, Kind: s.Kind}
		}
		enc := EncodeRegisterList(entries)
		if len(svcs) == 1 && string(EncodeRegister(svcs[0].Name, svcs[0].Kind)) != string(enc) {
			t.Fatal("EncodeRegister and EncodeRegisterList disagree on one entry")
		}
		return string(enc)
	}
	names := make([]string, len(svcs))
	for i, s := range svcs {
		names[i] = s.Name
	}
	enc := EncodeUnregisterList(names)
	if len(svcs) == 1 && string(EncodeUnregister(svcs[0].Name)) != string(enc) {
		t.Fatal("EncodeUnregister and EncodeUnregisterList disagree on one entry")
	}
	return string(enc)
}

// The multi-entry register fixtures, whose encode side is host guidance in
// the fixture runner, as tests of EncodeRegisterList.
func TestEncodeRegisterListBatchFixtures(t *testing.T) {
	fixtures := loadEnvelopeFixtures(t, "valid")
	for _, name := range []string{"register_batch_valid.json", "register_two_services.json"} {
		f, ok := fixtures[name]
		if !ok {
			t.Fatalf("no envelope/valid/%s", name)
		}
		svcs := allServices(t, f.Message)
		if got, want := encodeFixture(t, f.Op, svcs), `{"mcpwarp":`+compactJSON(t, f.Message)+`}`; got != want {
			t.Fatalf("%s: EncodeRegisterList\n got %s\nwant %s", name, got, want)
		}
	}
}

func allServices(t *testing.T, msg json.RawMessage) []fixtureService {
	t.Helper()
	var m struct {
		Services []fixtureService `json:"services"`
	}
	if err := json.Unmarshal(msg, &m); err != nil || len(m.Services) == 0 {
		t.Fatalf("no services in %s", msg)
	}
	return m.Services
}

func firstService(t *testing.T, msg json.RawMessage) fixtureService {
	t.Helper()
	var m struct {
		Services []json.RawMessage `json:"services"`
	}
	if err := json.Unmarshal(msg, &m); err != nil || len(m.Services) == 0 {
		t.Fatalf("no services[0] in %s", msg)
	}
	var s fixtureService
	_ = json.Unmarshal(m.Services[0], &s) // a non-string name decodes as ""
	return s
}

func requireUnknown(t *testing.T, msg json.RawMessage) {
	t.Helper()
	env, err := decodeFixtureMessage(msg)
	if err != nil {
		t.Fatalf("decode: %v, want an ignored unknown envelope", err)
	}
	u, ok := env.(*UnknownMsg)
	if !ok {
		t.Fatalf("decoded %T, want *UnknownMsg", env)
	}
	var m map[string]json.RawMessage
	_ = json.Unmarshal(msg, &m)
	if string(u.V) != string(m["v"]) || string(u.Op) != string(m["op"]) {
		t.Fatalf("UnknownMsg{V:%s, Op:%s}, want v=%s op=%s", u.V, u.Op, m["v"], m["op"])
	}
}

func TestEnvelopeFixturesValid(t *testing.T) {
	fixtures := loadEnvelopeFixtures(t, "valid")
	for _, name := range sortedNames(fixtures) {
		f := fixtures[name]
		t.Run(name, func(t *testing.T) {
			if !f.SchemaValid || !f.WireValid {
				t.Fatalf("valid fixture with schema_valid=%v wire_valid=%v", f.SchemaValid, f.WireValid)
			}
			if isAgentToEdge(f.Op) {
				svcs := allServices(t, f.Message)
				// The encode side of a multi-entry register is host guidance
				// (spec v0.8.0 §10.6); TestEncodeRegisterListBatchFixtures
				// keeps it as a test of EncodeRegisterList.
				if len(svcs) == 1 {
					if got, want := encodeFixture(t, f.Op, svcs), `{"mcpwarp":`+compactJSON(t, f.Message)+`}`; got != want {
						t.Fatalf("encoder output\n got %s\nwant %s", got, want)
					}
				}
				if len(svcs) > MaxServicesPerMessage {
					t.Fatalf("%d entries in a valid fixture, over MaxServicesPerMessage", len(svcs))
				}
				for _, svc := range svcs {
					if !IsValidServiceName(svc.Name) {
						t.Fatalf("IsValidServiceName(%q) = false", svc.Name)
					}
				}
				// The agent's own decoder does not know its outbound ops.
				requireUnknown(t, f.Message)
				return
			}
			env, err := decodeFixtureMessage(f.Message)
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			if want := expectedEnvelope(t, f.Message); !reflect.DeepEqual(env, want) {
				t.Fatalf("decoded %#v\nwant %#v", env, want)
			}
		})
	}
}

func TestEnvelopeFixturesInvalid(t *testing.T) {
	fixtures := loadEnvelopeFixtures(t, "invalid")
	for _, name := range sortedNames(fixtures) {
		f := fixtures[name]
		t.Run(name, func(t *testing.T) {
			if f.SchemaValid || f.Expect == nil {
				t.Fatalf("invalid fixture with schema_valid=%v expect=%v", f.SchemaValid, f.Expect)
			}
			code := f.Expect.ErrorCode
			if isAgentToEdge(f.Op) {
				switch {
				case code == "INVALID_NAME":
					if svc := firstService(t, f.Message); IsValidServiceName(svc.Name) {
						t.Fatalf("IsValidServiceName(%q) = true, want false", svc.Name)
					}
				case code == "UNSUPPORTED_VERSION" || code == "UNKNOWN_OP":
					requireUnknown(t, f.Message)
				case code == string(AppTooManyServices):
					// The edge's whole-frame answer to more than
					// MaxServicesPerMessage entries, which no agent may send.
					if n := len(allServices(t, f.Message)); n <= MaxServicesPerMessage {
						t.Fatalf("%d entries, want more than MaxServicesPerMessage", n)
					}
				case strings.HasPrefix(code, "SCHEMA_"):
					t.Skipf("agent→edge %s: the edge's verdict, not the agent's (§10.2)", code)
				default:
					t.Fatalf("unexpected agent→edge error_code %s", code)
				}
				return
			}
			if f.WireValid {
				if !strings.HasPrefix(code, "SCHEMA_") {
					t.Fatalf("wire_valid fixture with error_code %s", code)
				}
				env, err := decodeFixtureMessage(f.Message)
				if err != nil {
					t.Fatalf("decode: %v", err)
				}
				if want := expectedEnvelope(t, f.Message); !reflect.DeepEqual(env, want) {
					t.Fatalf("decoded %#v\nwant %#v", env, want)
				}
				return
			}
			if code == "UNSUPPORTED_VERSION" || code == "UNKNOWN_OP" {
				requireUnknown(t, f.Message)
				return
			}
			if code != ErrCodeMalformedEnvelope {
				t.Fatalf("unexpected edge→agent error_code %s", code)
			}
			env, err := decodeFixtureMessage(f.Message)
			var e *Error
			if !errors.As(err, &e) || e.Code != ErrCodeMalformedEnvelope {
				t.Fatalf("decode = (%#v, %v), want a MALFORMED_ENVELOPE *Error", env, err)
			}
			if e.Fatal {
				t.Fatal("MALFORMED_ENVELOPE must be non-fatal")
			}
			if !strings.Contains(e.Message, f.Expect.MessageContains) {
				t.Fatalf("message %q does not contain %q", e.Message, f.Expect.MessageContains)
			}
		})
	}
}

func TestDecodeEnvelopeNotOurs(t *testing.T) {
	for _, body := range []string{`{}`, `{"mcpwarp":null}`, `{"mcpwarp":[1]}`, `{"mcpwarp":"x"}`, `{"other":{"v":1}}`, `[1]`, `"s"`} {
		env, err := DecodeEnvelope(json.RawMessage(body))
		if env != nil || err != nil {
			t.Errorf("DecodeEnvelope(%s) = (%#v, %v), want (nil, nil)", body, env, err)
		}
	}
}

// IsTransientRegisterError is exactly §2.5's transient set, read from the
// pinned spec's table (the rows marked ✔ in its T column): hosts key their
// per-name re-sends on it, so it must not drift from the spec silently.
func TestIsTransientRegisterErrorSet(t *testing.T) {
	want := []RegisterErrorCode{RegisterInternal, RegisterNoResponders, RegisterTimeout, RegisterAborted,
		RegisterConnectionLost, RegisterMalformedReply, RegisterUnavailable, RegisterServiceError}
	doc := filepath.Join(mcpwarpFixtureDir(t, "envelope"), "..", "..", "..", "..", "docs", "MCPWARP.md")
	b, err := os.ReadFile(doc)
	if err != nil {
		t.Fatal(err)
	}
	_, sec, ok := strings.Cut(string(b), "### 2.5 ")
	if !ok {
		t.Fatal("no §2.5 in MCPWARP.md")
	}
	sec, _, _ = strings.Cut(sec, "\n### ")
	codeRE := regexp.MustCompile("`([A-Z_]+)`")
	var transient, refusals []RegisterErrorCode
	for _, line := range strings.Split(sec, "\n") {
		cols := strings.Split(line, "|")
		if len(cols) < 4 || !strings.Contains(cols[1], "`") {
			continue
		}
		for _, m := range codeRE.FindAllStringSubmatch(cols[1], -1) {
			if strings.TrimSpace(cols[2]) == "✔" {
				transient = append(transient, RegisterErrorCode(m[1]))
			} else {
				refusals = append(refusals, RegisterErrorCode(m[1]))
			}
		}
	}
	if !reflect.DeepEqual(transient, want) {
		t.Fatalf("§2.5 transient set %v, this SDK's %v", transient, want)
	}
	for _, c := range want {
		if !IsTransientRegisterError(c) {
			t.Errorf("IsTransientRegisterError(%s) = false", c)
		}
	}
	for _, c := range append(refusals, "SOME_FUTURE_CODE", "") {
		if IsTransientRegisterError(c) {
			t.Errorf("IsTransientRegisterError(%q) = true, want a refusal", c)
		}
	}
	if len(refusals) == 0 {
		t.Fatal("no refusal rows parsed from §2.5")
	}
}

func TestEncodeRegisterByteExact(t *testing.T) {
	if got, want := string(EncodeRegister("anki", KindHTTP)), `{"mcpwarp":{"v":1,"op":"register","services":[{"name":"anki","kind":"http"}]}}`; got != want {
		t.Errorf("EncodeRegister = %s, want %s", got, want)
	}
	if got, want := string(EncodeUnregister("anki")), `{"mcpwarp":{"v":1,"op":"unregister","services":[{"name":"anki"}]}}`; got != want {
		t.Errorf("EncodeUnregister = %s, want %s", got, want)
	}
}
