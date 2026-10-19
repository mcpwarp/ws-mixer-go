package mcpwarp

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mcpwarp/ws-mixer-go/wsmixer"
)

// spec/fixtures/mcpwarp/bridging/*.json (MCPWARP.md §10.3). Each fixture runs
// against a real agent over a loopback fake edge: the agent is put in
// sdkState the way §10.3 says (online: `registered` for its name; disabled:
// a matching `disable`; closing: close() called while the edge withholds
// `unregistered`), the edge opens one stream and writes `in`, and the stream
// bytes are compared with `out` -- byte-exact for an SDK-generated reply,
// by status line, header set (names case-insensitive, order free) and
// de-chunked body for an app response (spec/README.md "bridging/"). The app
// call is compared with `upstream`. While not online, the edge writes only
// the head first and the body after the reply arrived, so a reply that
// waited for (or read) the body fails. This bridge buffers the request body,
// so `*_reset` (outReset) fixtures, which apply only to a streaming adapter,
// are skipped (§10.3).

type bridgingFixture struct {
	Description string `json:"description"`
	SdkState    string `json:"sdkState"`
	Mode        string `json:"mode"`
	Target      string `json:"target"`
	Limits      struct {
		MaxRequestBodyBytes int64    `json:"maxRequestBodyBytes"`
		TargetTimeoutMs     *float64 `json:"targetTimeoutMs"`
	} `json:"limits"`
	Handler struct {
		Behaviour string `json:"behaviour"`
		Status    int    `json:"status"`
	} `json:"handler"`
	In       string `json:"in"`
	InExpand map[string]struct {
		Repeat string `json:"repeat"`
		Count  int    `json:"count"`
	} `json:"inExpand"`
	InRest   *string             `json:"inRest"`
	Upstream *upstreamCall       `json:"upstream"`
	Out      string              `json:"out"`
	OutReset *struct{ Code int } `json:"outReset"`
}

type upstreamCall struct {
	Method  string            `json:"method"`
	URL     string            `json:"url"`
	Headers map[string]string `json:"headers"`
	Body    *string           `json:"body"`
}

// callRecorder collects what the application received.
type callRecorder struct {
	mu    sync.Mutex
	calls []upstreamCall
}

func (c *callRecorder) add(method, u string, h http.Header, host string, body []byte) {
	headers := map[string]string{}
	for k, vs := range h {
		headers[strings.ToLower(k)] = strings.Join(vs, ", ")
	}
	if host != "" {
		headers["host"] = host
	}
	s := string(body)
	c.mu.Lock()
	c.calls = append(c.calls, upstreamCall{Method: method, URL: u, Headers: headers, Body: &s})
	c.mu.Unlock()
}

func (c *callRecorder) all() []upstreamCall {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]upstreamCall(nil), c.calls...)
}

// hangingTransport records the request the target adapter sends and never
// answers it (behaviour "hang").
type hangingTransport struct{ rec *callRecorder }

func (h hangingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	var body []byte
	if req.Body != nil {
		body, _ = io.ReadAll(req.Body)
	}
	h.rec.add(req.Method, req.URL.String(), req.Header, "", body)
	<-req.Context().Done()
	return nil, req.Context().Err()
}

// echoTransport is the local server for behaviour "echo" in target mode: it
// records the request and answers 200 with its content-type and body and no
// Content-Length.
type echoTransport struct{ rec *callRecorder }

func (e echoTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	var body []byte
	if req.Body != nil {
		body, _ = io.ReadAll(req.Body)
		_ = req.Body.Close()
	}
	e.rec.add(req.Method, req.URL.String(), req.Header, "", body)
	h := http.Header{}
	if ct := req.Header.Get("Content-Type"); ct != "" {
		h.Set("Content-Type", ct)
	}
	return &http.Response{
		Status: "200 OK", StatusCode: http.StatusOK,
		Proto: "HTTP/1.1", ProtoMajor: 1, ProtoMinor: 1,
		Header: h, Body: io.NopCloser(strings.NewReader(string(body))),
		ContentLength: -1, Request: req,
	}, nil
}

func TestBridgingFixtures(t *testing.T) {
	dir := mcpwarpFixtureDir(t, "bridging")
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".json") {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)
	for _, want := range []string{
		"mcp_post_ok", "non_mcp_404", "disabled_503", "body_413", "target_504", "handler_throw_500",
		"head_431", "notification_passthrough", "expect_continue_200",
		"precedence_disabled_unknown_path_404", "precedence_closing_over_cap_503",
	} {
		if !contains(names, want+".json") {
			t.Errorf("the corpus lacks %s.json (§10.3)", want)
		}
	}
	for _, name := range names {
		raw, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		var f bridgingFixture
		if err := json.Unmarshal(raw, &f); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			runBridgingFixture(t, &f)
		})
	}
}

func contains(xs []string, s string) bool {
	for _, x := range xs {
		if x == s {
			return true
		}
	}
	return false
}

func runBridgingFixture(t *testing.T, f *bridgingFixture) {
	if f.OutReset != nil {
		t.Skip("streaming-adapter fixture (outReset): this bridge buffers the request body, so a body over the cap is always a 413 before any response byte; covered by body_413 (§10.3)")
	}
	rec := &callRecorder{}
	edge := newFakeEdge(t)
	a, log := newTestAgent(t, edge.url, func(c *agentConfig) {
		c.maxBodyBytes = f.Limits.MaxRequestBodyBytes
		switch f.Mode {
		case "handler":
			c.handler = fixtureHandler(t, f.Handler.Behaviour, f.Handler.Status, rec)
		case "target":
			var rt http.RoundTripper
			switch f.Handler.Behaviour {
			case "hang":
				rt = hangingTransport{rec: rec}
			case "echo":
				rt = echoTransport{rec: rec}
			default:
				t.Fatalf("target behaviour %q", f.Handler.Behaviour)
			}
			u, err := url.Parse(f.Target)
			if err != nil {
				t.Fatal(err)
			}
			c.handler, c.target, c.transport = nil, u, rt
			if f.Limits.TargetTimeoutMs != nil {
				c.targetTimeout = time.Duration(*f.Limits.TargetTimeoutMs * float64(time.Millisecond))
			}
		default:
			t.Fatalf("mode %q", f.Mode)
		}
	})

	a.start()
	ec := edge.nextConn(t)
	ec.expectRegister(t, "notes")
	closeDone := make(chan struct{})
	switch f.SdkState {
	case "online":
		ec.send(t, registeredNotes)
		waitState(t, a, stateRegistered)
	case "disabled":
		// Before the registration: the disable carries an id only, so it is
		// held until the registered that brings that id (v0.6.3 §2.7).
		ec.send(t, `{"mcpwarp":{"v":1,"op":"disable","id":"srv_notes"}}`)
		ec.send(t, registeredNotes)
		waitState(t, a, stateDisabled)
	case "closing":
		ec.send(t, registeredNotes)
		waitState(t, a, stateRegistered)
		go func() {
			defer close(closeDone)
			a.close(context.Background())
		}()
		if got, want := string(ec.nextApp(t)), string(EncodeUnregister("notes")); got != want {
			t.Fatalf("app = %s, want unregister %s", got, want)
		}
		waitState(t, a, stateClosing)
		// The edge withholds `unregistered` until the request is done.
		defer func() {
			ec.send(t, `{"mcpwarp":{"v":1,"op":"unregistered","services":[{"name":"notes","id":"srv_notes"}]}}`)
			<-closeDone
		}()
	default:
		t.Fatalf("sdkState %q", f.SdkState)
	}

	in := f.In
	for marker, ex := range f.InExpand {
		in = strings.ReplaceAll(in, marker, strings.Repeat(ex.Repeat, ex.Count))
	}
	headLen, headFirst := strings.Index(in, "\r\n\r\n")+4, 0
	if f.SdkState != "online" && headLen >= 4 {
		headFirst = len(f.Out)
	}
	out, endErr := ec.roundTrip(t, []byte(in), headLen, headFirst)
	raw := string(out)

	if strings.HasPrefix(raw, "HTTP/1.1 1") {
		t.Fatalf("a 1xx precedes the response: %q", raw)
	}
	if n := len(regexp.MustCompile(`HTTP/1\.[01] \d{3}`).FindAllString(raw, -1)); n != 1 {
		t.Fatalf("%d status lines in %q, want 1", n, raw)
	}
	if endErr != io.EOF {
		var se *wsmixer.StreamError
		if errors.As(endErr, &se) {
			t.Fatalf("the stream ended with RESET %v (%q), want CLOSE", se.Code, se.Message)
		}
		t.Fatalf("the stream ended with %v, want CLOSE", endErr)
	}

	calls := rec.all()
	if f.Upstream == nil {
		if len(calls) != 0 {
			t.Fatalf("the app was called: %+v", calls)
		}
	} else {
		if len(calls) != 1 {
			t.Fatalf("%d app calls, want 1", len(calls))
		}
		got, want := calls[0], *f.Upstream
		if want.Body == nil {
			got.Body = nil
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("app call\n got %s\nwant %s", jsonOf(got), jsonOf(want))
		}
	}

	if fromApp := f.Upstream != nil && f.Handler.Behaviour == "echo"; !fromApp {
		if raw != f.Out {
			t.Fatalf("SDK reply\n got %q\nwant %q", raw, f.Out)
		}
	} else {
		got, want := parseHTTPResponse(t, raw), parseHTTPResponse(t, f.Out)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("app response\n got %+v\nwant %+v", got, want)
		}
	}
	badStatus := f.Handler.Behaviour == "status" && (f.Handler.Status < 200 || f.Handler.Status >= 600)
	if f.Handler.Behaviour == "throw" || badStatus {
		var handlerErrs int
		for _, ev := range log.of(evError) {
			if ev.origin == "" {
				handlerErrs++
			}
		}
		if handlerErrs != 1 {
			t.Fatalf("%d handler error events, want 1", handlerErrs)
		}
	}
}

// fixtureHandler is the fixture's app as an http.Handler: echo answers 200
// with the request's content-type and body and no length; throw panics;
// status answers that status with an empty body.
func fixtureHandler(t *testing.T, behaviour string, status int, rec *callRecorder) http.Handler {
	switch behaviour {
	case "echo":
		return echoHandler(func(r *http.Request, body []byte) { rec.add(r.Method, r.URL.String(), r.Header, r.Host, body) })
	case "status":
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			body, _ := io.ReadAll(r.Body)
			rec.add(r.Method, r.URL.String(), r.Header, r.Host, body)
			w.WriteHeader(status)
		})
	case "throw":
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			body, _ := io.ReadAll(r.Body)
			rec.add(r.Method, r.URL.String(), r.Header, r.Host, body)
			panic("fixture handler throws")
		})
	}
	t.Fatalf("handler behaviour %q", behaviour)
	return nil
}

func jsonOf(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}

type parsedResponse struct {
	StatusLine string
	Headers    []string // "name: value", names lower-cased, sorted
	Body       string
}

// parseHTTPResponse splits a raw response and de-chunks a chunked body.
func parseHTTPResponse(t *testing.T, raw string) parsedResponse {
	t.Helper()
	end := strings.Index(raw, "\r\n\r\n")
	if end < 0 {
		t.Fatalf("no end of head in %q", raw)
	}
	lines := strings.Split(raw[:end], "\r\n")
	p := parsedResponse{StatusLine: lines[0]}
	chunked := false
	for _, l := range lines[1:] {
		i := strings.IndexByte(l, ':')
		name, value := strings.ToLower(strings.TrimSpace(l[:i])), strings.TrimSpace(l[i+1:])
		if name == "transfer-encoding" && strings.EqualFold(value, "chunked") {
			chunked = true
		}
		p.Headers = append(p.Headers, name+": "+value)
	}
	sort.Strings(p.Headers)
	rest := raw[end+4:]
	if !chunked {
		p.Body = rest
		return p
	}
	var body strings.Builder
	for {
		i := strings.Index(rest, "\r\n")
		if i < 0 {
			t.Fatalf("truncated chunked body in %q", raw)
		}
		size, err := strconv.ParseInt(strings.SplitN(rest[:i], ";", 2)[0], 16, 64)
		if err != nil {
			t.Fatalf("bad chunk size in %q", raw)
		}
		rest = rest[i+2:]
		if size == 0 {
			if rest != "\r\n" {
				t.Fatalf("bytes after the last chunk: %q", rest)
			}
			break
		}
		if int64(len(rest)) < size+2 || rest[size:size+2] != "\r\n" {
			t.Fatalf("bad chunk framing in %q", raw)
		}
		body.WriteString(rest[:size])
		rest = rest[size+2:]
	}
	p.Body = body.String()
	return p
}
