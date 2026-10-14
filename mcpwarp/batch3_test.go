package mcpwarp

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mcpwarp/ws-mixer-go/wsmixer"
)

// ctl is a control byte, kept out of the source as a literal.
var ctl = string([]byte{0x01})

// §4.5: a request net/http cannot carry is the 400 unrepresentable reply in
// target mode as in handler mode, never a 502.
func TestBridgeUnrepresentable400(t *testing.T) {
	var called atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { called.Add(1) }))
	t.Cleanup(target.Close)
	u, _ := url.Parse(target.URL + "/mcp")
	want := reply(400, "request cannot be represented as a web Request")
	for _, mode := range []string{"target", "handler"} {
		t.Run(mode, func(t *testing.T) {
			ec := bridgeAgent(t, stateRegistered, func(c *agentConfig) {
				if mode == "target" {
					c.handler, c.target = nil, u
				} else {
					c.handler = http.HandlerFunc(func(http.ResponseWriter, *http.Request) { called.Add(1) })
				}
			})
			for _, in := range []string{
				"GET /mcp HTTP/1.1\r\nHost: h\r\nX-Bad: a" + ctl + "b\r\n\r\n",
				"GET /mcp HTTP/1.1\r\nHost: a" + ctl + "b\r\n\r\n",
			} {
				out, err := ec.roundTrip(t, []byte(in), 0, 0)
				if err != io.EOF || string(out) != want {
					t.Fatalf("got %q / %v\nwant %q", out, err, want)
				}
			}
		})
	}
	if called.Load() != 0 {
		t.Fatalf("the app was called %d times", called.Load())
	}
}

// targetQueryOf sends GET <raw> in target mode and returns the query the
// local server received.
func targetQueryOf(t *testing.T, raw string) string {
	t.Helper()
	got := make(chan string, 1)
	target := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) { got <- r.URL.RawQuery }))
	t.Cleanup(target.Close)
	u, _ := url.Parse(target.URL + "/mcp")
	ec := bridgeAgent(t, stateRegistered, func(c *agentConfig) { c.handler, c.target = nil, u })
	if _, err := ec.roundTrip(t, []byte("GET "+raw+" HTTP/1.1\r\nHost: h\r\n\r\n"), 0, 0); err != io.EOF {
		t.Fatal(err)
	}
	return <-got
}

// v0.6.3 §4.3: a lone '%' (no two hex digits after it) is forwarded as is,
// never rewritten to %25 (fixture target_lone_percent).
func TestBridgeTargetLonePercentKept(t *testing.T) {
	if q := targetQueryOf(t, "/mcp?s=%zz&u=50%"); q != "s=%zz&u=50%" {
		t.Fatalf("query %q, want s=%%zz&u=50%% unchanged", q)
	}
}

// v0.6.3 §4.3: '#' starts a fragment, dropped with everything after it
// (fixture target_fragment_dropped).
func TestBridgeTargetFragmentDropped(t *testing.T) {
	if q := targetQueryOf(t, "/mcp?a=1#x&b=2"); q != "a=1" {
		t.Fatalf("query %q, want a=1", q)
	}
}

// The forwarded target is percent-encoded from the received bytes exactly
// once: raw bytes encoded, valid escapes kept, never re-encoded.
func TestBridgeTargetEscapedOnce(t *testing.T) {
	const raw = "/mcp?q=a%20b&r=\xc3\xa4&t=<x>"
	const wantQuery = "q=a%20b&r=%C3%A4&t=%3Cx%3E"
	got := make(chan string, 2)
	target := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) { got <- r.URL.RawQuery }))
	t.Cleanup(target.Close)
	u, _ := url.Parse(target.URL + "/mcp")
	ec := bridgeAgent(t, stateRegistered, func(c *agentConfig) { c.handler, c.target = nil, u })
	if _, err := ec.roundTrip(t, []byte("GET "+raw+" HTTP/1.1\r\nHost: h\r\n\r\n"), 0, 0); err != io.EOF {
		t.Fatal(err)
	}
	if q := <-got; q != wantQuery {
		t.Fatalf("target query %q, want %q", q, wantQuery)
	}

	reqs := make(chan *http.Request, 1)
	ec2 := bridgeAgent(t, stateRegistered, func(c *agentConfig) {
		c.handler = http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) { reqs <- r })
	})
	if _, err := ec2.roundTrip(t, []byte("GET "+raw+" HTTP/1.1\r\nHost: h\r\n\r\n"), 0, 0); err != io.EOF {
		t.Fatal(err)
	}
	r := <-reqs
	if r.URL.RawQuery != wantQuery || r.RequestURI != raw {
		t.Fatalf("handler URL query %q RequestURI %q", r.URL.RawQuery, r.RequestURI)
	}
}

// A final status outside 200-599, from a handler or a target, is the 500
// handler-error reply (and an error event).
func TestBridgeStatusOutOfRange500(t *testing.T) {
	want := reply(500, "handler error")
	edge := newFakeEdge(t)
	a, log := newTestAgent(t, edge.url, func(c *agentConfig) {
		c.handler = http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(600)
			_, _ = w.Write([]byte("ignored"))
		})
	})
	ec := startRegistered(t, edge, a)
	out, err := ec.roundTrip(t, []byte("GET /mcp HTTP/1.1\r\nHost: h\r\n\r\n"), 0, 0)
	if err != io.EOF || string(out) != want {
		t.Fatalf("handler 600: got %q / %v", out, err)
	}
	if n := len(log.of(evError)); n != 1 {
		t.Fatalf("%d error events, want 1", n)
	}

	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(799) }))
	t.Cleanup(target.Close)
	u, _ := url.Parse(target.URL + "/mcp")
	ec2 := bridgeAgent(t, stateRegistered, func(c *agentConfig) { c.handler, c.target = nil, u })
	if out, err := ec2.roundTrip(t, []byte("GET /mcp HTTP/1.1\r\nHost: h\r\n\r\n"), 0, 0); err != io.EOF || string(out) != want {
		t.Fatalf("target 799: got %q / %v", out, err)
	}
}

// v0.6.3 §4: once the request line parsed as HEAD, every SDK-generated
// reply is head-only (Content-Length of the body it would have had), head
// parse errors included. No request line: the reply keeps its body.
func TestBridgeHeadRepliesHeadOnly(t *testing.T) {
	head := func(status int, msg string) string { return string(errorReply(status, msg, http.MethodHead)) }
	cases := []struct {
		name  string
		state exposeState
		in    string
		want  string
	}{
		{"404", stateRegistered, "HEAD /x HTTP/1.1\r\nHost: h\r\n\r\n", head(404, "not_found")},
		{"503", stateDisabled, "HEAD /mcp HTTP/1.1\r\nHost: h\r\n\r\n", head(503, "service disabled")},
		{"413", stateRegistered, "HEAD /mcp HTTP/1.1\r\nHost: h\r\nContent-Length: 95\r\n\r\n" + strings.Repeat("x", 95), head(413, "request body too large")},
		{"400 bad head", stateRegistered, "HEAD /mcp HTTP/1.1\r\n\r\n", head(400, "missing or duplicate Host header")},
		{"400 incomplete", stateRegistered, "HEAD /mcp HTTP/1.1\r\nHost: h\r\n", head(400, "incomplete request")},
		{"431", stateRegistered, "HEAD /mcp HTTP/1.1\r\nHost: h\r\nx: " + strings.Repeat("a", 70000) + "\r\n\r\n", head(431, "request head too large")},
		{"no request line", stateRegistered, "HEAD /mcp HTTP/2\r\nHost: h\r\n\r\n", reply(400, "malformed request line")},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ec := bridgeAgent(t, c.state, func(a *agentConfig) { a.maxBodyBytes = 16 })
			out, err := ec.roundTrip(t, []byte(c.in), 0, 0)
			if err != io.EOF || string(out) != c.want {
				t.Fatalf("got %q / %v\nwant %q", out, err, c.want)
			}
		})
	}
}

// §4.2: the 64 KiB head cap counts the terminating blank line.
func TestReadHeadCapCountsBlankLine(t *testing.T) {
	head := func(total int) string {
		base := "GET /mcp HTTP/1.1\r\nHost: h\r\nx: "
		return base + strings.Repeat("a", total-len(base)-4) + "\r\n\r\n"
	}
	if _, err := readRequestHead(readerOf(head(maxHeadBytes))); err != nil {
		t.Fatalf("a head of exactly %d bytes, blank line included: %v", maxHeadBytes, err)
	}
	_, err := readRequestHead(readerOf(head(maxHeadBytes + 1)))
	if he, ok := err.(*httpError); !ok || he.status != 431 {
		t.Fatalf("a head of %d bytes: %v, want 431", maxHeadBytes+1, err)
	}
}

// ExposeOptions.Logger reaches the ws-mixer client.
func TestExposeLoggerPassedToCore(t *testing.T) {
	logs := &logCapture{}
	edge := newFakeEdge(t)
	o := exposeOpts(edge.url)
	o.Logger = slog.New(logs)
	res := exposeAsync(context.Background(), o)
	ec := edge.nextConn(t)
	ec.expectRegister(t, "notes")
	ec.send(t, registeredNotes)
	h := await(t, res).h
	defer h.Close(context.Background())
	_ = ec.c.Close(uint32(wsmixer.InternalErrorCode), "edge restart")
	deadline := time.Now().Add(5 * time.Second)
	for !logs.has(slog.LevelError, "peer reported a connection error") {
		if time.Now().After(deadline) {
			t.Fatalf("the core's log line did not reach ExposeOptions.Logger: %q", logs.lines())
		}
		time.Sleep(10 * time.Millisecond)
	}
}
