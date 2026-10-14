package mcpwarp

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/mcpwarp/ws-mixer-go/wsmixer"
)

func reply(status int, msg string) string { return string(errorReply(status, msg, http.MethodGet)) }

// bridgeAgent starts an agent in state (registered or disabled) with mod
// applied and returns its edge connection.
func bridgeAgent(t *testing.T, state exposeState, mod func(*agentConfig)) *edgeConn {
	t.Helper()
	edge := newFakeEdge(t)
	a, _ := newTestAgent(t, edge.url, mod)
	ec := startRegistered(t, edge, a)
	if state == stateDisabled {
		ec.send(t, `{"mcpwarp":{"v":1,"op":"disable","id":"srv_notes","reason":"disabled"}}`)
		waitState(t, a, stateDisabled)
	}
	return ec
}

// §4.4: 431 > 404 > 503 > 413 > handler, each answered without reading a
// body that will be refused.
func TestBridgePrecedence(t *testing.T) {
	overCap := strings.Repeat("x", 95)
	bigHead := "x-padding: " + strings.Repeat("a", 70000) + "\r\n"
	cases := []struct {
		name  string
		state exposeState
		in    string
		want  string
	}{
		{"431 beats 404", stateRegistered, "GET /healthz HTTP/1.1\r\nHost: h\r\n" + bigHead + "\r\n", reply(431, "request head too large")},
		{"431 beats 503", stateDisabled, "GET /mcp HTTP/1.1\r\nHost: h\r\n" + bigHead + "\r\n", reply(431, "request head too large")},
		{"404 beats 503 and 413", stateDisabled, "POST /healthz HTTP/1.1\r\nHost: h\r\nContent-Length: 95\r\n\r\n" + overCap, reply(404, "not_found")},
		{"404 for /mcp/", stateRegistered, "GET /mcp/ HTTP/1.1\r\nHost: h\r\n\r\n", reply(404, "not_found")},
		{"404 for /MCP", stateRegistered, "GET /MCP HTTP/1.1\r\nHost: h\r\n\r\n", reply(404, "not_found")},
		{"404 for /a/../mcp", stateRegistered, "GET /a/../mcp HTTP/1.1\r\nHost: h\r\n\r\n", reply(404, "not_found")},
		{"503 beats 413", stateDisabled, "POST /mcp HTTP/1.1\r\nHost: h\r\nContent-Length: 95\r\n\r\n" + overCap, reply(503, "service disabled")},
		{"413 by Content-Length", stateRegistered, "POST /mcp HTTP/1.1\r\nHost: h\r\nContent-Length: 95\r\n\r\n" + overCap, reply(413, "request body too large")},
		{"413 chunked, cumulative", stateRegistered, "POST /mcp HTTP/1.1\r\nHost: h\r\nTransfer-Encoding: chunked\r\n\r\na\r\n0123456789\r\na\r\n0123456789\r\n0\r\n\r\n", reply(413, "request body too large")},
		{"400 before the body", stateRegistered, "POST /mcp HTTP/1.1\r\nHost: h\r\nContent-Length: 1\r\nTransfer-Encoding: chunked\r\n\r\n", reply(400, "both Transfer-Encoding and Content-Length")},
		{"400 missing Host", stateRegistered, "GET /mcp HTTP/1.1\r\n\r\n", reply(400, "missing or duplicate Host header")},
		{"400 incomplete", stateRegistered, "GET /mcp HTTP/1.1\r\nHost: h\r\n", reply(400, "incomplete request")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			called := false
			ec := bridgeAgent(t, tc.state, func(c *agentConfig) {
				c.maxBodyBytes = 16
				c.handler = http.HandlerFunc(func(http.ResponseWriter, *http.Request) { called = true })
			})
			out, err := ec.roundTrip(t, []byte(tc.in), 0, 0)
			if err != io.EOF {
				t.Fatalf("stream ended with %v, want CLOSE", err)
			}
			if string(out) != tc.want {
				t.Fatalf("got %q\nwant %q", out, tc.want)
			}
			if called {
				t.Fatal("the handler was called")
			}
		})
	}
}

// §4.5: no response headers within targetTimeout → 504, and the upstream
// request is cancelled.
func TestBridgeTargetTimeout504(t *testing.T) {
	cancelled := make(chan struct{})
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// net/http notices the client going away only once the body is consumed.
		_, _ = io.ReadAll(r.Body)
		<-r.Context().Done()
		close(cancelled)
	}))
	t.Cleanup(target.Close)
	u, _ := url.Parse(target.URL + "/mcp")
	ec := bridgeAgent(t, stateRegistered, func(c *agentConfig) {
		c.handler, c.target, c.targetTimeout = nil, u, 200*time.Millisecond
	})
	start := time.Now()
	out, err := ec.roundTrip(t, []byte("POST /mcp HTTP/1.1\r\nHost: h\r\nContent-Length: 2\r\n\r\n{}"), 0, 0)
	el := time.Since(start)
	if err != io.EOF || string(out) != reply(504, "local server timed out") {
		t.Fatalf("got %q / %v", out, err)
	}
	if el < 200*time.Millisecond || el > 2*time.Second {
		t.Fatalf("504 after %v, want about 200ms", el)
	}
	select {
	case <-cancelled:
	case <-time.After(2 * time.Second):
		t.Fatal("the upstream request was not cancelled")
	}
}

// §4.6 Request → target URL and Response → edge through net/http.
func TestBridgeTargetPassThrough(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if r.URL.RequestURI() != "/mcp?k=v&x=1" || r.Header.Get("X-Forwarded-Host") != "notes-tester.mcpwarp.test" ||
			r.Header.Get("X-Forwarded-Proto") != "https" || r.Header.Get("Accept-Encoding") != "identity" ||
			r.Header.Get("Mcp-Session-Id") != "abc" || r.Header.Get("Expect") != "" || r.Header.Get("Te") != "" ||
			r.Header.Get("X-Dropped") != "" || strings.HasPrefix(r.Host, "notes-tester") || string(body) != "{}" {
			http.Error(w, "bad upstream request: "+r.URL.RequestURI()+" "+r.Host, http.StatusTeapot)
			return
		}
		w.Header().Set("Mcp-Session-Id", "abc")
		w.Header().Add("Set-Cookie", "a=1")
		w.Header().Add("Set-Cookie", "b=2")
		w.Header().Set("Content-Length", "11")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	t.Cleanup(target.Close)
	u, _ := url.Parse(target.URL + "/mcp?k=v")
	ec := bridgeAgent(t, stateRegistered, func(c *agentConfig) { c.handler, c.target = nil, u })
	in := "POST /mcp?x=1 HTTP/1.1\r\nHost: notes-tester.mcpwarp.test\r\nAccept-Encoding: gzip\r\nConnection: x-dropped\r\nX-Dropped: 1\r\nTE: trailers\r\n" +
		"Expect: 100-continue\r\nMcp-Session-Id: abc\r\nContent-Length: 2\r\n\r\n{}"
	out, err := ec.roundTrip(t, []byte(in), 0, 0)
	if err != io.EOF {
		t.Fatalf("stream ended with %v", err)
	}
	got := parseHTTPResponse(t, string(out))
	if got.StatusLine != "HTTP/1.1 201 Created" || got.Body != `{"ok":true}` {
		t.Fatalf("response %+v", got)
	}
	for _, h := range []string{"content-length: 11", "mcp-session-id: abc", "set-cookie: a=1", "set-cookie: b=2"} {
		if !contains(got.Headers, h) {
			t.Fatalf("response headers %q lack %q", got.Headers, h)
		}
	}
}

func TestBridgeTargetUnreachable502(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	u, _ := url.Parse("http://" + addr + "/mcp")
	ec := bridgeAgent(t, stateRegistered, func(c *agentConfig) { c.handler, c.target = nil, u })
	out, err := ec.roundTrip(t, []byte("GET /mcp HTTP/1.1\r\nHost: h\r\n\r\n"), 0, 0)
	if err != io.EOF || string(out) != reply(502, "local server unreachable (ECONNREFUSED)") {
		t.Fatalf("got %q / %v", out, err)
	}
}

// §4.2 Head deadline: RESET CANCEL, no response.
func TestBridgeHeadDeadline(t *testing.T) {
	ec := bridgeAgent(t, stateRegistered, func(c *agentConfig) { c.headDeadline = 100 * time.Millisecond })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	st, err := ec.c.OpenStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Write([]byte("POST /mcp HTTP/1.1\r\nHost: h\r\n")); err != nil {
		t.Fatal(err)
	}
	out, err := io.ReadAll(readerCtx{ctx, st})
	var se *wsmixer.StreamError
	if len(out) != 0 || !errors.As(err, &se) || se.Code != wsmixer.CancelCode || se.Message != "request head deadline exceeded" {
		t.Fatalf("got %q / %v, want RESET CANCEL and no response", out, err)
	}
}

// §4.1 Response framing and failures for handler responses.
func TestBridgeHandlerResponses(t *testing.T) {
	cases := []struct {
		name    string
		method  string
		handler http.HandlerFunc
		want    string
		reset   string
	}{
		{"declared length", "GET", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Length", "2")
			_, _ = w.Write([]byte("ok"))
		}, "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok", ""},
		{"nothing written", "GET", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusAccepted)
		}, "HTTP/1.1 202 Accepted\r\nContent-Length: 0\r\n\r\n", ""},
		{"204 bodyless", "GET", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNoContent)
		}, "HTTP/1.1 204 No Content\r\n\r\n", ""},
		{"HEAD bodyless", "HEAD", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/plain")
			_, _ = w.Write([]byte("dropped"))
		}, "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\n\r\n", ""},
		{"1xx never sent", "GET", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusEarlyHints)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("a"))
			w.(http.Flusher).Flush()
			_, _ = w.Write([]byte("bc"))
		}, "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n1\r\na\r\n2\r\nbc\r\n0\r\n\r\n", ""},
		{"hop-by-hop dropped", "GET", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Connection", "x-private")
			w.Header().Set("X-Private", "1")
			w.Header().Set("Keep-Alive", "timeout=5")
			w.Header().Set("Transfer-Encoding", "gzip")
			w.Header().Set("X-Kept", "1")
			w.WriteHeader(http.StatusOK)
		}, "HTTP/1.1 200 OK\r\nX-Kept: 1\r\nContent-Length: 0\r\n\r\n", ""},
		{"short body", "GET", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Length", "10")
			_, _ = w.Write([]byte("abc"))
		}, "HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\nabc", "response body shorter than Content-Length"},
		{"panic after the head", "GET", func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte("abc"))
			panic("late")
		}, "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nabc\r\n", "response body failed"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ec := bridgeAgent(t, stateRegistered, func(c *agentConfig) { c.handler = tc.handler })
			out, err := ec.roundTrip(t, []byte(tc.method+" /mcp HTTP/1.1\r\nHost: h\r\n\r\n"), 0, 0)
			// A RESET discards whatever the edge had not read yet, so a reset
			// response can arrive cut short.
			if string(out) != tc.want && (tc.reset == "" || !strings.HasPrefix(tc.want, string(out))) {
				t.Fatalf("got %q\nwant %q", out, tc.want)
			}
			if tc.reset == "" {
				if err != io.EOF {
					t.Fatalf("stream ended with %v, want CLOSE", err)
				}
				return
			}
			var se *wsmixer.StreamError
			if !errors.As(err, &se) || se.Code != wsmixer.InternalErrorCode || se.Message != tc.reset {
				t.Fatalf("stream ended with %v, want RESET INTERNAL_ERROR %q", err, tc.reset)
			}
		})
	}
}

// §4.6 Request → handler: URL https://<Host><target>, Host, headers minus
// hop-by-hop, Connection-listed, Expect and Content-Length; absolute-form
// cut to path + query.
func TestBridgeHandlerRequest(t *testing.T) {
	got := make(chan *http.Request, 1)
	ec := bridgeAgent(t, stateRegistered, func(c *agentConfig) {
		c.handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { got <- r })
	})
	in := "GET https://ignored.example/mcp?x=1#frag HTTP/1.1\r\nHost: notes-tester.mcpwarp.test\r\nConnection: x-a\r\nX-A: 1\r\nUpgrade: h2c\r\nExpect: 100-continue\r\nmcp-session-id: abc\r\n\r\n"
	if _, err := ec.roundTrip(t, []byte(in), 0, 0); err != io.EOF {
		t.Fatalf("stream ended with %v", err)
	}
	r := <-got
	if r.URL.String() != "https://notes-tester.mcpwarp.test/mcp?x=1" || r.Host != "notes-tester.mcpwarp.test" || r.RequestURI != "/mcp?x=1" {
		t.Fatalf("URL %s Host %s RequestURI %s", r.URL, r.Host, r.RequestURI)
	}
	if len(r.Header) != 1 || r.Header.Get("Mcp-Session-Id") != "abc" {
		t.Fatalf("headers %v, want only Mcp-Session-Id", r.Header)
	}
	if r.Body != http.NoBody || r.ContentLength != 0 {
		t.Fatalf("a GET got a body")
	}
}
