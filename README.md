# wsmixer (Go)

`github.com/mcpwarp/ws-mixer/go/wsmixer`, package `wsmixer` — the Go implementation of the `ws-mixer.v1`
wire protocol: N independent, flow-controlled byte streams over one WebSocket connection. See
[`../docs/OVERVIEW.md`](../docs/OVERVIEW.md) for the normative spec. This package is a server (and a
minimal test/reference client) built on [`coder/websocket`](https://github.com/coder/websocket).

## Install

```sh
go get github.com/mcpwarp/ws-mixer/go/wsmixer
```

Requires Go 1.23+ (forced by `coder/websocket`'s own `go.mod`; the wire spec itself only assumes 1.22+).

## Server example

```go
package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/mcpwarp/ws-mixer/go/wsmixer"
	"github.com/mcpwarp/ws-mixer/go/wsmixerserver"
)

func main() {
	ln := wsmixerserver.NewListener(wsmixerserver.ServerOptions{
		Options: wsmixer.Options{
			Window:       256 << 10,
			MaxStreams:   64,
			PingInterval: 30 * time.Second,
			PingTimeout:  90 * time.Second,
		},
		Authenticate: func(ctx context.Context, h *wsmixer.Hello) (wsmixer.WelcomeMeta, error) {
			if h.Token != "expected-token" {
				return wsmixer.WelcomeMeta{}, wsmixer.Unauthorized("token invalid")
			}
			return wsmixer.WelcomeMeta{Meta: map[string]any{"public_url": "https://example.tunnels.dev"}}, nil
		},
		OnConn: func(c *wsmixer.Conn) {
			c.OnApp(func(body json.RawMessage) { log.Printf("app: %s", body) })
			go func() {
				st, err := c.OpenStream(context.Background())
				if err != nil {
					return
				}
				defer st.Close()
				st.Write([]byte("GET / HTTP/1.1\r\n\r\n"))
				st.CloseWrite()
			}()
		},
	})

	http.Handle("/v1/tunnel", ln)
	log.Fatal(http.ListenAndServe(":8080", nil))
}
```

## Client example

```go
package main

import (
	"context"
	"io"
	"log"

	"github.com/mcpwarp/ws-mixer/go/wsmixer"
)

func main() {
	conn, err := wsmixer.Dial(context.Background(), "ws://localhost:8080/v1/tunnel", wsmixer.ClientOptions{
		Token: "expected-token",
		OnStream: func(st *wsmixer.Stream) {
			// OnStream, OnApp, and OnDrain all fire from the same connection
			// goroutine, in wire order, so a handler that does I/O (like this
			// one) must hand off to its own goroutine rather than block here
			// -- otherwise it holds up delivery of every later stream/app/drain
			// event on this connection.
			go func() {
				req, _ := io.ReadAll(st)
				log.Printf("request: %s", req)
				st.Write([]byte("HTTP/1.1 200 OK\r\n\r\n"))
				st.CloseWrite()
			}()
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	<-conn.Done()
	log.Println(conn.Err())
}
```

`OnStream`, `OnApp`, and `OnDrain` all run on the same shared connection-owned delivery goroutine, one event
at a time, in the order their frames arrived on the wire. Register them at any point (before or after
`Dial`/accept); none of the three may block: a handler that does anything blocking (I/O, waiting on another
goroutine, calling `Drain` inline) must spawn its own goroutine for that work, or it holds up delivery of
every later stream/app/drain event on that connection. In particular, answer a peer-requested drain
(`PeerRequestedDrain`/`OnDrain`) by calling `Conn.Drain` from a goroutine, never inline from the callback.

The Go client has no reconnect/backoff logic (that is the JS SDK's job, per OVERVIEW.md section 2.9);
it is a minimal implementation sufficient for tests and a future full Go SDK.

## Tests and the spec fixtures

`../../spec/fixtures/` is the cross-SDK conformance oracle and this package's actual test data:

- `frame_test.go` decodes every `../../spec/fixtures/frames/*.json` and checks the result (or the error
  code and connection-fatal/stream-scoped classification) against the fixture.
- `control_test.go` runs every `../../spec/fixtures/control/**/*.json` through the hand-written
  `ParseControl` validator and asserts the accept/reject verdict matches the fixture's `wire_valid`.
- `sequence_test.go` replays `../../spec/fixtures/sequences/*.json` against a real `*Conn` wired to an
  in-memory fake WebSocket transport, driving `recv`/`send`/`expect` steps. The three fixtures that
  depend on wall-clock waits (`hello_timeout`, `ping_pong_then_dead_peer_timeout`,
  `drain_with_inflight_timeout`) are instead covered behaviorally in `timing_test.go` with short,
  scaled `Options` rather than literally sleeping ~2 real minutes.
- `integration_test.go` exercises the real `Listener` + `Dial` path end to end over an actual
  loopback WebSocket (`httptest.Server`): handshake, request/response streams, `app` messages,
  concurrent-stream fairness, and a goroutine-leak check.

Run everything:

```sh
go vet ./...
go test -race ./...
gofmt -l .
```
