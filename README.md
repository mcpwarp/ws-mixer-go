# ws-mixer-go

Go implementation of [`ws-mixer.v1`](https://github.com/mcpwarp/ws-mixer-spec): protocol core (frame
codec, control channel, stream state machine, flow control, keepalive) and the Go client — N
independent, flow-controlled byte streams over one WebSocket connection, built on
[`coder/websocket`](https://github.com/coder/websocket).

This repo is the protocol core and client SDK only. HTTP upgrade, auth policy, and the production
`Listener` live in the separate `ws-mixer-server` module; this module exposes `wsmixer.AcceptConn` for
anyone who wants to run the server side directly against their own `http.Handler`.

## Install

```sh
go get github.com/mcpwarp/ws-mixer-go
```

## API

| Type | Role |
|---|---|
| `Client` | Reconnecting high-level client (`client_reconnect.go`): owns the WIRE.md §2.9 state machine on top of `Dial`, reconnect on by default. `NewClient`/`Connect`/`Close`, `OnConnect`/`OnDisconnect`/`OnStream`/`OnApp`/`OnDrain`, `Stats()`. |
| `Conn` | One handshaken connection, dialed (`Dial`) or accepted (`AcceptConn`). Opens streams, sends `app`, drains, `Stats()`. |
| `Stream` | One byte stream. `io.Reader` + `io.Writer` + `Close()` + `CloseWrite()` + `Reset(code, msg)`. |
| `Options` | `Window`, `MaxStreams`, `PingInterval`, `PingTimeout`, `HelloTimeout`, `ReadLimit`, `Logger`, `Metrics`, plus the stream-0 flood-limit and refused-open-escalation knobs. |
| `ConnError` / `StreamError` | Carry an `ErrorCode` and message; `errors.As` targets for connection- and stream-scoped failures respectively. |
| `Metrics` | Interface the embedder implements (see [`docs/METRICS.md`](./docs/METRICS.md)), or `NoopMetrics{}` to disable observability. |

`Conn` is a concrete struct, not an interface — both a dialed and an accepted connection are the same
type, so `OnStream`/`OnApp`/`OnDrain` and `Drain` behave identically regardless of role:

```go
func (c *Conn) Session() string
func (c *Conn) Meta() json.RawMessage                       // hello.meta, verbatim
func (c *Conn) OpenStream(ctx context.Context) (*Stream, error)  // blocks if at MaxStreams; client-only
func (c *Conn) SendApp(ctx context.Context, body any) error
func (c *Conn) OnApp(fn func(body json.RawMessage))
func (c *Conn) OnStream(fn func(*Stream))
func (c *Conn) OnDrain(fn func(*DrainMsg))
func (c *Conn) Drain(ctx context.Context, reason string, opts DrainOptions) error
func (c *Conn) Close(code uint32, msg string) error
func (c *Conn) Done() <-chan struct{}
func (c *Conn) Err() error
func (c *Conn) Run()                                        // starts the read/write/keepalive loops

type Stream struct{ /* … */ }
func (s *Stream) Read(p []byte) (int, error)
func (s *Stream) Write(p []byte) (int, error)
func (s *Stream) CloseWrite() error                 // sends CLOSE; peer reads EOF
func (s *Stream) Close() error                      // CloseWrite; if the peer may still send, also Reset(CANCEL) so it does not stall on a window nobody drains
func (s *Stream) Reset(code ErrorCode, msg string) error
func (s *Stream) ID() uint32
```

`OpenStream` returns something `io.ReadWriteCloser`-shaped plus `CloseWrite()`, so a caller can hand it
straight to `httputil`-style copying. `Read` returns `io.EOF` after the peer's `CLOSE` and a `*wsmixer.StreamError`
after a `RESET` — the distinction the layer above needs.

`OnStream`, `OnApp`, and `OnDrain` all fire from one shared, connection-owned delivery goroutine, strictly
in the order their frames arrived on the wire, decoupled from the read loop so a slow callback never stalls
frame parsing — but also never runs concurrently with itself or the other two, so each must hand off to its
own goroutine for anything that blocks.

There is no `Listener` type in this package — accepting a connection over `net/http` (subprotocol/bearer
checks, `OnConn`, production auth policy) is [`ws-mixer-server`](https://github.com/mcpwarp/ws-mixer-server)'s
job, built on top of `AcceptConn` below.

## Client example

`wsmixer.Client` (`client_reconnect.go`) is the high-level client most callers want: it owns the full
[WIRE.md §2.9](https://github.com/mcpwarp/ws-mixer-spec/blob/main/docs/WIRE.md#29-sequences) reconnect
state machine — full-jitter backoff, `drain`/`4012`/`4013`/`4009` handling, a fatal set that never
retries — on top of the plain `Dial` below, and it is on by default:

```go
client := wsmixer.NewClient(url, wsmixer.StaticToken("expected-token"), wsmixer.ClientConfig{
	OnStream:     handleStream, // wired onto every (re)connect automatically -- no need to re-attach from OnConnect
	OnDisconnect: func(r wsmixer.DisconnectReason) { log.Printf("disconnected: %+v", r) },
})
if err := client.Connect(context.Background()); err != nil {
	log.Fatal(err)
}
defer client.Close(context.Background()) // drain{client_requested}, wait <=5s, close
```

`Token` is a `TokenProvider func(ctx) (string, error)`, called fresh on every dial — `StaticToken` above is
just a convenience wrapper for a token that never changes. `OnConnect` fires on every successful `welcome`,
first connection and every reconnect alike (WIRE.md: no resumption, so nothing from the old `*Conn`
survives); `OnDisconnect` fires for every disconnect, recoverable or fatal, as one `DisconnectReason`
(`Phase`, `WSCode`, `ErrorCode`/`ErrorName`, `HTTPStatus`, `Fatal`, `Message`, `Cause`). `Stats()` returns
the "ignore and count" counters (unknown frame types, stale frames, duplicate pongs, refused opens,
protocol violations, bytes in/out), cumulative across reconnects.

The lower-level, single-attempt `Dial` (used internally by `Client`, and still the right tool for a
harness or test that wants exactly one connection with no retry policy of its own) is unchanged:

```go
conn, err := wsmixer.Dial(context.Background(), "ws://localhost:8080/v1/tunnel", wsmixer.ClientOptions{
	Token: "expected-token",
	OnStream: func(st *wsmixer.Stream) {
		go func() {
			req, _ := io.ReadAll(st)
			st.Write([]byte("HTTP/1.1 200 OK\r\n\r\n"))
			st.CloseWrite()
		}()
	},
})
if err != nil {
	log.Fatal(err)
}
<-conn.Done()
```

`OnStream`, `OnApp`, and `OnDrain` fire on one shared per-connection goroutine, in wire order; a handler
that blocks (I/O, waiting on another goroutine, calling `Drain` inline) must hand off to its own goroutine
instead, or it stalls every later event on that connection.

Running the server side yourself (no production policy layer): accept the WebSocket upgrade with your own
`http.Handler`, then call `wsmixer.AcceptConn` to run the ws-mixer handshake over it — see
`wsmixer/accepthandler_test.go` for a minimal worked example. Most users running a real deployment want
`ws-mixer-server`'s `Listener` instead, which adds subprotocol/bearer-token pre-upgrade checks and
production auth policy on top of `AcceptConn`.

## Tests

`go test ./wsmixer/` runs the fixture-driven suites (`frame_test.go`, `control_test.go`,
`sequence_test.go`) against a checkout of [`ws-mixer-spec`](https://github.com/mcpwarp/ws-mixer-spec),
resolved by `wsmixer/specdir_test.go`'s `specDir` helper:

1. `$WSMIXER_SPEC_DIR`, if set -- must exist. Accepts either directory shape: this repo's own
   convention (repo-root, i.e. `$WSMIXER_SPEC_DIR/spec/fixtures` exists) or `ws-mixer-js`'s
   convention (the spec subdir itself, i.e. `$WSMIXER_SPEC_DIR/fixtures` exists directly).
2. `../../.spec/spec`, the checkout `make fetch-spec` populates from `spec.pin`.

If neither resolves, those suites skip with a reason rather than failing -- `go test ./...` stays
green without a network fetch.

## Conformance

`conformance/adapter` is the importable harness library behind this repo's
[conformance-runner](https://github.com/mcpwarp/ws-mixer-spec) adapter, covering both the `go-client` and
`go-server` roles behind a `ServerBackend` interface (docs/MIGRATION.md section 2.3). `cmd/conformance-adapter`
is the thin binary that wires it directly to `AcceptConn` (proving the core — not `ws-mixer-server` — conforms);
`ws-mixer-server` imports the same library for its own adapter, wired to its production `Listener` instead. Both
require `-tags conformance` to build. `cmd/testserver` is a stdin/stdout test harness used by `ws-mixer-js`'s
interop tests; it is not a supported public API.

See [`ws-mixer-spec`](https://github.com/mcpwarp/ws-mixer-spec) for the wire spec, client-SDK
requirements, and the fixture/conformance-fixture corpus this package is tested against.

## Docs

- [`docs/DECISIONS.md`](./docs/DECISIONS.md) — this repo's implementation decisions (WebSocket library
  choice, etc.); protocol decisions live in `ws-mixer-spec`'s `docs/DECISIONS.md`.
- [`docs/METRICS.md`](./docs/METRICS.md) — the `Metrics` callback interface and what each callback means.
