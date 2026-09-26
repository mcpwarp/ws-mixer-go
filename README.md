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
| `Client` | Reconnecting high-level client (`client_reconnect.go`): owns the WIRE.md §2.9 state machine on top of `Dial`, reconnect on by default. `NewClient`/`Connect`/`Close`/`CloseWith`, `OnConnect`/`OnDisconnect`/`OnStream`/`OnApp`/`OnDrain`, `Stats()`. |
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
func (c *Conn) PeerCloseReason() string                     // "" if this side initiated the close, or none was observed
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
state machine — full-jitter backoff, `drain`/`4012`/`4013`/`4009`/`4014` handling, a fatal set that never
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
just a convenience wrapper for a token that never changes. A `StaticToken` rejected by the server (HTTP 401,
`error{UNAUTHORIZED}`, or a bare pre-welcome WS close 4011) is fatal at once, not retried with backoff: the
same fixed token can never start working, so retrying it forever only hammers the auth service and hides the
problem. A real `TokenProvider` still gets WIRE.md section 2.9's one-time immediate refresh-retry on the same
rejections (call the provider again, redial right away, no backoff) — but that refresh-retry is itself a
once-only budget now, re-armed only once a connection has stayed up `ReconnectOptions.StableAfter` (below),
not on every dial attempt.

A `TokenProvider` error is normally fatal too, surfaced verbatim on `DisconnectReason.Cause` -- but a provider
that could not OBTAIN a token for a temporary reason (the network is briefly down, the auth server is
unreachable while refreshing an expired token) can mark that one failure as temporary instead, by wrapping
`wsmixer.ErrTokenUnavailable`:

```go
token := func(ctx context.Context) (string, error) {
	tok, err := refreshFromAuthServer(ctx)
	if err != nil {
		return "", fmt.Errorf("refreshing token: %w", wsmixer.ErrTokenUnavailable)
	}
	return tok, nil
}
```

Client then treats that attempt like an ordinary failed dial (phase `"dial"`, non-fatal, normal backoff, no
immediate retry) instead of ending the client -- detected via `errors.Is` only, never inferred from an error's
shape (no `Retryable()`/`Temporary()` duck-typing), so an accidental match can never turn a genuinely fatal
provider failure into an endless retry loop. An unmarked provider error stays fatal exactly as before.

`OnConnect` fires on every successful `welcome`, first connection and every reconnect alike (WIRE.md: no
resumption, so nothing from the old `*Conn` survives); `OnDisconnect` fires for every disconnect, recoverable
or fatal, as one `DisconnectReason` (`Phase`, `WSCode`, `ErrorCode`/`ErrorName`, `HTTPStatus`, `Fatal`,
`Message`, `CloseReason`, `Cause`). `Phase` is `"dial"`, `"handshake"`, or `"connected"`: any failure after
`websocket.Dial`'s own upgrade (the 101) and before `welcome` is `"handshake"` — including a transport
death/EOF with no close frame at all, and a shared per-attempt ctx deadline that happens to expire during the
welcome wait rather than during the dial itself — not just the `*ConnError`/close-frame cases; a dial
explicitly *cancelled* by the application (`Close`/`CloseWith` during the handshake wait, or the caller's own
cancelled `ctx` on a plain `Dial` — as opposed to that same `ctx`'s deadline simply elapsing) stays `"dial"`,
since that's not a failure to attribute to the peer or the transport at all.

`CloseReason` is the *peer's* WebSocket close-frame reason string, verbatim, whenever one was actually
observed — including when no ws-mixer `error{}` message preceded it (e.g. it was lost); "" when no close
frame was observed, this side initiated the close itself (`Close`/`CloseWith`/`Client.Close`/a protocol
violation), or one arrived right behind an `error{}` this side had already stopped reading for. A consumer
generally wants `CloseReason` first, falling back to `Message` when it's empty.

`ErrorCode`/`HasErrorCode`/`ErrorName` are set whenever the disconnect carries a ws-mixer error code, from any of
three sources: a peer's wire `error{}` message preceded the close (`ErrorCode` exactly that message's code); no
`error{}` message was ever seen (e.g. one lost the same way `CloseReason` above can be) but the close frame itself
carried a WS code in ws-mixer's own reserved range (4001-4999), in which case `ErrorCode` is derived mechanically
as `ErrorCode(wsCode-4000)` (`ws_close = 4000 + error_code`, run in reverse — never wrong, so never guessed); or a
ws-mixer error the SDK itself raises locally, with no close frame from the peer involved at all — a
missing/mismatched subprotocol echo on the upgrade (`UNSUPPORTED`, no close frame or HTTP status either), the
welcome timeout (its own locally generated `PROTOCOL_ERROR`/4001), and any other local protocol-violation failure
of an already-connected conn. Left unset for anything that isn't a ws-mixer code: an HTTP upgrade rejection
(`HTTPStatus` carries that instead), an abnormal closure, no close frame at all, or an ordinary non-ws-mixer WS
close code.

Backoff's attempt counter, and every once-only reconnect budget alongside it (the keepalive-timeout immediate
retry, the token refresh-retry above), reset only once a connection has stayed up
`ReconnectOptions.StableAfter` (default 10s) past its own `welcome` — not at `welcome` itself. A server that
welcomes a connection and then immediately closes it therefore cannot make the client redial about once a
second forever, or re-arm a once-only budget on every such cycle: the delay keeps climbing, and each budget
stays spent, until a connection genuinely stays up. `Client` reconnects after an `APPLICATION_CLOSE` (WS
4014) or `ENHANCE_YOUR_CALM` (WS 4009) starting at `Cap` instead, not the bottom of the backoff ladder: both
are deliberate post-`welcome` refusals, not failures, so backing off hard immediately is the right response
regardless of where the attempt counter happens to be. `ReconnectOptions.MaxAttempts` counts consecutive
reconnects that never reach a stable connection — a healthy client that drops and recovers occasionally is
never at risk of exhausting it.

Differs from the JS SDK: Go's `ReconnectOptions.StableAfter` follows Go's usual zero-value idiom — `0` means
the 10s default, not "no stability window at all" — and a tiny nonzero duration (e.g. `time.Nanosecond`)
approximates the old reset-at-`welcome` behavior instead. The JS SDK's `stableAfter: 0` means the opposite:
no stability window, i.e. reset-at-`welcome`. A `0` config value is therefore **not** portable between the
two SDKs.

An application closing a connection for its own reasons — one ws-mixer itself does not interpret, e.g. an
over-capacity refusal — should use `wsmixer.ApplicationCloseCode` with `Conn.Close`, or, on a `Client`,
`Client.CloseWith(ctx, message)`: unlike `Close` (graceful: `drain{client_requested}` → grace → `NO_ERROR`),
`CloseWith` closes the live connection at once with `error{APPLICATION_CLOSE,message}` then WS close `4014`,
and — like `Close` — stops the reconnect loop for good rather than letting the client redial (calling
`Conn.Close` directly on a connection a `Client` is managing looks like an ordinary disconnect to it instead,
and it reconnects as usual). Per spec D-2026-09-25-01, `CloseWith` takes a message only and always sends
`APPLICATION_CLOSE`: WIRE.md §2.8 allows an application exactly one connection-close code, so there is no
code parameter to choose. `Conn.Close` itself is unchanged and still takes an arbitrary `ErrorCode`; codes
`>= 0x1000_0000` remain stream-`Reset`-only, never valid for a connection close, and any code `> 999` is
clamped on the wire to `InternalErrorCode`'s 4002 (`error{}` still carries the real code).

`Stats()` returns the
"ignore and count" counters (unknown frame types, stale frames, duplicate pongs, refused opens, protocol
violations, bytes in/out), cumulative across reconnects.

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
2. `.spec/spec` at the repo root, the checkout `make fetch-spec` populates from `spec.pin` (also what CI's `WSMIXER_SPEC_DIR` points at).

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
