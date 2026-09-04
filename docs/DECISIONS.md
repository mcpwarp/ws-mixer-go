# Decision log

Status: **local**. This file records only `ws-mixer-go` implementation decisions. Protocol decisions are
recorded in [`ws-mixer-spec`](https://github.com/mcpwarp/ws-mixer-spec) → `docs/DECISIONS.md`; the wire
protocol itself is normatively specified in
[`ws-mixer-spec`'s `docs/WIRE.md`](https://github.com/mcpwarp/ws-mixer-spec/blob/main/docs/WIRE.md). Each
local entry below links back to the spec entry it implements, where one exists.

## 2026-08-26

- **D-2026-08-26-05a** — WebSocket library: [`coder/websocket`](https://github.com/coder/websocket), not
  `gorilla/websocket`. This is the Go half of
  [`D-2026-08-26-05`](https://github.com/mcpwarp/ws-mixer-spec/blob/main/docs/DECISIONS.md), which was
  originally one bullet ("Go server on `coder/websocket`; JS client on `ws`, streams exposed as Node
  `Duplex`") split into two independent implementation decisions — see
  [`D-2026-08-26-05b`](https://github.com/mcpwarp/ws-mixer-js/blob/main/docs/DECISIONS.md) for the JS half.

  Both the client dialer (`client.go`) and the server-side wire handshake (`AcceptConn`, `accept.go`) live
  in this package and both use `coder/websocket`.

  | | `coder/websocket` | `gorilla/websocket` |
  |---|---|---|
  | Write model | **synchronous, context-bounded, one writer at a time, no internal send queue** | manual `NextWriter`, no context |
  | Backpressure | free — the write blocks | you build it |
  | Default read limit | 32 KiB (`SetReadLimit` needed anyway — we set 65 544) | **unlimited** → memory-exhaustion DoS unless you set it |
  | Context support | throughout | none |
  | `net/http` upgrade | `websocket.Accept(w, r, opts)` | `Upgrader.Upgrade(w, r, nil)` |
  | Compression off | `CompressionDisabled` | option |

  `coder/websocket` wins on the two things that matter here: **context-aware synchronous writes give us
  [`WIRE.md` §2.6](https://github.com/mcpwarp/ws-mixer-spec/blob/main/docs/WIRE.md) rule 2 for free**, and
  the mandatory read limit is a one-liner rather than a security footgun. Caveat noted from the research:
  its `Ping(ctx)` requires a concurrent reader and a context expiry closes the *whole* connection —
  irrelevant for us, since neither the client nor `AcceptConn`'s server-side handshake use WS-native ping
  at all ([`WIRE.md` §2.9](https://github.com/mcpwarp/ws-mixer-spec/blob/main/docs/WIRE.md)'s in-band
  `ping`/`pong` on stream 0 is used instead, symmetrically on both roles).
