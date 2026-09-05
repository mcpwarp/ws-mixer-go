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

## 2026-09-05

- **D-2026-09-05-01** — Reconnecting client shape: a new, additive `Client` type
  (`wsmixer/client_reconnect.go`) built *on top of* the existing `Dial`/`ClientOptions`, which keep working
  completely unchanged (the conformance adapter's non-reconnect path, `cmd/testserver`, `ws-mixer-server`,
  and any external consumer that already calls `Dial` directly are all unaffected). `Client` owns the full
  [`WIRE.md` §2.9](https://github.com/mcpwarp/ws-mixer-spec/blob/main/docs/WIRE.md#29-sequences) reconnect
  state machine — full-jitter backoff seeded by unexported clock/rand fields on `ReconnectOptions` (settable
  only from white-box tests in this package, never part of the public surface) so backoff tests are
  deterministic without real sleeps — reconnect on by default (`ReconnectOptions.MaxAttempts` is Go's zero
  value, meaning unlimited; a negative value opts out entirely, mirroring the JS SDK's `maxAttempts:0`;
  superseded by D-2026-09-05-02, which replaces the negative-`MaxAttempts` convention with an explicit
  `ReconnectOptions.Disabled bool`).
  `TokenProvider` is a plain `func(ctx) (string, error)` rather than JS's string-or-callback union, since Go
  has no ambiguous "maybe a function" value; `StaticToken(tok)` covers the static-string case. `DialError`
  (wrapping `Dial`'s existing error text unchanged, `errors.As`-able) makes a failed upgrade's HTTP status
  inspectable without breaking `Dial`'s current error-string format, and `Conn.PeerCloseCode()`/`Conn.Welcome()`
  are two small additive `Conn` accessors `Client` needed (the raw observed WS close code when no `*ConnError`
  was involved, and the full `welcome` for `OnConnect`). `Conn.Stats()` exposes CLIENT-SDK.md's "ignore and
  count" counters directly as atomic fields on `Conn` (no new `Metrics` interface method, so existing
  embedders of `Metrics` are not a breaking-change surface); `Client.Stats()` accumulates them across
  reconnects. `Drain`'s doc comment calling it "server-only" was stale — the implementation was already
  role-agnostic — fixed alongside adding `DrainOptions.CloseCode` so `Client.Close` can finish its
  `WIRE.md` §2.10 rule-14 shutdown with `NO_ERROR`/1000 instead of the server-drain sequence's default
  `GOING_AWAY`/4012.

- **D-2026-09-05-02** — Fixes from the v0.4.0 client review, three worth recording:

  1. `ReconnectOptions.MaxAttempts < 0` meaning "reconnect disabled" (D-2026-09-05-01, mirroring the JS SDK's
     `maxAttempts:0`) is replaced with an explicit `ReconnectOptions.Disabled bool`. Pre-release, so this is a
     straight breaking change rather than an additive one: sign-overloading a count field reads badly at
     every call site and invites exactly the kind of "wait, is `-1` disabled or unlimited?" bug the review
     flagged. `MaxAttempts` itself keeps its existing meaning (0 = unlimited, N = a ceiling).

  2. `Client`'s reconnect state machine (`client_reconnect.go`) honours `drain.retry_after_ms` (`WIRE.md`
     section 2.9's drain field table: "reconnect hint. Absent = reconnect immediately with jitter") as the
     delay for the drain-triggered parallel reconnect, when the server sends one. This is a **deliberate
     deviation from the JS reference SDK**, which ignores `retry_after_ms` entirely on this path, toward the
     spec rather than JS parity. Everywhere else this package still tracks the JS SDK's behavior exactly
     (nit 1's attempt-increment-before-delay comment, kept deliberately non-literal against section 2.9's
     formula, is the other direction of the same "JS parity over a literal spec reading" call).

  3. `Client.Close` calling `OnConnect`/`OnDisconnect` directly on whichever goroutine decided to report them
     (`attemptLoop`/`watchConn`/their `reportAndSchedule`/`handleServerDrain` spawns -- all tracked by
     `Client.wg`) deadlocked `Close`'s own `wg.Wait()` whenever a callback called `Close()` itself, the single
     most natural thing a caller writes in `OnDisconnect`. Go has no clean way to ask "is the calling
     goroutine one `wg` is waiting on" (goroutine-local state isn't idiomatic Go), so the fix moves callback
     invocation onto one dedicated, never-`wg`-tracked goroutine (`Client.cbMu`/`cbCond`/`cbQueue`/`callbackLoop`) fed by every
     report site instead of calling directly: `wg.Wait()` no longer has any way to depend on the goroutine a
     callback happens to be running on.
