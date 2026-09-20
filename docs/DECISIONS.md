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

## 2026-09-20

Note: local ids below don't line up numerically with `ws-mixer-spec`'s own `D-2026-09-20-*` on the same
date -- local `-01` (`Close`-before-`Run`) is Go-only, with no spec counterpart; local `-02` implements
spec `D-2026-09-20-01`; local `-03` and `-04` both implement spec `D-2026-09-20-02`; local `-05` implements
spec `D-2026-09-20-03`. Each entry below links its own spec counterpart explicitly, so read the link, not
the number.

- **D-2026-09-20-01** — `Conn.Close` called before `Run` (a documented, normal case: `AcceptConn` returns
  a handshaken-but-not-running `*Conn`, and a real server-side `OnConn` callback can call `Close` right
  there to refuse the connection before it is ever run) used to always enqueue its `error{}` frame on
  `controlQueue` and sleep, exactly like the post-`Run` path -- but with no writer loop started yet, nothing
  ever drained that queue, so the frame was silently lost and only the bare WS close reached the peer.
  Fixed by giving `Close` the same `running` branch `fail` already has: not running means flush the control
  frames already queued (e.g. an `app` message sent via `SendApp` right before the refusal), in order, then
  write `error{}` synchronously via `writeControlNow` -- one shared 2s deadline for the whole flush, not 2s
  per frame, and only the frames already queued when `Close` was called (a length snapshot up front), so a
  concurrent `SendApp` producer can't extend it -- with no flush-window sleep needed, since the writes are
  already done by the time the socket closes.

  A second bug surfaced alongside it, in `Run`/`Close`/`fail`'s handling of the same window: `running` was
  read and the writer loop started in two separate, unsynchronized critical sections, so a `Close`/`fail`
  landing concurrently with `Run` could each decide they own `controlQueue` and both write to it -- not a
  corruption hazard (`coder/websocket`'s `Write` serializes internally, one writer at a time -- confirmed in
  `write.go`'s `writeMu`), but a real reordering one: a frame could land on the wire after `error{}`, which
  `WIRE.md` §2.8 requires to be last. Fixed with one new field, `preRunClosed`, set by `fail`/`Close` in the
  same `c.mu` critical section that reads `running`, and checked by `Run` in the same critical section it
  sets `running` in: whichever of {`Run` starting the writer loop, `fail`/`Close` claiming the pre-run path}
  gets to `c.mu` first is now the only one that can happen, by construction, not by timing. `Run()` called on
  a `Conn` `preRunClosed` this way returns immediately without starting any of its five goroutines at all --
  `Done()` still fires and the WS close handshake still completes with no reader loop running, since
  `coder/websocket`'s `Close` does its own internal read for the peer's close-frame reply (`close.go`'s
  `waitCloseHandshake`, which does not require an app-level reader goroutine).

- **D-2026-09-20-02** — `ApplicationCloseCode` (`errors.go`): implements
  [`ws-mixer-spec`'s `D-2026-09-20-01`](https://github.com/mcpwarp/ws-mixer-spec/blob/main/docs/DECISIONS.md),
  naming `0x0e` `APPLICATION_CLOSE` (WS close 4014). Added the exported constant, its `errorCodeNames` entry,
  and replaced the former "`0x0e` is reserved ... intentionally has no constant" stance and `String()`'s
  stale doc comment claiming it rendered `INTERNAL_ERROR`. This package still never produces it itself --
  it is sent only by an application, on either side, via `Conn.Close`. Reconnect classification
  (`watchConn`'s close-code switch): the same spec entry now specifies "start at cap" for a post-welcome
  4014, exactly like the existing 4009 case -- 4014 shares that `case` (`cl.rc.jitter(cl.rc.Cap)`) rather
  than falling to the `default` `fullJitter` branch, since it is a deliberate refusal made right after
  `onAttemptSucceeded` reset `attempt` to 0, where ordinary `fullJitter(attempt)` would never climb (a
  refused client would otherwise redial about once a second forever). A **handshake-phase** 4014 (before
  `welcome`) is unaffected and stays on `onAttemptFailed`'s `fullJitter` path: `attempt` was never reset in
  that case, since `welcome` was never reached, so the normal ladder still climbs as intended.

- **D-2026-09-20-03** — `Conn.PeerCloseReason()` / `DisconnectReason.CloseReason`: implements
  [`ws-mixer-spec`'s `D-2026-09-20-02`](https://github.com/mcpwarp/ws-mixer-spec/blob/main/docs/DECISIONS.md).
  Go specifics: both are peer-only, backed by a new `Conn.localCloseInitiated` field set by `fail`/`Close`/
  `handlePeerError` (every site that calls `c.ws.Close` with a real reason) in the same critical section
  each already uses to decide the rest of its own shutdown. This exists because a self-initiated close can
  come back around through the peer and be misread as something the peer said: `coder/websocket`'s peer
  echoes the exact code/reason it just received back to us verbatim, and `read.go`'s `handleControl` --
  running inline on our own blocked `readerLoop`, which already holds the read lock -- parses that echo the
  normal way and hands it back as a `CloseError`, unless `handleReadError` is told to ignore it.
  `TestClientSelfInitiatedCloseReasonIsEmpty`/`TestServerFailInitiatedCloseReasonIsEmpty` pin this on both a
  graceful `Client.Close` and a `fail()`-driven protocol violation, confirmed (by temporarily removing the
  guard) to actually reproduce the bug without the fix. Separately, "" when a stream-0 `error{}` message
  precedes the close: `handlePeerError` ends the read loop as soon as it parses `error{}`, so the close
  frame behind it is never read at all -- deliberate, pinned by the pre-existing
  `TestClientErrorThenCloseReasonUnobservedPostConnect`.

- **D-2026-09-20-04** — A bare pre-welcome close (no ws-mixer `error{}` frame, an SDK bug on the peer's
  part) is now reclassified `PhaseHandshake` with the peer's real `WSCode`/`CloseReason`, rather than
  `PhaseDial` with a misleading "no welcome within Nms" timeout message -- this is precisely the Go SDK bug
  [`ws-mixer-spec`'s `D-2026-09-20-02`](https://github.com/mcpwarp/ws-mixer-spec/blob/main/docs/DECISIONS.md)
  calls out by name. Fixing the phase surfaced a second gap: WIRE.md §2.9's fatal set (4010/4011) was
  honoured post-connect but not in this phase, so a peer that closed 4010/4011 raw, without ever sending
  `error{}`, retried forever instead of ending the client. `classifyFailureErr` now treats a bare `wsCode ==
  4010` as fatal outright, and `isUnauthorized` now also matches a bare `websocket.CloseError{Code: 4011}`,
  joining the exact same one-time immediate refresh-retry a `*ConnError{UnauthorizedCode}` already gets
  (`dialAndHandshake`) -- only a second 4011 is fatal, via the same `fatalOverride`/`forced` mechanism, not a
  new one. Caveat inherited from that existing mechanism, not new here: with `StaticToken` (not a real
  `TokenProvider`), `dialAndHandshake`'s `isRealProvider` check skips the refresh-retry entirely -- a single
  4011 then stays on the normal recoverable path, exactly like today's `*ConnError{UnauthorizedCode}`/HTTP
  401 behaviour with a static token. Deliberately unchanged: backoff for every other handshake-phase
  code, e.g. a bare 4009, still gets `fullJitter` rather than the connected phase's `jitter(Cap)` -- there
  is no `drain` message to read `retry_after_ms` from before `welcome` even completes, so there is
  nothing for the connected phase's special case to do differently here. Also fixed alongside:
  `classifyFailureErr`/`phaseFor` now check
  `*providerError` first, unconditionally, before extracting a phase or `wsCode` from the error chain -- a
  caller's own `TokenProvider` returning (or wrapping) a `*ConnError`/`websocket.CloseError` by coincidence
  must never be misread as this side's own handshake phase/close code.

  This covered only half of the spec entry's normative rule, though: the *other* handshake-phase close --
  the client's own welcome timeout, with no peer close frame at all -- still fell through
  `clientHandshake` (`client.go`) as a plain wrapped error on every non-`CloseError` read failure, so
  `phaseFor` saw neither a `*ConnError` nor a `websocket.CloseError` and reported `PhaseDial`/`WSCode 0`;
  worse, `Dial`'s deferred `ws.CloseNow()` then aborted the transport with no close frame at all, leaving
  the peer a bare `1006` it could never explain (BLOCKER, found in cross-repo review). The fix could not be
  "just return a `*ConnError` for a timeout instead" -- `clientHandshake` was reading with `ctx` wrapped in
  `context.WithTimeout(ctx, HelloTimeout)`, and coder/websocket's own `setupReadTimeout` tears the whole
  connection down (`c.close()`, via a `context.AfterFunc` on the read's context) the instant that context
  expires, before the caller ever sees the error -- confirmed by instrumenting `fail`'s own `ws.Close`,
  which came back `use of closed network connection`/`EOF`, never a real close handshake. `fail()` writing
  `error{PROTOCOL_ERROR}` and a graceful WS close afterward would find nothing left to write to. The fix
  instead mirrors `runServerHandshake`'s (`accept.go`) own hello-timeout handling exactly: a
  `time.AfterFunc(HelloTimeout, ...)` racing a plain `c.ws.Read(ctx)` (the caller's own `ctx`, undecorated),
  arbitrated by a `claimed` `atomic.Bool` `CompareAndSwap` so the timer and the read can never both act on
  the same outcome -- the timer's callback calls `fail()` itself (delivering the real
  `error{PROTOCOL_ERROR}`/WS close `4001` before anything is torn down), and `clientHandshake` returns the
  same `*ConnError` either way, so `Dial`'s own `fail(ce)` call for it is a harmless no-op (`closeOnce`).
  `Phase=handshake`/`WSCode=4001`/`ErrorName=PROTOCOL_ERROR`/`CloseReason=""`/`Fatal=false`, non-fatal with
  the normal climbing backoff (WIRE.md §2.9: "no welcome within 10s on the client side -> close 4001 and
  retry with backoff"), exactly `CLIENT-SDK.md`'s "Handshake-phase close" row and
  [`ws-mixer-spec`'s `D-2026-09-20-02`](https://github.com/mcpwarp/ws-mixer-spec/blob/main/docs/DECISIONS.md).
  A cancelled or expired *caller* `ctx` is deliberately kept distinct from this -- the `claimed` CAS only
  ever succeeds for the timer when `HelloTimeout` itself elapsed, so a caller-driven cancellation (or any
  other non-`CloseError` read failure, e.g. a plain transport death) still returns a plain wrapped error,
  phase dial, no synthesized close: the caller abandoning the dial is not something to blame the peer for,
  and a transport that's already gone has nothing left to gracefully close anyway.

  A second, independent bug hid behind the first, found by two reviewers after this fix first shipped:
  `client_reconnect.go`'s `dialAndHandshake` put a single `context.WithTimeout(ConnectTimeout)` ctx over
  both `websocket.Dial` and, inside it, `clientHandshake`'s welcome wait -- and that ctx is exactly what
  reaches `c.ws.Read(ctx)`. WIRE.md section 2.9's `ConnectTimeout` (bounds the dial) and section 2.10 step
  2's welcome-wait timeout (`HelloTimeout`, starting only after the 101) are two separate, sequential
  budgets with the same 10s default, so the ctx deadline (started before the dial) and the hello timer
  (started after it) were racing each other the entire time: whichever fired first decided the outcome, and
  the fix above only ever ran when the hello timer happened to win. Measured at real defaults, 6 runs: 3x
  handshake/4001 (the timer won), 3x dial/0 (the ctx deadline won and tore the transport down first, exactly
  the pre-fix symptom). The regression test added alongside the fix above didn't catch this because
  `testReconnectOptions`'s `ConnectTimeout` (2s) so outweighs its `HelloTimeout` (50ms) that the ctx deadline
  never has a chance to win. Fixed with no default or signature change: `dialAndHandshake`'s ctx is now
  `cl.rc.ConnectTimeout + cl.opts.HelloTimeout` (the latter already `SetDefaults()`'d on `cl.opts` by
  `NewClient`, never the zero value) -- long enough to cover both budgets in sequence rather than racing
  them. `ReconnectOptions.ConnectTimeout`'s doc now says so explicitly, including the worst realistic
  per-attempt duration (`ConnectTimeout + HelloTimeout + ~5s` for coder/websocket's own close handshake
  against a peer that's stopped reading), and `Dial`'s doc gained the matching one-line caller-facing
  version. `TestClientHandshakePhaseWelcomeTimeoutShippedRatio` (`close_reason_test.go`) pins this with
  `ConnectTimeout == HelloTimeout`, the ratio that actually shipped, run 5 times in a loop since the bug was
  a coin flip, not a certainty -- confirmed to fail with the one-line ctx fix reverted.

  Known and accepted, no code change: if a peer's pre-welcome close and this side's hello timer land in the
  same instant, the timer can win the `claimed` `CompareAndSwap` race first, so a genuine 4011 close loses
  its one-time refresh-retry (`isUnauthorized`) and just takes the ordinary recoverable path instead --
  microsecond-wide, self-correcting on the very next attempt, not worth the complexity of closing.

- **D-2026-09-20-05** — `wsCloseCode` (`conn.go`) clamps the WS close code `fail`/`Close`/`handlePeerError`
  send to `InternalErrorCode`'s mapped code (4002) whenever `ErrorCode.CloseCode()` lands outside the legal
  WS close-code range (1000, or 4000-4999) -- i.e. any code >= 1000, including every application error code
  `>= 0x1000_0000` that remains valid for a stream RESET (`errors.go`) but was never a legal *connection*
  close code to begin with. Implements
  [`ws-mixer-spec`'s `D-2026-09-20-03`](https://github.com/mcpwarp/ws-mixer-spec/blob/main/docs/DECISIONS.md)
  and the accompanying MUST in `WIRE.md` §2.8, which now gives the mechanical `ws_close = 4000 + error_code`
  rule this exact clamp for a code it otherwise has no answer for (the JS SDK implements the same). Go
  specifics: `coder/websocket` reacts to an invalid status code by sending no close frame at all and
  aborting the transport outright (`close.go`'s `writeClose` returns before ever writing), which would leave
  the peer nothing but a bare 1006: exactly the "peer learns nothing" failure `D-2026-09-20-01` above exists
  to prevent, just reached from a different code path. `error{}` still carries the real, unclamped code
  independently, and so do `Conn.CloseCode()`/`DisconnectReason.WSCode` on both sides (their doc comments
  now say so) -- both peers agree on the semantic code even on the rare wire where the close frame itself
  said 4002.

