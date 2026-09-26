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
spec `D-2026-09-20-03`; local `-06` implements spec `D-2026-09-20-04`; local `-07` implements spec
`D-2026-09-20-05`; local `-08` implements spec `D-2026-09-20-06`; local `-09` implements spec
`D-2026-09-20-07`; local `-10` (`deliveryLoop` flush-on-close) is Go-only, with no spec counterpart; local
`-11` implements spec `D-2026-09-20-08`; local `-12` implements spec `D-2026-09-20-09`; local `-13`
(`connClosedErr`, the abnormal-closure nil-error bug) is Go-only, with no spec counterpart. Each entry below
links its own spec counterpart explicitly, so read the link, not the number.

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

- **D-2026-09-20-06** — A rejected `StaticToken` is fatal at once, reversing the "let it go through the
  normal recoverable-failure path instead of forcing fatal" call `D-2026-09-20-04` above (nit 2) originally
  made: the same fixed token can never start working, so retrying it forever with backoff only hammers the
  auth service and hides the problem from the caller instead of ever fixing it. Implements
  [`ws-mixer-spec`'s `D-2026-09-20-04`](https://github.com/mcpwarp/ws-mixer-spec/blob/main/docs/DECISIONS.md),
  which resolves the same standing `CLIENT-SDK.md`/`WIRE.md` contradiction that entry documents. `dialAndHandshake`
  (`client_reconnect.go`) now returns `markFatal(err)` from the `!isRealProvider(cl.token)` branch instead of
  the bare `err` it used to -- `classifyFailureErr`'s existing `fatalOverride`/`forced` mechanism (already
  used for a real provider's second rejection) picks this up unchanged for all three rejection shapes
  (HTTP 401 `*DialError`, `*ConnError{UnauthorizedCode}`, and a bare pre-welcome `websocket.CloseError{Code:
  4011}`), so `Phase`/`HTTPStatus`/`WSCode`/`ErrorName` all come out exactly as they would for any other
  fatal failure of that shape. A `4011` received after `welcome` is unaffected: that path was already fatal.
  A real `TokenProvider` is unaffected too: it still gets the one-time immediate refresh-retry, and only a
  second rejection of *that* is fatal, exactly as before -- see D-2026-09-20-07 below for what changed about
  when that budget itself re-arms.

- **D-2026-09-20-07** — `ReconnectOptions.StableAfter` (default 10s): the backoff `attempt` counter, and
  every once-only reconnect budget alongside it (`keepaliveRetryUsed` -- WIRE.md §2.9's 4013 row --
  and the token refresh-retry `dialAndHandshake` grants a real `TokenProvider` on a 401/4011 rejection, new
  as of this same entry: previously an unconditional per-dial-attempt grant, now `cl.refreshRetryUsed`, a
  budget like the other two), are re-armed only once a connection has stayed up `StableAfter` past its own
  `welcome` -- not at `welcome` itself. Implements
  [`ws-mixer-spec`'s `D-2026-09-20-05`](https://github.com/mcpwarp/ws-mixer-spec/blob/main/docs/DECISIONS.md),
  including its choice of 10s and the Docker-restart-policy precedent recorded there. Before this, a server
  that sent `welcome` and immediately closed reset `attempt` (and every once-only budget) on every single
  cycle -- the client redialed about once a second forever instead of ever backing off, and, for the token
  budget specifically, minted a fresh auth-service hit every cycle too. Go specifics: `Client.onAttemptSucceeded`
  no longer resets any of the three directly; instead it spawns `armStability`, one goroutine per successful
  connection (`Client.wg`-tracked, so `Close`/`CloseWith` still reap it), which races `cl.rc.after(cl.rc.StableAfter)`
  -- the same injectable clock seam every backoff/jitter delay in this file already uses, so a white-box test
  can hold it deterministic -- against that same conn's own `Done()` and the client's `closeCh`; only if the
  `after` case wins **and** the conn is still `cl.conn` at that point does it reset all three. A drain
  hand-over's retiring connection ending does not cancel the *new* connection's own timer: each `armStability`
  call is scoped to the one `*Conn` it was launched for. `case 4009, 4014`'s "start at cap" in `watchConn`'s
  close-code switch is unchanged in behavior, but its comment no longer frames it as a workaround for
  `attempt` resetting on every `welcome` (that framing is stale now that it doesn't) -- it stands on its own
  as a deliberate "refused on purpose -> back off hard immediately" rule, matching the spec entry's own
  amendment to its `D-2026-09-20-01` rationale. `ReconnectOptions.MaxAttempts`'s doc comment now says what it
  actually bounds: consecutive reconnects that never reach a stable connection, not "reconnects since the
  client started" -- a healthy client that occasionally drops and recovers is never at risk of exhausting it.

  *Amended after review:* two bugs in this same area, found by a second review pass. (1) `watchConn`'s own
  `case 4013` `else` branch (the "already used" arm) was still directly resetting `cl.keepaliveRetryUsed = false`
  before falling through to normal-backoff `reportAndSchedule` -- pre-existing code, but masked at v0.4.1 by
  `onAttemptSucceeded`'s own reset-on-`welcome`, and a real bug once that welcome-reset was removed above: the
  budget un-spent *itself* on every second `4013`, so a server flapping `4013` forever produced an immediate
  retry on every other cycle instead of ever climbing past `fullJitter(2)`. Fixed by deleting those three
  lines -- only `armStability`, at stability, re-arms it now, consistently with the rest of this entry.
  Regression-tested with four consecutive `4013`s (two was not enough to catch it: the self-reset bug also
  survives exactly one repeat), reverted-and-confirmed-failing before the fix. (2) `dialAndHandshake`'s
  refresh-retry treated ANY failure of its own retry dial as a second rejection, `markFatal`-ing it
  unconditionally -- but a DNS blip/TCP reset/HTTP 5xx landing on that one specific dial is not a rejection,
  and CLIENT-SDK.md's rule is "a second **rejection** is fatal", not "a second failure of any kind is fatal".
  Fixed: only a genuine second `isUnauthorized` result is fatal; any other failure of the retry dial takes the
  ordinary recoverable path with normal backoff -- the budget still stays spent either way, since it is
  claimed unconditionally before the retry ever dials, so a later rejection (before stability) is still fatal
  with no further refresh attempt.

- **D-2026-09-20-08** — Any dial/handshake failure strictly after `websocket.Dial`'s own upgrade (the 101)
  and before `welcome` is `Phase=handshake`, not just the `*ConnError`/`websocket.CloseError` cases already
  handled -- a transport death, an EOF, an unreadable frame count too, `WSCode` staying whatever was actually
  observed (0 for none, never fabricated). Implements
  [`ws-mixer-spec`'s `D-2026-09-20-06`](https://github.com/mcpwarp/ws-mixer-spec/blob/main/docs/DECISIONS.md),
  which the same entry's own text says was found from a Go/JS divergence review: Go reported this case as
  `dial`, JS as `handshake` with an invented close code -- neither matched the now-normative rule. Go
  specifics: marks *where* the failure happened rather than guessing from the error's type, per the spec
  entry's own preference (stated in its Go SDK review context) -- `Dial` (`client.go`) wraps whatever
  `clientHandshake` returns in a new unexported, `Unwrap`-able `*postUpgradeError` before returning it,
  unless the failure ctx itself is what's cancelled (see below), so `errors.As` for a `*ConnError`/
  `websocket.CloseError`/`*DialError` still sees straight through it; `client_reconnect.go`'s `phaseFor`
  gains one more check for it, after the existing `*ConnError`/`websocket.CloseError` ones (unaffected: a
  `*ConnError` was already `phase: handshake`, wrapping it again is a no-op classification-wise) and after
  `*providerError` (still checked first, unconditionally, exactly as before) -- `*DialError` is never
  wrapped at all (it is returned before `clientHandshake` is ever reached, or -- the subprotocol-mismatch
  case -- deliberately bypasses this entirely), so it stays `phase: dial` as the spec entry requires. A dial
  the application itself cancels (`Close`/`CloseWith` during the handshake wait -- both tear down
  `dialAndHandshake`'s shared ctx via its `closeCh` watcher -- or a plain `Dial` caller's own cancelled ctx)
  is deliberately excluded: `Dial` checks `errors.Is(ctx.Err(), context.Canceled)` before wrapping and leaves
  that one case unwrapped, `phase: dial`, matching the spec entry's "a connect the application itself cancels
  in this window is not a handshake failure". `clientHandshake`'s own message text for the two cases this
  uncovers no longer claims a welcome timeout that didn't happen: a cancelled ctx says "hello/welcome wait
  ended", a genuine transport death says "connection lost before welcome completed the handshake".

  *Amended after review:* the cancellation check above originally read `ctx.Err() != nil`, not
  `errors.Is(ctx.Err(), context.Canceled)` -- a bug a second review pass probed and confirmed: `ctx.Err()`
  is also non-nil for a `context.DeadlineExceeded`, and `dialAndHandshake`'s one ctx covers
  `ConnectTimeout+HelloTimeout` in sum, so a dial that eats deep enough into that combined budget before the
  101 even lands can make the shared ctx's own deadline expire strictly *after* the 101 (during the welcome
  wait) but strictly *before* `clientHandshake`'s own separate `HelloTimeout` `time.AfterFunc` -- timed fresh
  from when the welcome wait itself starts, not from ctx creation -- ever gets a chance to fire. That is a
  genuine post-101 timeout, not an application cancellation, and the old check misclassified it `phase: dial`
  regardless (probed: `ConnectTimeout=1s`, `HelloTimeout=1s`, server delays the 101 by 1.2s). Fixed by checking
  specifically for `context.Canceled` -- `Close`/`CloseWith`'s own explicit `cancel()` call still produces
  exactly that, so the intended exemption is unaffected; a plain `Dial` caller's own
  `errors.Is(err, context.DeadlineExceeded)` check still sees straight through `*postUpgradeError`'s `Unwrap`
  either way, so wrapping a natural deadline expiry no longer changes what that sentinel check finds, only
  the `Phase` `Client` derives from it.

- **D-2026-09-20-09** — `Client.CloseWith(ctx, code ErrorCode, message string) error`: implements
  [`ws-mixer-spec`'s `D-2026-09-20-07`](https://github.com/mcpwarp/ws-mixer-spec/blob/main/docs/DECISIONS.md)
  (CLIENT-SDK.md's "Application close" row) for the reconnecting `Client`, which previously had no
  application-close API of its own -- calling `Conn.Close` directly on a connection `Client` was managing
  just looked like an ordinary disconnect to its reconnect loop, which redialed. Takes an `ErrorCode`, not
  the `uint32` `Conn.Close` does: `CloseWith`'s whole point is being the `Client`-level counterpart to
  `Conn.Close` for exactly the `ApplicationCloseCode` use case that constant's own doc comment already
  describes, so a call site reads as `cl.CloseWith(ctx, wsmixer.ApplicationCloseCode, "bye")` rather than
  forcing a `uint32` conversion at the call site for the common case; it converts internally where
  `Conn.Close`'s own signature requires it. Shares `Close`'s own machinery rather than duplicating it: a new
  `closeStart`/`closeAlreadyClosing` pair, factored out of `Close`'s previous inline body with no behavior
  change (`Close` itself is now `closeStart` plus its existing drain-then-close body), makes exactly the same
  state transitions `Close` always made -- `cl.closing`/`cl.state`/claiming `cl.gracefulConn`, stopping the
  backoff loop, resolving `Connect`'s first result -- so no reconnect is ever scheduled after either call, and
  the two are mutually idempotent (`closeStart`'s `cl.closing` check picks exactly one winner regardless of
  which of `Close`/`CloseWith` a racing caller used). Unlike `Close`, there is no drain/grace period: the live
  connection, if any, is closed immediately with `conn.Close(uint32(code), message)` -- one non-fatal
  `DisconnectReason` is reported for it, but (like `Close`'s own single report) not synthesized directly:
  `watchConn` observing that `conn.Close` call end the connection, with `cl.closing` already true, is the same
  mechanism that produces `Close`'s own one report. `wsCloseCode` clamping (`D-2026-09-20-05` above) applies
  exactly as for any `Conn.Close`, automatically, with no special handling needed here.
  `conformance/adapter/adapter.go`'s `close` command previously dropped `code`/`message` entirely for a
  `Client`-driven connection (routing everything through plain `Client.Close`, S4(c)'s original scope); it now
  calls `Client.CloseWith` for a non-zero `code` and `Client.Close` only for `0`/absent, per
  `docs/CONFORMANCE.md`'s `close` row.

  *Amended after review:* two bugs found by a second review pass, both pre-existing in `Close`'s own body
  before this entry's `closeStart` extraction (so present in `Close` too, not introduced by `CloseWith`).
  (1) `watchConn`'s own `cl.state = clientDisconnected` write (on the active conn ending) was unconditional --
  it could land after `closeStart` had already set `cl.state = clientClosed` but still before `wg.Wait()`
  returned, since `watchConn` is itself one of the goroutines `wg.Wait()` waits on: a connected client's
  `Close()`/`CloseWith()` call always returned with `State()=="disconnected"`, never `"closed"` (measured
  20/20). Every other writer of `cl.state` was checked for the same hazard: `goFatal`, `onAttemptFailed`'s
  fatal branch, and `reportAndSchedule`'s exhaustion/disabled branch all nil `cl.conn` in the very same
  critical section they set `clientClosed` in, which makes `watchConn`'s own `wasActive` check false before
  it would ever get a chance to overwrite the state -- only `closeStart` sets `clientClosed` without nil-ing
  `cl.conn` (the caller still needs to close/drain it itself), so it was the one path exposed. Fixed by
  guarding `watchConn`'s write on `cl.state != clientClosed`, a no-op for the already-safe fatal paths.
  (2) During a drain hand-over's window, `cl.conn` and `cl.retiringConn` can point at the very same live conn
  (the ownership invariant above `Close`) -- `Close`/`CloseWith` closing `retiring` on its own goroutine could
  race their own graceful/immediate close of `conn` on `Conn.Close`'s `closeOnce`, and losing that race would
  send `NO_ERROR`/1000 instead of the intended close. `closeLiveConns` (the fatal-path equivalent) already
  guarded with `retiring != conn`; `Close` and `CloseWith` now do too. In practice this race is narrow and
  hard to force deterministically (confirmed: a regression test using a gated second dial to hold the
  hand-over window open still passed 3/3 with the guard removed, since the synchronous `conn.Close` call a
  few lines below the retiring goroutine's dispatch consistently wins in practice) -- kept as
  defense-in-depth, matching the existing fatal-path guard, not as a reliably-reproducible bug fix.

- **D-2026-09-20-10** — `Conn.deliveryLoop` (`conn.go`) no longer drops an already-queued OnStream/OnApp/
  OnDrain event when the connection closes. Go-only, no spec counterpart (the wire behavior this fixes --
  every event received strictly before `error{}`, the always-last message on the wire, must still be
  delivered -- was already the intended contract; this is a Go implementation bug, not a protocol question).
  Diagnosed by a reviewer: the read loop (`dispatch.go`) enqueues an event with a non-blocking, buffered send
  and, without yielding, reads the next frame; when that next frame is the peer's `error{}`,
  `handlePeerError` closes the conn. By the time `deliveryLoop`'s goroutine is next scheduled, `c.closed` is
  already closed -- and the loop used to check `c.closed` with priority over `c.deliveryQueue` on every
  iteration (`select { case <-c.closed: return; default: }` ahead of the real select), so it returned without
  draining anything, even past that check the second `select` was a coin flip. Net effect: an `app` message
  (or `drain`, or a stream `OPEN`) that arrived just before a connection-fatal `error{}` was silently never
  delivered, violating `ClientOptions.OnApp`/`Conn.OnApp`'s own "called for every incoming app message"
  promise and diverging from the JS SDK, which delivers the same queued backlog. A deterministic unit test
  (queue two `app` events on `c.deliveryQueue`, close `c.closed`, then run `deliveryLoop`) failed 100% before
  the fix; the existing end-to-end `TestClientApplicationCloseIsRecoverable` flaked intermittently under
  `GOMAXPROCS=1 -race` for the same reason.

  Fixed by removing the priority pre-check: `deliveryLoop`'s only `select` now has just the two original
  cases, and its `<-c.closed` branch calls a new `flushDeliveryQueue` before returning -- a non-blocking inner
  loop (`select { case ev := <-c.deliveryQueue: ...; default: return }`) that drains whatever is already
  queued, in order, bounded at `cap(c.deliveryQueue)` iterations so it terminates unconditionally regardless
  of what could still be sending to the queue (in practice nothing is: `readerLoop`, the only producer, has
  always already stopped calling `dispatch` by the time anything closes `c.closed`). Per-event handler
  invocation is factored into a new `deliverEvent` helper so both the live path and the post-close flush
  invoke OnStream/OnApp/OnDrain identically, in the same wire order. `deliveryLoop`'s doc comment, which used
  to document the drop as intentional ("at most one more handler may start after close"), now says the truth:
  every already-queued event is still delivered after close, possibly briefly after `Conn.Close` returns or
  `Done()` fires -- `Conn.Close`'s own doc comment and `OnApp`'s (referenced by `OnStream`/`OnDrain`) now say
  the same.

  Checked for knock-on effects rather than assumed safe: (1) a *local* `Close()`/`fail()` with a backlog
  queued -- those events were received before the close, so delivering them is consistent with the wire, and
  nothing in `Client` (`client_reconnect.go`) relies on "no OnApp/OnStream/OnDrain after I called Close":
  `Client.Close`/`CloseWith` tear down no per-connection application state of their own after the conn call
  returns, and `handleServerDrain` (the one handler that could plausibly start a reconnect from a queued
  `drain` event) checks `cl.closing || cl.state == clientClosed` as its very first step under `cl.mu`, before
  ever touching `cl.retiringConn`/`drainReconnectScheduled` -- so a `drain` delivered from the flush after the
  application already called `Client.Close` still invokes the caller's own `OnDrain`, but never starts a
  reconnect on a closing client. (2) an `OnStream` event for a stream `OPEN` queued right before the conn
  died hands the handler a `*Stream` on an already-dead conn: `Stream.wait` (`stream.go`) selects on
  `s.conn.closed` alongside its context and its own notify channel, so `Read`/`Write` on that stream return
  the conn's own error promptly rather than hang -- confirmed by
  `TestDeliveryLoopFlushesQueuedOpenEventOnClose`, which reads from the delivered stream and asserts the read
  returns within 2s. This does depend on `c.err` already being non-nil by the time `c.closed` closes, which
  every real close path (`fail`/`Close`/`handlePeerError`) guarantees by construction (each sets `c.err`
  under `c.mu` in the same critical section that leads to closing `c.closed`) -- the test sets `c.err`
  explicitly for the same reason before closing `c.closed` itself, since it drives the conn directly rather
  than through a real close path. (3) the server role is identical: `deliveryLoop`/`flushDeliveryQueue`/
  `deliverEvent` are role-agnostic (`Conn`, not `Client`), and a server's own `OnStream`/`OnApp`/`OnDrain`
  registered via `Conn.OnStream`/`OnApp`/`OnDrain` directly get exactly the same flush.

  Tests (`wsmixer/conn_test.go`): `TestDeliveryLoopFlushesQueuedAppEventsOnClose` (the core regression, two
  queued `app` events delivered in order after close), `TestDeliveryLoopFlushesQueuedDrainEventOnClose`,
  `TestDeliveryLoopFlushesQueuedOpenEventOnClose` (with the dead-stream read-promptness check above),
  `TestDeliveryLoopFlushOrderingMixed` (open/app/drain interleaved, still delivered in enqueue order), and
  `TestDeliveryLoopFlushIsBounded` (a queue filled to `cap(c.deliveryQueue)` is still fully flushed and the
  loop still returns). `wsmixer/errors_test.go` adds `TestClientDrainThenErrorCloseDeliversOnDrain`, the
  end-to-end drain variant: a server sends `drain{}` then immediately fails the connection with an
  unrelated error (`c.sendControl` + `c.fail`, post-`Run` so `deliveryLoop` is actually the thing exercised --
  `fail`'s own pre-`Run` branch, unlike `Close`'s, does not flush `controlQueue`, a separate, narrower gap
  this entry does not touch), and the client's `OnDrain` still fires. Revert-proofed: temporarily restored the
  old priority pre-check, confirmed `TestDeliveryLoopFlushesQueuedAppEventsOnClose` fails 100% (`delivered app
  bodies = []`), then restored the fix and confirmed via `git diff` the file matched the fixed version.
  `GOMAXPROCS=1 go test -race -count=200 -timeout 1500s -run 'TestClientApplicationCloseIsRecoverable$'
  ./wsmixer/...`: 0/200 failures.

- **D-2026-09-20-11** — `ErrTokenUnavailable` (`client_reconnect.go`): implements
  [`ws-mixer-spec`'s `D-2026-09-20-08`](https://github.com/mcpwarp/ws-mixer-spec/blob/main/docs/DECISIONS.md),
  CLIENT-SDK.md's "Provider failure" row. A `TokenProvider` error used to be unconditionally fatal
  (`classifyFailureErr`'s `*providerError` branch always returned `fatal: true`); a provider can now mark one
  failure as temporary -- it couldn't OBTAIN a token for a transient reason (the network is briefly down, the
  auth server is unreachable mid-refresh), not that the token itself is bad -- by wrapping the new exported
  sentinel `var ErrTokenUnavailable = errors.New(...)`, matched with `errors.Is` only. `classifyFailureErr`'s
  `*providerError` branch (checked first and unconditionally, as before) now branches on
  `errors.Is(pe.cause, ErrTokenUnavailable)`: marked -> `phase: PhaseDial, fatal: false`, `Cause` still the
  provider's error verbatim (the same object, unwrapped one level, that the unmarked path already surfaced);
  unmarked -> `fatal: true`, exactly as before. Everything downstream of that classification is then the
  ordinary recoverable path with no further change needed: `onAttemptFailed` -> `reportAndSchedule` ->
  `fullJitter(next)`, `attempt` increments, `MaxAttempts` still applies (exhaustion still goes fatal, with the
  usual message), `ReconnectOptions.Disabled` behaves as for any other non-fatal failure, `StableAfter`'s
  stability rule is untouched, and there is no immediate retry.

  Detection is opt-in only, `errors.Is` alone -- no `Retryable()`/`Temporary()` duck-typing on the error's
  shape, matching the spec entry's explicit anti-inference rule (an accidental match on an otherwise-fatal
  provider error would silently turn it into an endless retry loop instead of ever reaching the caller).
  Pinned by `TestClientDuckTypedRetryableErrorStaysFatal`
  (`wsmixer/client_token_unavailable_test.go`): an error with both `Retryable() bool` and `Temporary() bool`
  methods but that does not wrap or `Is`-match `ErrTokenUnavailable` stays fatal.

  A `*fatalOverride` is never applied to a marked (or any) `*providerError`: `classifyFailureErr`'s
  `*providerError` check runs first, unconditionally, and always returns before the `*fatalOverride`/`forced`
  check further down the function is ever reached -- and `dialAndHandshake` never wraps a `*providerError` in
  `markFatal` in the first place (every `markFatal` call site there acts on the dial/handshake failure `err`
  after a successful token fetch, never on a token-fetch failure itself). The refresh-retry path
  (`dialAndHandshake`'s one-time 401/4011 immediate refresh-retry) needed no code change either: its own
  second token call already returns `&providerError{cause: err2}` on failure, which flows through the same
  now-marked-aware classification -- a marked failure there is non-fatal with `refreshRetryUsed` staying
  spent (claimed unconditionally before the retry ever calls the provider), exactly like a non-rejection
  failure of the retry's own dial already worked; a later rejection before stability still goes straight to
  `markFatal` with no further provider call, since the budget is already spent.

  First `Connect`: a marked failure on the very first attempt behaves identically to any other recoverable
  first-attempt failure (e.g. a plain dial-layer network error) -- `Client.Connect` blocks on
  `firstResultCh`, which a non-fatal `reportAndSchedule` never resolves; it keeps waiting through the
  scheduled retry and only returns once a later attempt actually welcomes, or the client eventually goes
  fatal (unmarked failure, or `MaxAttempts` exhaustion). Pinned by
  `TestClientMarkedProviderFailureNonFatalReconnects`, whose provider fails marked on call 1 and succeeds on
  call 2: `Connect` returns nil only after the second call.

  Tests (`wsmixer/client_token_unavailable_test.go`):
  `TestClientMarkedProviderFailureNonFatalReconnects` (one non-fatal report, `Cause` both `errors.Is`-matches
  and is the exact provider object, `fullJitter` backoff, the next attempt calls the provider again and
  connects), `TestClientMarkedProviderFailureExhaustsMaxAttempts` (repeated marked failures climb the
  `fullJitter` ladder exactly like an unmarked recoverable failure and still exhaust `MaxAttempts` to fatal),
  `TestClientCustomIsMethodTreatedAsMarked` (a custom error type implementing `Is` rather than `%w`-wrapping),
  `TestClientDuckTypedRetryableErrorStaysFatal` (the anti-duck-typing pin above),
  `TestClientMarkedRefreshRetryThenLaterFatal` (first dial 401, the refresh-retry's own provider call returns
  marked -> non-fatal, budget spent; a later 401 before stability, with the budget already spent, goes
  straight to fatal without a further provider call). `TestClientProviderErrorFatal` (unmarked, pre-existing)
  is unchanged and still passes. README's token section and the `TokenProvider`/`ErrTokenUnavailable` doc
  comments describe the marker and its opt-in rationale; `defaultSDKVersion`, `spec.pin`, and `COUNTS.json`
  are deliberately untouched (owner instruction: this entry adds no new wire behavior for the spec's own
  fixture/version tracking to pin).

- **D-2026-09-20-12** — Bare-close `ErrorCode`/`HasErrorCode`/`ErrorName` derivation: implements
  [`ws-mixer-spec`'s `D-2026-09-20-09`](https://github.com/mcpwarp/ws-mixer-spec/blob/main/docs/DECISIONS.md).
  Previously, `DisconnectReason.ErrorCode`/`HasErrorCode`/`ErrorName` were populated only when a ws-mixer wire
  `error{}` message actually preceded the disconnect (a `*ConnError`); a **bare** close -- a close frame
  carrying a WS code in ws-mixer's own reserved range (4001-4999) with no preceding `error{}`, e.g. because
  the frame was lost, mirroring the existing `CloseReason` gap this package already handles the same way --
  left all three unset, even though `ws_close = 4000 + error_code` (`WIRE.md` section 2.8) makes the mapping
  entirely mechanical and therefore never wrong. New `deriveBareCloseErrorCode(wsCode int) (code ErrorCode,
  has bool)` (`client_reconnect.go`) implements exactly that arithmetic, excluding 4000 (`NoError`)
  deliberately -- this package's own `Conn.Close` always sends `error{NoError,...}` ahead of a graceful close,
  so a genuinely bare 4000 would itself be a peer bug this derivation has no basis to paper over -- and
  excluding everything outside 4001-4999 (an ordinary WS close such as 1000/1001/1006/1007/1009/1011, or no
  close observed at all, wsCode 0): none of those are ws-mixer error codes, and `ErrorCode` must never be
  synthesized for them.

  Wired into both of `DisconnectReason`'s construction sites, the only two places that populate it:
  `classifyFailureErr`'s bare-`websocket.CloseError` fallback (handshake phase -- previously set `wsCode`/
  `closeReason` only) and `buildConnectedDisconnectReason`'s non-`*ConnError` branch (connected phase --
  previously set `WSCode` from `conn.PeerCloseCode()` only). Both call the same helper, so the two phases
  derive identically. An HTTP upgrade rejection (401/403/404/429/5xx, `*DialError`) is unaffected: that branch
  of `classifyFailureErr` returns before ever reaching the derivation, so `HasErrorCode` stays false and
  `HTTPStatus` alone carries the failure, exactly as the spec entry requires (the JS SDK's own now-fixed bug
  of labeling an HTTP 401/403 `UNAUTHORIZED` never existed on the Go side to begin with -- confirmed by
  reading `classifyFailureErr`'s `*DialError` branch, which sets no `ErrorCode` field at all).

  Report-only, confirmed by reading every consumer of these fields rather than assumed: `effectiveWSCode`
  (`watchConn`'s close-code switch) reads `conn.Err().(*ConnError)`/`conn.PeerCloseCode()` directly, never
  `DisconnectReason.ErrorCode`/`HasErrorCode`; `isUnauthorized` and the 4010-fatal/4011-refresh-retry
  classification inside `classifyFailureErr` itself all decide on `wsCode`/the error's own type via
  `errors.As`, computed and branched on before this derivation runs (the derivation is the last thing
  `classifyFailureErr`'s fallback does before returning); no other function reads `fi.errCode`/
  `fi.hasErrorCode` before this entry's change. `conformance/adapter/adapter.go`'s `OnDisconnect` handler
  already emits `error_code`/`error_name` whenever `r.HasErrorCode` is true (pre-existing code, unchanged) --
  a bare close now automatically carries those fields across the conformance harness with no adapter change
  needed, so a Go server's bare close reports the same shape a JS client's would.

  `DisconnectReason`'s `ErrorCode`/`HasErrorCode`/`ErrorName` field doc comment, and README's `DisconnectReason`
  paragraph, now describe both cases (`error{}`-derived and bare-close-derived) instead of only the former.

  Existing bare-close tests that asserted `HasErrorCode == false` were auditing exactly the gap this entry
  closes, so they were updated deliberately, not incidentally: `TestClientBareCloseReasonPostConnect` and
  `TestClientHandshakePhaseBareCloseReason` (bare 4009 -> now `EnhanceYourCalm`/`ENHANCE_YOUR_CALM`),
  `TestClientApplicationCloseBareStartsAtCap` (bare 4014 -> `ApplicationCloseCode`/`APPLICATION_CLOSE`), and
  `TestClientHandshakePhaseBareClose4010Fatal`/`TestClientHandshakePhaseBareClose4011OneRetryThenFatal` (bare
  4010/4011 -> `UnsupportedCode`/`UnauthorizedCode`, previously unchecked) each gained the new
  `HasErrorCode`/`ErrorCode`/`ErrorName` assertions matching the derived values. New tests:
  `TestClassifyFailureErr` (`client_reconnect_test.go`) gained table cases for a bare close in range (4014),
  an unknown ws-mixer code (4777 -> `INTERNAL_ERROR`), a bare 1001 (no derivation), an `error{}`-preceded 4009
  (unaffected, message's code wins), and HTTP 401/403/503/abnormal-closure (`HasErrorCode` false in every
  case) -- its assertion loop was also strengthened to check `hasErrorCode` unconditionally (previously only
  when a case expected `true`, which could never have caught a false positive).
  `TestBuildConnectedDisconnectReasonBareCloseDerivation` unit-tests the connected-phase site directly against
  a hand-built `*Conn` (no network): bare 4014 in range, bare 1001, and an abnormal closure with no close
  frame observed at all.

  *Amended after review:* the spec entry actually names **three** sources for `errorCode`/`errorName`, not
  two -- the third being a ws-mixer error the SDK itself raises locally, with no close frame from the peer at
  all. Two of that third source's own cases were already covered without any change, because Go's
  implementation happens to route them through the same `*ConnError` branch as an actual peer `error{}`
  message (the welcome timeout's locally generated `PROTOCOL_ERROR`/4001, and any other local
  protocol-violation `fail()`): `classifyFailureErr`'s `var ce *ConnError; if errors.As(err, &ce)` branch does
  not care whether the `*ConnError` came from the peer or from this side's own `fail()`, so both already set
  `HasErrorCode`/`ErrorCode` correctly before this amendment. The one case that was NOT covered: a
  missing/mismatched subprotocol echo on the upgrade (`Dial`, `client.go`) is a `*DialError{Mismatch: true}`,
  not a `*ConnError` -- `classifyFailureErr`'s `*DialError` branch returned before ever reaching the bare-close
  derivation above, so it reported `Phase: dial, Fatal: true, HasErrorCode: false` even though WIRE.md's Fatal
  set already means this is `UNSUPPORTED` (4010) by definition. Fixed: the `*DialError` branch now sets
  `fi.errCode = UnsupportedCode, fi.hasErrorCode = true` when `de.Mismatch` is set -- report-only, `de.Fatal`
  (already `true` for `Mismatch`) still decides fatality, unaffected. The JS SDK already reports the same code
  for this case. `DisconnectReason`'s field comment and README's paragraph now describe all three sources.
  Test: `TestClassifyFailureErr`'s "subprotocol mismatch fatal" case gained `hasCode: true, wantCode:
  UnsupportedCode` (previously unchecked, now also pinning `WSCode 0`/`HTTPStatus 0` via the unconditional
  `hasErrorCode` check the same amendment added).

- **D-2026-09-20-13** — `Conn.connClosedErr` (`conn.go`). Go-only, no spec counterpart (WIRE.md section 2.9
  already requires a handler be able to tell EOF from a dead tunnel; this is a Go implementation bug, not a
  protocol question). Pre-existing, found by a reviewer, untouched by any other entry in this file's
  `2026-09-20` section: when a connection dies ABNORMALLY -- a TCP reset, a plain EOF, a read timeout, the
  peer calling `CloseNow` with no close frame at all -- `handleReadError` (`dispatch.go`) deliberately leaves
  `c.err` nil (`if code != -1 && c.err == nil { c.err = ... }`: `websocket.CloseStatus` returns `-1` for
  anything that isn't an actual `websocket.CloseError`), then closes `c.closed`. Nothing sweeps the stream
  table on an abnormal conn death, so a live `*Stream`'s `wait()` (`stream.go`) hit
  `case <-s.conn.closed: return s.conn.Err()` and got back **nil**. `ReadContext`'s loop treats a nil
  `wait()` error as "state may have changed, recheck": it re-armed on a fresh `notifyCh` nobody would ever
  close and called `wait()` again, which -- `s.conn.closed` already closed -- hit the very same case again,
  forever: a hot spin, `Read` never returning. `WriteContext`'s two identical `return total, s.conn.Err()`
  sites had the mirror-image bug: a short byte count with a *nil* error, a dead tunnel silently reported as a
  successful (if partial) write.

  Fixed without touching `handleReadError`'s condition (which decides `DisconnectReason.Message` for an
  abnormal closure -- report-neutral, not this entry's concern) by adding
  `func (c *Conn) connClosedErr() error`, which returns `c.Err()` when non-nil, else `io.ErrUnexpectedEOF` --
  never nil. Swept the whole package for `conn.Err()`/`c.Err()` read immediately after observing `<-...closed`
  and switched every one that returns the conn's error to a caller after that observation:
  `stream.go`'s `wait()` (the `case <-s.conn.closed:` in the shared helper both `ReadContext` and
  `reserveSendCredit`/`WriteContext`'s credit wait go through), `WriteContext`'s own two direct
  `case <-s.conn.closed:` branches (the enqueue select and the post-enqueue "wait for done" select),
  `streams.go`'s `OpenStream` (the stream-slot-wait select), and `drain.go`'s `Drain` (the wait-for-drained-or-
  timeout select). Checked and left alone: `SendApp`/`sendControl`/`sendControlFrame` never call `conn.Err()`
  at all -- `sendControlFrame`'s own `case <-c.closed:` just drops the frame silently with no return value,
  a separate, pre-existing "no error reported" gap unrelated to the nil-vs-`io.ErrUnexpectedEOF` shape this
  entry fixes (reported, not fixed here); `allocateStreamLocked`/`OpenStream`'s own `if c.err != nil` guards
  are unaffected too, since they only ever act when `c.err` is already non-nil, and don't observe `c.closed`
  directly at all (a separate, narrower gap -- `OpenStream` can still register and appear to succeed on a
  conn that died abnormally moments earlier but left `c.err` nil, since nothing there checks `c.closed`
  itself; reported, not fixed here, out of this entry's scope).

  `io.ErrUnexpectedEOF` cannot be confused with a clean half-close: a peer `CLOSE` still resolves via
  `ReadContext`'s own `s.eof`/`s.err` checks, entirely before `wait()` (let alone `connClosedErr`) is ever
  reached, so `Read` keeps returning exactly `io.EOF` for that case; a `RESET` resolves the same way via
  `s.err`, a `*StreamError`; and `Conn.fail` (a real protocol violation, not an abnormal closure) sets
  `c.err` to a `*ConnError` before `c.closed` fires, so `connClosedErr` returns that verbatim, never falling
  through to `io.ErrUnexpectedEOF`.

  Tests (`wsmixer/stream_abnormal_close_test.go`): `TestReaderLoopAbnormalCloseLeavesConnErrNil` pins the
  premise itself against the real `readerLoop`/`handleReadError` path (`fakeWS.CloseNow()`, no close frame --
  `Conn.Err()` stays nil). `TestStreamReadReturnsPromptlyOnAbnormalClose` (a blocked `Read`, then abnormal
  close, asserts the read returns within a bounded 2s, the error is non-nil and not `io.EOF`, wraps
  `io.ErrUnexpectedEOF`, and a SECOND `Read` afterward also returns promptly -- pinning the hot-spin
  specifically) and `...WhenStartedAfterAbnormalClose` (the same, `Read` only called once already dead).
  `TestStreamWriteFirstSelectReturnsErrorOnAbnormalClose`/`...SecondSelectReturnsErrorOnAbnormalClose` cover
  `WriteContext`'s two `s.conn.closed` branches separately, deterministically (an un-run `*Conn` via
  `newTestSchedConn`, so nothing ever drains the queued chunk; the first test fills `outQueue` to capacity so
  the enqueue select can't race a same-instant successful send). `TestStreamReadPeerCloseStillReturnsIOEOF`/
  `...ResetStillReturnsStreamError`/`...ConnFailStillReturnsConnError` pin the three normal paths unchanged.
  Revert-proof: temporarily reverted `wait()`'s `s.conn.closed` case back to `return s.conn.Err()`,
  `TestStreamReadReturnsPromptlyOnAbnormalClose` failed as designed (`Read did not return within 2s of an
  abnormal close (hot spin)`, the test's own bound firing rather than the process hanging), then restored the
  fix (confirmed via `git diff` the file matched the fixed version).

  *Amended after review:* a second review pass, after this entry first shipped, measured three further
  problems in the same area, all fixed here rather than as separate entries since they are the same feature.

  (1) SHOULD-FIX, a regression this entry itself introduced: a reader parked in `wait()` when the peer's
  clean `CLOSE` (`s.eof`) and the conn's own death become ready in the *same instant* could still lose --
  `ReadContext` returned `wait()`'s error immediately with no re-check of `s.buf`/`s.eof`/`s.err`, so taking
  the `<-s.conn.closed` case now returned `connClosedErr`'s `io.ErrUnexpectedEOF` even though the stream had
  in fact ended cleanly (WIRE.md section 2.9: a handler must be able to tell "the response ended" from "the
  tunnel died"). Measured 400/400 wrong on the unfixed-for-this-amendment tree under a synchronization
  technique built to force the race deterministically (see the tests below) vs 400/400 correct with `wait()`
  reverted to its pre-`connClosedErr` form -- the old `return s.conn.Err()` happened to return nil for an
  abnormal closure, which sent `ReadContext`'s loop back around to re-check `s.eof` itself; that accidental
  re-check is exactly what this entry's own fix removed. With a NON-nil conn error (a peer `error{}`, a local
  `Close`, a keepalive timeout) both trees already lost the clean `io.EOF` on this same race, a pre-existing
  defect neither tree fixed until now. Fixed in ONE place, `ReadContext` (`stream.go`): when `wait()` returns
  an error, re-lock `s.mu` and only return that error if `len(s.buf) == 0 && !s.eof && s.err == nil`;
  otherwise `continue` the loop (which re-locks at its own top, matching the loop's existing lock invariant)
  so buffered data, a clean `CLOSE` (`io.EOF`), or a `RESET` (`*StreamError`) that landed in the same instant
  wins, per WIRE.md section 2.5 ("CLOSE preserves buffered data; RESET discards it"). Write side: checked for
  the analogous problem and found one, in `reserveSendCredit` (not `WriteContext`'s own two `s.conn.closed`
  selects, which have no data to lose -- a chunk that "successfully" reserved credit in this same race still
  has to clear `WriteContext`'s own `outQueue` select, which independently observes `s.conn.closed` and
  returns `connClosedErr` there, so there is no equivalent loss on that path): a `RESET` (which sets `s.err`)
  landing in the same instant `wait()` observes the conn dying could have its `*StreamError` masked by
  `wait()`'s own (possibly `connClosedErr`-synthesized) error, for the identical reason. Fixed the same way,
  one place: re-check `s.err` after `wait()` errors and `continue` (not return `wait()`'s error) if it is now
  set.

  Deterministic test technique (`wsmixer/stream_race_close_test.go`): parking a reader and then triggering
  both events through the normal `handleClose`/`handleReset`/`close(c.closed)` calls, in either goroutine
  order, turned out NOT to reproduce the race reliably in this environment -- the runtime schedules a freshly
  spawned reader goroutine fast enough that it almost always resolves via the notify channel alone before a
  second, sequentially-issued mutation even runs. The tests instead set the stream's terminal state DIRECTLY
  under `s.mu` (skipping `notifyLocked`, so the stream's own notify channel is deliberately never closed) and
  then close `c.closed` -- forcing `wait()` through its `<-s.conn.closed` branch on every single trial, with
  the terminal state already sitting there for `ReadContext`'s re-check to find, deterministically and
  independently of `wait()`'s own already-proven-correct channel-select nondeterminism. Five tests:
  `TestParkedReadCleanCloseRacingAbnormalConnDeathReturnsIOEOF`,
  `TestParkedReadBufferedDataRacingAbnormalConnDeathReturnsData` (the data first, a second `Read` then
  `io.EOF`), `TestParkedReadResetRacingAbnormalConnDeathReturnsStreamError`, and the two non-nil-conn-error
  variants `TestParkedReadCleanCloseRacingPeerErrorReturnsIOEOF` (`handlePeerError`)/
  `...RacingLocalFailReturnsIOEOF` (`Conn.fail`). Revert-proof: temporarily reverted `ReadContext`'s re-check
  back to returning `wait()`'s error immediately -- all five failed deterministically (100%, not a
  probabilistic sample), then restored the fix and re-confirmed all five pass.

  (2) `Client.Close` (`client_reconnect.go`) could now return `io.ErrUnexpectedEOF` for an entirely ORDINARY
  shutdown: `Close` assigns `Conn.Drain`'s result to its own return value, and `Drain`'s `case <-c.closed:`
  branch (this entry's original change) now returns `connClosedErr`'s `io.ErrUnexpectedEOF` whenever the
  peer's transport dies abnormally -- no close frame -- while this side is still waiting out its own
  `drain{client_requested}`. `Drain` itself stays truthful (a real caller of `Drain` directly still needs to
  know this happened), but `Close`'s own contract is that the client ends up closed, not that the peer
  completed the close handshake -- a peer that simply drops the connection mid-drain is exactly as "closed"
  from the caller's point of view as one that finishes gracefully. Fixed by filtering
  `errors.Is(err, io.ErrUnexpectedEOF)` to nil at `Close`'s own `conn.Drain(...)` call site, the one place
  whose result `Close` actually returns to its caller. Checked every other `Drain` consumer: `CloseWith`
  never calls `Drain` at all (it closes the live conn directly with `Conn.Close`, which -- unlike `Drain` --
  always returns nil regardless of how the conn ends, so it was never affected); the only other call site,
  `allocateStreamLocked`'s `go c.Drain(...)` on stream-id exhaustion (`streams.go`), already discards its
  result (fire-and-forget). Test: `TestClientCloseTreatsAbnormalDrainEndAsSuccess`
  (`client_reconnect_test.go`) -- a server that drops the raw transport (`OnRawConn` + `ws.CloseNow()`, no
  close frame) the instant it observes the client's `drain{client_requested}` (`Conn.OnDrain`), with one
  stream deliberately left open so `Drain`'s own `waitForDrainedOrEmpty()` doesn't resolve before the
  transport dies (an empty stream table would resolve `Drain`'s select before ever reaching its
  `s.conn.closed` branch, missing the case entirely) -- asserts `Close()` returns nil, `State()` is
  `"closed"`, and exactly one non-fatal `DisconnectReason` is reported. Revert-proof: temporarily removed the
  filter, the test failed with `Close() = unexpected EOF` (measured 4/5 runs, since `waitForDrainedOrEmpty`
  occasionally still won its own race against the abnormal close), then restored the filter and confirmed
  20/20.

  (3) `OpenStream` (`streams.go`) on an abnormally-dead conn allocated a stream id and returned `(st, nil)` as
  if it had succeeded: every guard inside its loop (and `allocateStreamLocked`) checks `c.err != nil`, never
  `c.closed` itself -- an abnormal closure leaves `c.err` nil (same root cause as this entry's original
  change), so all of them pass straight through. Fixed with a cheap, non-blocking guard at the very top of
  `OpenStream`, before its loop: `select { case <-c.closed: return nil, c.connClosedErr(); default: }`.
  Test: `TestOpenStreamOnAbnormallyDeadConnFailsCleanly` (`wsmixer/stream_abnormal_close_test.go`) -- a conn
  killed abnormally via `fakeWS.CloseNow()`, then `OpenStream` must return a non-nil error
  (`io.ErrUnexpectedEOF`) and a nil `*Stream`, with `c.highestOpened` unchanged (nothing allocated).
  Revert-proof: temporarily removed the guard, the test failed (`OpenStream succeeded ... on an
  abnormally-dead conn`), then restored it and re-confirmed.

  (4) Doc nits from the same review: `providerError`'s doc comment (client_reconnect.go) said "marks a
  TokenProvider failure: fatal, no retry" unqualified -- reworded to say fatal unless the cause wraps
  `ErrTokenUnavailable` (D-2026-09-20-11 above). `DisconnectReason`'s `ErrorCode` field doc and README's
  matching paragraph described the third `errorCode` source as "today, only a missing/mismatched subprotocol
  echo" and source (1) as "a wire error{} message" -- both reworded to the spec's actual three-source
  definition: the welcome timeout's locally generated `PROTOCOL_ERROR`/4001 and any other local
  protocol-violation `fail()` are ALSO spec source (3) (a ws-mixer error the SDK itself raises locally, no
  close frame from the peer involved) even though Go's implementation happens to route them through the same
  `*ConnError` branch as an actual peer `error{}` message -- the code path is shared, but the spec
  classification is not source (1) for those two cases. `SendApp`'s doc comment now says explicitly that
  delivery is best-effort and it returns nil even when the connection has already ended (it always did; the
  doc just didn't say so) -- left the underlying silent-drop behavior itself unchanged, out of scope
  (`sendControlFrame`'s own `case <-c.closed:` has no error return at all to plumb one through).

## 2026-09-25

- **D-2026-09-25-01** — `Client.CloseWith` (`client_reconnect.go`): implements
  [`ws-mixer-spec`'s `D-2026-09-25-01`](https://github.com/mcpwarp/ws-mixer-spec/blob/main/docs/DECISIONS.md),
  which settles that the application-close API takes a message only and always sends
  `APPLICATION_CLOSE` -- `WIRE.md` §2.8 allows an application exactly one connection-close code, so a
  caller-supplied code was never meaningful there. Dropped `CloseWith`'s `code ErrorCode` parameter;
  it now always closes with `ApplicationCloseCode` internally, both for the active conn and for a
  retiring one during a drain hand-over. `Conn.Close` itself is untouched -- it keeps taking an
  arbitrary `ErrorCode`, since it is the lower-level primitive `CloseWith` is built on, not the
  application-close API this decision constrains. The conformance adapter's `close` command
  (`conformance/adapter/adapter.go`) now accepts only code `0`/absent (routed to `Client.Close`) or
  `14` (routed to `Client.CloseWith`) when a `Client` is driving the connection, rejecting anything
  else with `cmdErr`; the plain-conn path (no `Client`, exercising `Conn.Close` directly for wire-level
  conformance cases) now enforces the same `0`/`14` rule, routing `14` to `Conn.Close` with
  `ApplicationCloseCode` instead of accepting an arbitrary code. Breaking change, released as
  v0.6.0: v0.5.0 had no external `CloseWith` consumer, so no deprecation shim. Updated every caller in
  `wsmixer/client_closewith_test.go` (all of which already passed `ApplicationCloseCode`, so this was a
  mechanical drop of the argument, not a behavior change to any test) and the README's matching
  paragraph; that test file's and `client_reconnect.go`'s own comments describing `CloseWith`'s
  cross-SDK alignment were updated too, since they still referred to a caller-supplied code. `spec.pin`
  stays at `v0.4.0` -- the wire itself is unchanged, only the Go SDK's own `Client`-level API surface.

## 2026-09-26

- **D-2026-09-26-01** — v0.7.0 follow-ups: `SendApp` on a dead conn, a single version source, test
  hardening, and the conformance adapter's handshake-gap flake. Go-only, no spec counterpart.

  (1) `Conn.SendApp` (`streams.go`) now returns an error when the conn has already ended, closing the
  "reported, not fixed" gap D-2026-09-20-13 left open: it used to go through `sendControl`/
  `sendControlFrame`, whose `case <-c.closed:` drops the frame with no return value, so a dead conn looked
  exactly like a healthy one. `SendApp` now does its own enqueue: a non-blocking `c.closed` check first
  (needed because `select` picks at random among ready cases -- a `controlQueue` with room would otherwise
  still accept the frame about half the time after `c.closed` fired), then `select` on `controlQueue` vs
  `c.closed`. Both closed branches return `connClosedErr`, the same precedence `Stream.Read`/`Write` use:
  the recorded close error (`*ConnError` for a clean `Close`, a peer `error{}` or a local `fail`), else
  `io.ErrUnexpectedEOF` for an abnormal closure. The doc comment says nil means *queued*, never delivered --
  a conn that dies right after the enqueue (or concurrently with the call) can still lose the frame; no
  attempt is made to close that race, since nothing short of an end-to-end ack could. `sendControl`/
  `sendControlFrame` are unchanged (their other callers -- ping, pong, drain -- all discard the result).
  Consumers: the conformance adapter's `send_app` (`conformance/adapter/adapter.go`) already routes any
  `SendApp` error to `cmdErr("send_app: ...")`, and `cmd/testserver`'s `send_app` to its own error output;
  no spec scenario or fixture issues `send_app` after a close (only `app_roundtrip.json` uses it, on a live
  connection), so none relied on the old silent no-op. Tests (`wsmixer/sendapp_test.go`):
  `TestSendAppLiveConnQueuesFrame` (nil, and the app{} frame reaches the wire),
  `TestSendAppAfterAbnormalCloseReturnsUnexpectedEOF` (`fakeWS.CloseNow()`, no close frame), and
  `TestSendAppAfterCleanCloseReturnsCloseError` (`Close(0, "bye")`: the exact `Conn.Err()` value, a
  `*ConnError{NO_ERROR, "bye"}`). The two dead-conn tests call `SendApp` 32 times each so the random
  `select` would be caught; revert-proof: with the up-front `c.closed` check removed both failed on the
  2nd/3rd call, then restored.

  (2) Single version source: new `internal/version` package, `const SDK = "0.7.0"`. `wsmixer`'s
  `defaultSDKVersion` (the default `hello.agent.sdk_version`) is now `version.SDK`, and
  `cmd/conformance-adapter` reports `version.SDK` on its `ready` event and client-role `hello.agent`
  instead of the long-stale hardcoded `"0.1.0"`. Exported API unchanged (`internal/` is not importable
  outside this module; `conformance/adapter.Config.SDKVersion` still lets another thin main -- e.g.
  ws-mixer-server's -- pass its own).

  (3) Test hardening, from review:
  - `wsmixer/stream_race_close_test.go`: `parkedRead` slept 5ms and assumed the reader had parked in
    `Stream.wait()`; if it hadn't, `ReadContext` resolved via its own top-of-loop checks and all five tests
    passed without reaching the post-`wait()` re-check they exist for. `parkedRead` now records the reader
    goroutine's `goroutine N [` header and polls `runtime.Stack(all)` until that goroutine's own frame list
    contains `(*Stream).wait(` (bounded 2s, `t.Fatal` otherwise). Revert-proof: with `ReadContext`'s
    re-check replaced by an unconditional `return 0, err`, all five failed 3/3, then restored.
  - `TestClientCloseWithDuringDrainHandoverSendsApplicationCode`: deleted. Its own comment admitted it
    passed with the review-item-5 guard removed, and that can't be fixed: since D-2026-09-25-01 the
    retiring-conn branch closes with the same `ApplicationCloseCode` and message as the active-conn
    branch, so whichever wins the `closeOnce` race, the wire shows the identical 4014 + message -- the
    guard has no observable effect to assert. What the test did observe (CloseWith → 4014 on the wire) is
    already pinned by `TestClientCloseWithApplicationCloseCode`.
  - `TestClientFatalStateExactlyClosed`: polled until `State()=="closed"`, so it could not tell "closed and
    stays closed" from "closed, then overwritten". It now samples `State()` from inside the fatal
    `OnDisconnect` itself (goFatal sets `clientClosed` before reporting, so it must already read `closed`
    there) and again after `cl.wg.Wait()` (every client goroutine has exited). The comment no longer claims
    to guard watchConn's `clientClosed` guard -- that guard is a no-op on the fatal path (goFatal nils
    `cl.conn` in the same critical section); removing it fails `TestClientCloseStateExactlyClosed` and
    `TestClientCloseWithApplicationCloseCode` instead (verified). Removing goFatal's `clientClosed` write
    fails this test on both samples (verified).
  - `TestClientCloseWithDuringDial`: the fixed 300ms sleep couldn't distinguish "dial in flight" from
    "not started yet" or backoff. It now waits for the listener's TCP accept (the upgrade then hangs by
    construction), asserts `State()=="dialing"` before `CloseWith`, and afterwards asserts exactly one
    accept and that `Connect` returned an error. New `TestClientCloseWithDuringBackoff` covers the backoff
    half the old comment also claimed: first dial gets HTTP 503 (recoverable), `rc.after` never fires, the
    test waits for `State()=="backoff"`, then `CloseWith` must return promptly with exactly one request seen.
  - `TestClientAttemptResetProofRevertsWithoutStabilityGate`: deleted -- an always-`t.Skip` test used as
    documentation. Its text, kept here: the revert-proof for D-2026-09-20-07 (StableAfter) was performed
    during development and is not run in CI (there is no supported way to flip the production gate off from
    a test): temporarily restoring `onAttemptSucceeded`'s old `cl.attempt = 0` /
    `cl.keepaliveRetryUsed = false` (removing `armStability`'s gating) and rerunning
    `TestClientAttemptResetOnlyAfterStability` produced exactly the predicted failure --
    `delay[1] = 20ms, want 40ms`, `delay[2] = 20ms, want 80ms`, `delay[3] = 20ms, want 160ms` (each
    "climbing: attempt never reset, stability never elapsed"), every delay collapsing back to
    `rc.fullJitter(1)` -- and restoring the fix (`git diff` against the pre-revert file was empty) made it
    pass again.

  (4) Conformance flake: the go-server fixture `window_exhaustion_then_resume` failed once in CI with
  `step 2 (send): open_stream failed: open_stream before listen/connect completed`. Cause, adapter side: the
  peer observes the handshake finishing before the adapter records the conn. Server role:
  `wsmixer.AcceptConn` writes `welcome` to the socket and only then returns, after which the backend calls
  `OnConn` → `setConn` on the HTTP handler goroutine. The runner (`conformance/runner/driver`) paces
  fixture steps on the wire: step 1 (`send welcome`) is observe-only and completes the moment the raw actor
  reads `welcome`, and step 2 (`send OPEN`) immediately writes `open_stream` to the adapter's stdin -- which
  the stdin loop could process before the handler goroutine reached `setConn`, so `getConn()` was still nil.
  The client role has the same gap (`Dial` returns once `welcome` is read; the connect goroutine calls
  `setConn` after). `listen`'s own ack is not the problem (`srv.Serve` on an already-bound listener just
  queues connections in the backlog). Fixed in the adapter: `adapterState.awaitConn` returns the conn if
  set, else waits up to 5s (`connWaitBudget`) on `connReady`, a channel the first `setConn` closes;
  `open_stream`, `send_app`, `drain` and the plain-conn `close` path use it instead of `getConn`. Test
  (`conformance/adapter/race_test.go`): `TestOpenStreamInHandshakeGapWaitsForConn` holds `OnConn` behind a
  gate after the client has its `welcome`, issues `open_stream` inside that gap, releases the gate 50ms
  later, and asserts the client sees the stream and no `error` event was emitted. Revert-proof: with
  `open_stream` back on `getConn`, it failed 3/3 with the exact CI message, then restored. The runner never
  waits for the adapter's own server-role `connected` event before sending the next command; that is
  consistent with CONFORMANCE.md (§1.2 lists `connected` as an event, not a gate) and needs no spec change,
  though awaiting it after an observed `welcome` would make the runner robust to other adapters with the
  same gap.
