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

## 2026-10-13

- **D-2026-10-13-01** — Header-less upgrades (`accept.go`): implements
  [`ws-mixer-spec`'s `D-2026-10-13-01`](https://github.com/mcpwarp/ws-mixer-spec/blob/main/docs/DECISIONS.md),
  which makes the upgrade's `Authorization` header optional so browser-class clients can connect.
  `runServerHandshake` used to reject an empty `bearer` outright; it now treats `bearer == ""` as "the upgrade
  carried no header" and lets `hello.token` alone reach `Authenticate`. A non-empty `bearer` must still equal
  `hello.token` (`UNAUTHORIZED`/4011, same message as before), and an empty `hello.token` is answered with
  `UNAUTHORIZED` "missing hello.token" -- in practice unreachable over the wire, since `parseHello` already
  rejects a token outside 1..4096 bytes as `PROTOCOL_ERROR`, but it keeps `AcceptConn` from ever calling
  `Authenticate` with an empty credential. Telling an absent header from a malformed one (HTTP 401) is the
  caller's job: `AcceptConn` only ever sees the extracted token. The client side is unchanged -- `Dial`
  always sends the header, which Go can. Test: `TestAcceptConnBearerRule` (`wsmixer/accept_test.go`).
  `spec.pin` → `v0.5.0`, `version.SDK` → `0.8.0`.

## 2026-10-14

- **D-2026-10-14-01** — New package `mcpwarp` (`github.com/mcpwarp/ws-mixer-go/mcpwarp`): implements
  [`ws-mixer-spec`'s `D-2026-10-14-01`](https://github.com/mcpwarp/ws-mixer-spec/blob/main/docs/DECISIONS.md)
  (the mcpwarp layer, `docs/MCPWARP.md`) at spec `v0.6.1` (`D-2026-10-14-06`), both tiers: Tier 1 (§2
  envelope, §3 identity/registration/take-over, §4 request bridging, §8 runtime) and Tier 2 (§5 `Expose`, §6
  `NewTunnel`, §7 `DescribeStatus`, Go names per §9). `wsmixer` stays free of mcpwarp types; the layer uses
  its public API only. Layout: `envelope.go` (codec, code tables), `errors.go` (`*Error`), `session.go`
  (registration state machine, timers injected), `http1.go` + `bridge.go` (HTTP/1.1 over a stream,
  `http.Handler` and target proxy), `transport.go` (a `transport` seam over `wsmixer.Client`), `agent.go`
  (one exposed service: session + bridge + transport, every wait a timer or a callback), `options.go`
  (`ExposeOptions`, §5.2 validation), `expose.go` (`Handle`, events), `tunnel.go` (controller),
  `statustext.go` + generated `statustext_tables.go` (`go generate ./mcpwarp/` runs
  `internal/genstatustext` over the pinned `spec/fixtures/mcpwarp/status-text/*.json`; a test fails when the
  checked-in tables drift). `spec.pin` → `v0.6.1`. Fixture runners: `TestEnvelopeFixtures*` (§10.2),
  `TestBridgingFixtures` (§10.3, a real agent over a loopback fake edge built on `AcceptConn`, the way
  §10.3 reaches `sdkState`), `TestTunnelFixtures` (§10.4, the real controller over a scripted ws-mixer
  client and a fake clock: tunnel/README.md's "runner that fakes the ws-mixer client itself"),
  `TestStatusTextFixtures` (§10.5). They run in `go.yml`'s `test`/`race` jobs and verbosely in the new
  `mcpwarp-fixtures` job; under `CI` a missing corpus fails instead of skipping. Counts at `v0.6.1`:
  envelope valid 17/17, invalid 34 pass + 9 skipped (agent→edge `SCHEMA_*` hints: the edge's verdict, not
  the agent's, §10.2); bridging 13 + 1 skipped (`body_413_streamed_reset`: a streaming-adapter fixture, this
  bridge buffers, §10.3); tunnel 33 + 1 skipped (`07_invalid_config` sets `token`/`name` to `null`, which
  Go's string fields cannot hold; its `kind` case is `TestTunnelInvalidKindAtStart`); status text 33/33.
  `schema_valid` is not re-checked (no JSON Schema dependency); `check-fixtures.mjs` holds that verdict.

- **D-2026-10-14-02** — Additive `wsmixer` API, each the minimum the layer needed:
  `ClientOptions.HTTPClient` / `ClientConfig.HTTPClient *http.Client`, passed to coder/websocket's
  `DialOptions.HTTPClient`: MCPWARP.md §8's caller-supplied connector (dialer, proxy, TLS), surfaced as
  `ExposeOptions.WebSocket` / `TunnelConfig.WebSocket`. `ClientConfig.OnReconnect func(ReconnectInfo)`,
  `ReconnectInfo{Attempt, Delay, Cause}`, `ReconnectDrain`, `ReconnectTokenRefresh`: §5.9 needs `onRetry`
  before every scheduled redial (backoff incl. zero delays, drain replacement, the token-refresh redial),
  which the layer cannot see otherwise; queued on the callback goroutine after the disconnect it answers
  and before the redial's `OnConnect`, so a zero-delay dial may already be under way when it runs.
  `DisconnectReason.Superseded`: marks the end of a drained connection whose replacement has received
  welcome, the §2.7/§6.8 "superseded" test, decided by the client that knows connection identity.
  `Client.CloseImmediately(ctx)`: error{NO_ERROR} + close 1000 at once, no `drain{client_requested}`, no
  grace; the take-over close of spec v0.6.2 R4 (`Close` drains, `CloseWith` sends `APPLICATION_CLOSE`), and
  called from `OnDrain` it marks the client closed before the drain's replacement dial can start.

- **D-2026-10-14-03** — Rulings implemented ahead of their spec tags (v0.6.2 final / v0.6.3 draft, ruled by
  the spec session, approved for v0.9.0), each with its test:
  an OPEN crossing this side's own `drain{client_requested}` is RESET `REFUSED_STREAM` for that stream only,
  never a connection error; the `last_stream_id` rule applies only after the server's drain
  (`TestClientOwnDrainRefusesLateOpen`).
  Drain hand-over: the drained connection keeps serving after its replacement's welcome and is retired at
  the drain's `deadline_ms` + 1000 ms (30 s without one), or as soon as no stream is left on it; survivors
  RESET `CANCEL`, then error{NO_ERROR} + close 1000; its end is reported once with `Superseded`, and its
  counters are folded into `Client.Stats()` once (`TestClientRetiresDrainedConnAtDeadline`,
  `TestClientRetiresDrainedConnWhenEmpty`, `TestClientStatsAfterHandoverCountEachConnOnce`). This replaces
  closing it at the replacement's welcome.
  Integer control fields accept any integral JSON number (`1.0`, `1e3`), parsed exactly; fractions, bools and
  strings are rejected (`TestControlIntegerFieldsAcceptIntegralNumbers`).
  `token` 4096, `error.message` 1024, `drain.message` 256 and `session` 64 count Unicode code points, as JSON
  Schema `maxLength` (`TestControlStringLimitsCountCodePoints`).
  Take-over close (`conflict` op or `drain{replaced}`): RESET `CANCEL` on streams being served, then
  `CloseImmediately`: no drain frame, no grace, no unregister (`TestClientCloseImmediately`,
  `TestAgentStickyConflictClosesClient`, `TestAgentDrainReplacedIsTakeOver`); the close's disconnect is
  reported after the conflict (`TestAgentReplacedConflictBeforeDisconnect`).
  A request net/http cannot carry (an unparsable URL, a control byte in `Host` or a forwarded header) is 400
  `request cannot be represented as a web Request` in target mode as in handler mode, never 502
  (`TestBridgeUnrepresentable400`).
  The forwarded target is percent-encoded once from the received bytes: bytes outside RFC 3986's allowed set
  become `%XX`, every `%` is kept as is (a valid escape and a lone `%` alike), `#` drops the fragment and
  everything after it; `RequestURI` stays as received (`TestBridgeTargetEscapedOnce`,
  `TestBridgeTargetLonePercentKept`, `TestBridgeTargetFragmentDropped`).
  A final handler or target status outside 200–599 is the 500 `handler error` reply plus an error event
  (`TestBridgeStatusOutOfRange500`).
  The 64 KiB head cap counts the terminating blank line (`TestReadHeadCapCountsBlankLine`).
  Once the request line parsed as `HEAD`, every SDK-generated reply is head-only, head-parse errors (400,
  431) included; without a parsable request line the reply keeps its body (`TestBridgeHeadRepliesHeadOnly`).

- **D-2026-10-14-04** — Remaining Go choices and deviations in `mcpwarp`:
  the request body is buffered whole (≤ `MaxRequestBodyBytes`, default 4 MiB) before the app runs, so a body
  over the cap is always a 413 before any response byte and §4.2's streaming rows never apply.
  `""` means absent: `Conflict.ServerID` (JS omits `serverId`), `ServiceError.ID` (an explicit `"id":""` is
  treated as absent), empty `TunnelStatus` fields.
  Header casing: the handler sees net/http-canonical request headers; response headers go out as the
  handler spelled them, sorted like net/http.
  Edge cancellation: `wsmixer.Stream` has no close notification, so a RESET is polled every 100 ms beside
  the connection's `Done`.
  Connection identity: `Client`'s `OnApp` carries no connection, so each connection's own `OnApp` is bound
  in `OnConnect`; messages before that are attributed to the newest connection; a connection's end is
  observed through its `Done` plus `Client.Conn()`.
  `Configure` never yields `INVALID_CONFIG` in Go: §6.2 checks 1, 2 and 4–7 are dynamic-type checks Go's
  types rule out; an invalid config surfaces at start (T7/T8).
  `ExposeOptions.Token string` plus `TokenProvider` (the provider wins); the token-refresh redial exists
  only with a provider, a static token is fatal on its first rejection (CLIENT-SDK.md).
  `ReconnectOptions` zero values take the defaults (JS `base: 0` means 0); `MaxAttempts` 0 is unlimited;
  negatives are validation errors.
  `WebSocket` is an `*http.Client`, compared by identity in `TunnelConfig`, not a factory function.
  `Dispose(ctx)` bounds its wait by ctx's deadline (5 s without one), clamps a passed deadline to 0, always
  returns nil; `Configure`/`Stop` return `ctx.Err()` only when ctx ends first.
  `Handle.Events()` buffers without bound; its pump starts on the first call and closes the channel after
  `EventClose`. Tunnel callbacks (listeners, `OnRetry`, `Logger`) run on one deliverer, outside the lock and
  after the work queue, so a callback may call `Configure`/`Stop`/`Dispose`; `Subscribe`'s first call goes
  through that deliverer (immediate when idle, else right after the current callback).
  `ExposeState` with `Expose*` constants and `Registration() (Registration, bool)` (§9 names neither shape).
  Validation messages keep §5.2's JS option names verbatim; `URL` must be ws/wss (not a §5.2 row).
  Spots where `v0.6.1` is silent and the v0.6.2 draft was followed: `hello.meta.client` `"sdk-go"`,
  `version` = `version.SDK` (R2); envelope defects checked in the order name, id, url, created (R8);
  `Target` must be http/https at validation (R3); the drain `onRetry` delay is the real one, including a
  `retry_after_ms` hint (R6). `ExposeOptions.Logger` is passed to `wsmixer.Options.Logger`.
  `version.SDK` → `0.9.0`.

## 2026-10-15

- **D-2026-10-15-01** — v0.9.1: `spec.pin` → `v0.6.3` (implements
  [`ws-mixer-spec`'s `D-2026-10-14-07`](https://github.com/mcpwarp/ws-mixer-spec/blob/main/docs/DECISIONS.md)
  and the v0.6.3 rulings; two of them landed here at `v0.6.2`, ahead of their tag).
  At `v0.6.2` the new envelope defect-order fixtures and `tunnel/37` passed unchanged; `tunnel/36` (an
  observable HTTP 401/403/404 on the upgrade) failed in the runner, not the SDK: the fake ws-mixer client
  ignored `dial_failed.httpStatus`. It now injects the disconnect reason tunnel/README.md prescribes,
  `{phase: dial, fatal, httpStatus}`, fatal for 401/403/404 with the static token, as `wsmixer.Client`
  decides it (`TestClientStaticToken401Fatal`; 403/404 in `newDialError`).
  WIRE.md §2.5 (v0.6.3): a WINDOW for a retired stream id is discarded and counted stale whatever its
  increment, 0 included; increment 0 on a live stream is RESET `PROTOCOL_ERROR`. `DecodeFrame` no longer
  rejects increment 0 (a behaviour change for direct callers); `handleStreamWindow` checks liveness first,
  then the increment (`zeroWindowIncrement`). The frame-fixture runner applies that same check to a decoded
  WINDOW, so `frames/window_increment_zero` (a live-stream verdict) stays green
  (`TestWindowIncrementZeroStaleVersusLive`); `sequences/window_stale_retired_stream` passes.
  MCPWARP.md §2.7 (v0.6.3): a `disable` carries an id only, so one with an id the handle does not know is
  held, not adopted, and applied when a later `registered` on the same connection brings that id: the
  service goes straight to disabled, with no `registered` event (no tunnel `online`) in between
  (`tunnel/42`); the connection's end, the next `welcome` or an `enable` drops what is held
  (`TestSessionDisableWithUnknownIDIsHeld`, `TestSessionHeldDisableDroppedWithItsConnection`,
  `TestSessionEnableClearsHeldDisable`). The bridging runner reaches `sdkState: disabled` that way;
  `TestSessionTimersCancelledByDisable` now registers first. §2.7 Ended connection: a `registered`,
  `disable` or `enable` delivered after its connection ended is ignored; `conflict` and `error` still apply
  (`tunnel/44`, `TestSessionEndedConnectionMessages`). "Ended" is the connection's own `Done()`, not a flag
  set by `onConnEnd`: `wsmixer` closes `Done` before reporting the disconnect, but runs the transport's `Done`
  watcher (`onConnEnd`), `OnDisconnect` and the flushed `OnApp` deliveries on three unordered goroutines. The
  tunnel fake now ends a connection the same way (Done, disconnect, flush, then `connEnd`). The wsmixer
  transport also tells the session about a connection before binding its `(*Conn).OnApp`, so a message
  delivered while `OnConnect` runs takes the unattributed path instead of being dropped as superseded
  (`TestAgentAppMessageDuringOnConnectNotDropped`). Every unknown `disable` id is held, also after a server
  id is known, several at once (`TestSessionUnknownDisableHeldAfterIDKnown`); the `registered` that applies
  one sets no registration (§5.4: `expose()` resolves disabled without one).
  §4.3: `[` and `]` join the allowed set (WHATWG parity, `a[]=1`; `target_allowed_set`). §4.5: `CONNECT`,
  `TRACE` and `TRACK` are answered 400 `request cannot be represented as a web Request` before the app is
  called (`handler_connect_400`), as is a request the HTTP client cannot carry (`target_unrepresentable_400`).
  §6.2 token provider: new `TunnelConfig.TokenSource`, an interface `Token(ctx) (string, error)`, used instead
  of `Token` when set, plus `TokenSourceFunc(wsmixer.TokenProvider) TokenSource`, which returns a new pointer
  on every call. Configure's idempotence compares sources with `==` (a pointer gives identity; `Token` is
  ignored while either config has a source); a source whose value `==` would panic on (a non-comparable type, or a
  func/map/slice inside an interface field) never matches, guarded with `reflect.Value.Comparable` instead
  of panicking. A plain func field was rejected: Go
  funcs are not comparable, and comparing closure pointers needed `unsafe` and still could not tell a method
  value's receivers apart. The tunnel hands `source.Token` to `wsmixer.Client` as its `TokenProvider`. A
  pre-`welcome`
  401/4011 with the refresh unspent: `wsmixer.Client` calls the provider again and redials at once, reporting
  `OnReconnect{1, 0, "token-refresh"}`; the tunnel takes that report as the failed dial (T48,
  connecting/`RETRYING`, `tunnel/38`). `ErrTokenUnavailable` is a failed dial with normal backoff (T46,
  `tunnel/39`), an unmarked provider error `DISCONNECTED_FATAL` (T47, `tunnel/40`). Open: `wsmixer.Client`
  emits no `OnDisconnect` for that first rejected token (reporting it breaks the refresh-retry contract its
  tests pin), so `Expose`'s `EventDisconnect` does not see it.
  Runners: bridging gained handler `status` and the target-mode `echo` local server; the tunnel fake
  ws-mixer client gained the provider path (`token_provider` events), drain `retry_after_ms` (as
  `wsmixer.Client` honours it, `tunnel/41`), `disabled` with an `id`, and post-disconnect delivery
  (`flush_after`, capability `post-disconnect-delivery`). `body_413_streamed_reset` and `tunnel/07` stay
  skipped.
  README: a "Migrating to v0.9.0" note on `DisconnectReason.Superseded` and per-connection `(*Conn).OnApp`.
  `version.SDK` → `0.9.1`.

## 2026-10-19

- **D-2026-10-19-01** — v0.10.0: `spec.pin` → `v0.7.0` (the multi-service profile, MCPWARP.md §2.1-§2.7,
  §4.8, §6A, §9.2, §10.6), plus the binding v0.7.1 rulings of D-2026-10-19-04. Package `mcpwarp` additions:
  `MaxServicesPerMessage` (100), `RegisterEntry{Name, Kind}`, `EncodeRegisterList`, `EncodeUnregisterList`
  (the §9.2 list encoders; `EncodeRegister`/`EncodeUnregister` stay as the one-entry forms and must agree with
  them, which the envelope runner checks); `ServerIDHeader`, `ServerNameHeader`; the service group
  (`NewServiceGroup`, `ServiceGroupOptions`, `ServiceGroupConfig`, `ServiceConfig`, `ServiceGroup` with
  `Configure`, `Stop`, `Dispose`, `AddService`, `RemoveService`, `Enable`, `Disable`, `Status`, `Subscribe`);
  `ServiceState` and its six constants, `ServiceStatus`, `ServiceGroupStatus`; `DescribeServiceStatus` and
  `StatusMessages.Services`; status-text tables with `service.*` and `lastError.NO_SERVICE_REGISTERED`
  (regenerated; the drift test covers them). `DecodeEnvelope` needed no change: it already validated and kept
  every entry of a list. `register_two_services` and `register_batch_valid` are compared through
  `EncodeRegisterList`; `register_batch_101_invalid` checks that the group never builds a frame over 100.
- **D-2026-10-19-02** — The service group (§6A) is its own controller over its own registration state machine
  (`group.go`, `group_session.go`), sharing the agent, transport, bridge, codec and the tunnel's status
  constants; `Expose` and `NewTunnel` are not rebuilt on top of it. Their §5/§6 semantics differ from §6A's in
  ways a group of one cannot express without special cases (a whole-connection sticky `conflict` versus a
  per-name `taken_over`, a refusal fatal before the first registration versus G5, `disabled` as a connection
  state versus a per-service one), and their fixtures and tests pin them; a group of one observes the same
  wire, which is §6A's "conforming subset". The agent runs either machine behind one `registrar` interface;
  the bridge gained a per-stream `route` hook and a per-service `app` (handler or target).
  Choices: one snapshot per processed input (the session reports services and the connection-level effect
  together; a reply that is also G2/G3/G5 is one snapshot); a conflict needs a name this connection *holds*
  (got a `services[]` entry since), not merely one it *may hold* in §6A.2's sense (sent a register), which
  `tunnel/ms_05` requires for the refused `zeta`; the §6A.2 checks (duplicate name, name syntax, §5.2 rows
  1-4 and 8) run at start and before a services-only diff, as the tunnel's run at start; a validation error
  shows no services, a fatal error keeps them (as G5 does); a local `Disable` lives in the controller, so it
  survives restarts and services-only diffs and is cleared by `Enable`, removal, `Stop`, `Dispose` or a fatal
  error; reply entries apply only to names of the outstanding frame that may still go out (removed, locally
  disabled and taken-over names are ignored); a group's token is `Token`/`TokenSource` as in `TunnelConfig`;
  handlers are compared with `==` behind the same `reflect.Value.Comparable` guard as `TokenSource` (a
  `HandlerFunc` never compares equal, so such a reconfigure is applied as a no-op swap: no frame, no
  snapshot). A close's unregister frames carry a context mark (`closeFrameKey`) only so the fixtures' fake
  edge can answer them itself (tunnel/README.md runner rule 5). On a per-name `conflict` the agent also RESETs
  that service's in-flight streams with CANCEL (the edge already has); the other names and the connection go
  on (`TestGroupPerNameConflictResetsOnlyThatService`, real transport).
- **D-2026-10-19-03** — §4.8 routing: after the path check, the group selects the service per stream: with
  `TrustServerIDHeader` the first `X-Mcpwarp-Server-Id` (name case-insensitive) matched exactly against the
  last id learned per name, no `Host` fallback; else the `Host` label (port dropped, lowercased, first label)
  split at its last `-`, name and username checked against their grammars. No service → `503 service
  unavailable`; `taken_over` → RESET CANCEL; dashboard-disabled → `503 service disabled`; locally disabled,
  rejected, unregistered, or pending and never active → `503 service unavailable`; closing → `503 service
  closing`. `X-Mcpwarp-Server-Name` is forwarded and never routed by. The trust option defaults off.
- **D-2026-10-19-04** — v0.7.1 rulings applied ahead of their tag (binding for v0.10.0; they override v0.7.0's
  wording), in both the single-service session and the group: (1) the OVERLOADED backoff has its own counter,
  `min(1000·2^(n−1), 30000)` ms over consecutive OVERLOADED only, reset by any other reply; transient refusals
  keep §3.2's counter, reset by a successful register (a single service's second consecutive OVERLOADED now
  waits 2 s, not 1 s; `TestSessionOverloadedResend` updated); (2) a `registered` listing none of the frame's
  names is re-sent after the transient backoff (`register failed: INTERNAL: unlisted in reply`; no state
  change for a single service); (3) one frame in flight: an `enable` or subset register during a pending frame
  queues until its reply, and `close()` may unregister while a register is outstanding; (4) any whole-frame
  error op other than OVERLOADED and CONNECTION_LIMIT answers the outstanding frame (a group's register names
  → `rejected`, G7; UNSUPPORTED_VERSION then fatal); (5) a reply settling the frame cancels its re-send timer;
  (6) after OVERLOADED the slot stays reserved and triggers during the pause fold into the one re-send; (7)
  `close()`'s unregister wait ends early on the reply to the unregister's own frame (an `unregistered`, or a
  whole-frame error op), not at the 2000 ms budget; per the spec session's ruling of 2026-10-19 a reply to a
  register still in flight when the close began continues the wait, and a register dropped by OVERLOADED then
  is abandoned, never re-sent. Replies are matched to frames by op, an error op (which names no frame) by
  frame order, so a close of more than 100 names keeps one frame in flight; (8) a held disable is cleared by
  a replacement connection's `welcome` too (both machines clear holds on every `welcome`). No `ms_` fixture contradicts them: none mixes OVERLOADED with all-transient
  replies. The v0.7.0 texts that do — §2.6 OVERLOADED re-send, §3.2 Transient retry, §6A.7 Backoff counter
  and tunnel/README.md's group-fixture streak line ("counts only OVERLOADEDs and all-transient registereds")
  — describe one shared counter; the runner follows ruling 1 (its expected OVERLOADED attempt resets on any
  non-OVERLOADED reply). Tests: `TestGroupOverloadedCounterSeparate`, `TestGroupSettlingCancelsResend`,
  `TestGroupQueueAndFoldBehindInFlight`, `TestGroupCloseWhileRegisterOutstanding`,
  `TestGroupCloseFramesOfAHundred`, `TestGroupEmptyReplyResends`, `TestSessionEmptyReplyAndWholeFrameError`,
  `TestSessionCloseWaitEndsOnAnyAnswer`.
- **D-2026-10-19-05** — Core (`wsmixer`, no API change): `onAttemptSucceeded` resolves `Connect`'s first
  result under `cl.mu`, before `watchConn` starts, so `Connect` returns nil once a welcome landed and a fatal
  close right after is reported through `OnDisconnect` only (the race of 2026-10-18's flake;
  `TestClientFatalStateExactlyClosed` and `TestClientFatalTerminationDoesNotLeakCallbackLoop` now require
  nil). CLIENT-SDK.md v0.7.0's Rejected token row: with a real provider the first rejection before welcome is
  reported as a non-fatal `OnDisconnect` (`PhaseDial`/`HTTPStatus` 401, or `PhaseHandshake`/`WSCode`
  4011/`UNAUTHORIZED`), then `OnReconnect{1, 0, token-refresh}`, then the redial (MCPWARP.md §11 row 53).
  This closes D-2026-10-15-01's open item: `Expose`'s `EventDisconnect` sees that dial, and the tunnel's
  compensation (a `token-refresh` retry read as a failed dial) is gone, the disconnect now drives T48. Tests
  moved to the new contract: `TestClientSecond401Fatal`,
  `TestClientHandshakePhaseBareClose4011OneRetryThenFatal`,
  `TestClientRefreshRetryRetryDialHTTPFailureNotFatal`, `…TransportFailureNotFatal`,
  `…WelcomeTimeoutNotFatal`, `…BareClose4010Fatal`, `TestClientMarkedRefreshRetryThenLaterFatal` (each now
  takes the first report first), and `TestClient401RefreshRetrySuccess` pins the order disconnect, reconnect,
  connect. The tunnel fixtures' fake client reports that disconnect too. `isRealProvider` stays.
- **D-2026-10-19-06** — Held `disable` at the handle (spec ruling of 2026-10-19: §5.9 wins over §5.4): the
  `registered` that brings a held disable's id records the registration (url, id, `Registration()`), emits
  `EventRegistered`, then applies the disable (`EventDisabled`, state `disabled`), back to back. This reverts
  v0.9.1's "no registration" path. The tunnel collapses the pair into one `disabled`/`DISABLED` snapshot, no
  `online` in between (T50; the registered event says a held disable follows; `tunnel/42` unchanged).
  `version.SDK` → `0.10.0`; README: a service-group section and "Migrating to v0.10.0".
- **D-2026-10-19-07** — r1 and the CLI's integration check. Fixes: `AddService`/`RemoveService` read, edit
  and store the desired config under one lock (concurrent edits no longer lose each other,
  `TestServiceGroupConcurrentAddService`); a stray `registered` resets the OVERLOADED streak (ruling 1); the
  close frame matching above (`TestGroupCloseFramesMatchTheirReplies`); `wsmixer`: a Close that lands
  between a first rejected token and its refresh still reports the rejection but announces no
  token-refresh reconnect and makes no redial (`TestClient401RefreshSkippedWhenClosing`, via the test-only
  seam `probeBeforeTokenRefresh`, like `probeWidenWindow`). The flake root cause: `TestClient401RefreshRetrySuccess`'s
  `OnConnect` closed a channel bare, so a second `OnConnect` (a reconnect under `-race` load) panicked and
  killed the binary; it and every other bare close in a client callback now go through `onceCloser`
  (`client_reconnect_test.go`, `client_closewith_test.go`, `client_reconnect_regression_test.go` ×4).
  Additions: `ServiceGroupOptions.Metrics` (passed to `wsmixer.Options.Metrics`) and `(*ServiceGroup).Client()`
  (the running connection's client; one per configuration, the same across reconnects, replaced on a restart;
  the session id is `Client().Conn().Session()`, no new accessor). A dashboard-disabled service has `Code`
  `DISABLED` (disable op and `SERVER_DISABLED` alike), a locally disabled one `Code` `""`. A group service
  whose target is unreachable is logged warn with its name and target URL; §6A.4 gives an active service no
  `lastError`, so it is not in its status. `CONNECTION_LIMIT` is unchanged pending a spec ruling: a
  non-fatal error (logged warn; `connecting`/`INTERNAL` if the group was still connecting, else unchanged),
  then the 4014 close is an ordinary lost connection (`connecting`/`DISCONNECTED`, backoff redial, the full
  list re-registered on the next welcome).
- **D-2026-10-19-08** — `spec.pin` → `v0.7.1` (8ea3790), which tags D-2026-10-19-04's rulings and adds:
  (i) only register triggers fold into a dropped register's re-send, within 100 names, the dropped names
  first, then the folded ones in trigger order; a name removed or locally disabled during the pause is left
  out and, if the connection may hold it, its `unregister` is the next frame once the re-send is answered
  (`TestGroupPauseFoldOrderAndOwedUnregister`, `tunnel/ms_12`); (ii) a whole-frame error op answering an
  outstanding `unregister` settles it like `unregistered`, with a warn line (`TestGroupErrorOpSettlesUnregister`);
  (iii) a restart through `Configure`/`Stop` starts a new transient counter (each generation has its own
  session), and a reply mixing transient and non-transient entries resets it, so the transient names wait
  1000 ms (`TestServiceGroupRestartResetsTransientCounter`, `TestGroupMixedReplyTransientCounterResets`,
  `tunnel/ms_13`); (iv) a single-service handle applies a `registered` naming its service with no frame
  outstanding (already so; `tunnel/18`); (v) `CONNECTION_LIMIT` answers no frame, the 4014 close frees the slot
  (`TestGroupConnectionLimitAnswersNoFrame`); (vi) a re-send with no names left is not sent and frees the slot,
  and a connecting group with nothing else to send is then G1's "nothing to send" (online unless G5)
  (`TestGroupEmptyResendIsNothingToSend`, `tunnel/ms_14`); (vii) a register dropped by OVERLOADED was never
  processed, so a name only it carried is not "may hold" and owes no unregister (the frame remembers which
  names it made "may hold" and an OVERLOADED takes that back), and G7 excludes `UNSUPPORTED_VERSION`: it answers
  the frame, names keep their state, then T24 (`TestGroupUnsupportedVersionNotRejected`). Before this the group
  unregistered such names, rejected names on `UNSUPPORTED_VERSION`, left a freed empty re-send unevaluated and
  logged nothing for an error op answering an unregister. Two more rulings (spec v0.7.2, binding now):
  (viii) a scheduled transient re-send counts as queued, so a paused re-send that empties out while one is
  pending leaves a connecting group at `connecting`/`RETRYING` (G3) until it is answered; G1's "nothing to
  send" needs nothing outstanding, queued or scheduled (`TestGroupEmptyResendWithTransientPendingStaysRetrying`;
  `tunnel/ms_14` unchanged); (ix) a name that owes an unregister from a disable or removal during an
  OVERLOADED pause and is enabled or added again in the same pause folds into the re-send, and the owed
  unregister is dropped from the queue, so the edge never gets an unregister after the register of a name
  the group shows active (`TestGroupPauseUndoDropsOwedUnregister`: disable+enable and remove+add).
  A Remove+Add within a pause carries the old entry's may-hold/holds marks, so a later removal still owes
  the unregister (`TestGroupReaddInPauseKeepsHold`). A transient re-send whose names were all disabled or
  removed before its timer fires sends nothing, frees itself and re-evaluates the group like an empty
  OVERLOADED re-send (`TestGroupEmptyTransientResendReevaluates`). Likewise a frame settled by `registered`,
  `unregistered` or a whole-frame error op re-evaluates the group (G1/G5) when nothing is left outstanding,
  queued or scheduled, so a connecting group never stays `RETRYING` with nothing to send
  (`TestGroupSettleReevaluatesWhenNothingLeft`).

## 2026-10-23

- **D-2026-10-23-01** — v0.10.1: `spec.pin` → `v0.7.2` (389bae8, docs only over v0.7.1; every runner's counts
  unchanged). Implements its §2.6/§6A.7 placement rule (spec D-2026-10-19-05): a name of an OVERLOADED-dropped
  register that is removed or locally disabled during the pause leaves the paused frame's names (it still owes
  its unregister if the connection may hold it, ruling i), and a re-add or re-enable in the same pause folds it
  in again among the folded-in names in trigger order, after the dropped frame's remaining names, cancelling
  the owed unregister (ruling ix). Dropped `[beta, gamma]`, remove `beta`, add `delta`, re-add `beta` → re-send
  `[gamma, delta, beta]`; v0.10.0 kept `beta` in its original slot (`TestGroupPauseReaddedDroppedNameFoldsLast`,
  remove/add and disable/enable). `version.SDK` → `0.10.1`.
  Core (Anatoly-approved, retry semantics): a server draining with no streams open sends `drain` and closes
  4012 at once; when that close was observed before the drain was delivered, `handleServerDrain` dropped the
  flushed drain and the client took an ordinary loss's backoff (`{Attempt: n, Cause: "draining: rollout"}`,
  the `TestExposeOnRetry` flake). The connection now records the server's drain on arrival
  (`Conn.receivedDrain`), and `watchConn` treats a 4012 or 1000 close after it as the drain hand-over: the drain
  redial (`OnReconnect{0, jitter or retry_after_ms, drain}`), then the non-fatal report; any other close code
  after a drain keeps its own verdict, a fatal one stays fatal (`TestClientDrainThenGoingAwayCloseSeenFirstIsHandOver`).
  WIRE.md §2.8 SHOULD: `(*Stream).Reset` refuses a connection-only code (NO_ERROR, FLOW_CONTROL_ERROR,
  FRAME_SIZE_ERROR, UNSUPPORTED, UNAUTHORIZED, GOING_AWAY, KEEPALIVE_TIMEOUT, APPLICATION_CLOSE) on either role
  with an error wrapping the new `wsmixer.ErrResetCode`, sends nothing and leaves the stream as it was; unknown
  and application codes stay allowed (`TestStreamResetRejectsConnectionOnlyCodes`); `mcpwarp` resets only with
  CANCEL and INTERNAL_ERROR.
  A local RESET (`(*Stream).Reset`, the dispatcher's `resetStream`) and a CLOSE that completes the close now
  retire the stream before the frame is queued, so a peer frame for that id arriving once the frame is out
  (WINDOW increment 0 included) is discarded as stale (WIRE.md §2.5), never answered with
  RESET(PROTOCOL_ERROR) for a stream already reset (`TestStreamResetRetiresBeforeSending`).
  Fixed in v0.11.0 (D-2026-10-24-01; was open, pre-existing, present in v0.10.0): a server `drain` that arrives after `welcome` but before the client
  has taken ownership of the connection (published it as `cl.conn` in `onAttemptSucceeded`; the drain is
  delivered on the connection's own delivery goroutine, unordered against that) is not handled as a drain:
  delivered before `dialOnce` binds its handler it is dropped (`client_reconnect.go:1475`, the
  `boundConn.Load()` nil check), after the bind but before ownership `handleServerDrain` closes the connection
  as stale with NO_ERROR (`client_reconnect.go:1866-1878`), so the client takes whatever redial that close
  leads to (an ordinary loss's) instead of the drain hand-over. Not fixed in v0.10.1: it changes the core's
  connection-ownership and retry ordering. The tests that hit it (`TestClientDrainRaceWinnerThenDrainAgain`,
  `TestClientDrainThenErrorCloseDeliversOnDrain`) now wait for the client to own the connection before the
  server drains, so they are deterministic.

## 2026-10-24

- **D-2026-10-24-01** — v0.11.0, BREAKING (Anatoly's decision): every SDK offers the same functionality, so the
  multi-service controller of D-2026-10-19-02 leaves `mcpwarp`. Removed: `NewServiceGroup`, `ServiceGroup` and
  its methods (`Configure`, `Stop`, `Dispose`, `AddService`, `RemoveService`, `Enable`, `Disable`, `Status`,
  `Subscribe`, `Client`), `ServiceGroupOptions` (incl. `TrustServerIDHeader`, `Metrics`), `ServiceGroupConfig`,
  `ServiceConfig`, `ServiceState` and its six constants, `ServiceStatus`, `ServiceGroupStatus`,
  `DescribeServiceStatus`, `StatusMessages.Services`, the `service.*` and `lastError.NO_SERVICE_REGISTERED`
  status-text tables (a §6A.9 key, the group's G5 text, not a tunnel lastError) and `DescribeStatus`'s G5 detail
  rule; `group.go`, `group_session.go`, `group_status.go` and their tests and fixture runner; the bridge's
  per-stream route hook (Host label split, `X-Mcpwarp-Server-Id` trust gate), the agent's `registrar` interface,
  per-name stream reset and stream tagging, the group's 502 warn line, the `closeFrameKey` and
  `transportConfig.group` seams. `Expose` and `NewTunnel` behave as in v0.10.1 (their session, the v0.7.x
  rulings, the drain and Reset fixes are unchanged); `wsmixer` is unchanged.
  Kept, because a multi-service host such as the mcpwarp CLI builds on them: `RegisterEntry`,
  `EncodeRegisterList`, `EncodeUnregisterList`, `MaxServicesPerMessage`, `EncodeRegister`/`EncodeUnregister`,
  `DecodeEnvelope` with its per-name messages (it validates and keeps every entry of a batch reply, and the
  single-service session ignores other names, §2.3), `RegisterErrorCode`/`AppErrorCode`/`IsTransientRegisterError`
  (its doc now lists §2.5's transient set; `TestIsTransientRegisterErrorSet` compares it with the pinned spec's
  table), `ServiceNameRE`/`ServiceNameMax`/`IsValidServiceName`/`DefaultURL`, `ServerIDHeader`/`ServerNameHeader`
  (moved to `envelope.go`), `TokenSource`/`TokenSourceFunc`, `Error`, and the `Expose`/`Tunnel`/status-text API.
  `version.SDK` → `0.11.0`; README: the service-group section replaced by a pointer to §6A, and "Migrating to
  v0.11.0". `spec.pin` → `v0.8.0` (5962fec), which makes §6A informative and classifies the fixtures (MCPWARP.md §10.6,
  spec/README.md "Fixture classification"). The runners follow it, skipping with "host guidance (spec v0.8.0
  §10.6)": tunnel `ms_01`..`ms_14`; the four `multi_service_*` bridging fixtures; status-text `cases.json` cases
  with `input.service` or an `input.status` carrying `services` (`rules.json` `hostOnlyCases`, whose two
  markers the runner takes from its text); the
  `rules.json` `hostOnly` keys are neither generated nor compared (the generator reads that list); the encoder
  comparison of `register_batch_valid`/`register_two_services` (kept as `TestEncodeRegisterListBatchFixtures`).
  Every envelope decode case, tunnel 01..45 and every other bridging and status-text case run; §10.6's
  "schema verdict" rows are checker tooling, so the runners assert wire verdicts only.
  Core (`wsmixer`, CLIENT-SDK.md Handler delivery, spec D-2026-10-24-03): a Client's connection delivers
  nothing between `welcome` and becoming current. Its frames (`app`, `drain`, stream `OPEN`) wait in the
  delivery queue, in wire order, and are delivered from the moment the Client makes it current (under `cl.mu`,
  as `onAttemptSucceeded` publishes it): until `OnConnect` binds a handler of its own they reach
  `ClientConfig`'s handlers (mcpwarp's unattributed path). A drain in that window is now the drain hand-over,
  not a stale close. Whether the connection became current or ended first is decided once, under the
  connection's lock: one that became current owes its frames even if it closes before `OnConnect` runs (an
  `error`/`conflict` op right before a 4014 still reaches the layer, MCPWARP.md §2.7); one that ended first
  drops them, and only its disconnect is reported. A connection that loses a drain race is still closed as
  stale. `Dial` alone delivers at once, as before (`TestClientFramesBeforeCurrentAreQueued`: drain hand-over,
  app messages in order once current, frames dropped for a connection that closed first, frames still
  delivered for one that closed after becoming current but before its `OnConnect`).
  Single-service session (spec v0.8.0 §2.6, D-2026-10-24-03): during an OVERLOADED pause a `registered` naming
  the service settles the handle, cancels the re-send and resets the OVERLOADED counter (unchanged); an empty
  `registered` or an error op answers nothing and leaves the counter and the re-send as they are (they used to
  settle the frame, reset the counter and, for the empty reply, schedule a transient re-send); a `registered`
  with nothing outstanding and no pause leaves the OVERLOADED counter alone (it used to reset it), the transient
  counter following §3.2 as before (`TestSessionOverloadedPauseReplies`). `version.SDK` stays `0.11.0`.
