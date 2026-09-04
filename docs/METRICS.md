# Metrics

Status: normative for this repo's `Metrics` interface. The wire protocol these callbacks observe is
specified in [`ws-mixer-spec`'s `docs/WIRE.md`](https://github.com/mcpwarp/ws-mixer-spec/blob/main/docs/WIRE.md).
For the Prometheus metric names, labels, and deployment guidance built on top of this interface, see
[`ws-mixer-server`'s `docs/OBSERVABILITY.md`](https://github.com/mcpwarp/ws-mixer-server/blob/main/docs/OBSERVABILITY.md).

`wsmixer.Metrics` is a callback interface an embedder implements to wire this package's events into
whatever metrics library it uses. The core package has no Prometheus (or any other) dependency: pass
`wsmixer.NoopMetrics{}` (the default when `Options.Metrics` is left nil) to disable observability
entirely.

Every method must return promptly — each is called synchronously from the connection's
read/write/handshake goroutines and must never block on I/O.

```go
type Metrics interface {
    ConnectionOpened(session, role string)
    ConnectionClosed(session string, closeCode int, errorCode string)
    HandshakeFailed(stage string)
    StreamOpened(session string, streamID uint32)
    StreamClosed(session string, streamID uint32, duration time.Duration)
    StreamReset(session string, streamID uint32, code ErrorCode)
    PingRTT(session string, rtt time.Duration)
    KeepaliveTimeout(session string)
    DrainStarted(session string, reason string)
    DrainCompleted(session string, reason string)
    DrainReceived(session string, reason string)
    ProtocolViolation(session string, code string)
    StaleFrameDiscarded(session string, streamID uint32)
    UnknownFrameType(session string, frameType uint8)
    AppMessage(session string, direction string)
    ControlMessage(session string, msgType string, direction string)
    WindowUpdate(session string, streamID uint32, increment uint32)
    DuplicatePong(session string, id int64)
    StreamCancelledOnDrain(session string, streamID uint32)
    BytesTransferred(session string, direction string, n int64)
    SendWindowBlocked(session string, streamID uint32, d time.Duration)
    RecvWindowSample(session string, streamID uint32, remaining int64)
}
```

`role` is `"client"` or `"server"` — both roles call `ConnectionOpened`/`ConnectionClosed`, since a `*Conn`
is symmetric regardless of which side dialed or accepted it.

## What each callback means

| Callback | Fires when |
|---|---|
| `ConnectionOpened(session, role)` | The handshake (`hello`/`welcome`) completed successfully, on both the dialing client and the accepting side of `AcceptConn`. |
| `ConnectionClosed(session, closeCode, errorCode)` | The connection is fully torn down. `closeCode` is the WS close code actually sent/received (`4000 + errorCode` per the mechanical rule, or `1000`/other non-`ws-mixer` codes); `errorCode` is the wire error name, e.g. `"KEEPALIVE_TIMEOUT"`. |
| `HandshakeFailed(stage)` | The handshake did not complete — `stage` names where it failed (e.g. `"server"` for a failed `AcceptConn`). |
| `StreamOpened(session, streamID)` | A stream transitions to `open` — sent or received `OPEN`. |
| `StreamClosed(session, streamID, duration)` | A stream is **fully retired**: `CLOSE` sent and received, or `RESET` either way (`WIRE.md` §2.5's "fully closed"). `duration` is wall-clock lifetime since `OPEN`/`newStream`. |
| `StreamReset(session, streamID, code)` | A stream ended via `RESET`, in either direction, with the given `ErrorCode`. |
| `PingRTT(session, rtt)` | A `pong` was matched to its `ping`; `rtt` is measured from this side's own clock, so no clock-skew problem. Doubles as the tunnel latency SLI. |
| `KeepaliveTimeout(session)` | No `pong` was received within `ping_timeout`; the connection is about to fail with `KEEPALIVE_TIMEOUT`. |
| `DrainStarted(session, reason)` | This side called `Conn.Drain(reason, opts)` and started the drain sequence. |
| `DrainCompleted(session, reason)` | This side's drain sequence finished (survivors reset, connection closed with `GOING_AWAY`). |
| `DrainReceived(session, reason)` | This side received a `drain` message from the peer. |
| `ProtocolViolation(session, code)` | Something hit a connection-fatal error path (`code` is the resulting `ErrorCode` name). |
| `StaleFrameDiscarded(session, streamID)` | A frame arrived for a stream that was open and is now gone — the benign race `WIRE.md` §2.5 decision 3 calls out; discarded, not an error. |
| `UnknownFrameType(session, frameType)` | A frame header carried a `type` byte this implementation does not recognize. Ignored per the wire spec, but counted. |
| `AppMessage(session, direction)` | An `app` control message crossed the wire; `direction` is `"send"` or `"recv"`. |
| `ControlMessage(session, msgType, direction)` | Any stream-0 message crossed the wire; `msgType` is the `t` discriminator. |
| `WindowUpdate(session, streamID, increment)` | A `WINDOW` frame was sent or received for the given stream. |
| `DuplicatePong(session, id)` | A `pong` arrived for a `ping` id this side had already matched — tolerated and counted, not an error. |
| `StreamCancelledOnDrain(session, streamID)` | A stream survived past `Drain`'s deadline and was `RESET(CANCEL)`ed. Non-zero in a normal rollout means `deadline_ms` is too short. |
| `BytesTransferred(session, direction, n)` | `n` DATA **payload** bytes (not counting the 8-byte frame header) crossed the wire on one stream; `direction` is `"send"` or `"recv"`. |
| `SendWindowBlocked(session, streamID, d)` | An application `Write()` had to wait `d` for send credit to become available — the number that tells you whether the configured window is right. Never called for a write that had credit immediately. |
| `RecvWindowSample(session, streamID, remaining)` | A sampled snapshot of remaining receive credit on one stream, taken on every `DATA` frame. |

One gauge from the original design, `wsmixer_socket_buffered_bytes`, is deliberately **not** part of this
interface: `coder/websocket`'s `Write` is synchronous and returns only once the write syscall completes
(see [`docs/DECISIONS.md`](./DECISIONS.md)'s write-model comparison), so this package has no queued-byte
count to sample. An embedder that needs that gauge has to read it from the OS socket (e.g. `/proc/net` or
the underlying fd's send-buffer size), not from `wsmixer`.
