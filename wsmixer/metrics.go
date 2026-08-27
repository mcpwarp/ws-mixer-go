package wsmixer

import "time"

// Metrics is the callback interface an embedder implements to wire ws-mixer's
// counters and gauges into whatever metrics library it uses (OVERVIEW.md
// section 3.4). The core package has no Prometheus (or any other) dependency;
// pass NoopMetrics{} (the default) to disable observability entirely.
//
// Every method must return promptly: it is called from the connection's
// read/write/handshake goroutines and must never block on I/O.
//
// wsmixer_socket_buffered_bytes (OVERVIEW.md section 3.4) is deliberately not
// part of this interface: coder/websocket does not expose the underlying
// net.Conn's or its own internal write buffer's queued-byte count (Write is
// synchronous and returns only once the write syscall completes, per section
// 3.1's comparison table), so there is nothing for this package to sample. An
// embedder that needs this gauge has to get it from the OS socket (e.g.
// /proc/net or SO_ANY on the underlying fd), not from ws-mixer.
type Metrics interface {
	ConnectionOpened(session, role string)
	ConnectionClosed(session string, closeCode int, errorCode string)
	HandshakeFailed(stage string)
	StreamOpened(session string, streamID uint32)
	// StreamClosed fires once a stream is fully retired (OVERVIEW.md section
	// 2.5's "fully closed" point). duration is wall-clock lifetime since
	// OPEN/newStream, feeding wsmixer_stream_duration_seconds.
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

	// BytesTransferred feeds wsmixer_bytes_total{direction}: n DATA payload
	// bytes ("send" or "recv") crossed the wire on one stream.
	BytesTransferred(session string, direction string, n int64)
	// SendWindowBlocked feeds wsmixer_send_window_blocked_seconds: an
	// application Write() had to wait d for send credit to become available
	// (OVERVIEW.md section 3.4: "the number that tells you whether 256 KiB is
	// right"). Never called for a Write that had credit immediately.
	SendWindowBlocked(session string, streamID uint32, d time.Duration)
	// RecvWindowSample feeds wsmixer_recv_window_bytes: a sampled snapshot of
	// remaining receive credit on one stream, taken on every DATA frame.
	RecvWindowSample(session string, streamID uint32, remaining int64)
}

// NoopMetrics discards every event. It is the default when Options.Metrics is
// left nil.
type NoopMetrics struct{}

func (NoopMetrics) ConnectionOpened(session, role string)                              {}
func (NoopMetrics) ConnectionClosed(session string, closeCode int, code string)        {}
func (NoopMetrics) HandshakeFailed(stage string)                                       {}
func (NoopMetrics) StreamOpened(session string, streamID uint32)                       {}
func (NoopMetrics) StreamClosed(session string, streamID uint32, d time.Duration)      {}
func (NoopMetrics) StreamReset(session string, streamID uint32, code ErrorCode)        {}
func (NoopMetrics) PingRTT(session string, rtt time.Duration)                          {}
func (NoopMetrics) KeepaliveTimeout(session string)                                    {}
func (NoopMetrics) DrainStarted(session string, reason string)                         {}
func (NoopMetrics) DrainCompleted(session string, reason string)                       {}
func (NoopMetrics) DrainReceived(session string, reason string)                        {}
func (NoopMetrics) ProtocolViolation(session string, code string)                      {}
func (NoopMetrics) StaleFrameDiscarded(session string, streamID uint32)                {}
func (NoopMetrics) UnknownFrameType(session string, frameType uint8)                   {}
func (NoopMetrics) AppMessage(session string, direction string)                        {}
func (NoopMetrics) ControlMessage(session, msgType, direction string)                  {}
func (NoopMetrics) WindowUpdate(session string, streamID uint32, increment uint32)     {}
func (NoopMetrics) DuplicatePong(session string, id int64)                             {}
func (NoopMetrics) StreamCancelledOnDrain(session string, streamID uint32)             {}
func (NoopMetrics) BytesTransferred(session string, direction string, n int64)         {}
func (NoopMetrics) SendWindowBlocked(session string, streamID uint32, d time.Duration) {}
func (NoopMetrics) RecvWindowSample(session string, streamID uint32, remaining int64)  {}
