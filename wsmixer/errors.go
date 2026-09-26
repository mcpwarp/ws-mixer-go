package wsmixer

import "fmt"

// ErrorCode is a value from the ws-mixer.v1 wire error code table (WIRE.md
// §2.8). It is shared between stream RESET frames and the connection-level
// `error` control message.
type ErrorCode uint32

// The v1 error code space. 0x0000_0000-0x0000_0fff is reserved for ws-mixer;
// codes >= 0x1000_0000 are free for the layer above and are never produced by
// this package. Unknown codes received on the wire are treated as InternalError.
const (
	NoError           ErrorCode = 0x00
	ProtocolErrorCode ErrorCode = 0x01
	InternalErrorCode ErrorCode = 0x02
	FlowControlError  ErrorCode = 0x03
	FrameSizeError    ErrorCode = 0x04
	StreamClosedCode  ErrorCode = 0x05
	RefusedStreamCode ErrorCode = 0x06
	CancelCode        ErrorCode = 0x07
	StreamLimitCode   ErrorCode = 0x08
	EnhanceYourCalm   ErrorCode = 0x09
	UnsupportedCode   ErrorCode = 0x0a
	UnauthorizedCode  ErrorCode = 0x0b
	GoingAwayCode     ErrorCode = 0x0c
	KeepaliveTimeout  ErrorCode = 0x0d
	// ApplicationCloseCode: the application layer above ws-mixer closed the
	// connection for a reason ws-mixer does not interpret (WIRE.md
	// §2.8's error code table). This package never produces it itself
	// -- it is sent only by an application, on either side, via Conn.Close.
	ApplicationCloseCode ErrorCode = 0x0e
)

var errorCodeNames = map[ErrorCode]string{
	NoError:              "NO_ERROR",
	ProtocolErrorCode:    "PROTOCOL_ERROR",
	InternalErrorCode:    "INTERNAL_ERROR",
	FlowControlError:     "FLOW_CONTROL_ERROR",
	FrameSizeError:       "FRAME_SIZE_ERROR",
	StreamClosedCode:     "STREAM_CLOSED",
	RefusedStreamCode:    "REFUSED_STREAM",
	CancelCode:           "CANCEL",
	StreamLimitCode:      "STREAM_LIMIT",
	EnhanceYourCalm:      "ENHANCE_YOUR_CALM",
	UnsupportedCode:      "UNSUPPORTED",
	UnauthorizedCode:     "UNAUTHORIZED",
	GoingAwayCode:        "GOING_AWAY",
	KeepaliveTimeout:     "KEEPALIVE_TIMEOUT",
	ApplicationCloseCode: "APPLICATION_CLOSE",
}

// String renders the error code's wire name, e.g. "FLOW_CONTROL_ERROR".
// An unrecognized code (anything not in the table above, including any code
// >= 0x1000_0000) renders as "INTERNAL_ERROR", matching the "unknown codes
// MUST NOT trigger special behaviour" rule.
func (c ErrorCode) String() string {
	if name, ok := errorCodeNames[c]; ok {
		return name
	}
	return "INTERNAL_ERROR"
}

// CloseCode returns the WebSocket close code for this error, per the mechanical
// rule ws_close = 4000 + error_code, with NO_ERROR mapping to 1000.
func (c ErrorCode) CloseCode() int {
	if c == NoError {
		return 1000
	}
	return 4000 + int(c)
}

// ParseErrorCode looks up an error code by its wire name (e.g. "STREAM_LIMIT").
// It returns false if the name is not one of the v1 codes.
func ParseErrorCode(name string) (ErrorCode, bool) {
	for code, n := range errorCodeNames {
		if n == name {
			return code, true
		}
	}
	return 0, false
}

// ConnError is a connection-fatal failure: it desynchronizes shared connection
// state (credit accounting, the stream id space, framing) and always results in
// an `error` control message followed by a WebSocket close at CloseCode().
type ConnError struct {
	Code            ErrorCode
	Message         string
	StreamID        uint32 // offending stream, if attributable; 0 if none
	HasStreamID     bool
	LastStreamID    uint32
	HasLastStreamID bool
}

func (e *ConnError) Error() string {
	if e.StreamID != 0 || e.HasStreamID {
		return fmt.Sprintf("ws-mixer: connection error %s (stream %d): %s", e.Code, e.StreamID, e.Message)
	}
	return fmt.Sprintf("ws-mixer: connection error %s: %s", e.Code, e.Message)
}

// CloseCode returns the WebSocket close code this connection error maps to.
func (e *ConnError) CloseCode() int { return e.Code.CloseCode() }

// StreamError is scoped to a single stream: it produces a RESET(code, message)
// frame on that stream only, and the connection remains usable.
type StreamError struct {
	Code     ErrorCode
	Message  string
	StreamID uint32
}

func (e *StreamError) Error() string {
	return fmt.Sprintf("ws-mixer: stream %d error %s: %s", e.StreamID, e.Code, e.Message)
}

// newConnErrorf builds a *ConnError with a formatted message.
func newConnErrorf(code ErrorCode, format string, args ...any) *ConnError {
	return &ConnError{Code: code, Message: fmt.Sprintf(format, args...)}
}

// newStreamErrorf builds a *StreamError with a formatted message.
func newStreamErrorf(streamID uint32, code ErrorCode, format string, args ...any) *StreamError {
	return &StreamError{Code: code, StreamID: streamID, Message: fmt.Sprintf(format, args...)}
}
