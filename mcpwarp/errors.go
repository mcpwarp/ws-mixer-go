package mcpwarp

// Error codes the layer itself produces (§5.4), besides the RegisterErrorCode
// and AppErrorCode values the edge sends.
const (
	// ErrCodeMalformedEnvelope: an inbound envelope failed the §2.8 decode
	// rules. Always non-fatal: the message is dropped.
	ErrCodeMalformedEnvelope = "MALFORMED_ENVELOPE"
	// ErrCodeClosed: the exposure was closed before it registered.
	ErrCodeClosed = "CLOSED"
	// ErrCodeUnauthorized: the token was rejected (close 4011, or HTTP 401 at the upgrade).
	ErrCodeUnauthorized = "UNAUTHORIZED"
	// ErrCodeTakenOver: another connection took the name over before the first registration.
	ErrCodeTakenOver = "TAKEN_OVER"
	// ErrCodeDisconnectedFatal: any other fatal transport outcome, including
	// Reconnect.MaxAttempts running out.
	ErrCodeDisconnectedFatal = "DISCONNECTED_FATAL"
)

// Error is the mcpwarp layer's error type (JS McpwarpError). Code is a
// RegisterErrorCode or AppErrorCode value, ErrCodeMalformedEnvelope, or one
// of the SDK profile's own codes; unknown codes from the edge are carried
// as-is.
type Error struct {
	Code    string
	Message string
	// Fatal is true when the error ended the exposure.
	Fatal       bool
	ServiceName string
	// Cause is the underlying error, when there is one (Unwrap returns it).
	Cause error
}

func (e *Error) Error() string {
	if e.Message == "" {
		return e.Code
	}
	return e.Message
}

func (e *Error) Unwrap() error { return e.Cause }
