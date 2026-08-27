package wsmixer

import (
	"encoding/binary"
	"fmt"
	"unicode/utf8"
)

// FrameType is the first byte of the 8-byte mux frame header (OVERVIEW.md
// section 2.2). Values outside the five known ones are legal on the wire and
// MUST be ignored (and counted) rather than rejected, so FrameType is not a
// closed enum.
type FrameType uint8

const (
	FrameOpen   FrameType = 0x00
	FrameData   FrameType = 0x01
	FrameWindow FrameType = 0x02
	FrameClose  FrameType = 0x03
	FrameReset  FrameType = 0x04
)

var frameTypeNames = map[FrameType]string{
	FrameOpen:   "OPEN",
	FrameData:   "DATA",
	FrameWindow: "WINDOW",
	FrameClose:  "CLOSE",
	FrameReset:  "RESET",
}

// Known reports whether t is one of the five v1 frame types.
func (t FrameType) Known() bool {
	_, ok := frameTypeNames[t]
	return ok
}

// String renders the frame type's wire name, or "UNKNOWN(0xNN)" for a type
// outside the v1 set.
func (t FrameType) String() string {
	if name, ok := frameTypeNames[t]; ok {
		return name
	}
	return fmt.Sprintf("UNKNOWN(0x%02x)", uint8(t))
}

const (
	frameHeaderSize = 8
	// MaxMessageSize is the largest legal WebSocket message: 8-byte header plus
	// a 64 KiB payload (OVERVIEW.md section 2.4).
	MaxMessageSize = frameHeaderSize + 65536
	// MaxStreamZeroPayload is the control-channel message size cap (OVERVIEW.md
	// section 2.4 and 2.7).
	MaxStreamZeroPayload = 16384
	// streamIDMask masks off the reserved high bit of a 32-bit stream id,
	// leaving the 31 bits of actual id space (OVERVIEW.md section 2.5).
	streamIDHighBit uint32 = 0x8000_0000
)

// Frame is one decoded ws-mixer mux frame: the 8-byte header plus its payload.
// One WebSocket binary message carries exactly one Frame.
type Frame struct {
	Type     FrameType
	Flags    uint8
	StreamID uint32
	Payload  []byte
}

// DecodeFrame parses one WebSocket message into a Frame per OVERVIEW.md
// sections 2.2-2.4. The returned error is either a *ConnError (connection-fatal)
// or a *StreamError (scoped to Frame.StreamID); both are safe to inspect even
// though the returned *Frame is nil in the error case.
func DecodeFrame(msg []byte) (*Frame, error) {
	if len(msg) < frameHeaderSize {
		return nil, newConnErrorf(ProtocolErrorCode, "frame header too short: %d bytes, need at least %d", len(msg), frameHeaderSize)
	}
	if len(msg) > MaxMessageSize {
		return nil, newConnErrorf(FrameSizeError, "message of %d bytes exceeds the %d byte limit", len(msg), MaxMessageSize)
	}

	typ := FrameType(msg[0])
	flags := msg[1]
	// msg[2:4] is reserved; ignore on receive.
	streamID := binary.BigEndian.Uint32(msg[4:8])
	payload := msg[8:]

	if streamID&streamIDHighBit != 0 {
		return nil, newConnErrorf(ProtocolErrorCode, "stream id 0x%x has the reserved high bit set", streamID)
	}

	f := &Frame{Type: typ, Flags: flags, StreamID: streamID, Payload: payload}

	if !typ.Known() {
		// Unknown type: structurally accepted. The caller ignores it and
		// increments a counter (OVERVIEW.md section 2.2).
		return f, nil
	}

	switch typ {
	case FrameOpen, FrameClose, FrameWindow, FrameReset:
		if streamID == 0 {
			return nil, newConnErrorf(ProtocolErrorCode, "%s is not legal on stream 0 (control channel)", typ)
		}
	}

	switch typ {
	case FrameOpen:
		if streamID%2 == 0 {
			return nil, newConnErrorf(ProtocolErrorCode, "OPEN for even stream id %d: only the server opens streams and server ids are always odd", streamID)
		}

	case FrameData:
		if streamID == 0 && len(payload) > MaxStreamZeroPayload {
			return nil, newConnErrorf(EnhanceYourCalm, "stream 0 payload of %d bytes exceeds the %d byte control-channel limit", len(payload), MaxStreamZeroPayload)
		}

	case FrameWindow:
		if len(payload) != 4 {
			return nil, newConnErrorf(FrameSizeError, "WINDOW payload is %d bytes, must be exactly 4", len(payload))
		}
		increment := binary.BigEndian.Uint32(payload)
		if increment == 0 {
			return nil, newStreamErrorf(streamID, ProtocolErrorCode, "WINDOW increment of 0 is not legal (range is 1..2^31-1)")
		}
		if increment&streamIDHighBit != 0 {
			return nil, newConnErrorf(FlowControlError, "WINDOW increment %d would push the send window past 2^31-1", increment)
		}

	case FrameReset:
		if len(payload) < 4 {
			return nil, newConnErrorf(FrameSizeError, "RESET payload is %d bytes, must be at least 4", len(payload))
		}
	}

	return f, nil
}

// WindowIncrement returns the 4-byte increment carried by a WINDOW frame.
// The caller must have already established f.Type == FrameWindow.
func (f *Frame) WindowIncrement() uint32 {
	return binary.BigEndian.Uint32(f.Payload)
}

// ResetCode returns the error code carried by a RESET frame. The caller must
// have already established f.Type == FrameReset.
func (f *Frame) ResetCode() ErrorCode {
	return ErrorCode(binary.BigEndian.Uint32(f.Payload[:4]))
}

// ResetMessage returns the (UTF-8 sanitized) message carried by a RESET frame.
// Invalid UTF-8 bytes are replaced with U+FFFD rather than rejected
// (OVERVIEW.md section 2.3).
func (f *Frame) ResetMessage() string {
	return sanitizeUTF8(f.Payload[4:])
}

// sanitizeUTF8 replaces each invalid byte in b with U+FFFD, one replacement
// character per invalid byte (not one per invalid run), matching how
// utf8.DecodeRune advances one byte at a time on a decode error.
func sanitizeUTF8(b []byte) string {
	if utf8.Valid(b) {
		return string(b)
	}
	buf := make([]rune, 0, len(b))
	for len(b) > 0 {
		r, size := utf8.DecodeRune(b)
		buf = append(buf, r)
		b = b[size:]
	}
	return string(buf)
}

// EncodeFrame serializes a Frame to its wire form: 8-byte header followed by
// the payload. Flags and the reserved bytes are always written as 0.
func EncodeFrame(f *Frame) []byte {
	out := make([]byte, frameHeaderSize+len(f.Payload))
	out[0] = uint8(f.Type)
	out[1] = 0
	out[2] = 0
	out[3] = 0
	binary.BigEndian.PutUint32(out[4:8], f.StreamID)
	copy(out[8:], f.Payload)
	return out
}

// EncodeOpen builds an OPEN frame for streamID.
func EncodeOpen(streamID uint32) []byte {
	return EncodeFrame(&Frame{Type: FrameOpen, StreamID: streamID})
}

// EncodeData builds a DATA frame carrying payload on streamID.
func EncodeData(streamID uint32, payload []byte) []byte {
	return EncodeFrame(&Frame{Type: FrameData, StreamID: streamID, Payload: payload})
}

// EncodeWindow builds a WINDOW frame granting increment additional credit on
// streamID. increment must be in 1..2^31-1.
func EncodeWindow(streamID uint32, increment uint32) []byte {
	payload := make([]byte, 4)
	binary.BigEndian.PutUint32(payload, increment)
	return EncodeFrame(&Frame{Type: FrameWindow, StreamID: streamID, Payload: payload})
}

// EncodeClose builds a CLOSE frame (half-close) for streamID.
func EncodeClose(streamID uint32) []byte {
	return EncodeFrame(&Frame{Type: FrameClose, StreamID: streamID})
}

// EncodeReset builds a RESET frame for streamID with the given error code and
// an optional human-readable message.
func EncodeReset(streamID uint32, code ErrorCode, message string) []byte {
	payload := make([]byte, 4+len(message))
	binary.BigEndian.PutUint32(payload[:4], uint32(code))
	copy(payload[4:], message)
	return EncodeFrame(&Frame{Type: FrameReset, StreamID: streamID, Payload: payload})
}
