package wsmixer

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

type frameFixture struct {
	Description string         `json:"description"`
	Hex         string         `json:"hex"`
	Valid       bool           `json:"valid"`
	Decoded     map[string]any `json:"decoded"`
	Expect      *struct {
		ErrorCode       string `json:"error_code"`
		ConnectionFatal bool   `json:"connection_fatal"`
	} `json:"expect"`
}

func loadFrameFixtures(t *testing.T) []frameFixture {
	t.Helper()
	dir := filepath.Join(specDir(t), "fixtures", "frames")
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("reading fixtures dir: %v", err)
	}
	var out []frameFixture
	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".json" {
			continue
		}
		data, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			t.Fatalf("reading %s: %v", e.Name(), err)
		}
		var f frameFixture
		if err := json.Unmarshal(data, &f); err != nil {
			t.Fatalf("parsing %s: %v", e.Name(), err)
		}
		f.Description = e.Name() + ": " + f.Description
		out = append(out, f)
	}
	if len(out) == 0 {
		t.Fatal("no frame fixtures found")
	}
	return out
}

func TestFrameFixtures(t *testing.T) {
	for _, f := range loadFrameFixtures(t) {
		f := f
		t.Run(f.Description, func(t *testing.T) {
			raw, err := hex.DecodeString(f.Hex)
			if err != nil {
				t.Fatalf("bad fixture hex: %v", err)
			}
			frame, err := DecodeFrame(raw)

			if f.Valid {
				if err != nil {
					t.Fatalf("expected valid decode, got error: %v", err)
				}
				checkDecoded(t, frame, f.Decoded)
				return
			}

			if err == nil {
				t.Fatalf("expected decode error, got none (frame=%+v)", frame)
			}
			var connErr *ConnError
			var streamErr *StreamError
			switch {
			case errors.As(err, &connErr):
				if !f.Expect.ConnectionFatal {
					t.Fatalf("got connection-fatal ConnError %v, fixture expects stream-scoped", connErr)
				}
				if connErr.Code.String() != f.Expect.ErrorCode {
					t.Fatalf("error code = %s, want %s", connErr.Code, f.Expect.ErrorCode)
				}
			case errors.As(err, &streamErr):
				if f.Expect.ConnectionFatal {
					t.Fatalf("got stream-scoped StreamError %v, fixture expects connection-fatal", streamErr)
				}
				if streamErr.Code.String() != f.Expect.ErrorCode {
					t.Fatalf("error code = %s, want %s", streamErr.Code, f.Expect.ErrorCode)
				}
			default:
				t.Fatalf("unexpected error type %T: %v", err, err)
			}
		})
	}
}

func checkDecoded(t *testing.T, frame *Frame, decoded map[string]any) {
	t.Helper()
	if want, ok := decoded["type"].(string); ok && frame.Type.String() != want {
		t.Errorf("type = %s, want %s", frame.Type, want)
	}
	if want, ok := decoded["flags"].(float64); ok && uint8(want) != frame.Flags {
		t.Errorf("flags = %d, want %d", frame.Flags, uint8(want))
	}
	if want, ok := decoded["stream_id"].(float64); ok && uint32(want) != frame.StreamID {
		t.Errorf("stream_id = %d, want %d", frame.StreamID, uint32(want))
	}
	if want, ok := decoded["payload_hex"].(string); ok {
		if got := hex.EncodeToString(frame.Payload); got != want {
			t.Errorf("payload_hex = %s, want %s", got, want)
		}
	}
	if want, ok := decoded["payload_length"].(float64); ok {
		if got := len(frame.Payload); got != int(want) {
			t.Errorf("payload length = %d, want %d", got, int(want))
		}
	}
	if want, ok := decoded["increment"].(float64); ok {
		if got := frame.WindowIncrement(); got != uint32(want) {
			t.Errorf("increment = %d, want %d", got, uint32(want))
		}
	}
	if want, ok := decoded["code"].(float64); ok {
		if got := frame.ResetCode(); got != ErrorCode(want) {
			t.Errorf("reset code = %d, want %d", got, uint32(want))
		}
	}
	if want, ok := decoded["message"].(string); ok {
		if got := frame.ResetMessage(); got != want {
			t.Errorf("reset message = %q, want %q", got, want)
		}
	}
}

func TestFrameEncodeDecodeRoundTrip(t *testing.T) {
	cases := [][]byte{
		EncodeOpen(1),
		EncodeData(1, []byte("hello world")),
		EncodeData(0, []byte(`{"t":"ping","id":1}`)),
		EncodeWindow(1, 131072),
		EncodeClose(1),
		EncodeReset(1, CancelCode, "cancelled by client"),
	}
	for _, raw := range cases {
		f, err := DecodeFrame(raw)
		if err != nil {
			t.Fatalf("decode failed: %v", err)
		}
		if got := EncodeFrame(f); hex.EncodeToString(got) != hex.EncodeToString(raw) {
			t.Errorf("round trip mismatch: got %x want %x", got, raw)
		}
	}
}
