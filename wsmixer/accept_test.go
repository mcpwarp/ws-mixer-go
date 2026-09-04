package wsmixer

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

// TestAcceptConnZeroValueOptionsDefaults is the regression test for
// AcceptConn applying Options.SetDefaults() itself: a caller passing a
// zero-value AcceptOptions{} must not panic (HelloTimeout==0 used to fire
// the hello-timeout callback immediately, which then hit a nil Metrics
// interface) and must send a welcome carrying the documented defaults, not
// zero values.
func TestAcceptConnZeroValueOptionsDefaults(t *testing.T) {
	ws := newFakeWS()
	done := make(chan struct{})
	var c *Conn
	var err error
	go func() {
		defer close(done)
		c, err = AcceptConn(context.Background(), ws, "test-token", AcceptOptions{})
	}()

	ws.feedInbound(EncodeData(0, []byte(`{"t":"hello","v":1,"token":"test-token","agent":{"sdk":"test","sdk_version":"0.0.0"}}`)))

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("AcceptConn never returned")
	}
	if err != nil {
		t.Fatalf("AcceptConn: %v", err)
	}

	var welcome WelcomeMsg
	select {
	case b := <-ws.outbound:
		frame, ferr := DecodeFrame(b)
		if ferr != nil {
			t.Fatalf("decoding welcome frame: %v", ferr)
		}
		if err := json.Unmarshal(frame.Payload, &welcome); err != nil {
			t.Fatalf("unmarshaling welcome: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no welcome sent")
	}

	if welcome.Window != 262144 {
		t.Errorf("welcome.window = %d, want 262144", welcome.Window)
	}
	if welcome.MaxStreams != 64 {
		t.Errorf("welcome.max_streams = %d, want 64", welcome.MaxStreams)
	}
	if c.opts.ReadLimit != MaxMessageSize {
		t.Errorf("ReadLimit = %d, want %d", c.opts.ReadLimit, MaxMessageSize)
	}
}

// TestAcceptConnSessionIDOverride covers AcceptOptions.SessionID: when set,
// the welcome (and Conn.Session()) must use its value instead of the default
// crypto/rand-generated id.
func TestAcceptConnSessionIDOverride(t *testing.T) {
	ws := newFakeWS()
	done := make(chan struct{})
	var c *Conn
	var err error
	go func() {
		defer close(done)
		c, err = AcceptConn(context.Background(), ws, "test-token", AcceptOptions{
			SessionID: func() string { return "fixed-session-id" },
		})
	}()

	ws.feedInbound(EncodeData(0, []byte(`{"t":"hello","v":1,"token":"test-token","agent":{"sdk":"test","sdk_version":"0.0.0"}}`)))

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("AcceptConn never returned")
	}
	if err != nil {
		t.Fatalf("AcceptConn: %v", err)
	}
	if c.Session() != "fixed-session-id" {
		t.Errorf("Session() = %q, want %q", c.Session(), "fixed-session-id")
	}

	var welcome WelcomeMsg
	select {
	case b := <-ws.outbound:
		frame, ferr := DecodeFrame(b)
		if ferr != nil {
			t.Fatalf("decoding welcome frame: %v", ferr)
		}
		if err := json.Unmarshal(frame.Payload, &welcome); err != nil {
			t.Fatalf("unmarshaling welcome: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no welcome sent")
	}
	if welcome.Session != "fixed-session-id" {
		t.Errorf("welcome.session = %q, want %q", welcome.Session, "fixed-session-id")
	}
}
