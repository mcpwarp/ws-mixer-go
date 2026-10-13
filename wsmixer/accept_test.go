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

// TestAcceptConnBearerRule pins WIRE.md §2.1's Auth row on the server side:
// an empty bearer (the upgrade carried no Authorization header) leaves
// hello.token as the sole credential and still reaches Authenticate, while a
// non-empty bearer must equal hello.token. An empty hello.token never gets
// that far: hello validation rejects it as PROTOCOL_ERROR (§2.7).
func TestAcceptConnBearerRule(t *testing.T) {
	cases := []struct {
		name         string
		bearer       string
		token        string
		rejectAuth   bool
		wantErr      bool
		wantClose    int
		wantAuthCall bool
	}{
		{name: "header matches hello.token", bearer: "test-token", token: "test-token", wantAuthCall: true},
		{name: "no header, hello.token authenticates", bearer: "", token: "test-token", wantAuthCall: true},
		{name: "no header, Authenticate rejects hello.token", bearer: "", token: "bad-token", rejectAuth: true, wantErr: true, wantClose: 4011, wantAuthCall: true},
		{name: "no header, empty hello.token", bearer: "", token: "", wantErr: true, wantClose: 4001},
		{name: "header does not match hello.token", bearer: "test-token", token: "other-token", wantErr: true, wantClose: 4011},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ws := newFakeWS()
			done := make(chan struct{})
			var c *Conn
			var err error
			var authToken string
			var authCalled bool
			go func() {
				defer close(done)
				c, err = AcceptConn(context.Background(), ws, tc.bearer, AcceptOptions{
					Authenticate: func(_ context.Context, h *Hello) (WelcomeMeta, error) {
						authCalled, authToken = true, h.Token
						if tc.rejectAuth {
							return WelcomeMeta{}, Unauthorized("token rejected")
						}
						return WelcomeMeta{}, nil
					},
				})
			}()

			hello, _ := json.Marshal(map[string]any{
				"t": "hello", "v": 1, "token": tc.token,
				"agent": map[string]string{"sdk": "test", "sdk_version": "0.0.0"},
			})
			ws.feedInbound(EncodeData(0, hello))

			select {
			case <-done:
			case <-time.After(2 * time.Second):
				t.Fatal("AcceptConn never returned")
			}
			if authCalled != tc.wantAuthCall {
				t.Errorf("Authenticate called = %v, want %v", authCalled, tc.wantAuthCall)
			}
			if authCalled && authToken != tc.token {
				t.Errorf("Authenticate saw token %q, want %q", authToken, tc.token)
			}
			if !tc.wantErr {
				if err != nil {
					t.Fatalf("AcceptConn: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatal("AcceptConn succeeded, want a handshake failure")
			}
			if got := c.CloseCode(); got != tc.wantClose {
				t.Errorf("CloseCode() = %d, want %d", got, tc.wantClose)
			}
		})
	}
}
