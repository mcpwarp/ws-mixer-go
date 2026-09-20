package wsmixer

// Tests for ApplicationCloseCode (0x0e, WIRE.md section 2.8): named in the
// wire error code table, but never produced by this package -- an
// application uses it via Conn.Close to close for a reason ws-mixer itself
// does not interpret.

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func TestApplicationCloseCode(t *testing.T) {
	if got := ApplicationCloseCode.String(); got != "APPLICATION_CLOSE" {
		t.Errorf("String() = %q, want APPLICATION_CLOSE", got)
	}
	if got := ApplicationCloseCode.CloseCode(); got != 4014 {
		t.Errorf("CloseCode() = %d, want 4014", got)
	}
	code, ok := ParseErrorCode("APPLICATION_CLOSE")
	if !ok || code != ApplicationCloseCode {
		t.Errorf("ParseErrorCode(APPLICATION_CLOSE) = %v/%v, want %v/true", code, ok, ApplicationCloseCode)
	}
}

func TestUnknownErrorCodeRendersInternalError(t *testing.T) {
	if got := ErrorCode(0x0f).String(); got != "INTERNAL_ERROR" {
		t.Errorf("String() = %q, want INTERNAL_ERROR", got)
	}
}

// TestClientApplicationCloseIsRecoverable checks the exact production shape
// a real refusal takes: a server's OnConn callback sends a best-effort app
// notice and then closes with ApplicationCloseCode SYNCHRONOUSLY, before Run
// is ever called (testAcceptHandler's OnConn(c); c.Run(); <-c.Done() order,
// matching AcceptConn's documented contract) -- deterministically exercising
// Close's pre-Run path (conn.go), not racing it. The client must see the app
// message, then the error{} (WS 4014/APPLICATION_CLOSE), non-fatal --
// client_reconnect.go's close-code switch schedules jitter(cap) for 4014,
// not fullJitter (see TestClientApplicationCloseStartsAtCap for that
// timing assertion; this test only checks the frame/report shape).
func TestClientApplicationCloseIsRecoverable(t *testing.T) {
	const wantMessage = "CONNECTION_LIMIT: test"
	appCh := make(chan json.RawMessage, 1)
	_, url := startTestServer(t, testAcceptHandler{OnConn: func(c *Conn) {
		_ = c.SendApp(context.Background(), map[string]any{"reason": "over_capacity"})
		_ = c.Close(uint32(ApplicationCloseCode), wantMessage)
	}})
	rec := newDisconnectRecorder()
	fa := &fakeAfter{}
	rc := testReconnectOptions(fa, 0.5)
	rc.MaxAttempts = 1 // one failure is all this test needs; APPLICATION_CLOSE is non-fatal and would otherwise reconnect forever
	cl := NewClient(url, StaticToken("tok"), ClientConfig{
		Reconnect: rc,
		OnApp: func(body json.RawMessage) {
			select {
			case appCh <- body:
			default:
			}
		},
		OnDisconnect: rec.onDisconnect,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := cl.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer cl.Close(context.Background())

	select {
	case <-appCh:
	case <-time.After(2 * time.Second):
		t.Fatal("client never received the app notice sent just before the refusal")
	}

	r := rec.waitNext(t)
	if r.WSCode != 4014 {
		t.Errorf("WSCode = %d, want 4014", r.WSCode)
	}
	if r.ErrorName != "APPLICATION_CLOSE" {
		t.Errorf("ErrorName = %q, want APPLICATION_CLOSE", r.ErrorName)
	}
	if r.Message != wantMessage {
		t.Errorf("Message = %q, want %q", r.Message, wantMessage)
	}
	if r.Fatal {
		t.Errorf("Fatal = true, want false")
	}
}

// TestClientApplicationCloseIsRecoverablePostRun exercises Close's post-Run
// path deterministically instead of racing it: OnConn's goroutine polls
// c.running (white-box -- this is an in-package test) until Run has
// actually started the writer loop before calling Close. Both this and
// TestClientApplicationCloseIsRecoverable's pre-Run shape must converge on
// the same client-observed result.
func TestClientApplicationCloseIsRecoverablePostRun(t *testing.T) {
	const wantMessage = "CONNECTION_LIMIT: test"
	_, url := startTestServer(t, testAcceptHandler{OnConn: func(c *Conn) {
		go func() {
			deadline := time.Now().Add(2 * time.Second)
			for {
				c.mu.Lock()
				running := c.running
				c.mu.Unlock()
				if running || time.Now().After(deadline) {
					break
				}
				time.Sleep(time.Millisecond)
			}
			_ = c.Close(uint32(ApplicationCloseCode), wantMessage)
		}()
	}})
	rec := newDisconnectRecorder()
	fa := &fakeAfter{}
	rc := testReconnectOptions(fa, 0.5)
	rc.MaxAttempts = 1 // one failure is all this test needs; APPLICATION_CLOSE is non-fatal and would otherwise reconnect forever
	cl := NewClient(url, StaticToken("tok"), ClientConfig{
		Reconnect:    rc,
		OnDisconnect: rec.onDisconnect,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := cl.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer cl.Close(context.Background())

	r := rec.waitNext(t)
	if r.WSCode != 4014 {
		t.Errorf("WSCode = %d, want 4014", r.WSCode)
	}
	if r.ErrorName != "APPLICATION_CLOSE" {
		t.Errorf("ErrorName = %q, want APPLICATION_CLOSE", r.ErrorName)
	}
	if r.Fatal {
		t.Errorf("Fatal = true, want false")
	}
}
