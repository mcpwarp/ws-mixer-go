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

// TestClientDrainThenErrorCloseDeliversOnDrain is the drain-message
// companion to TestClientApplicationCloseIsRecoverable, covering the same
// deliveryLoop flush-on-close regression (conn.go) one queued event kind
// over: a server that sends drain{} and then, without waiting for anything,
// fails the connection outright (bypassing Conn.Drain's own wait-then-close
// sequence entirely, via the same low-level c.sendControl + c.fail this
// package's own drain/error-path machinery is built on) must still have the
// drain delivered to the client's OnDrain -- it arrived strictly before the
// error{} that ended the connection (OVERVIEW.md section 2.8: error{} is
// always last). Pre-fix, deliveryLoop's priority close-check could drop this
// exactly like the app-message case.
func TestClientDrainThenErrorCloseDeliversOnDrain(t *testing.T) {
	_, url := startTestServer(t, testAcceptHandler{OnConn: func(c *Conn) {
		// deliveryLoop (the thing under test) only exists once Run() has
		// started it, unlike Close's separate pre-Run flush path (see
		// TestClientApplicationCloseIsRecoverablePostRun's identical polling
		// for the same reason) -- wait for the writer loop to actually be
		// running before sending drain, so the drain frame reaches the wire
		// (fail's own pre-Run branch, unlike Close's, does not flush
		// controlQueue -- a separate, narrower gap from D-2026-09-20-01, not
		// this test's concern) and fail() races the real post-Run path this
		// regression is about.
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
			_ = c.sendControl(&DrainMsg{T: "drain", Reason: "maintenance"})
			c.fail(newConnErrorf(InternalErrorCode, "test: connection died right after drain"))
		}()
	}})

	drainCh := make(chan *DrainMsg, 1)
	rec := newDisconnectRecorder()
	fa := &fakeAfter{}
	rc := testReconnectOptions(fa, 0.5)
	// Reconnect disabled: this test is only about delivery ordering, not the
	// reconnect state machine -- a live rc lets handleServerDrain kick off
	// its own parallel reconnect on every cycle (the test server sends drain
	// then fails every connection it accepts), which never converges on its
	// own. Disabled makes handleServerDrain mark drainedNoReconnect instead,
	// so watchConn ends the client with exactly one fatal report once this
	// one connection's own fail() lands.
	rc.Disabled = true
	cl := NewClient(url, StaticToken("tok"), ClientConfig{
		Reconnect: rc,
		OnDrain: func(d *DrainMsg) {
			select {
			case drainCh <- d:
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
	case d := <-drainCh:
		if d.Reason != "maintenance" {
			t.Errorf("OnDrain reason = %q, want maintenance", d.Reason)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("client never received the drain sent just before the fatal close")
	}

	r := rec.waitNext(t)
	if r.ErrorName != "INTERNAL_ERROR" {
		t.Errorf("ErrorName = %q, want INTERNAL_ERROR", r.ErrorName)
	}
}
