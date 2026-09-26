package wsmixer

// Tests for Conn.SendApp's error on a conn that has already ended: it reports
// the same connClosedErr Stream.Read/Write do, instead of silently dropping
// the frame and returning nil.

import (
	"context"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"time"
)

type appSendCounter struct {
	NoopMetrics
	sends atomic.Int64
}

func (m *appSendCounter) AppMessage(_ string, direction string) {
	if direction == "send" {
		m.sends.Add(1)
	}
}

func waitConnClosed(t *testing.T, c *Conn) {
	t.Helper()
	select {
	case <-c.closed:
	case <-time.After(2 * time.Second):
		t.Fatal("c.closed never closed")
	}
}

func TestSendAppLiveConnQueuesFrame(t *testing.T) {
	ws := newFakeWS()
	c := newUnrunConn(ws)
	c.Run()
	defer func() { _ = ws.CloseNow() }()

	if err := c.SendApp(context.Background(), map[string]any{"x": 1}); err != nil {
		t.Fatalf("SendApp on a live conn = %v, want nil", err)
	}
	select {
	case b := <-ws.outbound:
		f, err := DecodeFrame(b)
		if err != nil || f.StreamID != 0 {
			t.Fatalf("expected a stream-0 frame, got %v (err=%v)", f, err)
		}
		msg, _ := ParseControl(f.Payload)
		if _, ok := msg.(*AppMsg); !ok {
			t.Fatalf("expected app{}, got %T", msg)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("app{} never reached the wire")
	}
}

// The loops repeat the call because a controlQueue with room and an
// already-closed c.closed are both ready at once: without SendApp's up-front
// closed check, select would pick the queue (and return nil) about half the
// time.
func TestSendAppAfterAbnormalCloseReturnsUnexpectedEOF(t *testing.T) {
	ws := newFakeWS()
	c := newUnrunConn(ws)
	c.Run()
	defer func() { _ = ws.CloseNow() }()

	_ = ws.CloseNow() // no WS close frame: Err() stays nil
	waitConnClosed(t, c)

	for i := 0; i < 32; i++ {
		if err := c.SendApp(context.Background(), map[string]any{"i": i}); !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("call %d: SendApp after abnormal close = %v, want io.ErrUnexpectedEOF", i, err)
		}
	}
}

func TestSendAppAfterCleanCloseReturnsCloseError(t *testing.T) {
	ws := newFakeWS()
	c := newUnrunConn(ws)
	c.Run()
	defer func() { _ = ws.CloseNow() }()

	if err := c.Close(0, "bye"); err != nil {
		t.Fatalf("Close: %v", err)
	}
	waitConnClosed(t, c)

	for i := 0; i < 32; i++ {
		err := c.SendApp(context.Background(), map[string]any{"i": i})
		var ce *ConnError
		if !errors.As(err, &ce) || ce.Code != NoError || ce.Message != "bye" {
			t.Fatalf("call %d: SendApp after Close = %v, want the close error (NO_ERROR, \"bye\")", i, err)
		}
		if err != c.Err() {
			t.Fatalf("call %d: SendApp error %v is not Conn.Err() %v", i, err, c.Err())
		}
	}
}

// An unrun conn has no writer draining controlQueue, so once it is full the
// enqueue can only be abandoned via ctx; nothing was queued, so nothing may
// be counted as sent.
func TestSendAppFullQueueCancelledCtxReturnsCtxErr(t *testing.T) {
	ws := newFakeWS()
	c := newUnrunConn(ws)
	defer func() { _ = ws.CloseNow() }()
	m := &appSendCounter{}
	c.opts.Metrics = m

	for len(c.controlQueue) < cap(c.controlQueue) {
		c.controlQueue <- EncodeData(0, []byte(`{"t":"app","body":{}}`))
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan error, 1)
	go func() { done <- c.SendApp(ctx, map[string]any{"x": 1}) }()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("SendApp on a full queue with a cancelled ctx = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("SendApp ignored its cancelled ctx and blocked on the full controlQueue")
	}
	if n := m.sends.Load(); n != 0 {
		t.Fatalf("AppMessage(send) counted %d times for a frame that was never queued", n)
	}
}
