package wsmixer

import (
	"context"
	"errors"
	"testing"
	"time"
)

// WIRE.md §2.5 (v0.6.3): a WINDOW for a retired stream is discarded and
// counted stale whatever its increment, 0 included -- no RESET; an
// increment of 0 on a live stream is RESET PROTOCOL_ERROR.
func TestWindowIncrementZeroStaleVersusLive(t *testing.T) {
	connCh := make(chan *Conn, 1)
	_, url := startTestServer(t, testAcceptHandler{OnConn: func(c *Conn) { connCh <- c }})
	held := make(chan *Stream, 2)
	cc := dialTestClient(t, url, ClientOptions{OnStream: func(st *Stream) { held <- st }})
	sc := <-connCh
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Retire stream A on both sides (CLOSE both ways).
	sA, err := sc.OpenStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = sA.Write([]byte("a"))
	_ = sA.CloseWrite()
	cA := <-held
	_, _ = cA.ReadContext(ctx, make([]byte, 8))
	_ = cA.CloseWrite()
	_, _ = sA.ReadContext(ctx, make([]byte, 8)) // the client's CLOSE: A is retired on both sides
	waitFor := func(cond func() bool, what string) {
		t.Helper()
		deadline := time.Now().Add(2 * time.Second)
		for !cond() {
			if time.Now().After(deadline) {
				t.Fatal(what)
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
	waitFor(func() bool { return cA.State() == "closed" }, "stream A never retired on the client")
	clientStale, serverStale := cc.Stats().StaleFrames, sc.Stats().StaleFrames

	sc.sendControlFrame(EncodeWindow(sA.ID(), 0))
	waitFor(func() bool { return cc.Stats().StaleFrames == clientStale+1 }, "the WINDOW on the retired stream was not counted stale")
	time.Sleep(100 * time.Millisecond)
	if got := sc.Stats().StaleFrames; got != serverStale {
		t.Fatalf("the server saw %d stale frames, so the client RESET a retired stream", got-serverStale)
	}

	// Stream B is live: increment 0 is RESET PROTOCOL_ERROR.
	sB, err := sc.OpenStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = sB.Write([]byte("b"))
	<-held
	sc.sendControlFrame(EncodeWindow(sB.ID(), 0))
	_, err = sB.ReadContext(ctx, make([]byte, 8))
	if _, err2 := sB.ReadContext(ctx, make([]byte, 8)); err == nil {
		err = err2
	}
	var se *StreamError
	if !errors.As(err, &se) || se.Code != ProtocolErrorCode {
		t.Fatalf("live stream ended with %v, want RESET PROTOCOL_ERROR", err)
	}
	select {
	case <-cc.Done():
		t.Fatalf("the connection ended (%v); increment 0 is stream-scoped", cc.Err())
	default:
	}
}
