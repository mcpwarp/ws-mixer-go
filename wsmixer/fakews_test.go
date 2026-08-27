package wsmixer

import (
	"context"
	"io"
	"sync"

	"github.com/coder/websocket"
)

// fakeWS is an in-memory stand-in for *websocket.Conn used by the sequence
// test harness: inbound messages are fed by the test, outbound messages are
// captured for assertion.
type fakeWS struct {
	inbound   chan []byte
	outbound  chan []byte
	closed    chan struct{}
	closeOnce sync.Once
}

func newFakeWS() *fakeWS {
	return &fakeWS{
		inbound:  make(chan []byte, 64),
		outbound: make(chan []byte, 64),
		closed:   make(chan struct{}),
	}
}

func (f *fakeWS) feedInbound(b []byte) {
	select {
	case f.inbound <- b:
	case <-f.closed:
	}
}

func (f *fakeWS) Read(ctx context.Context) (websocket.MessageType, []byte, error) {
	select {
	case b := <-f.inbound:
		return websocket.MessageBinary, b, nil
	case <-f.closed:
		return 0, nil, io.EOF
	case <-ctx.Done():
		return 0, nil, ctx.Err()
	}
}

func (f *fakeWS) Write(ctx context.Context, typ websocket.MessageType, p []byte) error {
	cp := append([]byte(nil), p...)
	select {
	case f.outbound <- cp:
		return nil
	case <-f.closed:
		return io.ErrClosedPipe
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (f *fakeWS) Close(code websocket.StatusCode, reason string) error {
	f.closeOnce.Do(func() { close(f.closed) })
	return nil
}

func (f *fakeWS) CloseNow() error { return f.Close(websocket.StatusNormalClosure, "") }

func (f *fakeWS) SetReadLimit(n int64) {}
