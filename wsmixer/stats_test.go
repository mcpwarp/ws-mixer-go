package wsmixer

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"
)

// newHandshakenClientConn builds a client-role *Conn against a fakeWS,
// completing a real clientHandshake against a synthetic welcome (mirrors
// sequence_test.go's performHandshakePrelude for the "role client, first
// step recv welcome" case), so Stats tests can feed raw frames directly
// without a real network round trip.
func newHandshakenClientConn(t *testing.T) (*Conn, *fakeWS) {
	t.Helper()
	fake := newFakeWS()
	opts := Options{
		Window: 262144, MaxStreams: 2,
		PingInterval: time.Hour, PingTimeout: 2 * time.Hour, HelloTimeout: time.Hour,
		Logger: discardLogger(), Metrics: NoopMetrics{},
	}
	opts.SetDefaults()
	c := newConn(fake, RoleClient, opts)

	welcome := &WelcomeMsg{
		T: "welcome", V: 1, Session: "sess-1", Window: 262144, MaxStreams: 2,
		PingInterval: 30000, PingTimeout: 90000,
	}
	wb, err := json.Marshal(welcome)
	if err != nil {
		t.Fatalf("marshal welcome: %v", err)
	}
	fake.feedInbound(EncodeData(0, wb))
	if err := clientHandshake(context.Background(), c, ClientOptions{
		Options: opts, Token: "test-token",
		Agent: AgentInfo{SDK: "stats-test", SDKVersion: "0.0.0"},
	}); err != nil {
		t.Fatalf("clientHandshake: %v", err)
	}
	<-fake.outbound // drain the hello clientHandshake sent
	t.Cleanup(func() { _ = fake.Close(0, "") })
	return c, fake
}

// streamByID is a small test helper reaching into Conn's internal stream
// table (white-box test, same package).
func (c *Conn) streamByID(id uint32) *Stream {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.streams[id]
}

func waitForNoStream(t *testing.T, c *Conn, id uint32) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if c.streamByID(id) == nil {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("stream %d was never retired", id)
}

// TestStatsCounters drives one handshaken client *Conn through each "ignore
// and count" counter CLIENT-SDK.md requires, asserting Stats() reflects it.
func TestStatsCounters(t *testing.T) {
	c, fake := newHandshakenClientConn(t)

	// RefusedOpens: MaxStreams=2's cap is exhausted after two live streams,
	// so a third OPEN is refused with RESET(STREAM_LIMIT).
	openCh := make(chan *Stream, 4)
	c.OnStream(func(st *Stream) { openCh <- st })
	// Shorten the ping interval before Run() starts pingLoop's ticker (it
	// reads c.pingInterval once, at startup) so DuplicatePongs below has a
	// real outgoing ping to answer without a real 30s wait.
	c.pingInterval = 20 * time.Millisecond
	c.Run()

	// UnknownFrameTypes: a frame type byte outside the v1 five.
	fake.feedInbound(EncodeFrame(&Frame{Type: FrameType(0x7f), StreamID: 0}))
	fake.feedInbound(EncodeOpen(1))
	fake.feedInbound(EncodeOpen(3))
	for i := 0; i < 2; i++ {
		select {
		case <-openCh:
		case <-time.After(2 * time.Second):
			t.Fatal("stream never delivered to OnStream")
		}
	}
	fake.feedInbound(EncodeOpen(5)) // refused: max_streams=2 already open

	// StaleFrames: close stream 1 both ways, then send more DATA for it.
	fake.feedInbound(EncodeFrame(&Frame{Type: FrameClose, StreamID: 1}))
	if err := c.streamByID(1).CloseWrite(); err != nil {
		t.Fatalf("CloseWrite: %v", err)
	}
	waitForNoStream(t, c, 1)
	fake.feedInbound(EncodeData(1, []byte("late")))

	// DuplicatePongs: observe our own outgoing ping's id, then answer it
	// twice -- the second is a duplicate.
	pingID := waitForOutgoingPing(t, fake)
	pong := []byte(fmt.Sprintf(`{"t":"pong","id":%d}`, pingID))
	fake.feedInbound(EncodeData(0, pong))
	fake.feedInbound(EncodeData(0, pong))

	// Give the async dispatch/delivery loops time to process everything
	// queued above.
	deadline := time.Now().Add(2 * time.Second)
	var s Stats
	for time.Now().Before(deadline) {
		s = c.Stats()
		if s.UnknownFrameTypes >= 1 && s.RefusedOpens >= 1 && s.StaleFrames >= 1 && s.DuplicatePongs >= 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if s.UnknownFrameTypes < 1 {
		t.Errorf("UnknownFrameTypes = %d, want >= 1", s.UnknownFrameTypes)
	}
	if s.RefusedOpens < 1 {
		t.Errorf("RefusedOpens = %d, want >= 1", s.RefusedOpens)
	}
	if s.StaleFrames < 1 {
		t.Errorf("StaleFrames = %d, want >= 1", s.StaleFrames)
	}
	if s.DuplicatePongs < 1 {
		t.Errorf("DuplicatePongs = %d, want >= 1", s.DuplicatePongs)
	}
	if s.BytesIn <= 0 {
		t.Errorf("BytesIn = %d, want > 0", s.BytesIn)
	}
	if s.BytesOut <= 0 {
		t.Errorf("BytesOut = %d, want > 0", s.BytesOut)
	}
}

// waitForOutgoingPing drains fake.outbound (discarding anything that isn't a
// stream-0 ping -- other control/stream frames may also be in flight) until
// it finds our own conn's autonomous ping, returning its id.
func waitForOutgoingPing(t *testing.T, fake *fakeWS) int64 {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		select {
		case b := <-fake.outbound:
			frame, err := DecodeFrame(b)
			if err != nil || frame.StreamID != 0 {
				continue
			}
			var probe struct {
				T  string `json:"t"`
				ID int64  `json:"id"`
			}
			if json.Unmarshal(frame.Payload, &probe) == nil && probe.T == "ping" {
				return probe.ID
			}
		case <-deadline:
			t.Fatal("no outgoing ping observed")
		}
	}
}
