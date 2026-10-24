package mcpwarp

import (
	"context"
	"log/slog"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/mcpwarp/ws-mixer-go/wsmixer"
)

// A status listener may call Configure: the call returns instead of
// waiting behind the callback it runs in.
func TestTunnelConfigureFromListener(t *testing.T) {
	tun, err := NewTunnel(TunnelOptions{Integration: testIntegration, Handler: http.NotFoundHandler()})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	var once sync.Once
	tun.Subscribe(func(s TunnelStatus) {
		if s.State == StateError {
			once.Do(func() {
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				done <- tun.Configure(ctx, TunnelConfig{Enabled: false})
			})
		}
	})
	go func() {
		_ = tun.Configure(context.Background(), TunnelConfig{Enabled: true, Token: "t", Name: "notes", Kind: "ftp"})
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Configure from a listener = %v, want it to complete", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Configure from a listener never returned")
	}
	if got := tun.Status(); got != (TunnelStatus{State: StateOff}) {
		t.Fatalf("status %+v, want off", got)
	}
}

// §6.4: Subscribe's first call runs before it returns when nothing is being
// delivered; while another goroutine flushes, that flusher delivers it,
// exactly once and before any later status.
func TestTunnelSubscribeImmediateWhileFlushing(t *testing.T) {
	_, tun := newFakeTunnel(t, nil)
	var idle []TunnelStatus
	tun.Subscribe(func(s TunnelStatus) { idle = append(idle, s) })
	if len(idle) != 1 || idle[0] != snapOff {
		t.Fatalf("idle Subscribe: listener got %+v before returning, want one off", idle)
	}

	tun.mu.Lock()
	tun.flushing = true // another goroutine's flush in progress
	tun.mu.Unlock()
	var got []TunnelStatus
	tun.Subscribe(func(s TunnelStatus) { got = append(got, s) })
	tun.mu.Lock()
	tun.setStatusLocked(snapConnecting) // a change queued after the registration
	tun.flushing = false
	tun.unlockAndFlush() // the other flusher finishing
	if len(got) != 2 || got[0] != snapOff || got[1] != snapConnecting {
		t.Fatalf("listener got %+v, want off then connecting, once each", got)
	}
}

// Subscribe racing status changes: every listener sees a contiguous run of
// the statuses in their order (its first call included), and is never
// invoked on two goroutines at once.
func TestTunnelSubscribeConcurrentWithChanges(t *testing.T) {
	tun, err := NewTunnel(TunnelOptions{Integration: testIntegration, Handler: http.NotFoundHandler()})
	if err != nil {
		t.Fatal(err)
	}
	type rec struct {
		mu       sync.Mutex
		got      []TunnelStatus
		inFlight int32
		overlap  bool
	}
	listen := func(r *rec) func(TunnelStatus) {
		return func(s TunnelStatus) {
			r.mu.Lock()
			r.inFlight++
			if r.inFlight > 1 {
				r.overlap = true
			}
			r.got = append(r.got, s)
			first := len(r.got) == 1
			r.mu.Unlock()
			if first {
				// A slow first call: one made outside the deliverer overlaps
				// the deliverer's next call for this listener.
				time.Sleep(2 * time.Millisecond)
			}
			r.mu.Lock()
			r.inFlight--
			r.mu.Unlock()
		}
	}
	primary := &rec{}
	tun.Subscribe(listen(primary))

	changes := make(chan struct{})
	go func() {
		defer close(changes)
		ctx := context.Background()
		for i := 0; i < 60; i++ {
			_ = tun.Configure(ctx, TunnelConfig{Enabled: true, Token: "t", Name: "notes", Kind: "ftp"}) // connecting → error
			_ = tun.Configure(ctx, TunnelConfig{Enabled: false})                                        // off
		}
	}()
	var mu sync.Mutex
	var subs []*rec
	var wg sync.WaitGroup
	for g := 0; g < 6; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				r := &rec{}
				mu.Lock()
				subs = append(subs, r)
				mu.Unlock()
				unsub := tun.Subscribe(listen(r))
				if i%2 == 0 {
					unsub()
				}
			}
		}()
	}
	wg.Wait()
	<-changes
	// Deliveries are asynchronous whenever another goroutine is delivering,
	// and the one deliverer works through every listener's slow first call
	// (2 ms each, ~120 of them): wait, bounded, until every listener has had
	// its first call; one that never gets it is a lost delivery.
	deadline := time.Now().Add(10 * time.Second)
	for {
		pending := -1
		mu.Lock()
		for i, r := range append([]*rec{primary}, subs...) {
			r.mu.Lock()
			if len(r.got) == 0 && pending < 0 {
				pending = i
			}
			r.mu.Unlock()
		}
		mu.Unlock()
		if pending < 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("listener %d never got its first snapshot (§6.4: Subscribe calls l immediately)", pending-1)
		}
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond) // let the deliveries in flight land

	contiguous := func(all, part []TunnelStatus) bool {
		for start := 0; start+len(part) <= len(all); start++ {
			ok := true
			for j := range part {
				if all[start+j] != part[j] {
					ok = false
					break
				}
			}
			if ok {
				return true
			}
		}
		return false
	}
	primary.mu.Lock()
	defer primary.mu.Unlock()
	if primary.overlap {
		t.Fatal("the primary listener ran on two goroutines at once")
	}
	for i, r := range subs {
		r.mu.Lock()
		if r.overlap {
			t.Fatalf("listener %d ran on two goroutines at once", i)
		}
		if len(r.got) == 0 || !contiguous(primary.got, r.got) {
			t.Fatalf("listener %d got %+v: not a contiguous run of %d primary statuses", i, r.got, len(primary.got))
		}
		r.mu.Unlock()
	}
}

// §6.9: SERVER_DISABLED logs "the service is disabled in the mcpwarp
// dashboard"; a disable op logs its reason.
func TestTunnelServerDisabledLogLine(t *testing.T) {
	logs := &logCapture{}
	r, tun := newFakeTunnel(t, func(o *TunnelOptions) { o.Logger = slog.New(logs) })
	tun.configureAsync(notesConfig)
	r.quiesce()
	r.ev("connected", nil)
	r.ev("welcome", nil)
	r.ev("register_error", map[string]any{"code": "SERVER_DISABLED", "message": "server disabled", "id": "srv_notes"})
	r.ev("disabled", map[string]any{"reason": "maintenance"})
	if !logs.has(slog.LevelWarn, "the service is disabled in the mcpwarp dashboard") ||
		!logs.has(slog.LevelWarn, "the service was disabled (maintenance)") || logs.has(slog.LevelWarn, "the service was disabled (disabled)") {
		t.Fatalf("log lines %q", logs.lines())
	}
}

// A drain{replaced} take-over reports conflict before the close's own
// disconnect, also when another goroutine is flushing the session.
func TestAgentReplacedConflictBeforeDisconnect(t *testing.T) {
	r := &tunnelRunner{t: t, clock: &fakeClock{}, overload: 1}
	log := &eventLog{}
	a := newAgent(agentConfig{
		token: wsmixer.StaticToken("t"), name: "notes", integration: testIntegration, handler: http.NotFoundHandler(),
		emit: log.add, after: r.clock.after, newTransport: r.newTransport,
	})
	a.start()
	r.ev("connected", nil)
	r.ev("welcome", nil)
	r.registered()
	a.session.mu.Lock()
	a.session.flushing = true // another goroutine's flush in progress
	a.session.mu.Unlock()
	r.connEvent("drain", map[string]any{"reason": "replaced"})
	a.session.mu.Lock()
	a.session.flushing = false
	a.session.unlockAndFlush()

	conflict, disconnect := -1, -1
	log.mu.Lock()
	for i, ev := range log.events {
		if ev.kind == evConflict && conflict < 0 {
			conflict = i
		}
		if ev.kind == evDisconnect && disconnect < 0 {
			disconnect = i
		}
	}
	log.mu.Unlock()
	if conflict < 0 || disconnect < 0 || disconnect < conflict {
		t.Fatalf("conflict at %d, disconnect at %d: the conflict must come first", conflict, disconnect)
	}
}

// A dispose deadline already past waits for nothing and logs 0 ms.
func TestTunnelDisposePastDeadlineLogsZero(t *testing.T) {
	logs := &logCapture{}
	r, tun := newFakeTunnel(t, func(o *TunnelOptions) { o.Logger = slog.New(logs) })
	r.online(tun, notesConfig)
	r.ev("hold", nil)
	done := tun.disposeAsync(-3 * time.Millisecond)
	r.quiesce()
	r.advance(0)
	if !isClosed(done) || !logs.has(slog.LevelWarn, "dispose() gave up waiting after 0 ms") {
		t.Fatalf("resolved %v, logs %q", isClosed(done), logs.lines())
	}
}

// An *Error without a message is logged by its code once.
func TestTechnicalCodeOnce(t *testing.T) {
	if got := technical(&Error{Code: "UNKNOWN_OP"}); got != "UNKNOWN_OP" {
		t.Fatalf("technical = %q", got)
	}
	if got := technical(&Error{Code: "UNKNOWN_OP", Message: "unknown op ping"}); got != "UNKNOWN_OP: unknown op ping" {
		t.Fatalf("technical = %q", got)
	}
}
