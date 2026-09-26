package wsmixer

// Tests for Change 2 (docs/DECISIONS.md 2026-09-20, StableAfter): the
// backoff attempt counter and every once-only reconnect budget
// (keepaliveRetryUsed, refreshRetryUsed) are re-armed only once a
// connection has stayed up ReconnectOptions.StableAfter past its own
// welcome (armStability), not at welcome itself.

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// hybridAfter behaves like fakeAfter (fires every requested delay
// immediately) except for calls requesting exactly heldDuration, which are
// held back until a test explicitly releases them via releaseAllHeld.
// armStability calls rc.after(StableAfter) from the very same seam every
// backoff/jitter delay in this file uses (deliberately, per StableAfter's
// own doc comment, so tests stay deterministic) -- a test proving something
// about stability NOT having elapsed yet (as opposed to every other delay
// here, which is happy to fire instantly) needs a way to hold that one
// specific value back independently of everything else.
type hybridAfter struct {
	mu           sync.Mutex
	calls        []time.Duration
	held         []chan time.Time
	heldDuration time.Duration
}

func newHybridAfter(heldDuration time.Duration) *hybridAfter {
	return &hybridAfter{heldDuration: heldDuration}
}

func (h *hybridAfter) after(d time.Duration) <-chan time.Time {
	h.mu.Lock()
	h.calls = append(h.calls, d)
	if d == h.heldDuration {
		ch := make(chan time.Time, 1)
		h.held = append(h.held, ch)
		h.mu.Unlock()
		return ch
	}
	h.mu.Unlock()
	ch := make(chan time.Time, 1)
	ch <- time.Now()
	return ch
}

func (h *hybridAfter) snapshot() []time.Duration {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]time.Duration(nil), h.calls...)
}

// heldCount returns how many heldDuration calls have been requested so far
// (released or not).
func (h *hybridAfter) heldCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.held)
}

// releaseAllHeld fires every currently held channel and forgets them, then
// reports how many it released. Safe even for a held channel whose own
// goroutine already stopped waiting on it (e.g. armStability returned via
// its conn.Done() case instead): the send is buffered, so nobody has to
// ever receive it.
func (h *hybridAfter) releaseAllHeld() int {
	h.mu.Lock()
	toRelease := h.held
	h.held = nil
	h.mu.Unlock()
	for _, ch := range toRelease {
		ch <- time.Now()
	}
	return len(toRelease)
}

// waitForHeldCount polls h until at least n heldDuration calls have been
// requested, or fails the test.
func waitForHeldCount(t *testing.T, h *hybridAfter, n int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if h.heldCount() >= n {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("only %d StableAfter arms requested after 2s, want >= %d", h.heldCount(), n)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// waitForHybridBackoffCalls polls h until it has recorded at least n
// non-stability (backoff/jitter) delays, or fails the test.
func waitForHybridBackoffCalls(t *testing.T, h *hybridAfter, n int) []time.Duration {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		calls := nonStabilityCalls(h.snapshot(), h.heldDuration)
		if len(calls) >= n {
			return calls
		}
		if time.Now().After(deadline) {
			t.Fatalf("only %d non-stability delays recorded after 2s, want >= %d (raw: %v)", len(calls), n, h.snapshot())
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// clientAttempt/clientKeepaliveRetryUsed/clientRefreshRetryUsed are
// white-box accessors for Client's unexported reconnect-budget state, used
// only by this package's own tests.
func clientAttempt(cl *Client) int {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	return cl.attempt
}

func clientRefreshRetryUsed(cl *Client) bool {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	return cl.refreshRetryUsed
}

func clientKeepaliveRetryUsed(cl *Client) bool {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	return cl.keepaliveRetryUsed
}

// stabilityTestReconnectOptions is testReconnectOptions's stability-test
// counterpart: Base/Cap/ConnectTimeout match, but rc.after is h.after (a
// hybridAfter, not a plain fakeAfter), so armStability's StableAfter arm
// (h.heldDuration) can be held back independently of every backoff/jitter
// delay these tests configure.
func stabilityTestReconnectOptions(h *hybridAfter, rnd float64) ReconnectOptions {
	rc := ReconnectOptions{
		Base: 10 * time.Millisecond, Cap: time.Second, ConnectTimeout: 2 * time.Second,
		StableAfter: h.heldDuration,
	}
	rc.after = h.after
	rc.randFloat64 = fixedRand(rnd)
	return rc
}

// TestClientAttemptResetOnlyAfterStability replaces the old
// TestClientAttemptResetOnWelcome (which pinned reset-on-welcome): a server
// that welcomes a connection and then immediately closes it, repeated N
// times, must never let the backoff attempt counter reset -- stability
// never elapses (the StableAfter arm is held, never released) -- so the
// computed delay keeps climbing exactly like a run of ordinary failures
// that never welcome at all.
func TestClientAttemptResetOnlyAfterStability(t *testing.T) {
	const cycles = 4
	_, url := startTestServer(t, testAcceptHandler{OnConn: func(c *Conn) {
		go func() { _ = c.Close(uint32(ProtocolErrorCode), "welcome then immediate close") }()
	}})

	rec := newDisconnectRecorder()
	h := newHybridAfter(10 * time.Second) // never released in this test
	rc := stabilityTestReconnectOptions(h, 1)
	rc.MaxAttempts = cycles + 2
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

	calls := waitForHybridBackoffCalls(t, h, cycles)
	for i := 0; i < cycles; i++ {
		want := rc.fullJitter(i + 1)
		if calls[i] != want {
			t.Errorf("delay[%d] = %v, want %v (climbing: attempt never reset, stability never elapsed)", i, calls[i], want)
		}
	}
}

// TestClientAttemptResetAfterStabilityElapses: a connection that survives
// StableAfter, then drops, must have its backoff delay back at the first
// rung -- not accumulating from wherever a prior, never-reset failure left
// it.
func TestClientAttemptResetAfterStabilityElapses(t *testing.T) {
	var n atomic.Int32
	_, url := startTestServer(t, testAcceptHandler{OnConn: func(c *Conn) {
		if n.Add(1) == 1 {
			// First connection: fail immediately, bumping attempt 0->1.
			go func() { _ = c.Close(uint32(ProtocolErrorCode), "force one failed cycle first") }()
		}
		// Second connection (and the eventual third, forced below): stay up.
	}})

	rec := newDisconnectRecorder()
	h := newHybridAfter(10 * time.Second)
	rc := stabilityTestReconnectOptions(h, 1)
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

	// First failure: attempt 0->1, delay = fullJitter(1).
	rec.waitNext(t)
	waitForHybridBackoffCalls(t, h, 1)

	// The second connection lands and welcomes, arming its own stability
	// timer (held). The first (already-closed) connection also welcomed
	// before this test forced it to fail (testAcceptHandler's OnConn only
	// ever fires post-welcome), so it armed and held one too -- releasing
	// it below is harmless: that connection's own armStability goroutine
	// already returned via conn.Done() once it closed.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && cl.Conn() == nil {
		time.Sleep(5 * time.Millisecond)
	}
	if cl.Conn() == nil {
		t.Fatal("client never reconnected")
	}
	waitForHeldCount(t, h, 2)

	// Let stability actually elapse.
	h.releaseAllHeld()
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && clientAttempt(cl) != 0 {
		time.Sleep(5 * time.Millisecond)
	}
	if got := clientAttempt(cl); got != 0 {
		t.Fatalf("attempt = %d, want 0 after stability elapsed", got)
	}

	// Now drop the (stable) connection and confirm the resulting delay is
	// back at the bottom rung, not fullJitter(2) (which it would be had the
	// earlier failure's attempt=1 never actually been cleared).
	conn := cl.Conn()
	if conn == nil {
		t.Fatal("client not connected")
	}
	_ = conn.Close(uint32(ProtocolErrorCode), "drop after stability")
	rec.waitNext(t)

	calls := waitForHybridBackoffCalls(t, h, 2)
	want := rc.fullJitter(1)
	if got := calls[1]; got != want {
		t.Errorf("post-stability delay = %v, want %v (rc.fullJitter(1): reset actually took effect)", got, want)
	}
}

// TestClientStabilityIgnoresSupersededConnAcrossDrainHandover covers both
// "the stability timer does not fire for a superseded/retired conn" and
// the addendum's per-connection requirement: a drain hand-over's OLD
// (retiring) connection ending, seconds after the NEW connection's own
// welcome, must not cancel the new connection's own stability timer --
// each armStability call is scoped to exactly the *Conn it was launched
// for (conn.Done()), never to "whichever conn happens to be active now".
func TestClientStabilityIgnoresSupersededConnAcrossDrainHandover(t *testing.T) {
	connCh := make(chan *Conn, 8)
	_, url := startTestServer(t, testAcceptHandler{OnConn: func(c *Conn) { connCh <- c }})

	rec := newDisconnectRecorder()
	h := newHybridAfter(10 * time.Second)
	rc := stabilityTestReconnectOptions(h, 0)
	var onConnectCount atomic.Int32
	cl := NewClient(url, StaticToken("tok"), ClientConfig{
		Reconnect:    rc,
		OnDisconnect: rec.onDisconnect,
		OnConnect:    func(*Conn, *WelcomeMsg) { onConnectCount.Add(1) },
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := cl.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer cl.Close(context.Background())

	c1 := <-connCh
	// Bump attempt away from zero first, so the eventual reset below is
	// actually observable instead of a no-op "0 -> 0".
	_ = c1.Close(uint32(ProtocolErrorCode), "force one failed cycle first")
	rec.waitNext(t)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && clientAttempt(cl) == 0 {
		time.Sleep(5 * time.Millisecond)
	}
	if clientAttempt(cl) == 0 {
		t.Fatal("attempt was never bumped by the forced failure")
	}

	c2 := <-connCh            // the reconnect after the forced failure
	waitForHeldCount(t, h, 1) // c2's own stability arm

	// Drain c2 -- a parallel reconnect (c3) starts while c2 stays alive.
	// Keep a stream open so c2 doesn't close the instant it drains.
	if _, err := c2.OpenStream(context.Background()); err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	go func() { _ = c2.Drain(context.Background(), "rollout", DrainOptions{Deadline: 10 * time.Second}) }()

	c3 := <-connCh            // the drain-triggered replacement
	waitForHeldCount(t, h, 2) // c3's own stability arm, alongside c2's still-pending one
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && onConnectCount.Load() < 3 {
		time.Sleep(5 * time.Millisecond)
	}
	if onConnectCount.Load() < 3 {
		t.Fatal("the drain-triggered replacement never landed")
	}

	// Let c2 (the drain hand-over's retiring connection) actually finish,
	// strictly after c3's own welcome: the case addendum B calls out.
	_ = c2.Close(uint32(GoingAwayCode), "drain deadline")
	select {
	case <-c2.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("c2 never closed")
	}
	_ = c3

	// Release every held stability arm: c2's own (harmless -- its
	// armStability goroutine already returned via conn.Done() once c2
	// ended, so nothing is listening on this channel anymore) and c3's (the
	// one that matters: it must still fire and reset attempt).
	h.releaseAllHeld()

	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && clientAttempt(cl) != 0 {
		time.Sleep(5 * time.Millisecond)
	}
	if got := clientAttempt(cl); got != 0 {
		t.Errorf("attempt = %d, want 0 (c3's own stability timer must still fire, unaffected by c2 -- its drain-hand-over predecessor -- ending first)", got)
	}
}

// TestClientKeepalive4013NotReArmedByWelcomeThenClose pins the once-only
// 4013 immediate-retry budget (keepaliveRetryUsed) against exactly the bug
// StableAfter exists to fix: before this change, keepaliveRetryUsed was
// cleared at every welcome, so a server that welcomes and immediately
// closes with 4013 made every single cycle look like the very first one --
// an unbounded string of one-second-apart immediate retries, never once
// backing off. With the budget re-armed only at stability, a second (and
// third, and fourth, ...) 4013 arriving before any intervening welcome ever
// stabilizes must all take the normal backoff path -- not just the very
// next one: watchConn's own case 4013 else-branch used to un-spend the
// budget itself on every recurring 4013 (a second, separate bug from the
// welcome-reset one, masked by it at v0.4.1 -- see docs/DECISIONS.md
// 2026-09-20), so a flapping server got an immediate retry on every OTHER
// cycle forever, never climbing past fullJitter(2). Four consecutive
// cycles, not two, is what actually catches that: two only proves the
// budget survived once, which the buggy self-reset also does.
func TestClientKeepalive4013NotReArmedByWelcomeThenClose(t *testing.T) {
	const cycles = 4
	var closeCount atomic.Int32
	_, url := startTestServer(t, testAcceptHandler{OnConn: func(c *Conn) {
		closeCount.Add(1)
		go func() { _ = c.Close(uint32(KeepaliveTimeout), "test keepalive") }()
	}})

	rec := newDisconnectRecorder()
	h := newHybridAfter(10 * time.Second) // never released: stability must not intervene
	rc := stabilityTestReconnectOptions(h, 1)
	rc.MaxAttempts = cycles + 2
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

	// First 4013: immediate retry, no backoff delay recorded for it.
	r := rec.waitNext(t)
	if r.WSCode != 4013 {
		t.Fatalf("reason.WSCode = %d, want 4013", r.WSCode)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && closeCount.Load() < 2 {
		time.Sleep(5 * time.Millisecond)
	}
	if closeCount.Load() < 2 {
		t.Fatal("client never redialed after the first 4013")
	}

	// Every 4013 after the first, on a fresh connection whose own welcome
	// happened but never reached stability (h never releases its held
	// StableAfter arm): keepaliveRetryUsed must stay "used" from the first
	// cycle onward, so every one of these takes the normal-backoff branch,
	// climbing fullJitter(2), (3), (4) -- never another immediate retry.
	for i := 2; i <= cycles; i++ {
		r = rec.waitNext(t)
		if r.WSCode != 4013 {
			t.Fatalf("cycle %d: reason.WSCode = %d, want 4013", i, r.WSCode)
		}
		calls := waitForHybridBackoffCalls(t, h, i-1)
		want := rc.fullJitter(i) // attempt increments by 1 every cycle, used or not
		if got := calls[i-2]; got != want {
			t.Errorf("cycle %d: 4013 delay = %v, want %v (normal backoff, budget stays spent)", i, got, want)
		}
	}
}

// TestClientKeepalive4013BudgetReArmedAfterStability is the positive half:
// once a connection survives StableAfter, the 4013 immediate-retry budget
// is spent again -- the next 4013 gets its own immediate retry, not normal
// backoff.
func TestClientKeepalive4013BudgetReArmedAfterStability(t *testing.T) {
	var n atomic.Int32
	stable := make(chan struct{}, 1)
	_, url := startTestServer(t, testAcceptHandler{OnConn: func(c *Conn) {
		switch n.Add(1) {
		case 1:
			go func() { _ = c.Close(uint32(KeepaliveTimeout), "test keepalive") }()
		case 2:
			select {
			case stable <- struct{}{}:
			default:
			}
			// stays up: let the test release stability, then close it too.
		default:
			go func() { _ = c.Close(uint32(KeepaliveTimeout), "test keepalive") }()
		}
	}})

	rec := newDisconnectRecorder()
	h := newHybridAfter(10 * time.Second)
	rc := stabilityTestReconnectOptions(h, 1)
	rc.MaxAttempts = 5
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

	// First 4013: immediate retry, budget spent.
	rec.waitNext(t)
	select {
	case <-stable:
	case <-time.After(2 * time.Second):
		t.Fatal("second connection never landed")
	}

	// Let this second connection's own stability timer fire. heldCount 2,
	// not 1: the FIRST connection's own welcome also armed and held a
	// stability call before it closed (testAcceptHandler's OnConn only
	// ever fires post-welcome) -- waiting for only 1 could be satisfied by
	// that stale arm alone, racing ahead of the second connection's own
	// call actually being recorded.
	waitForHeldCount(t, h, 2)
	h.releaseAllHeld()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && clientKeepaliveRetryUsed(cl) {
		time.Sleep(5 * time.Millisecond)
	}
	if clientKeepaliveRetryUsed(cl) {
		t.Fatal("keepaliveRetryUsed never cleared after stability elapsed")
	}

	// Now drop the stable connection with another 4013: it must get its
	// own fresh immediate retry (delay 0), not normal backoff.
	conn := cl.Conn()
	if conn == nil {
		t.Fatal("client not connected")
	}
	_ = conn.Close(uint32(KeepaliveTimeout), "test keepalive after stability")
	r := rec.waitNext(t)
	if r.WSCode != 4013 {
		t.Fatalf("reason.WSCode = %d, want 4013", r.WSCode)
	}
	if calls := nonStabilityCalls(h.snapshot(), h.heldDuration); len(calls) != 0 {
		t.Errorf("backoff delays recorded = %v, want none (the post-stability 4013 got its own immediate retry)", calls)
	}
}

// TestClientMaxAttemptsAcrossNeverStableCycles pins MaxAttempts' updated
// meaning (its own doc comment, D-2026-09-20): attempts now accumulate
// across short-lived successful connections, so a server that keeps
// welcoming and immediately closing -- never once letting a connection
// reach stability -- still exhausts MaxAttempts and goes fatal, exactly
// like a server that never welcomes at all (TestClientMaxAttemptsExhaustion).
func TestClientMaxAttemptsAcrossNeverStableCycles(t *testing.T) {
	const maxAttempts = 3
	_, url := startTestServer(t, testAcceptHandler{OnConn: func(c *Conn) {
		go func() { _ = c.Close(uint32(ProtocolErrorCode), "welcome then immediate close") }()
	}})

	rec := newDisconnectRecorder()
	h := newHybridAfter(10 * time.Second) // never released
	rc := stabilityTestReconnectOptions(h, 0)
	rc.MaxAttempts = maxAttempts
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

	var last DisconnectReason
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		last = rec.waitNext(t)
		if last.Fatal {
			break
		}
	}
	if !last.Fatal {
		t.Fatalf("final reason.Fatal = false, want true: %+v", last)
	}
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && cl.State() != "closed" {
		time.Sleep(10 * time.Millisecond)
	}
	if got := cl.State(); got != "closed" {
		t.Errorf("State() = %s, want closed after MaxAttempts exhausted across never-stable welcome cycles", got)
	}
}

// TestClientArmStabilityDoesNotLeakAfterClose pins armStability's own
// goroutine lifetime bound: h never releases its held StableAfter arm (so
// armStability's own `<-cl.rc.after(...)` case can never fire on its own),
// meaning the only way each of these goroutines -- and Close() itself,
// which wg.Wait()s on them -- can ever return is via closeCh. A leak here
// would make Close() itself hang, not just leave a stray goroutine behind.
func TestClientArmStabilityDoesNotLeakAfterClose(t *testing.T) {
	const n = 10
	_, url := startTestServer(t, testAcceptHandler{})
	before := settledGoroutines(t)

	for i := 0; i < n; i++ {
		h := newHybridAfter(10 * time.Second)
		rc := stabilityTestReconnectOptions(h, 0)
		cl := NewClient(url, StaticToken("tok"), ClientConfig{Reconnect: rc})
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := cl.Connect(ctx); err != nil {
			cancel()
			t.Fatalf("client #%d: Connect: %v", i, err)
		}
		cancel()
		done := make(chan struct{})
		go func() { _ = cl.Close(context.Background()); close(done) }()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Fatalf("client #%d: Close() did not return within 3s -- armStability wedged wg.Wait()?", i)
		}
	}

	after := settledGoroutines(t)
	if after > before+5 {
		t.Errorf("goroutine count %d -> %d after %d clients + Close (armStability leak?)", before, after, n)
	}
}

// --- Addendum A: the 401/4011 refresh-retry is a once-only budget too ----

// TestClientRefreshRetryBudgetFatalWhenNotYetStable: a rejection, refreshed
// successfully, welcomed, then closed again before stability ever elapses,
// followed by another rejected redial -- must be fatal at once, with no
// second refresh-retry call to the provider for that second rejection
// (only one HTTP request follows it: the rejected redial itself).
func TestClientRefreshRetryBudgetFatalWhenNotYetStable(t *testing.T) {
	var reqNum atomic.Int32
	var tokenCalls atomic.Int32
	inner := newTestListener(testAcceptHandler{
		Options: Options{Logger: discardLogger()},
		OnConn: func(c *Conn) {
			go func() { _ = c.Close(uint32(ProtocolErrorCode), "welcome then immediate close, before stability") }()
		},
	})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := reqNum.Add(1)
		if n == 1 || n == 3 {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		inner.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	url := "ws" + srv.URL[len("http"):] + "/tunnel"

	token := TokenProvider(func(context.Context) (string, error) {
		tokenCalls.Add(1)
		return "tok", nil
	})

	rec := newDisconnectRecorder()
	h := newHybridAfter(10 * time.Second) // held: stability never elapses across this scenario
	rc := stabilityTestReconnectOptions(h, 0)
	cl := NewClient(url, token, ClientConfig{
		Reconnect:    rc,
		OnDisconnect: rec.onDisconnect,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := cl.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer cl.Close(context.Background())

	var last DisconnectReason
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		last = rec.waitNext(t)
		if last.Fatal {
			break
		}
	}
	if !last.Fatal {
		t.Fatalf("final reason.Fatal = false, want true (the refresh-retry budget was already spent): %+v", last)
	}
	// req#1: rejected (the budget's primary rejection). req#2: the
	// refresh-retry's own redial, which succeeds and welcomes. req#3: the
	// next cycle's own primary dial, rejected again -- since stability
	// never elapsed, refreshRetryUsed is still spent, so this goes straight
	// to fatal with no req#4 refresh attempt.
	if got := reqNum.Load(); got != 3 {
		t.Errorf("HTTP requests = %d, want exactly 3 (401, refresh success, then a rejected redial with no further refresh)", got)
	}
	if got := tokenCalls.Load(); got != 3 {
		t.Errorf("token provider calls = %d, want exactly 3 (one per HTTP request above: the refresh-retry itself only ever happened once, on the very first rejection)", got)
	}
}

// TestClientRefreshRetryRetryDialHTTPFailureNotFatal pins item 2 of the
// review: dialAndHandshake's refresh-retry used to force ANY failure of its
// own redial fatal, not just another rejection -- a DNS blip/TCP reset/HTTP
// 5xx landing on that one particular dial killed the client permanently
// even though it was never actually rejected a second time. A 401 followed
// by an HTTP 503 on the refresh-retry's own dial must take the ordinary
// recoverable path (reported non-fatal, backoff scheduled) -- the budget
// still stays spent, though (claimed unconditionally before the retry ever
// dialed), so a LATER 401 (on the backoff-scheduled redial that follows,
// before stability) is still fatal, with no further refresh attempt.
func TestClientRefreshRetryRetryDialHTTPFailureNotFatal(t *testing.T) {
	var reqNum atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch reqNum.Add(1) {
		case 1, 3:
			http.Error(w, "unauthorized", http.StatusUnauthorized)
		default: // 2: not a rejection -- an ordinary HTTP 5xx.
			http.Error(w, "service unavailable", http.StatusServiceUnavailable)
		}
	}))
	t.Cleanup(srv.Close)
	url := "ws" + srv.URL[len("http"):] + "/tunnel"

	token := TokenProvider(func(context.Context) (string, error) { return "tok", nil })

	rec := newDisconnectRecorder()
	h := newHybridAfter(10 * time.Second)     // held: stability never elapses (the server never welcomes at all)
	rc := stabilityTestReconnectOptions(h, 1) // rand=1, not 0: a zero delay would take waitBackoff's fast path and never call rc.after at all
	cl := NewClient(url, token, ClientConfig{
		Reconnect:    rc,
		OnDisconnect: rec.onDisconnect,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// Connect itself doesn't fail: req#1 (401) triggers the inline
	// refresh-retry, req#2 (503) is a non-fatal, ordinary dial failure --
	// Connect never observes a fatal outcome from this first cycle.
	go func() { _ = cl.Connect(ctx) }()
	defer cl.Close(context.Background())

	r := rec.waitNext(t)
	if r.Fatal {
		t.Fatalf("reason.Fatal = true, want false (the refresh-retry's own dial failed non-rejection): %+v", r)
	}
	if r.HTTPStatus != 503 {
		t.Errorf("reason.HTTPStatus = %d, want 503", r.HTTPStatus)
	}
	if clientRefreshRetryUsed(cl) != true {
		t.Error("refreshRetryUsed = false, want true (still spent: claimed before the retry's dial ran)")
	}
	waitForHybridBackoffCalls(t, h, 1) // normal backoff was scheduled for the 503

	// The backoff-scheduled redial (req#3) is rejected again -- since the
	// budget is still spent, this is fatal with no req#4 refresh attempt.
	r = rec.waitNext(t)
	if !r.Fatal {
		t.Fatalf("reason.Fatal = false, want true (a later rejection with the budget still spent): %+v", r)
	}
	if got := reqNum.Load(); got != 3 {
		t.Errorf("HTTP requests = %d, want exactly 3 (401, non-rejection 503, then a rejected redial with no further refresh)", got)
	}
}

// TestClientRefreshRetryRetryDialTransportFailureNotFatal is
// TestClientRefreshRetryRetryDialHTTPFailureNotFatal's transport-level
// sibling: the refresh-retry's own dial fails below the HTTP layer
// entirely -- still not a rejection, still not fatal. The second request
// just hangs (never responds), with a short per-attempt ConnectTimeout, so
// that one specific dial fails with a plain context-deadline error; a
// hijack-then-close was tried first and discarded -- Go's http.Transport
// silently retried the GET on a fresh connection and landed straight on
// the *next* 401 handler branch within the same dialOnce call, never
// actually exercising the non-rejection path this test needs.
func TestClientRefreshRetryRetryDialTransportFailureNotFatal(t *testing.T) {
	var reqNum atomic.Int32
	hang := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := reqNum.Add(1)
		if n == 2 {
			<-hang // never respond: this one dial attempt times out
			return
		}
		http.Error(w, "unauthorized", http.StatusUnauthorized)
	}))
	// Registered in this order so t.Cleanup's LIFO unwind closes hang
	// (unblocking the stuck handler goroutine) BEFORE srv.Close(), which
	// otherwise waits up to 5s for every connection to go idle.
	t.Cleanup(srv.Close)
	t.Cleanup(func() { close(hang) })
	url := "ws" + srv.URL[len("http"):] + "/tunnel"

	token := TokenProvider(func(context.Context) (string, error) { return "tok", nil })

	rec := newDisconnectRecorder()
	h := newHybridAfter(10 * time.Second)
	rc := stabilityTestReconnectOptions(h, 1) // rand=1, not 0: see the HTTP-failure sibling's own note
	rc.ConnectTimeout = 300 * time.Millisecond
	cl := NewClient(url, token, ClientConfig{
		// dialAndHandshake's one ctx covers ConnectTimeout+HelloTimeout in
		// sum (its own doc comment) -- HelloTimeout must also be kept
		// small, or the default (10s) swamps ConnectTimeout and the
		// hanging dial below doesn't time out within this test's own
		// budget at all.
		Options:      Options{HelloTimeout: 200 * time.Millisecond},
		Reconnect:    rc,
		OnDisconnect: rec.onDisconnect,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() { _ = cl.Connect(ctx) }()
	defer cl.Close(context.Background())

	r := rec.waitNext(t)
	if r.Fatal {
		t.Fatalf("reason.Fatal = true, want false (the refresh-retry's own dial failed at the transport level): %+v", r)
	}
	if r.HTTPStatus != 0 {
		t.Errorf("reason.HTTPStatus = %d, want 0 (never reached an HTTP response)", r.HTTPStatus)
	}
	if clientRefreshRetryUsed(cl) != true {
		t.Error("refreshRetryUsed = false, want true (still spent)")
	}
	waitForHybridBackoffCalls(t, h, 1)

	r = rec.waitNext(t)
	if !r.Fatal {
		t.Fatalf("reason.Fatal = false, want true (a later rejection with the budget still spent): %+v", r)
	}
	if got := reqNum.Load(); got != 3 {
		t.Errorf("HTTP requests = %d, want exactly 3", got)
	}
}

// TestClientRefreshRetryRetryDialWelcomeTimeoutNotFatal covers coverage
// gap (a) from the review: every existing refresh-retry test stops at the
// dial layer (the retry's own websocket.Dial call failing) -- none pin the
// retry succeeding at the 101 and then failing POST-upgrade, during the
// welcome wait itself. A 401, refreshed, whose retry dial upgrades but then
// times out waiting for welcome, must take the ordinary recoverable path
// (Phase handshake, WSCode 4001, ErrorName PROTOCOL_ERROR, non-fatal,
// backoff scheduled) -- isUnauthorized(err2) is false for a welcome-timeout
// *ConnError{ProtocolErrorCode}, so this exercises dialAndHandshake's
// review-item-2 `!isUnauthorized` branch one layer higher than the dial
// failures the other refresh-retry tests use. The budget still stays
// spent, so a later rejection (before stability) is fatal with no further
// refresh.
func TestClientRefreshRetryRetryDialWelcomeTimeoutNotFatal(t *testing.T) {
	var reqNum atomic.Int32
	inner := newTestListener(testAcceptHandler{
		Options: Options{Logger: discardLogger()},
		OnRawConn: func(ws *websocket.Conn) {
			// Upgrade succeeds (the 101), then read the client's hello and
			// go silent forever -- the client's own HelloTimeout is what
			// ends this, not the peer.
			for {
				_, _, err := ws.Read(context.Background())
				if err != nil {
					return
				}
			}
		},
	})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := reqNum.Add(1)
		if n == 1 || n == 3 {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		inner.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	url := "ws" + srv.URL[len("http"):] + "/tunnel"

	token := TokenProvider(func(context.Context) (string, error) { return "tok", nil })

	rec := newDisconnectRecorder()
	h := newHybridAfter(10 * time.Second)
	rc := stabilityTestReconnectOptions(h, 1)
	rc.ConnectTimeout = 2 * time.Second
	cl := NewClient(url, token, ClientConfig{
		// HelloTimeout well below ConnectTimeout (the shipped-defaults
		// ratio, not the equal-timeout coin flip
		// TestClientHandshakePhaseWelcomeTimeoutShippedRatio probes): the
		// HelloTimeout AfterFunc reliably wins the race against the shared
		// ctx's own deadline, landing on the graceful 4001 path
		// deterministically.
		Options:      Options{HelloTimeout: 150 * time.Millisecond},
		Reconnect:    rc,
		OnDisconnect: rec.onDisconnect,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() { _ = cl.Connect(ctx) }()
	defer cl.Close(context.Background())

	r := rec.waitNext(t)
	if r.Fatal {
		t.Fatalf("reason.Fatal = true, want false (a welcome timeout on the refresh-retry's own dial is not a rejection): %+v", r)
	}
	if r.Phase != PhaseHandshake {
		t.Errorf("reason.Phase = %v, want handshake", r.Phase)
	}
	if r.WSCode != 4001 {
		t.Errorf("reason.WSCode = %d, want 4001", r.WSCode)
	}
	if !r.HasErrorCode || r.ErrorName != "PROTOCOL_ERROR" {
		t.Errorf("reason.ErrorName = %q (has=%v), want PROTOCOL_ERROR", r.ErrorName, r.HasErrorCode)
	}
	if !clientRefreshRetryUsed(cl) {
		t.Error("refreshRetryUsed = false, want true (still spent)")
	}
	waitForHybridBackoffCalls(t, h, 1)

	// The backoff-scheduled redial (req#3) is rejected again -- since the
	// budget is still spent, this is fatal with no req#4 refresh attempt.
	r = rec.waitNext(t)
	if !r.Fatal {
		t.Fatalf("reason.Fatal = false, want true (a later rejection with the budget still spent): %+v", r)
	}
	if got := reqNum.Load(); got != 3 {
		t.Errorf("HTTP requests = %d, want exactly 3 (401, welcome-timeout refresh, then a rejected redial with no further refresh)", got)
	}
}

// TestClientRefreshRetryRetryDialBareClose4010Fatal covers coverage gap (b)
// from the review: the refresh-retry's own dial getting a bare pre-welcome
// close 4010 (UNSUPPORTED -- WIRE.md section 2.9's fatal set, keyed on the
// close code alone, not on whether error{} preceded it) must still be
// fatal, even though review item 2's `!isUnauthorized(err2)` early return
// now takes err2 down the ordinary path instead of dialAndHandshake's own
// markFatal call -- classifyFailureErr's independent `wsCode == 4010`
// fatal rule (client_reconnect.go) is what actually catches it, downstream
// in onAttemptFailed, proving the fatal set survived the new early return.
func TestClientRefreshRetryRetryDialBareClose4010Fatal(t *testing.T) {
	var reqNum atomic.Int32
	var accepts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := reqNum.Add(1)
		if n == 1 {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		ln := newTestListener(testAcceptHandler{OnRawConn: func(ws *websocket.Conn) {
			accepts.Add(1)
			_ = ws.Close(4010, "unsupported protocol version")
		}})
		ln.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	url := "ws" + srv.URL[len("http"):] + "/tunnel"

	token := TokenProvider(func(context.Context) (string, error) { return "tok", nil })

	rec := newDisconnectRecorder()
	fa := &fakeAfter{}
	cl := NewClient(url, token, ClientConfig{
		Reconnect:    testReconnectOptions(fa, 0),
		OnDisconnect: rec.onDisconnect,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := cl.Connect(ctx); err == nil {
		t.Fatal("Connect should have failed fatally on a bare 4010 close on the refresh-retry's own dial")
	}
	defer cl.Close(context.Background())

	r := rec.waitNext(t)
	if !r.Fatal {
		t.Errorf("reason.Fatal = false, want true (4010 is fatal regardless of what rejected the primary dial)")
	}
	if r.WSCode != 4010 {
		t.Errorf("reason.WSCode = %d, want 4010", r.WSCode)
	}
	if got := reqNum.Load(); got != 2 {
		t.Errorf("HTTP requests = %d, want exactly 2 (401, then the refresh-retry's own dial)", got)
	}
	if got := accepts.Load(); got != 1 {
		t.Errorf("server accepted %d raw connections, want exactly 1", got)
	}
	if got := cl.State(); got != "closed" {
		t.Errorf("State() = %s, want closed", got)
	}
}

// TestClientRefreshRetryBudgetReArmedAfterStability is
// TestClientRefreshRetryBudgetFatalWhenNotYetStable's counterpart: once the
// refreshed connection survives StableAfter, the budget is spent again --
// so the next rejection gets its own refresh-retry rather than going fatal.
func TestClientRefreshRetryBudgetReArmedAfterStability(t *testing.T) {
	var reqNum atomic.Int32
	var tokenCalls atomic.Int32
	connected := make(chan struct{}, 1)
	inner := newTestListener(testAcceptHandler{
		Options: Options{Logger: discardLogger()},
		OnConn: func(c *Conn) {
			select {
			case connected <- struct{}{}:
			default:
			}
		},
	})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := reqNum.Add(1)
		if n == 1 || n == 3 {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		inner.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	url := "ws" + srv.URL[len("http"):] + "/tunnel"

	token := TokenProvider(func(context.Context) (string, error) {
		tokenCalls.Add(1)
		return "tok", nil
	})

	rec := newDisconnectRecorder()
	h := newHybridAfter(10 * time.Second)
	rc := stabilityTestReconnectOptions(h, 0)
	cl := NewClient(url, token, ClientConfig{
		Reconnect:    rc,
		OnDisconnect: rec.onDisconnect,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := cl.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer cl.Close(context.Background())

	select {
	case <-connected:
	case <-time.After(2 * time.Second):
		t.Fatal("client never reached the refreshed connection")
	}
	waitForHeldCount(t, h, 1)
	if released := h.releaseAllHeld(); released != 1 {
		t.Fatalf("released %d held StableAfter arms, want exactly 1", released)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && clientRefreshRetryUsed(cl) {
		time.Sleep(5 * time.Millisecond)
	}
	if clientRefreshRetryUsed(cl) {
		t.Fatal("refreshRetryUsed never cleared after stability elapsed")
	}

	conn := cl.Conn()
	if conn == nil {
		t.Fatal("client not connected")
	}
	_ = conn.Close(uint32(ProtocolErrorCode), "drop after stability, force another rejected redial")

	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && reqNum.Load() < 4 {
		time.Sleep(5 * time.Millisecond)
	}
	if got := reqNum.Load(); got < 4 {
		t.Fatalf("HTTP requests = %d, want >= 4 (the rejected redial got its own refresh-retry, not a fatal)", got)
	}
	for _, r := range rec.snapshotAll() {
		if r.Fatal {
			t.Fatalf("unexpected fatal disconnect: %+v", r)
		}
	}
}
