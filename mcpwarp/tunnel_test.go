package mcpwarp

import (
	"context"
	"errors"
	"log/slog"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"
)

var notesConfig = TunnelConfig{Enabled: true, Token: "t", Name: "notes"}

func (r *tunnelRunner) ev(name string, p map[string]any) {
	r.t.Helper()
	if p == nil {
		p = map[string]any{}
	}
	r.connEvent(name, p)
	r.quiesce()
}

func (r *tunnelRunner) registered() {
	r.ev("registered", map[string]any{"services": []any{map[string]any{"url": "https://notes-tester.mcpwarp.test/mcp", "id": "srv_notes", "created": true}}})
}

func (r *tunnelRunner) online(tun *Tunnel, c TunnelConfig) {
	tun.configureAsync(c)
	r.quiesce()
	r.ev("connected", nil)
	r.ev("welcome", nil)
	r.registered()
}

func requireSnaps(t *testing.T, r *tunnelRunner, want ...TunnelStatus) {
	t.Helper()
	if len(r.snaps) != len(want) {
		t.Fatalf("snapshots %s\nwant %s", snapString(r.snaps), snapString(want))
	}
	for i := range want {
		if r.snaps[i] != want[i] {
			t.Fatalf("snapshot %d: %+v, want %+v", i, r.snaps[i], want[i])
		}
	}
}

var (
	snapOff        = TunnelStatus{State: StateOff}
	snapConnecting = TunnelStatus{State: StateConnecting}
	snapOnline     = TunnelStatus{State: StateOnline, URL: "https://notes-tester.mcpwarp.test/mcp"}
)

// §6.2 step 3: a config equal to the desired one (token compared trimmed)
// changes nothing; step 4: back-to-back calls, only the last dials.
func TestTunnelConfigureIdempotentAndSerialized(t *testing.T) {
	r, tun := newFakeTunnel(t, nil)
	r.online(tun, notesConfig)
	for _, c := range []TunnelConfig{notesConfig, {Enabled: true, Token: " t\n", Name: "notes"}} {
		if ch := tun.configureAsync(c); !isClosed(ch) {
			t.Fatal("an identical configure did not resolve at once")
		}
		r.quiesce()
	}
	if len(r.dials) != 1 {
		t.Fatalf("%d dials after identical configures, want 1", len(r.dials))
	}
	requireSnaps(t, r, snapOff, snapConnecting, snapOnline)

	r.ev("disconnect", map[string]any{"fatal": false, "wsCode": 1006.0}) // the closing below needs no unregister answer
	for _, name := range []string{"alpha", "bravo", "charlie"} {
		tun.configureAsync(TunnelConfig{Enabled: true, Token: "t", Name: name})
	}
	r.quiesce()
	if len(r.dials) != 2 {
		t.Fatalf("%d dials after a burst of three configures, want 2 (one for the last)", len(r.dials))
	}
	if got := r.dials[1].tr.cfg.name; got != "charlie" {
		t.Fatalf("the burst dialed %q, want charlie", got)
	}
}

// §6.2 Idempotence: a TokenSource is compared with == and wins over Token,
// so the same source with a different Token is the same config, and another
// TokenSourceFunc of the same func is a new one.
func TestTunnelConfigureProviderByIdentity(t *testing.T) {
	tok := "t1"
	f := func(context.Context) (string, error) { return tok, nil }
	p := TokenSourceFunc(f)
	r, tun := newFakeTunnel(t, nil)
	cfg := TunnelConfig{Enabled: true, TokenSource: p, Name: "notes"}
	r.online(tun, cfg)
	for _, c := range []TunnelConfig{cfg, {Enabled: true, Token: "ignored", TokenSource: p, Name: "notes"}} {
		if ch := tun.configureAsync(c); !isClosed(ch) {
			t.Fatal("an identical configure did not resolve at once")
		}
		r.quiesce()
	}
	if len(r.dials) != 1 {
		t.Fatalf("%d dials after identical configures, want 1", len(r.dials))
	}

	r.ev("disconnect", map[string]any{"fatal": false, "wsCode": 1006.0})
	tok = "t2"
	tun.configureAsync(TunnelConfig{Enabled: true, TokenSource: TokenSourceFunc(f), Name: "notes"})
	r.quiesce()
	if len(r.dials) != 2 {
		t.Fatalf("%d dials after a configure with a fresh TokenSourceFunc, want 2", len(r.dials))
	}
	if got, _ := r.dials[1].tr.cfg.token(context.Background()); got != "t2" {
		t.Fatalf("the new dial's token = %q, want t2", got)
	}

	// A non-comparable source never matches, and comparing it does not panic.
	r.ev("disconnect", map[string]any{"fatal": false, "wsCode": 1006.0})
	s := sliceSource{"t3"}
	for range 2 {
		tun.configureAsync(TunnelConfig{Enabled: true, TokenSource: s, Name: "notes"})
		r.quiesce()
		r.ev("disconnect", map[string]any{"fatal": false, "wsCode": 1006.0})
	}
	if len(r.dials) != 4 {
		t.Fatalf("%d dials after two configures with a non-comparable source, want 4", len(r.dials))
	}

	// A comparable type holding a func in an interface field: == would panic
	// on the values, so it never matches either.
	h := ifaceSource{inner: func() {}}
	for range 2 {
		tun.configureAsync(TunnelConfig{Enabled: true, TokenSource: h, Name: "notes"})
		r.quiesce()
		r.ev("disconnect", map[string]any{"fatal": false, "wsCode": 1006.0})
	}
	if len(r.dials) != 6 {
		t.Fatalf("%d dials after two configures with a func inside an interface field, want 6", len(r.dials))
	}
}

type ifaceSource struct{ inner any }

func (ifaceSource) Token(context.Context) (string, error) { return "t4", nil }

type sliceSource []string

func (s sliceSource) Token(context.Context) (string, error) { return s[0], nil }

func isClosed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

// §6.3: stop() during the raw dial resolves once that dial is gone, and
// nothing that dial does later reaches a listener.
func TestTunnelStopDuringDialLeaksNoEvent(t *testing.T) {
	r, tun := newFakeTunnel(t, nil)
	tun.configureAsync(notesConfig)
	r.quiesce()
	stopped := tun.stopAsync()
	r.quiesce()
	if !isClosed(stopped) {
		t.Fatal("stop() did not resolve")
	}
	dial := r.dials[0]
	r.connEvent("connected", map[string]any{"conn": 1.0})
	r.connEvent("welcome", map[string]any{"conn": 1.0})
	r.connEvent("registered", map[string]any{"conn": 1.0, "services": []any{map[string]any{"url": "https://x/mcp", "id": "srv", "created": true}}})
	r.quiesce()
	r.advance(2 * time.Minute)
	requireSnaps(t, r, snapOff, snapConnecting, snapOff)
	if dial.welcomed || len(r.dials) != 1 {
		t.Fatalf("the abandoned dial came alive (welcomed %v) or redialed (%d dials)", dial.welcomed, len(r.dials))
	}
}

// §6.3 dispose(): bounded by its timeout while a connection is held open,
// final afterwards; the public Dispose takes the bound from ctx.
func TestTunnelDisposeBounded(t *testing.T) {
	r, tun := newFakeTunnel(t, nil)
	r.online(tun, notesConfig)
	r.ev("hold", nil)
	done := tun.disposeAsync(100 * time.Millisecond)
	r.quiesce()
	r.advance(99 * time.Millisecond)
	if isClosed(done) {
		t.Fatal("dispose resolved before its timeout with the connection held")
	}
	r.advance(time.Millisecond)
	if !isClosed(done) {
		t.Fatal("dispose did not resolve at its timeout")
	}
	if !isClosed(tun.configureAsync(TunnelConfig{Enabled: true, Token: "t2", Name: "notes"})) {
		t.Fatal("configure after dispose did not resolve at once")
	}
	r.quiesce()
	if len(r.dials) != 1 {
		t.Fatal("configure after dispose dialed")
	}

	r2, tun2 := newFakeTunnel(t, nil)
	r2.online(tun2, notesConfig)
	r2.ev("hold", nil)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	if err := tun2.Dispose(ctx); err != nil {
		t.Fatalf("Dispose = %v", err)
	}
	if el := time.Since(start); el > 2*time.Second {
		t.Fatalf("Dispose took %v with a 100ms ctx", el)
	}
	var got []TunnelStatus
	tun2.Subscribe(func(s TunnelStatus) { got = append(got, s) })
	if len(got) != 1 || got[0] != snapOff {
		t.Fatalf("subscribe after dispose got %+v, want one off", got)
	}
}

// §6.4: subscribe delivers at once and on change; unsubscribe stops it; a
// panicking listener is logged and the others are still called.
func TestTunnelSubscribe(t *testing.T) {
	logs := &logCapture{}
	r, tun := newFakeTunnel(t, func(o *TunnelOptions) { o.Logger = slog.New(logs) })
	var second []TunnelStatus
	unsub := tun.Subscribe(func(s TunnelStatus) { second = append(second, s) })
	tun.Subscribe(func(TunnelStatus) { panic("listener bug") })
	tun.configureAsync(notesConfig)
	r.quiesce()
	unsub()
	r.ev("connected", nil)
	r.ev("welcome", nil)
	r.registered()
	if len(second) != 2 || second[0] != snapOff || second[1] != snapConnecting {
		t.Fatalf("second listener got %+v, want off, connecting and nothing after unsubscribe", second)
	}
	requireSnaps(t, r, snapOff, snapConnecting, snapOnline)
	if !logs.has(slog.LevelError, "status listener threw: listener bug") {
		t.Fatalf("no listener-panic log line in %q", logs.lines())
	}
	if got := tun.Status(); got != snapOnline {
		t.Fatalf("Status() = %+v", got)
	}
}

// §6.9: every error line is "<CODE>: <message>", and the token is never
// logged, not even through a refused register's server message.
func TestTunnelLoggingPrefixAndTokenRedaction(t *testing.T) {
	const token = "secret-token-4711"
	logs := &logCapture{}
	r, tun := newFakeTunnel(t, func(o *TunnelOptions) { o.Logger = slog.New(logs) })
	cfg := TunnelConfig{Enabled: true, Token: token, Name: "notes"}
	r.online(tun, cfg)
	r.ev("app_error", map[string]any{"code": "UNKNOWN_OP", "message": "unknown op ping"})
	r.ev("disconnect", map[string]any{"fatal": false, "wsCode": 1006.0})
	r.advance(fakeBackoff)
	r.ev("connected", nil)
	r.ev("welcome", nil)
	r.ev("register_error", map[string]any{"code": "QUOTA_EXCEEDED", "message": "quota exceeded"})
	tun.configureAsync(TunnelConfig{Enabled: true, Token: token, Name: "Bad_Name"})
	r.quiesce()

	codeRE := regexp.MustCompile(`^[A-Z_]+: `)
	var errs int
	for _, l := range logs.all() {
		if strings.Contains(l.msg, token) {
			t.Fatalf("the token was logged: %q", l.msg)
		}
		if strings.HasPrefix(l.msg, "mcpwarp: ") {
			t.Fatalf("log line with the mcpwarp: prefix: %q", l.msg)
		}
		if l.level == slog.LevelError {
			errs++
			if !codeRE.MatchString(l.msg) {
				t.Fatalf("error line without a CODE: prefix: %q", l.msg)
			}
		}
	}
	for _, want := range []struct {
		level slog.Level
		msg   string
	}{
		{slog.LevelInfo, `connecting as "notes"`},
		{slog.LevelInfo, "online at https://notes-tester.mcpwarp.test/mcp"},
		{slog.LevelWarn, "UNKNOWN_OP: unknown op ping"},
		{slog.LevelWarn, "disconnected: closed 1006"},
		{slog.LevelError, "QUOTA_EXCEEDED: quota exceeded"},
		{slog.LevelError, "INVALID_NAME: invalid service `name`: lowercase letters, digits and inner dashes, at most 30 characters"},
	} {
		if !logs.has(want.level, want.msg) {
			t.Errorf("no %v line %q in %q", want.level, want.msg, logs.lines())
		}
	}
	if errs != 2 {
		t.Errorf("%d error lines, want 2", errs)
	}
}

// T8 (the reachable half of fixture 07): an invalid kind is caught when the
// connection starts -- connecting first, then error/INVALID_CONFIG, no dial.
func TestTunnelInvalidKindAtStart(t *testing.T) {
	r, tun := newFakeTunnel(t, nil)
	tun.configureAsync(TunnelConfig{Enabled: true, Token: "t", Name: "notes", Kind: "ftp"})
	r.quiesce()
	requireSnaps(t, r, snapOff, snapConnecting, TunnelStatus{State: StateError, Code: CodeInvalidConfig, LastError: lastInvalidConfig})
	if len(r.dials) != 0 {
		t.Fatal("an invalid kind dialed")
	}
	// The desired config was cleared: the same config starts again (T37).
	tun.configureAsync(TunnelConfig{Enabled: true, Token: "t", Name: "notes", Kind: "ftp"})
	r.quiesce()
	if n := len(r.snaps); n != 5 {
		t.Fatalf("%d snapshots, want the same config to restart (5)", n)
	}
}

func TestNewTunnelValidation(t *testing.T) {
	cases := []struct {
		opts TunnelOptions
		msg  string
	}{
		{TunnelOptions{Integration: testIntegration}, "mcpwarp: pass exactly one of `handler` or `target`"},
		{TunnelOptions{Integration: "nope", Target: "http://127.0.0.1:1/mcp"}, "mcpwarp: `integration` must be an RFC 4122 UUID in text form; the server silently ignores anything else"},
		{TunnelOptions{Integration: testIntegration, Target: "http://127.0.0.1:1/mcp", Agent: Agent{OS: "\x01"}}, "mcpwarp: `agent.os` must be 1-64 printable ASCII characters"},
	}
	for _, c := range cases {
		_, err := NewTunnel(c.opts)
		if !errors.Is(err, ErrInvalidOptions) || err.Error() != c.msg {
			t.Errorf("NewTunnel = %v, want %q", err, c.msg)
		}
	}
}

// logCapture is a slog.Handler recording every line.
type logCapture struct {
	mu   sync.Mutex
	recs []logRec
}

type logRec struct {
	level slog.Level
	msg   string
}

func (l *logCapture) Enabled(context.Context, slog.Level) bool { return true }
func (l *logCapture) Handle(_ context.Context, r slog.Record) error {
	l.mu.Lock()
	l.recs = append(l.recs, logRec{r.Level, r.Message})
	l.mu.Unlock()
	return nil
}
func (l *logCapture) WithAttrs([]slog.Attr) slog.Handler { return l }
func (l *logCapture) WithGroup(string) slog.Handler      { return l }

func (l *logCapture) all() []logRec {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]logRec(nil), l.recs...)
}

func (l *logCapture) has(level slog.Level, msg string) bool {
	for _, r := range l.all() {
		if r.level == level && r.msg == msg {
			return true
		}
	}
	return false
}

func (l *logCapture) lines() []string {
	var out []string
	for _, r := range l.all() {
		out = append(out, r.level.String()+" "+r.msg)
	}
	return out
}
