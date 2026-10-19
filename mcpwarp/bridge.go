package mcpwarp

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/mcpwarp/ws-mixer-go/wsmixer"
)

// mcpPath is the only request path forwarded (§4.3): exact, case-sensitive,
// query ignored.
const mcpPath = "/mcp"

const (
	headDeadline         = 10 * time.Second
	defaultTargetTimeout = 30 * time.Second
	edgeCancelPoll       = 100 * time.Millisecond
)

// bridgeStream is the part of *wsmixer.Stream the bridge uses.
type bridgeStream interface {
	ReadContext(ctx context.Context, p []byte) (int, error)
	Write(p []byte) (int, error)
	CloseWrite() error
	Reset(code wsmixer.ErrorCode, msg string) error
	LastError() error
}

// bridge serves one inbound HTTP/1.1 request per stream (§4). The request
// body is buffered whole before the application is called, so the §4.2
// streaming-adapter rows never apply: a body over the cap is always a 413
// before any response byte.
type bridge struct {
	availability  func() (conflict bool, unavailable string)
	maxBodyBytes  int64
	handler       http.Handler
	target        *url.URL
	targetTimeout time.Duration
	onError       func(error)
	headDeadline  time.Duration

	// route, when set, selects the application per stream after the path
	// check (§4.8) and replaces availability and handler/target.
	route func(st bridgeStream, head *requestHead) routeResult

	single *app
}

// app is where one service's requests go: a handler, or a target URL with
// its client and timeout.
type app struct {
	name          string // a group's service name; "" for a single service
	handler       http.Handler
	target        *url.URL
	targetTimeout time.Duration
	targetClient  *http.Client
}

// routeResult is a multi-service selection (§4.8 Precedence with
// selection): reset (row 3c), unavailable (a 503 message, rows 3a, 3b,
// 3d, 3e), or the app to call.
type routeResult struct {
	app         *app
	reset       bool
	unavailable string
}

func newApp(handler http.Handler, target *url.URL, targetTimeout time.Duration, transport http.RoundTripper) *app {
	a := &app{handler: handler, target: target, targetTimeout: targetTimeout}
	if a.targetTimeout <= 0 {
		a.targetTimeout = defaultTargetTimeout
	}
	if target != nil {
		if transport == nil {
			transport = http.DefaultTransport
		}
		a.targetClient = &http.Client{
			Transport:     transport,
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		}
	}
	return a
}

func newBridge(b bridge, transport http.RoundTripper) *bridge {
	if b.maxBodyBytes <= 0 {
		b.maxBodyBytes = defaultMaxBodyBytes
	}
	if b.headDeadline <= 0 {
		b.headDeadline = headDeadline
	}
	if b.onError == nil {
		b.onError = func(error) {}
	}
	if b.route == nil {
		b.single = newApp(b.handler, b.target, b.targetTimeout, transport)
	}
	return &b
}

// serve answers one stream; it never panics and always ends the stream with
// CLOSE or RESET (or leaves it to a connection that is already gone).
// connDone is the connection's Done channel, for edge cancellation.
func (b *bridge) serve(st bridgeStream, connDone <-chan struct{}) {
	if b.route == nil {
		if conflict, _ := b.availability(); conflict {
			_ = st.Reset(wsmixer.CancelCode, takenOverMessage)
			return
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r := &byteReader{ctx: ctx, src: st}

	var claimed atomic.Bool
	timer := time.AfterFunc(b.headDeadline, func() {
		if claimed.CompareAndSwap(false, true) {
			_ = st.Reset(wsmixer.CancelCode, "request head deadline exceeded")
		}
	})
	head, err := readRequestHead(r)
	timer.Stop()
	if !claimed.CompareAndSwap(false, true) {
		return // the deadline RESET the stream
	}
	if err != nil {
		method := http.MethodGet // no request line: the reply has its body
		if head != nil {
			method = head.method
		}
		var he *httpError
		switch {
		case errors.As(err, &he):
			b.refuse(st, r, he.status, he.msg, method)
		case err == errIncomplete:
			b.refuse(st, r, http.StatusBadRequest, "incomplete request", method)
		}
		// Otherwise the stream was reset or the connection lost: nobody to answer.
		return
	}

	if requestPath(head.target) != mcpPath {
		b.refuse(st, r, http.StatusNotFound, "not_found", head.method)
		return
	}
	sel := routeResult{app: b.single}
	if b.route != nil {
		sel = b.route(st, head)
	} else {
		sel.reset, sel.unavailable = b.availability()
	}
	if sel.reset {
		_ = st.Reset(wsmixer.CancelCode, takenOverMessage)
		return
	} else if sel.unavailable != "" {
		b.refuse(st, r, http.StatusServiceUnavailable, sel.unavailable, head.method)
		return
	}
	body, err := readRequestBody(r, head, b.maxBodyBytes)
	if err != nil {
		var he *httpError
		if errors.As(err, &he) {
			b.refuse(st, r, he.status, he.msg, head.method)
		}
		return
	}

	target, ok := representable(head)
	if !ok {
		b.reply(st, http.StatusBadRequest, unrepresentable, head.method)
		return
	}
	go b.watchEdgeCancel(ctx, cancel, st, connDone)
	if sel.app.handler != nil {
		b.serveHandler(ctx, st, sel.app, head, target, body)
	} else {
		b.serveTarget(ctx, st, sel.app, head, target, body)
	}
}

// unrepresentable is §4.5's reply text for a request the runtime's request
// type cannot hold, in handler and target mode alike.
const unrepresentable = "request cannot be represented as a web Request"

// representable reports whether net/http can carry the request as received:
// its target percent-encoded once (escapeOnce) must make a valid
// https://<Host><target> URL, and every forwarded header value, Host
// included, must be one Go's HTTP client accepts. It returns that target.
func representable(head *requestHead) (string, bool) {
	// §4.5: CONNECT, TRACE and TRACK are refused in both modes, so every
	// runtime agrees (a web Request cannot carry them).
	switch strings.ToUpper(head.method) {
	case "CONNECT", "TRACE", "TRACK":
		return "", false
	}
	target := escapeOnce(head.target)
	if _, err := url.Parse("https://" + head.host + target); err != nil {
		return "", false
	}
	if !validHeaderValue(head.host) {
		return "", false
	}
	for _, h := range forwardableHeaders(head.headers) {
		if !validHeaderValue(h.value) {
			return "", false
		}
	}
	return target, true
}

// validHeaderValue is net/http's rule (httpguts.ValidHeaderFieldValue): no
// control byte other than HTAB, no DEL.
func validHeaderValue(v string) bool {
	for i := 0; i < len(v); i++ {
		if c := v[i]; (c < 0x20 && c != '\t') || c == 0x7f {
			return false
		}
	}
	return true
}

// escapeOnce percent-encodes a received request target exactly once
// (§4.3): every byte outside RFC 3986's allowed set becomes %XX, '%' is
// kept as it is (a valid %XX escape and a lone '%' alike, never
// re-encoded), and '#' starts a fragment that is dropped with everything
// after it.
func escapeOnce(s string) string {
	if i := strings.IndexByte(s, '#'); i >= 0 {
		s = s[:i]
	}
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c == '%' || urlByte(c):
			b.WriteByte(c)
		default:
			fmt.Fprintf(&b, "%%%02X", c)
		}
	}
	return b.String()
}

// urlByte is §4.3's allowed set, passed through unescaped: RFC 3986
// unreserved, sub-delims, ":@/?", and "[]" (WHATWG parity, a[]=1 queries).
func urlByte(c byte) bool {
	switch {
	case 'a' <= c && c <= 'z', 'A' <= c && c <= 'Z', '0' <= c && c <= '9':
		return true
	}
	return strings.IndexByte("-._~!$&'()*+,;=:@/?[]", c) >= 0
}

// refuse writes an SDK-generated reply, drains whatever request bytes are
// still unread, then CLOSEs (§4.4 Ending a refused stream: never RESET).
func (b *bridge) refuse(st bridgeStream, r *byteReader, status int, msg, method string) {
	if _, err := st.Write(errorReply(status, msg, method)); err != nil {
		return
	}
	r.discard()
	_ = st.CloseWrite()
}

// watchEdgeCancel cancels ctx when the edge RESETs the stream or the
// connection ends before the response is done (§4.1 Edge cancel).
// wsmixer.Stream has no close notification, so a RESET is polled.
func (b *bridge) watchEdgeCancel(ctx context.Context, cancel context.CancelFunc, st bridgeStream, connDone <-chan struct{}) {
	t := time.NewTicker(edgeCancelPoll)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-connDone:
			cancel()
			return
		case <-t.C:
			var se *wsmixer.StreamError
			if errors.As(st.LastError(), &se) {
				cancel()
				return
			}
		}
	}
}

// serveHandler is §4.6 Request → handler plus §4.5's handler rows.
func (b *bridge) serveHandler(ctx context.Context, st bridgeStream, a *app, head *requestHead, target string, body []byte) {
	req, err := handlerRequest(ctx, head, target, body)
	if err != nil {
		b.reply(st, http.StatusBadRequest, unrepresentable, head.method)
		return
	}
	w := newStreamResponseWriter(st, head.method)
	w.onBadStatus = b.onError
	var panicked any
	func() {
		defer func() { panicked = recover() }()
		a.handler.ServeHTTP(w, req)
	}()
	if panicked != nil {
		b.onError(fmt.Errorf("mcpwarp: handler panicked: %v", panicked))
		if !w.headSent {
			b.reply(st, http.StatusInternalServerError, "handler error", head.method)
		} else if w.err == errBadStatus {
			_ = st.CloseWrite()
		} else if w.err == nil {
			_ = st.Reset(wsmixer.InternalErrorCode, "response body failed")
		}
		return
	}
	w.finish()
}

// reply writes an SDK-generated reply to a request whose body was read.
func (b *bridge) reply(st bridgeStream, status int, msg, method string) {
	if _, err := st.Write(errorReply(status, msg, method)); err != nil {
		return
	}
	_ = st.CloseWrite()
}

// handlerRequest builds the *http.Request a handler sees: URL
// https://<Host><target> with target escaped once (representable),
// RequestURI the target as received, Host, the forwardable headers
// (canonicalised, as net/http does), the body unless GET/HEAD or unframed.
func handlerRequest(ctx context.Context, head *requestHead, target string, body []byte) (*http.Request, error) {
	u, err := url.Parse("https://" + head.host + target)
	if err != nil {
		return nil, err
	}
	header := http.Header{}
	for _, h := range forwardableHeaders(head.headers) {
		if !strings.EqualFold(h.name, "host") {
			header.Add(h.name, h.value)
		}
	}
	req := &http.Request{
		Method:     head.method,
		URL:        u,
		Proto:      "HTTP/1." + strconv.Itoa(head.minor),
		ProtoMajor: 1,
		ProtoMinor: head.minor,
		Header:     header,
		Body:       http.NoBody,
		Host:       head.host,
		RequestURI: head.target,
	}
	if body != nil && len(body) > 0 && head.method != http.MethodGet && head.method != http.MethodHead {
		req.Body = io.NopCloser(bytes.NewReader(body))
		req.ContentLength = int64(len(body))
	}
	return req.WithContext(ctx), nil
}

// serveTarget is §4.6 Request → target URL plus §4.5's target rows.
func (b *bridge) serveTarget(ctx context.Context, st bridgeStream, a *app, head *requestHead, target string, body []byte) {
	u := *a.target
	if q := targetQuery(target); q != "" {
		if u.RawQuery == "" {
			u.RawQuery = q
		} else {
			u.RawQuery += "&" + q
		}
	}
	var reqBody io.Reader
	if body != nil && len(body) > 0 && head.method != http.MethodGet && head.method != http.MethodHead {
		reqBody = bytes.NewReader(body)
	}
	tctx, tcancel := context.WithCancel(ctx)
	defer tcancel()
	req, err := http.NewRequestWithContext(tctx, head.method, u.String(), reqBody)
	if err != nil {
		b.reply(st, http.StatusBadRequest, unrepresentable, head.method)
		return
	}
	for _, h := range forwardableHeaders(head.headers) {
		if !strings.EqualFold(h.name, "host") {
			req.Header.Add(h.name, h.value)
		}
	}
	req.Header.Set("Accept-Encoding", "identity")
	req.Header.Set("X-Forwarded-Host", head.host)
	req.Header.Set("X-Forwarded-Proto", "https")

	// targetTimeout bounds only the wait for response headers; the body may
	// stream past it.
	var timedOut atomic.Bool
	timer := time.AfterFunc(a.targetTimeout, func() {
		timedOut.Store(true)
		tcancel()
	})
	resp, err := a.targetClient.Do(req)
	if !timer.Stop() || timedOut.Load() {
		if resp != nil {
			_ = resp.Body.Close()
		}
		b.reply(st, http.StatusGatewayTimeout, "local server timed out", head.method)
		return
	}
	if err != nil {
		msg := "local server unreachable"
		if code := errnoName(err); code != "" {
			msg += " (" + code + ")"
		}
		b.reply(st, http.StatusBadGateway, msg, head.method)
		if a.name != "" {
			// A group's service: logged with its name and target (§6A.4 has
			// no lastError for an active service, so the log is the only place).
			b.onError(fmt.Errorf("mcpwarp: service %q: %s at %s", a.name, msg, a.target.Redacted()))
		}
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 600 {
		b.reply(st, http.StatusInternalServerError, "handler error", head.method)
		b.onError(fmt.Errorf("mcpwarp: target answered status %d", resp.StatusCode))
		return
	}
	w := newStreamResponseWriter(st, head.method)
	for k, vs := range resp.Header {
		w.header[k] = append([]string(nil), vs...)
	}
	if resp.Uncompressed {
		// The transport decoded the body: re-serve it without the encoding.
		w.header.Del("Content-Encoding")
		w.header.Del("Content-Length")
	}
	w.WriteHeader(resp.StatusCode)
	buf := make([]byte, 32*1024)
	for {
		n, rerr := resp.Body.Read(buf)
		if n > 0 {
			if _, werr := w.Write(buf[:n]); werr != nil {
				return
			}
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			if w.err == nil {
				_ = st.Reset(wsmixer.InternalErrorCode, "response body failed")
				b.onError(fmt.Errorf("mcpwarp: target response body failed: %w", rerr))
			}
			return
		}
	}
	w.finish()
}

// targetQuery is the query of an escapeOnce target (its fragment is gone).
func targetQuery(target string) string {
	i := strings.IndexByte(target, '?')
	if i < 0 {
		return ""
	}
	return target[i+1:]
}

// errnoName names the OS error behind a failed connection, the Go analogue
// of the JS runtime's cause code ("ECONNREFUSED"); "" when there is none.
func errnoName(err error) string {
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return "ENOTFOUND"
	}
	var errno syscall.Errno
	if !errors.As(err, &errno) {
		return ""
	}
	switch errno {
	case syscall.ECONNREFUSED:
		return "ECONNREFUSED"
	case syscall.ECONNRESET:
		return "ECONNRESET"
	case syscall.ETIMEDOUT:
		return "ETIMEDOUT"
	case syscall.EHOSTUNREACH:
		return "EHOSTUNREACH"
	case syscall.ENETUNREACH:
		return "ENETUNREACH"
	case syscall.EPIPE:
		return "EPIPE"
	}
	return ""
}

// streamResponseWriter is the http.ResponseWriter (and http.Flusher) a
// handler writes to: the head goes out on the first body write, Flush or
// handler return; a declared valid Content-Length frames the body as is,
// anything else is chunked with one chunk per Write, each written to the
// stream immediately. 1xx statuses are dropped: the agent never sends one.
type streamResponseWriter struct {
	st     bridgeStream
	method string
	header http.Header

	status   int
	snapshot http.Header // the header as of WriteHeader
	headSent bool
	bodyless bool
	chunked  bool
	length   int64 // declared Content-Length, -1 if none
	written  int64
	err      error // first stream write failure, or errBadStatus

	// onBadStatus reports a final status outside 200-599, which is
	// answered with the 500 handler-error reply instead.
	onBadStatus func(error)
}

var errBadStatus = errors.New("mcpwarp: handler status outside 200-599")

func newStreamResponseWriter(st bridgeStream, method string) *streamResponseWriter {
	return &streamResponseWriter{st: st, method: method, header: http.Header{}, length: -1}
}

func (w *streamResponseWriter) Header() http.Header { return w.header }

func (w *streamResponseWriter) WriteHeader(code int) {
	if code < 100 || code > 999 {
		panic(fmt.Sprintf("invalid WriteHeader code %v", code))
	}
	if w.status != 0 || code < 200 {
		return
	}
	w.status = code
	w.snapshot = w.header.Clone()
}

func (w *streamResponseWriter) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	if w.err != nil {
		return 0, w.err
	}
	if isBodyless(w.method, w.status) {
		if w.method == http.MethodHead {
			return len(p), nil
		}
		return 0, http.ErrBodyNotAllowed
	}
	if len(p) == 0 {
		return 0, nil
	}
	if err := w.sendHead(false); err != nil {
		return 0, err
	}
	if !w.chunked {
		var tooLong error
		if remaining := w.length - w.written; int64(len(p)) > remaining {
			p, tooLong = p[:remaining], http.ErrContentLength
		}
		n, err := w.st.Write(p)
		w.written += int64(n)
		if err != nil {
			w.err = err
			return n, err
		}
		return n, tooLong
	}
	chunk := make([]byte, 0, len(p)+16)
	chunk = append(chunk, strconv.FormatInt(int64(len(p)), 16)...)
	chunk = append(chunk, "\r\n"...)
	chunk = append(chunk, p...)
	chunk = append(chunk, "\r\n"...)
	if _, err := w.st.Write(chunk); err != nil {
		w.err = err
		return 0, err
	}
	w.written += int64(len(p))
	return len(p), nil
}

// Flush sends the head now if it has not gone out; body writes are never
// buffered.
func (w *streamResponseWriter) Flush() {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	_ = w.sendHead(false)
}

// sendHead writes the status line and headers once. finishing means the
// handler returned without writing a body: an undeclared length is then 0.
func (w *streamResponseWriter) sendHead(finishing bool) error {
	if w.headSent || w.err != nil {
		return w.err
	}
	w.headSent = true
	if w.status >= 600 {
		w.bodyless = true
		if _, err := w.st.Write(errorReply(http.StatusInternalServerError, "handler error", w.method)); err != nil {
			w.err = err
			return err
		}
		if w.onBadStatus != nil {
			w.onBadStatus(fmt.Errorf("mcpwarp: handler answered status %d", w.status))
		}
		w.err = errBadStatus
		return w.err
	}
	h := w.snapshot
	w.bodyless = isBodyless(w.method, w.status)
	if !w.bodyless {
		if cl, ok := declaredLength(h); ok {
			w.length = cl
		} else if finishing {
			w.length = 0
		} else {
			w.chunked = true
		}
	}
	var b bytes.Buffer
	b.WriteString("HTTP/1.1 " + strconv.Itoa(w.status) + " " + reasonPhrase(w.status) + "\r\n")
	writeResponseHeaders(&b, h)
	switch {
	case w.bodyless:
	case w.chunked:
		b.WriteString("Transfer-Encoding: chunked\r\n")
	default:
		b.WriteString("Content-Length: " + strconv.FormatInt(w.length, 10) + "\r\n")
	}
	b.WriteString("\r\n")
	if _, err := w.st.Write(b.Bytes()); err != nil {
		w.err = err
	}
	return w.err
}

// finish completes the response after the handler returned, then CLOSEs.
func (w *streamResponseWriter) finish() {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	if err := w.sendHead(true); err != nil {
		if err == errBadStatus {
			_ = w.st.CloseWrite() // the 500 reply is complete
		}
		return
	}
	switch {
	case w.bodyless:
	case w.chunked:
		if _, err := w.st.Write([]byte("0\r\n\r\n")); err != nil {
			return
		}
	case w.written < w.length:
		_ = w.st.Reset(wsmixer.InternalErrorCode, "response body shorter than Content-Length")
		return
	}
	_ = w.st.CloseWrite()
}

// declaredLength is a single valid Content-Length in h.
func declaredLength(h http.Header) (int64, bool) {
	vs := h.Values("Content-Length")
	if len(vs) != 1 || !digitsRE.MatchString(strings.TrimSpace(vs[0])) {
		return 0, false
	}
	n, err := strconv.ParseInt(strings.TrimSpace(vs[0]), 10, 64)
	return n, err == nil
}

// writeResponseHeaders is §4.6 Response → edge: every header except the
// hop-by-hop set, Connection-listed names, Content-Length and
// Transfer-Encoding (re-derived), one line per value, names as the handler
// spelled them, sorted like net/http. Values have CR/LF replaced with spaces.
func writeResponseHeaders(b *bytes.Buffer, h http.Header) {
	listed := connectionTokens(h.Values("Connection"))
	keys := make([]string, 0, len(h))
	for k := range h {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		n := strings.ToLower(k)
		if hopByHop[n] || listed[n] || n == "content-length" || !isToken(k) {
			continue
		}
		for _, v := range h[k] {
			b.WriteString(k + ": " + strings.TrimSpace(newlineToSpace.Replace(v)) + "\r\n")
		}
	}
}

var newlineToSpace = strings.NewReplacer("\r", " ", "\n", " ")
