package mcpwarp

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
)

// §4.2 parsing limits.
const (
	maxHeadBytes        = 64 * 1024
	maxChunkLine        = 4096
	defaultMaxBodyBytes = 4 * 1024 * 1024
)

// httpError is a malformed or over-limit request; status is the reply it
// maps to and msg the reply's `error` text (§4.2, verbatim).
type httpError struct {
	status int
	msg    string
}

func (e *httpError) Error() string { return e.msg }

func badRequest(msg string) *httpError { return &httpError{status: http.StatusBadRequest, msg: msg} }

var errBodyTooLarge = &httpError{status: http.StatusRequestEntityTooLarge, msg: "request body too large"}

// errIncomplete is a clean end of stream inside a message.
var errIncomplete = errors.New("mcpwarp: stream ended inside the request")

// byteReader buffers a stream's reads for parsing.
type byteReader struct {
	ctx context.Context
	src interface {
		ReadContext(ctx context.Context, p []byte) (int, error)
	}
	buf []byte
	eof bool
}

// fill reads once more; false at a clean EOF. Any other read error (RESET,
// connection lost) is returned as is.
func (r *byteReader) fill() (bool, error) {
	if r.eof {
		return false, nil
	}
	var tmp [32 * 1024]byte
	for {
		n, err := r.src.ReadContext(r.ctx, tmp[:])
		r.buf = append(r.buf, tmp[:n]...)
		if err == io.EOF {
			r.eof = true
			return n > 0, nil
		}
		if err != nil {
			return false, err
		}
		if n > 0 {
			return true, nil
		}
	}
}

// readHead returns the message head up to (not including) the blank line;
// 431 when the head, its terminating blank line included, is over max
// bytes; errIncomplete on EOF first.
func (r *byteReader) readHead(max int) ([]byte, error) {
	scanFrom := 0
	for {
		if i := bytes.Index(r.buf[max0(scanFrom-3):], []byte("\r\n\r\n")); i >= 0 {
			i += max0(scanFrom - 3)
			if i+4 > max {
				return nil, &httpError{status: http.StatusRequestHeaderFieldsTooLarge, msg: "request head too large"}
			}
			head := append([]byte(nil), r.buf[:i]...)
			r.buf = r.buf[i+4:]
			return head, nil
		}
		// max bytes without a complete terminator: it can only end past max.
		if len(r.buf) >= max {
			return nil, &httpError{status: http.StatusRequestHeaderFieldsTooLarge, msg: "request head too large"}
		}
		scanFrom = len(r.buf)
		ok, err := r.fill()
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, errIncomplete
		}
	}
}

func max0(n int) int {
	if n < 0 {
		return 0
	}
	return n
}

func (r *byteReader) readExactly(n int64) ([]byte, error) {
	for int64(len(r.buf)) < n {
		ok, err := r.fill()
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, errIncomplete
		}
	}
	out := append([]byte(nil), r.buf[:n]...)
	r.buf = r.buf[n:]
	return out, nil
}

// readLine returns one line without its CRLF; 400 "line too long" when the
// line is longer than max bytes, whether or not its CRLF is already buffered.
func (r *byteReader) readLine(max int) (string, error) {
	scanFrom := 0
	for {
		if i := bytes.Index(r.buf[max0(scanFrom-1):], []byte("\r\n")); i >= 0 {
			i += max0(scanFrom - 1)
			if i > max {
				return "", badRequest("line too long")
			}
			line := string(r.buf[:i])
			r.buf = r.buf[i+2:]
			return line, nil
		}
		// max line bytes plus a trailing CR may still end in a CRLF.
		if len(r.buf) > max+1 {
			return "", badRequest("line too long")
		}
		scanFrom = len(r.buf)
		ok, err := r.fill()
		if err != nil {
			return "", err
		}
		if !ok {
			return "", errIncomplete
		}
	}
}

// discard reads and drops everything up to EOF (granting stream window as
// it goes); it returns at EOF or on the first read error.
func (r *byteReader) discard() {
	r.buf = nil
	for !r.eof {
		if _, err := r.fill(); err != nil {
			return
		}
		r.buf = nil
	}
}

type framing int

const (
	framingNone framing = iota
	framingLength
	framingChunked
)

type headerField struct{ name, value string }

// requestHead is one validated request head; the body is still unread.
type requestHead struct {
	method  string
	target  string // origin-form path + query
	minor   int    // HTTP/1.<minor>
	host    string
	headers []headerField // as received, minus Expect
	framing framing
	length  int64
}

var requestLineRE = regexp.MustCompile(`^([^ ]+) ([^ ]+) HTTP/1\.([01])$`)
var tokenRE = regexp.MustCompile("^[!#$%&'*+\\-.^_`|~0-9A-Za-z]+$")
var absoluteTargetRE = regexp.MustCompile(`(?i)^https?://[^/?#]*`)
var schemeAuthorityRE = regexp.MustCompile(`(?i)^[a-z][a-z0-9+.-]*://[^/?#]*`)
var chunkSizeRE = regexp.MustCompile(`^[0-9a-fA-F]+$`)
var digitsRE = regexp.MustCompile(`^[0-9]+$`)

func isToken(s string) bool { return tokenRE.MatchString(s) }

// readRequestHead reads and validates one request head (§4.2).
// On a failure the returned head is nil, or carries only the method when
// the request line parsed: the error reply to a HEAD is head-only too.
func readRequestHead(r *byteReader) (*requestHead, error) {
	raw, err := r.readHead(maxHeadBytes)
	if err != nil {
		return methodOnly(r.buf), err
	}
	head, err := parseRequestHead(string(raw))
	if err != nil {
		return methodOnly(raw), err
	}
	return head, nil
}

// methodOnly is the method of b's request line, if its first line is a
// complete, valid one.
func methodOnly(b []byte) *requestHead {
	i := bytes.Index(b, []byte("\r\n"))
	if i < 0 {
		i = len(b)
	}
	m := requestLineRE.FindStringSubmatch(string(b[:i]))
	if m == nil || !isToken(m[1]) {
		return nil
	}
	return &requestHead{method: m[1]}
}

func parseRequestHead(raw string) (*requestHead, error) {
	lines := strings.Split(raw, "\r\n")
	m := requestLineRE.FindStringSubmatch(lines[0])
	if m == nil || !isToken(m[1]) {
		return nil, badRequest("malformed request line")
	}
	target, err := parseTarget(m[2])
	if err != nil {
		return nil, err
	}
	all, err := parseHeaderLines(lines[1:])
	if err != nil {
		return nil, err
	}
	var hosts []string
	for _, h := range all {
		if strings.EqualFold(h.name, "host") {
			hosts = append(hosts, h.value)
		}
	}
	if len(hosts) != 1 || hosts[0] == "" {
		return nil, badRequest("missing or duplicate Host header")
	}
	head := &requestHead{method: m[1], target: target, minor: int(m[3][0] - '0'), host: hosts[0]}
	for _, h := range all {
		if !strings.EqualFold(h.name, "expect") {
			head.headers = append(head.headers, h)
		}
	}
	head.framing, head.length, err = bodyFraming(all)
	if err != nil {
		return nil, err
	}
	return head, nil
}

// parseTarget keeps origin-form as is and cuts absolute-form to its path +
// query as written (fragment dropped, not normalised).
func parseTarget(raw string) (string, error) {
	if strings.HasPrefix(raw, "/") {
		return raw, nil
	}
	if auth := absoluteTargetRE.FindString(raw); auth != "" {
		if u, err := url.Parse(raw); err != nil || u.Host == "" {
			return "", badRequest("malformed request target")
		}
		rest := raw[len(auth):]
		if i := strings.IndexByte(rest, '#'); i >= 0 {
			rest = rest[:i]
		}
		if !strings.HasPrefix(rest, "/") {
			rest = "/" + rest
		}
		return rest, nil
	}
	return "", badRequest("unsupported request target form")
}

// requestPath is §4.3's requestPath: the path of a raw target, scheme and
// authority stripped, cut at the first ? or #, not normalised.
func requestPath(target string) string {
	auth := schemeAuthorityRE.FindString(target)
	rest := target[len(auth):]
	if i := strings.IndexAny(rest, "?#"); i >= 0 {
		rest = rest[:i]
	}
	if auth != "" && rest == "" {
		return "/"
	}
	return rest
}

func parseHeaderLines(lines []string) ([]headerField, error) {
	out := make([]headerField, 0, len(lines))
	for _, line := range lines {
		if strings.HasPrefix(line, " ") || strings.HasPrefix(line, "\t") {
			return nil, badRequest("obsolete header line folding")
		}
		colon := strings.IndexByte(line, ':')
		if colon <= 0 {
			return nil, badRequest("malformed header line")
		}
		name := line[:colon]
		if !isToken(name) {
			return nil, badRequest("invalid header name " + string(marshalNoEscape(name)))
		}
		out = append(out, headerField{name: name, value: strings.Trim(line[colon+1:], " \t")})
	}
	return out, nil
}

func bodyFraming(all []headerField) (framing, int64, error) {
	var te, cl []string
	for _, h := range all {
		switch strings.ToLower(h.name) {
		case "transfer-encoding":
			te = append(te, h.value)
		case "content-length":
			cl = append(cl, h.value)
		}
	}
	if len(te) > 0 && len(cl) > 0 {
		return 0, 0, badRequest("both Transfer-Encoding and Content-Length")
	}
	if len(te) > 0 {
		if len(te) != 1 || strings.ToLower(strings.TrimSpace(te[0])) != "chunked" {
			return 0, 0, badRequest("unsupported Transfer-Encoding")
		}
		return framingChunked, 0, nil
	}
	if len(cl) == 0 {
		return framingNone, 0, nil
	}
	values := strings.Split(strings.Join(cl, ","), ",")
	for i, v := range values {
		values[i] = strings.TrimSpace(v)
		if !digitsRE.MatchString(values[i]) {
			return 0, 0, badRequest("malformed Content-Length")
		}
	}
	for _, v := range values[1:] {
		if v != values[0] {
			return 0, 0, badRequest("conflicting Content-Length values")
		}
	}
	n, err := strconv.ParseInt(values[0], 10, 64)
	if err != nil || n > 1<<53-1 {
		return 0, 0, badRequest("malformed Content-Length")
	}
	return framingLength, n, nil
}

// readRequestBody reads the body head.framing announces: nil for none,
// 413 past max (Content-Length checked before reading, chunked cumulatively).
func readRequestBody(r *byteReader, head *requestHead, max int64) ([]byte, error) {
	var body []byte
	var err error
	switch head.framing {
	case framingNone:
		return nil, nil
	case framingLength:
		if head.length > max {
			return nil, errBodyTooLarge
		}
		body, err = r.readExactly(head.length)
	case framingChunked:
		body, err = readChunkedBody(r, max)
	}
	if err == errIncomplete {
		return nil, badRequest("stream ended inside the request body")
	}
	return body, err
}

// readChunkedBody reads RFC 9112 §7.1 chunks; extensions ignored, trailers
// read and dropped.
func readChunkedBody(r *byteReader, max int64) ([]byte, error) {
	body := []byte{}
	var total int64
	for {
		line, err := r.readLine(maxChunkLine)
		if err != nil {
			return nil, err
		}
		sizeText := strings.TrimSpace(strings.SplitN(line, ";", 2)[0])
		if !chunkSizeRE.MatchString(sizeText) {
			return nil, badRequest("malformed chunk size")
		}
		size, err := strconv.ParseInt(sizeText, 16, 64)
		if err != nil {
			return nil, errBodyTooLarge // too many hex digits for int64: far past any cap
		}
		if size == 0 {
			break
		}
		if size > max-total {
			return nil, errBodyTooLarge
		}
		total += size
		data, err := r.readExactly(size)
		if err != nil {
			return nil, err
		}
		body = append(body, data...)
		crlf, err := r.readLine(maxChunkLine)
		if err != nil {
			return nil, err
		}
		if crlf != "" {
			return nil, badRequest("missing CRLF after chunk data")
		}
	}
	for {
		line, err := r.readLine(maxChunkLine)
		if err != nil {
			return nil, err
		}
		if line == "" {
			return body, nil
		}
	}
}

// hopByHop is the connection-scoped header set never forwarded (§4.6).
var hopByHop = map[string]bool{
	"connection":          true,
	"keep-alive":          true,
	"proxy-connection":    true,
	"proxy-authenticate":  true,
	"proxy-authorization": true,
	"te":                  true,
	"trailer":             true,
	"transfer-encoding":   true,
	"upgrade":             true,
}

// connectionTokens returns the lower-cased names listed in Connection.
func connectionTokens(values []string) map[string]bool {
	out := map[string]bool{}
	for _, v := range values {
		for _, t := range strings.Split(v, ",") {
			if t = strings.ToLower(strings.TrimSpace(t)); t != "" {
				out[t] = true
			}
		}
	}
	return out
}

// forwardableHeaders is §4.6 Request → handler: minus hop-by-hop,
// Connection-listed, Expect and Content-Length. Host stays in (callers that
// carry it elsewhere drop it).
func forwardableHeaders(headers []headerField) []headerField {
	var conn []string
	for _, h := range headers {
		if strings.EqualFold(h.name, "connection") {
			conn = append(conn, h.value)
		}
	}
	listed := connectionTokens(conn)
	out := make([]headerField, 0, len(headers))
	for _, h := range headers {
		n := strings.ToLower(h.name)
		if hopByHop[n] || listed[n] || n == "expect" || n == "content-length" {
			continue
		}
		out = append(out, h)
	}
	return out
}

// reasonPhrases is the RFC 9110 phrase table of §4.5 (413 is "Content Too
// Large", not Go's older "Request Entity Too Large").
var reasonPhrases = map[int]string{
	200: "OK", 201: "Created", 202: "Accepted", 204: "No Content", 206: "Partial Content",
	301: "Moved Permanently", 302: "Found", 303: "See Other", 304: "Not Modified",
	307: "Temporary Redirect", 308: "Permanent Redirect",
	400: "Bad Request", 401: "Unauthorized", 403: "Forbidden", 404: "Not Found",
	405: "Method Not Allowed", 406: "Not Acceptable", 408: "Request Timeout", 409: "Conflict",
	410: "Gone", 413: "Content Too Large", 415: "Unsupported Media Type",
	422: "Unprocessable Content", 429: "Too Many Requests", 431: "Request Header Fields Too Large",
	500: "Internal Server Error", 501: "Not Implemented", 502: "Bad Gateway",
	503: "Service Unavailable", 504: "Gateway Timeout",
}

// reasonPhrase is the status line's phrase: the §4.5 table, else Go's own
// text for statuses an application may answer with, else "Unknown".
func reasonPhrase(status int) string {
	if p, ok := reasonPhrases[status]; ok {
		return p
	}
	if p := http.StatusText(status); p != "" {
		return p
	}
	return "Unknown"
}

// isBodyless: responses to HEAD, and 1xx/204/304, never carry a body.
func isBodyless(method string, status int) bool {
	return method == http.MethodHead || status < 200 || status == http.StatusNoContent || status == http.StatusNotModified
}

// errorReply is §4.5's byte-exact SDK-generated reply; to HEAD the same
// head and no body.
func errorReply(status int, msg, method string) []byte {
	body := marshalNoEscape(map[string]string{"error": msg})
	var b bytes.Buffer
	b.WriteString("HTTP/1.1 " + strconv.Itoa(status) + " " + reasonPhrase(status) + "\r\n")
	b.WriteString("Content-Type: application/json\r\n")
	b.WriteString("Content-Length: " + strconv.Itoa(len(body)) + "\r\n\r\n")
	if !isBodyless(method, status) {
		b.Write(body)
	}
	return b.Bytes()
}
