package mcpwarp

import (
	"context"
	"io"
	"strings"
	"testing"
)

// oneShotSource hands its whole buffer to the first read, like a DATA frame
// that carries the line and its CRLF together; then EOF.
type oneShotSource struct{ data []byte }

func (s *oneShotSource) ReadContext(_ context.Context, p []byte) (int, error) {
	if len(s.data) == 0 {
		return 0, io.EOF
	}
	n := copy(p, s.data)
	s.data = s.data[n:]
	return n, nil
}

func readerOf(s string) *byteReader {
	return &byteReader{ctx: context.Background(), src: &oneShotSource{data: []byte(s)}}
}

// §4.2 Chunk-size line ≤ 4096 bytes, also when its CRLF is already buffered.
func TestReadChunkedBodyLineLimitWithBufferedCRLF(t *testing.T) {
	ext := func(lineLen int) string { return "1;" + strings.Repeat("x", lineLen-2) }
	_, err := readChunkedBody(readerOf(ext(5000)+"\r\nX\r\n0\r\n\r\n"), 1<<20)
	if he, ok := err.(*httpError); !ok || he.status != 400 || he.msg != "line too long" {
		t.Fatalf("5000-byte chunk line: err %v, want 400 line too long", err)
	}
	_, err = readChunkedBody(readerOf(ext(maxChunkLine+1)+"\r\nX\r\n0\r\n\r\n"), 1<<20)
	if he, ok := err.(*httpError); !ok || he.msg != "line too long" {
		t.Fatalf("4097-byte chunk line: err %v, want line too long", err)
	}
	body, err := readChunkedBody(readerOf(ext(maxChunkLine)+"\r\nX\r\n0\r\n\r\n"), 1<<20)
	if err != nil || string(body) != "X" {
		t.Fatalf("4096-byte chunk line: (%q, %v), want accepted", body, err)
	}
}

// §4.2 Request body cap, chunked cumulatively: a huge second chunk must not
// overflow the running total past the cap check.
func TestReadChunkedBodyCapNoOverflow(t *testing.T) {
	head := &requestHead{framing: framingChunked}
	_, err := readRequestBody(readerOf("1\r\nA\r\n7fffffffffffffff\r\n"), head, 16)
	if err != errBodyTooLarge {
		t.Fatalf("err %v, want 413 request body too large", err)
	}
}
