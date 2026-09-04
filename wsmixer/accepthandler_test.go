package wsmixer

import (
	"context"
	"net/http"
	"strings"

	"github.com/coder/websocket"
)

// testAcceptHandler is a minimal http.Handler built directly on AcceptConn,
// the same glue wsmixerserver.Listener provides in production, minus its
// policy (no pre-upgrade 400/401, no AuthenticateRequest): that policy layer
// is what wsmixerserver owns and tests itself now (docs/MIGRATION.md section
// 0.5). It exists so go/wsmixer's own loopback tests (integration_test.go)
// keep exercising a real HTTP+WebSocket round trip without importing
// wsmixerserver, which would make core tests depend on the server package.
type testAcceptHandler struct {
	Options
	ServerInfo   *ServerInfo
	Authenticate func(ctx context.Context, h *Hello) (WelcomeMeta, error)
	OnConn       func(*Conn)
}

// newTestListener builds a *testAcceptHandler, applying Options.SetDefaults
// and defaulting OnConn the way wsmixerserver.NewListener does.
func newTestListener(h testAcceptHandler) *testAcceptHandler {
	h.Options.SetDefaults()
	if h.OnConn == nil {
		h.OnConn = func(*Conn) {}
	}
	return &h
}

// testBearerToken is a minimal, non-validating stand-in for
// wsmixerserver's parseBearerToken: this harness has no pre-upgrade policy
// to enforce, it only needs to hand AcceptConn the token from the header.
func testBearerToken(header string) string {
	const prefix = "bearer "
	if len(header) <= len(prefix) || !strings.EqualFold(header[:len(prefix)], prefix) {
		return ""
	}
	return strings.TrimSpace(header[len(prefix):])
}

func (h *testAcceptHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ws, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		Subprotocols:    []string{Subprotocol},
		CompressionMode: websocket.CompressionDisabled,
	})
	if err != nil {
		return
	}
	defer ws.CloseNow()
	ws.SetReadLimit(h.ReadLimit)

	bearer := testBearerToken(r.Header.Get("Authorization"))
	c, err := AcceptConn(r.Context(), ws, bearer, AcceptOptions{
		Options:      h.Options,
		ServerInfo:   h.ServerInfo,
		Authenticate: h.Authenticate,
		Request:      r,
	})
	if err != nil {
		return
	}
	h.OnConn(c)
	c.Run()
	<-c.Done()
}
