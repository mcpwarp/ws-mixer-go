//go:build conformance

// Command conformance-adapter wires ws-mixer-go/conformance/adapter's
// stdin/stdout harness (the ws-mixer conformance runner's Go SDK adapter,
// docs/CONFORMANCE.md section 1) directly onto go/wsmixer's bare
// wsmixer.AcceptConn, with no HTTP-layer policy of its own (no pre-upgrade
// 400/401) -- this adapter's harness has no fixture that exercises that
// policy layer, which belongs to the separate HTTP/upgrade/auth layer
// instead (docs/MIGRATION.md section 0.5). This is what go/wsmixer's own CI
// runs, proving the protocol core conforms in both matrix roles
// (docs/MIGRATION.md section 2.3) with no dependency on that separate layer.
//
// Must be built with `-tags conformance` (conformance/README.md) -- without
// that tag, go/wsmixer/conformance_hooks.go is excluded from the build and
// wsmixer.AllowSubfloorTiming (used by the adapter package) does not exist.
package main

import (
	"net/http"
	"os"
	"strings"

	"github.com/coder/websocket"
	"github.com/mcpwarp/ws-mixer-go/conformance/adapter"
	"github.com/mcpwarp/ws-mixer-go/internal/version"
	"github.com/mcpwarp/ws-mixer-go/wsmixer"
)

// acceptBackend implements adapter.ServerBackend directly on
// wsmixer.AcceptConn (see the package doc comment).
type acceptBackend struct{}

func (acceptBackend) Handler(cfg adapter.ServerConfig) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		bearer := bearerToken(r.Header.Get("Authorization"))
		ws, err := websocket.Accept(w, r, &websocket.AcceptOptions{
			Subprotocols:    []string{wsmixer.Subprotocol},
			CompressionMode: websocket.CompressionDisabled,
		})
		if err != nil {
			return
		}
		defer ws.CloseNow()

		opts := cfg.Options
		opts.SetDefaults()
		ws.SetReadLimit(opts.ReadLimit)

		c, err := wsmixer.AcceptConn(r.Context(), ws, bearer, wsmixer.AcceptOptions{
			Options:      opts,
			Authenticate: cfg.Authenticate,
			Request:      r,
		})
		if err != nil {
			return
		}
		cfg.OnConn(c)
		c.Run()
		<-c.Done()
	})
}

// bearerToken extracts the token from "Authorization: Bearer <token>",
// case-insensitively on the scheme. This harness has no pre-upgrade rejection
// policy (that lives in the separate HTTP/upgrade/auth layer): an
// empty/malformed header just yields an empty bearer, which
// wsmixer.AcceptConn's own hello.token check rejects.
func bearerToken(header string) string {
	const prefix = "bearer "
	if len(header) <= len(prefix) || !strings.EqualFold(header[:len(prefix)], prefix) {
		return ""
	}
	return strings.TrimSpace(header[len(prefix):])
}

func main() {
	os.Exit(adapter.Run(adapter.Config{
		Backend:    acceptBackend{},
		SDK:        "ws-mixer-go",
		SDKVersion: version.SDK,
	}))
}
