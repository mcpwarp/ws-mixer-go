//go:build conformance

package wsmixer

// AllowSubfloorTiming is the conformance runner's Go SDK adapter's only
// escape hatch into wsmixer-internal state: it flips the unexported
// Options.allowSubfloorTiming field so Conn.applyWelcome (client.go) skips
// the wire's ping_interval >= 5000ms / ping_timeout >= 2x ping_interval floor
// checks, letting a scaled-clock conformance run (--time-scale) exercise a
// real Dial against a welcome carrying sub-floor values. This file is
// compiled in only under the `conformance` build tag (see
// conformance/README.md and the Go adapter's build command); a normal `go
// build ./...` never sees it, so production code has no way to set this
// field.
func AllowSubfloorTiming(o *Options) {
	o.allowSubfloorTiming = true
}
