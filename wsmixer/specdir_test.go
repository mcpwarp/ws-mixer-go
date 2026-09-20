package wsmixer

import (
	"os"
	"path/filepath"
	"testing"
)

// specDir locates a checkout of ws-mixer-spec (docs/MIGRATION.md section
// 3, step-3 "go" fixup) so the fixture-driven tests (frame_test.go,
// control_test.go, sequence_test.go) can find spec/fixtures/* without the
// monorepo's shared root. The return value is the `spec/` directory itself
// (i.e. callers join straight onto "fixtures/...", no further "spec"
// component needed).
//
// Resolution order:
//  1. $WSMIXER_SPEC_DIR, if set — must exist, and must contain fixtures
//     either directly or one level down under `spec/`. ws-mixer-go's own
//     convention for this variable is repo-root (`$WSMIXER_SPEC_DIR/spec/fixtures`),
//     but ws-mixer-js's test/helpers/spec-dir.ts convention is the spec
//     subdir itself (`$WSMIXER_SPEC_DIR/fixtures`) — both are tolerated so
//     the same env var works against either kind of checkout.
//  2. <repo-root>/.spec/spec, the checkout `make fetch-spec` populates from
//     spec.pin (this is also what CI's WSMIXER_SPEC_DIR points at, and
//     matches ws-mixer-js's own `.spec/spec at repo root` convention
//     exactly). This test file lives in wsmixer/, one level below the repo
//     root, so it's one ".." from here, not two.
//
// If neither exists, the fixture is not fetched yet: skip rather than fail,
// so `go test ./...` stays green without a network fetch (CI always runs
// `make fetch-spec` first; docs/MIGRATION.md section 2.5).
func specDir(t *testing.T) string {
	t.Helper()
	if d := os.Getenv("WSMIXER_SPEC_DIR"); d != "" {
		if _, err := os.Stat(d); err != nil {
			t.Fatalf("WSMIXER_SPEC_DIR=%s: %v", d, err)
		}
		if sub := filepath.Join(d, "spec", "fixtures"); dirExists(sub) {
			return filepath.Join(d, "spec")
		}
		if dirExists(filepath.Join(d, "fixtures")) {
			return d
		}
		t.Fatalf("WSMIXER_SPEC_DIR=%s: found neither spec/fixtures nor fixtures under it", d)
	}
	d := filepath.Join("..", ".spec", "spec")
	if _, err := os.Stat(d); err != nil {
		t.Skipf("no spec checkout found (set WSMIXER_SPEC_DIR or run `make fetch-spec`): %v", err)
	}
	return d
}

func dirExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.IsDir()
}
