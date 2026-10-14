package mcpwarp

import (
	"os"
	"path/filepath"
	"testing"
)

// mcpwarpFixtureDir returns spec/fixtures/mcpwarp/<set> from a ws-mixer-spec
// checkout, resolved like wsmixer/specdir_test.go's specDir: $WSMIXER_SPEC_DIR
// (repo root or its spec/ subdir), else <repo-root>/.spec/spec, the checkout
// `make fetch-spec` populates from spec.pin. Without a checkout, or with one
// older than v0.6.0 (no mcpwarp fixtures), the test skips -- except under CI,
// where the spec is always cloned at spec.pin and a missing set is a broken
// pin.
func mcpwarpFixtureDir(t *testing.T, set string) string {
	t.Helper()
	spec := ""
	if d := os.Getenv("WSMIXER_SPEC_DIR"); d != "" {
		if _, err := os.Stat(d); err != nil {
			t.Fatalf("WSMIXER_SPEC_DIR=%s: %v", d, err)
		}
		switch {
		case dirExists(filepath.Join(d, "spec", "fixtures")):
			spec = filepath.Join(d, "spec")
		case dirExists(filepath.Join(d, "fixtures")):
			spec = d
		default:
			t.Fatalf("WSMIXER_SPEC_DIR=%s: found neither spec/fixtures nor fixtures under it", d)
		}
	} else {
		spec = filepath.Join("..", ".spec", "spec")
	}
	dir := filepath.Join(spec, "fixtures", "mcpwarp", set)
	if !dirExists(dir) {
		msg := "no " + dir + " (set WSMIXER_SPEC_DIR or run `make fetch-spec`; mcpwarp fixtures need spec v0.6.0+)"
		if os.Getenv("CI") != "" {
			t.Fatal(msg)
		}
		t.Skip(msg)
	}
	return dir
}

func dirExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.IsDir()
}
