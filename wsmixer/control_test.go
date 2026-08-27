package wsmixer

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

type controlFixture struct {
	Path        string
	Description string          `json:"description"`
	Message     json.RawMessage `json:"message"`
	Raw         *string         `json:"raw"`
	SchemaValid bool            `json:"schema_valid"`
	WireValid   bool            `json:"wire_valid"`
}

func loadControlFixtures(t *testing.T) []controlFixture {
	t.Helper()
	root := filepath.Join("..", "..", "spec", "fixtures", "control")
	var out []controlFixture
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || filepath.Ext(path) != ".json" {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		var f controlFixture
		if err := json.Unmarshal(data, &f); err != nil {
			t.Fatalf("parsing %s: %v", path, err)
		}
		rel, _ := filepath.Rel(root, path)
		f.Path = rel
		out = append(out, f)
		return nil
	})
	if err != nil {
		t.Fatalf("walking control fixtures: %v", err)
	}
	if len(out) == 0 {
		t.Fatal("no control fixtures found")
	}
	return out
}

// TestControlFixtures asserts that ParseControl's pass/fail verdict agrees
// with each fixture's wire_valid, per spec/README.md: "SDK runtime validators
// assert wire_valid". Fixtures that fail only the strict test-time schema
// (schema_valid:false, wire_valid:true, e.g. unknown top-level fields) are
// expected to parse successfully here.
func TestControlFixtures(t *testing.T) {
	for _, f := range loadControlFixtures(t) {
		f := f
		t.Run(f.Path, func(t *testing.T) {
			var raw []byte
			if f.Raw != nil {
				raw = []byte(*f.Raw)
			} else {
				raw = f.Message
			}
			_, err := ParseControl(raw)
			if f.WireValid {
				if err != nil {
					t.Fatalf("expected wire_valid=true, got parse error: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected wire_valid=false, but ParseControl accepted it")
			}
		})
	}
}

func TestControlEnvelopeErrorsAreConnectionFatal(t *testing.T) {
	for _, f := range loadControlFixtures(t) {
		if f.WireValid {
			continue
		}
		var raw []byte
		if f.Raw != nil {
			raw = []byte(*f.Raw)
		} else {
			raw = f.Message
		}
		_, err := ParseControl(raw)
		if err == nil {
			t.Errorf("%s: expected error", f.Path)
			continue
		}
		if _, ok := err.(*ConnError); !ok {
			t.Errorf("%s: expected *ConnError, got %T", f.Path, err)
		}
	}
}
