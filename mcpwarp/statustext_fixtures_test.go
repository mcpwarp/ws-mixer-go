package mcpwarp

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
)

// spec/fixtures/mcpwarp/status-text (MCPWARP.md §7, §10.5): the generated
// tables (statustext_tables.go, `go generate`) must equal what the pinned
// locale files and rules.json give, every legal pair must describe to its
// file entry, and every cases.json input to its expected text. A drift
// between the checked-in tables and spec.pin fails here.

type statusEntry struct {
	Label  string  `json:"label"`
	Detail *string `json:"detail"`
}

func readStatusJSON(t *testing.T, dir, name string, v any) {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, v); err != nil {
		t.Fatalf("%s: %v", name, err)
	}
}

func TestStatusTextFixtures(t *testing.T) {
	dir := mcpwarpFixtureDir(t, "status-text")
	var rules struct {
		Locales     []string          `json:"locales"`
		ColonJoiner map[string]string `json:"colonJoiner"`
		ZhAlias     string            `json:"zhAlias"`
		Rewrite     struct {
			Pattern     string `json:"pattern"`
			Replacement string `json:"replacement"`
		} `json:"lastErrorColonRewrite"`
		InvalidConfig struct {
			Generic string `json:"genericLastError"`
		} `json:"invalidConfigComposition"`
		HostOnly      []string `json:"hostOnly"`
		HostOnlyCases string   `json:"hostOnlyCases"`
	}
	readStatusJSON(t, dir, "rules.json", &rules)

	t.Run("rules.json", func(t *testing.T) {
		if !reflect.DeepEqual(StatusLocales, rules.Locales) {
			t.Fatalf("StatusLocales %v, rules.json %v", StatusLocales, rules.Locales)
		}
		if !reflect.DeepEqual(statusJoiners, rules.ColonJoiner) {
			t.Fatalf("joiners %q, rules.json %q", statusJoiners, rules.ColonJoiner)
		}
		if rules.Rewrite.Pattern != `:\s+` || rules.Rewrite.Replacement != "; " || rules.InvalidConfig.Generic != lastInvalidConfig {
			t.Fatalf("rewrite/generic rule changed: %+v %q", rules.Rewrite, rules.InvalidConfig.Generic)
		}
		if resolveLocale("zh") != rules.ZhAlias {
			t.Fatalf("zh resolves to %s, want %s", resolveLocale("zh"), rules.ZhAlias)
		}
	})

	hostOnly := map[string]bool{}
	for _, k := range rules.HostOnly {
		hostOnly[k] = true
	}
	if len(hostOnly) == 0 {
		t.Fatal("rules.json has no hostOnly keys (spec v0.8.0+)")
	}
	var enKeys []string
	for _, locale := range rules.Locales {
		var file map[string]statusEntry
		readStatusJSON(t, dir, locale+".json", &file)
		keys := make([]string, 0, len(file))
		for k := range file {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		if locale == "en" {
			enKeys = keys
		}
		t.Run(locale+".json", func(t *testing.T) {
			if !reflect.DeepEqual(keys, enKeys) {
				t.Fatalf("key set %v, want en.json's", keys)
			}
			states, codes := map[TunnelState]string{}, map[StatusCode]string{}
			pairs := 0
			for _, k := range keys {
				e := file[k]
				state, code, _ := strings.Cut(k, ".")
				want := StatusText{Label: e.Label}
				if e.Detail != nil {
					want.Detail = *e.Detail
				}
				if hostOnly[k] {
					continue // rules.json hostOnly (§10.6): host guidance, optional in an SDK's tables
				}
				pairs++
				states[TunnelState(state)] = e.Label
				if code != "none" {
					s := e.Label
					if e.Detail != nil {
						s = *e.Detail
					}
					codes[StatusCode(code)] = s
				}
				st := TunnelStatus{State: TunnelState(state)}
				if code != "none" {
					st.Code = StatusCode(code)
				}
				if got := DescribeStatus(st, DescribeOptions{Locale: locale}); got != want {
					t.Errorf("%s: DescribeStatus = %+v, want %+v", k, got, want)
				}
			}
			if pairs != 17 {
				t.Fatalf("%d <state>.<CODE> keys, want the 17 legal pairs", pairs)
			}
			table := statusTables[locale]
			if !reflect.DeepEqual(table.states, states) || !reflect.DeepEqual(table.codes, codes) {
				t.Fatalf("generated tables drifted from %s.json; run `go generate ./mcpwarp/`", locale)
			}
		})
	}

	var cases struct {
		Cases []struct {
			Description string `json:"description"`
			Input       struct {
				Status struct {
					State     string          `json:"state"`
					URL       string          `json:"url"`
					Code      string          `json:"code"`
					LastError string          `json:"lastError"`
					Services  json.RawMessage `json:"services"`
				} `json:"status"`
				Service json.RawMessage `json:"service"`
				Locale  *string         `json:"locale"`
			} `json:"input"`
			Expect statusEntry `json:"expect"`
		} `json:"cases"`
	}
	readStatusJSON(t, dir, "cases.json", &cases)
	// rules.json hostOnlyCases is prose; take its two markers from it, and
	// fail when its wording changes so the runner is updated with it.
	byService := strings.Contains(rules.HostOnlyCases, "input.service")
	byStatusServices := strings.Contains(rules.HostOnlyCases, "input.status carries services")
	if !byService || !byStatusServices {
		t.Fatalf("rules.json hostOnlyCases %q: update the host-only case markers", rules.HostOnlyCases)
	}
	for i, c := range cases.Cases {
		t.Run("cases.json/"+c.Description, func(t *testing.T) {
			s := c.Input.Status
			if byService && c.Input.Service != nil || byStatusServices && s.Services != nil {
				t.Skip(hostGuidanceSkip)
			}
			opts := DescribeOptions{}
			if c.Input.Locale != nil {
				opts.Locale = *c.Input.Locale
			}
			got := DescribeStatus(TunnelStatus{State: TunnelState(s.State), URL: s.URL, Code: StatusCode(s.Code), LastError: s.LastError}, opts)
			want := StatusText{Label: c.Expect.Label}
			if c.Expect.Detail != nil {
				want.Detail = *c.Expect.Detail
			}
			if got != want {
				t.Fatalf("case %d: got %+v, want %+v", i, got, want)
			}
		})
	}
}

func TestDescribeStatusOverrides(t *testing.T) {
	got := DescribeStatus(TunnelStatus{State: StateError, Code: CodeQuotaExceeded}, DescribeOptions{
		Locale: "de",
		Messages: StatusMessages{
			States: map[TunnelState]string{StateError: "Kaputt", "bogus": "x"},
			Codes:  map[StatusCode]string{CodeQuotaExceeded: "Voll"},
		},
	})
	if got != (StatusText{Label: "Kaputt", Detail: "Voll"}) {
		t.Fatalf("got %+v", got)
	}
	if got := DescribeStatus(TunnelStatus{State: "weird"}, DescribeOptions{}); got != (StatusText{Label: "weird"}) {
		t.Fatalf("unknown state: %+v", got)
	}
}
