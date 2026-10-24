package mcpwarp

import (
	"regexp"
	"strings"
)

//go:generate go run ./internal/genstatustext -in ../.spec/spec/fixtures/mcpwarp/status-text -pin ../spec.pin -out statustext_tables.go

// StatusText is a TunnelStatus as UI text (§7); Detail is "" when absent.
type StatusText struct {
	Label  string
	Detail string
}

// StatusMessages overrides strings of the resolved locale's table, per
// call. Keys outside the table are ignored.
type StatusMessages struct {
	States map[TunnelState]string
	Codes  map[StatusCode]string
}

// DescribeOptions selects the locale ("" is en) and per-call overrides.
type DescribeOptions struct {
	Locale   string
	Messages StatusMessages
}

// StatusLocales are the shipped locales, in resolution order.
var StatusLocales = append([]string(nil), statusLocaleOrder...)

type statusTable struct {
	states map[TunnelState]string
	codes  map[StatusCode]string
}

// jsSpace is JavaScript's \s and String.prototype.trim whitespace set.
var jsSpace = string([]rune{
	'\t', '\n', '\v', '\f', '\r', ' ', 0x00a0, 0x1680,
	0x2000, 0x2001, 0x2002, 0x2003, 0x2004, 0x2005, 0x2006, 0x2007, 0x2008, 0x2009, 0x200a,
	0x2028, 0x2029, 0x202f, 0x205f, 0x3000, 0xfeff,
})

// lastErrorColon is rules.json's lastErrorColonRewrite, /:\s+/g.
var lastErrorColon = regexp.MustCompile(`:[` + regexp.QuoteMeta(jsSpace) + `]+`)

// resolveLocale is §7 rule 2: trimmed, "_" read as "-", lowercased; an exact
// match, else the first locale with the same language subtag, else en.
func resolveLocale(locale string) string {
	tag := strings.ToLower(strings.ReplaceAll(strings.Trim(locale, jsSpace), "_", "-"))
	for _, l := range statusLocaleOrder {
		if strings.ToLower(l) == tag {
			return l
		}
	}
	lang, _, _ := strings.Cut(tag, "-")
	for _, l := range statusLocaleOrder {
		if ll, _, _ := strings.Cut(strings.ToLower(l), "-"); ll == lang {
			return l
		}
	}
	return "en"
}

// DescribeStatus turns a status into UI text (§7): Label is the state's
// string, Detail the code's sentence (for INVALID_CONFIG combined with
// LastError), or LastError when there is no known code; Detail is left out
// when empty or equal to Label. It never fails.
func DescribeStatus(s TunnelStatus, o DescribeOptions) StatusText {
	locale := resolveLocale(o.Locale)
	table := statusTables[locale]
	label, ok := table.states[s.State]
	if !ok {
		label = string(s.State)
	} else if v, ok := o.Messages.States[s.State]; ok {
		label = v
	}
	sentence, known := table.codes[s.Code]
	if known {
		if v, ok := o.Messages.Codes[s.Code]; ok {
			sentence = v
		}
	}
	var detail string
	switch {
	case !known:
		detail = s.LastError
	case s.Code == CodeInvalidConfig && s.LastError != "":
		switch {
		case s.LastError == lastInvalidConfig:
			detail = sentence
		case strings.HasPrefix(s.LastError, sentence):
			detail = s.LastError
		default:
			detail = sentence + statusJoiners[locale] + lastErrorColon.ReplaceAllString(s.LastError, "; ")
		}
	default:
		detail = sentence
	}
	out := StatusText{Label: label}
	if detail != label {
		out.Detail = detail
	}
	return out
}
