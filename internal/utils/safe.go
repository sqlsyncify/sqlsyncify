package utils

import (
	"errors"
	"net/url"
	"path/filepath"
	"regexp"
	"strings"
)

var (
	siteRe       = regexp.MustCompile(`^[a-zA-Z0-9_\-@]+$`)
	langRe       = regexp.MustCompile(`^[A-Za-z0-9_-]{1,32}$`)
	identRe      = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,127}$`)
	datasourceRe = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$`)
)

const maxSiteLen = 64

// CheckLangFormat reports whether lang is safe to substitute into a URL or SQL literal.
func CheckLangFormat(lang string) bool {
	return langRe.MatchString(lang)
}

// SafeIdent reports whether name can be interpolated as a SQL identifier.
func SafeIdent(name string) bool {
	return identRe.MatchString(name)
}

// SafeDatasourceName reports whether name is a single datasource file stem.
func SafeDatasourceName(name string) bool {
	return datasourceRe.MatchString(name)
}

// SafeIndexToken reports whether name is safe to place in an Elasticsearch path.
func SafeIndexToken(name string) bool {
	if len(name) == 0 || len(name) > 128 {
		return false
	}
	if name[0] == '.' || name[0] == '-' || name[0] == '_' {
		return false
	}
	for _, c := range name {
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '_', c == '-', c == '.':
		default:
			return false
		}
	}
	return true
}

// SafeDocType reports whether an ES 5.x document type is safe to embed in bulk metadata.
func SafeDocType(name string) bool {
	if name == "" || len(name) > 128 {
		return false
	}
	for _, c := range name {
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '_', c == '-', c == '.':
		default:
			return false
		}
	}
	return true
}

// IsGeneratedSiteIndex reports whether index is a timestamped index for one of prefixes.
// Generated names look like "{prefix}_YYYYMMDDHHMMSS". Dot-prefixed system indexes never match.
func IsGeneratedSiteIndex(index string, prefixes ...string) bool {
	if index == "" || strings.HasPrefix(index, ".") {
		return false
	}
	for _, prefix := range prefixes {
		prefix = strings.TrimSpace(prefix)
		if prefix == "" {
			continue
		}
		rest, ok := strings.CutPrefix(index, prefix+"_")
		if !ok || len(rest) != 14 {
			continue
		}
		digits := true
		for _, c := range rest {
			if c < '0' || c > '9' {
				digits = false
				break
			}
		}
		if digits {
			return true
		}
	}
	return false
}

// ParseESAddresses splits a comma-separated Elasticsearch address list.
// Only http and https URLs without userinfo, query, or fragment are accepted.
func ParseESAddresses(raw string) ([]string, error) {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		u, err := url.Parse(part)
		if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil {
			return nil, errors.New("invalid elasticsearch address")
		}
		if u.RawQuery != "" || u.Fragment != "" || strings.Contains(u.Path, "..") || strings.ContainsAny(u.Path, "\\\"<>") {
			return nil, errors.New("invalid elasticsearch address")
		}
		out = append(out, u.String())
	}
	if len(out) == 0 {
		return nil, errors.New("elasticsearch address is empty")
	}
	return out, nil
}

// SafeHTTPBase checks the base URL substituted into index settings for the synonym plugin.
func SafeHTTPBase(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil {
		return "", errors.New("invalid AppHost")
	}
	if u.RawQuery != "" || u.Fragment != "" {
		return "", errors.New("invalid AppHost")
	}
	if strings.ContainsAny(u.Host, "\\\"'<> \t\r\n") || strings.ContainsAny(u.Path, "\\\"'<> \t\r\n{}") || strings.Contains(u.Path, "..") {
		return "", errors.New("invalid AppHost")
	}
	return strings.TrimRight(u.String(), "/"), nil
}

// ResolveUnder joins elem onto root and rejects paths that escape root.
func ResolveUnder(root string, elem ...string) (string, error) {
	rootAbs, err := filepath.Abs(root)
	if err != nil {
		return "", err
	}
	parts := make([]string, 0, len(elem)+1)
	parts = append(parts, rootAbs)
	parts = append(parts, elem...)
	full := filepath.Clean(filepath.Join(parts...))
	rel, err := filepath.Rel(rootAbs, full)
	if err != nil {
		return "", err
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", errors.New("path escapes root")
	}
	return full, nil
}

// SQLDirective reads a single-line `-- marker` value such as `-- key=` or `-- ds=`.
func SQLDirective(sql, marker string) (string, bool, error) {
	pos := strings.Index(sql, marker)
	if pos < 0 {
		return "", false, nil
	}
	rest := sql[pos+len(marker):]
	end := strings.IndexByte(rest, '\n')
	if end < 0 {
		return "", true, errors.New("unterminated sql directive")
	}
	return strings.Trim(rest[:end], " \t\r"), true, nil
}
