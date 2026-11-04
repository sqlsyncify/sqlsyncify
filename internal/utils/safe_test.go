package utils

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCheckSiteFormatBounds(t *testing.T) {
	if CheckSiteFormat("../etc/passwd") || CheckSiteFormat("a/b") || CheckSiteFormat("a.b") {
		t.Fatal("path characters must be rejected")
	}
	if !CheckSiteFormat("wordpress") || !CheckSiteFormat("wp_site-1") {
		t.Fatal("ordinary site names must be accepted")
	}
	if CheckSiteFormat(strings.Repeat("a", maxSiteLen+1)) {
		t.Fatal("overlong site name must be rejected")
	}
}

func TestResolveUnder(t *testing.T) {
	root := t.TempDir()
	got, err := ResolveUnder(root, "sites", "wordpress", "synonym.txt")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(got, root) {
		t.Fatalf("resolved path %s escaped %s", got, root)
	}
	if _, err := ResolveUnder(root, "..", "secret"); err == nil {
		t.Fatal("parent traversal must be rejected")
	}
}

func TestParseESAddresses(t *testing.T) {
	got, err := ParseESAddresses(" http://127.0.0.1:9200, https://es.internal:9200/prefix ")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("got %#v", got)
	}
	for _, raw := range []string{"file:///tmp/x", "http://user:pass@127.0.0.1:9200", "http://127.0.0.1:9200/?q=1", ""} {
		if _, err := ParseESAddresses(raw); err == nil {
			t.Fatalf("accepted unsafe address %q", raw)
		}
	}
}

func TestSafeHTTPBase(t *testing.T) {
	got, err := SafeHTTPBase("http://127.0.0.1:8080/")
	if err != nil || got != "http://127.0.0.1:8080" {
		t.Fatalf("got %q err %v", got, err)
	}
	if _, err := SafeHTTPBase(`http://127.0.0.1:8080/a"b`); err == nil {
		t.Fatal("quote in AppHost must be rejected")
	}
}

func TestIsGeneratedSiteIndex(t *testing.T) {
	const prefix = "wordpress"
	if !IsGeneratedSiteIndex("wordpress_20241114173209", prefix) {
		t.Fatal("generated index should match")
	}
	for _, name := range []string{".kibana", "wordpress", "other_20241114173209", "wordpress_20241114173209_old", "wordpress_abcdefghijklmn"} {
		if IsGeneratedSiteIndex(name, prefix) {
			t.Fatalf("unexpected match %s", name)
		}
	}
}

func TestSQLDirective(t *testing.T) {
	sql := "-- ds=wordpress_db\n-- key=ID\nSELECT 1\n"
	ds, ok, err := SQLDirective(sql, "-- ds=")
	if err != nil || !ok || ds != "wordpress_db" {
		t.Fatalf("ds=%q ok=%v err=%v", ds, ok, err)
	}
	key, ok, err := SQLDirective(sql, "-- key=")
	if err != nil || !ok || key != "ID" {
		t.Fatalf("key=%q ok=%v err=%v", key, ok, err)
	}
	if _, ok, err := SQLDirective("-- ds=no-newline", "-- ds="); err == nil || !ok {
		t.Fatalf("unterminated directive ok=%v err=%v", ok, err)
	}
}

func TestPHPSerializeBounded(t *testing.T) {
	ok := `a:1:{s:5:"width";i:1024;}`
	if !PHPSerializeBounded(ok) {
		t.Fatal("ordinary payload should pass")
	}
	if PHPSerializeBounded(`s:2000000000:"x";`) {
		t.Fatal("huge declared length must be rejected")
	}
	if PHPSerializeBounded(`s:-1:"x";`) {
		t.Fatal("negative length must be rejected")
	}
	if PHPSerializeBounded(`O:8:"stdClass":0:{}`) {
		t.Fatal("objects must be rejected")
	}
	// Quote inside the string is length-delimited, not escaped.
	inner := "a\";"
	payload := `s:3:"` + inner + `";`
	if !PHPSerializeBounded(payload) {
		t.Fatalf("length-delimited string should pass: %q", payload)
	}
}

func TestScanDirSkipsNonRegular(t *testing.T) {
	dir := t.TempDir()
	sqlFile := filepath.Join(dir, "posts.sql")
	if err := os.WriteFile(sqlFile, []byte("SELECT 1"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(sqlFile, filepath.Join(dir, "link.sql")); err != nil {
		t.Fatal(err)
	}
	files, err := ScanDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 || filepath.Base(files[0]) != "posts.sql" {
		t.Fatalf("got %#v", files)
	}
}
