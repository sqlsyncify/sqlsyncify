package export

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestBulkMetaEscapesDocumentID(t *testing.T) {
	meta, err := bulkMeta("a\"b\n", "post")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(string(meta), "\n") != 1 {
		t.Fatalf("meta must be one ndjson line: %q", meta)
	}
	var parsed map[string]map[string]string
	if err := json.Unmarshal(bytesTrimNL(meta), &parsed); err != nil {
		t.Fatal(err)
	}
	if parsed["index"]["_id"] != "a\"b\n" || parsed["index"]["_type"] != "post" {
		t.Fatalf("unexpected meta %#v", parsed)
	}
}

func bytesTrimNL(b []byte) []byte {
	if len(b) > 0 && b[len(b)-1] == '\n' {
		return b[:len(b)-1]
	}
	return b
}
