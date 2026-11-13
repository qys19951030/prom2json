package batch

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeTargetsFile(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "targets.json")
	if err := os.WriteFile(path, []byte(content), 0o666); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadTargetsValid(t *testing.T) {
	path := writeTargetsFile(t, `[
	  {"name": "node-1", "url": "http://localhost:9090/metrics"},
	  {"name": "app_2", "url": "https://example.com/metrics"}
	]`)
	targets, err := LoadTargets(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(targets) != 2 {
		t.Fatalf("expected 2 targets, got %d", len(targets))
	}
	if targets[0].Name != "node-1" || targets[1].URL != "https://example.com/metrics" {
		t.Fatalf("unexpected targets: %+v", targets)
	}
}

func TestLoadTargetsValidationErrors(t *testing.T) {
	for _, tc := range []struct {
		desc        string
		content     string
		wantIndex   int
		wantField   string
		wantMessage string
	}{
		{
			desc:        "duplicate name",
			content:     `[{"name":"a","url":"http://x/"},{"name":"a","url":"http://y/"}]`,
			wantIndex:   1,
			wantField:   "name",
			wantMessage: "duplicates entry at index 0",
		},
		{
			desc:        "name with invalid characters",
			content:     `[{"name":"Bad Name!","url":"http://x/"}]`,
			wantIndex:   0,
			wantField:   "name",
			wantMessage: "characters outside",
		},
		{
			desc:        "missing name",
			content:     `[{"url":"http://x/"}]`,
			wantIndex:   0,
			wantField:   "name",
			wantMessage: "must not be empty",
		},
		{
			desc:        "missing url",
			content:     `[{"name":"a"}]`,
			wantIndex:   0,
			wantField:   "url",
			wantMessage: "must not be empty",
		},
		{
			desc:        "non-http scheme",
			content:     `[{"name":"a","url":"ftp://x/metrics"}]`,
			wantIndex:   0,
			wantField:   "url",
			wantMessage: "http or https",
		},
		{
			desc:        "schemeless url",
			content:     `[{"name":"a","url":"localhost:9090/metrics"}]`,
			wantIndex:   0,
			wantField:   "url",
			wantMessage: "http or https",
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			path := writeTargetsFile(t, tc.content)
			_, err := LoadTargets(path)
			if err == nil {
				t.Fatal("expected an error, got nil")
			}
			var verr *ValidationError
			if !errors.As(err, &verr) {
				t.Fatalf("expected a ValidationError, got %T: %v", err, err)
			}
			if verr.Index != tc.wantIndex || verr.Field != tc.wantField {
				t.Fatalf("expected index %d field %q, got index %d field %q",
					tc.wantIndex, tc.wantField, verr.Index, verr.Field)
			}
			if !strings.Contains(err.Error(), tc.wantMessage) {
				t.Fatalf("error %q does not mention %q", err, tc.wantMessage)
			}
		})
	}
}

func TestLoadTargetsMalformed(t *testing.T) {
	path := writeTargetsFile(t, `{"name":"not-an-array"}`)
	if _, err := LoadTargets(path); err == nil {
		t.Fatal("expected an error for a non-array targets file, got nil")
	}
	path = writeTargetsFile(t, `[{"name":"a","url":`)
	if _, err := LoadTargets(path); err == nil {
		t.Fatal("expected an error for truncated JSON, got nil")
	}
}

func TestLoadTargetsMissingFile(t *testing.T) {
	if _, err := LoadTargets(filepath.Join(t.TempDir(), "does-not-exist.json")); err == nil {
		t.Fatal("expected an error for a missing file, got nil")
	}
}
