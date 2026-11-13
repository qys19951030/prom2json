package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func writeTargetsFile(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "targets.json")
	if err := os.WriteFile(path, []byte(content), 0o666); err != nil {
		t.Fatal(err)
	}
	return path
}

func okServer(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		fmt.Fprint(w, "# HELP up_metric Up metric.\n# TYPE up_metric gauge\nup_metric 1\n")
	}))
	t.Cleanup(srv.Close)
	return srv
}

func runBatch(t *testing.T, targetsFile, outDir string) int {
	t.Helper()
	return runBatchMode(targetsFile, outDir, 4, 5*time.Second, 1, "", "", false, "")
}

func TestRunBatchModeAllSuccessExitZero(t *testing.T) {
	srv := okServer(t)
	targetsFile := writeTargetsFile(t,
		fmt.Sprintf(`[{"name":"one","url":%q}]`, srv.URL))
	outDir := filepath.Join(t.TempDir(), "out")
	if code := runBatch(t, targetsFile, outDir); code != 0 {
		t.Fatalf("expected exit code 0, got %d", code)
	}
	for _, name := range []string{"one.json", "manifest.json", "summary.json"} {
		if _, err := os.Stat(filepath.Join(outDir, name)); err != nil {
			t.Fatalf("expected %s to exist: %v", name, err)
		}
	}
}

func TestRunBatchModePartialFailureExitOne(t *testing.T) {
	okSrv := okServer(t)
	failSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(failSrv.Close)
	targetsFile := writeTargetsFile(t,
		fmt.Sprintf(`[{"name":"good","url":%q},{"name":"bad","url":%q}]`, okSrv.URL, failSrv.URL))
	outDir := filepath.Join(t.TempDir(), "out")
	if code := runBatch(t, targetsFile, outDir); code != 1 {
		t.Fatalf("expected exit code 1, got %d", code)
	}
	if _, err := os.Stat(filepath.Join(outDir, "good.json")); err != nil {
		t.Fatalf("expected good.json to exist: %v", err)
	}
	if _, err := os.Stat(filepath.Join(outDir, "bad.json")); !os.IsNotExist(err) {
		t.Fatalf("expected no bad.json, stat error: %v", err)
	}
}

func TestRunBatchModeInvalidTargetsExitTwo(t *testing.T) {
	srv := okServer(t)
	for _, tc := range []struct {
		desc    string
		content string
	}{
		{
			desc:    "duplicate name",
			content: fmt.Sprintf(`[{"name":"a","url":%q},{"name":"a","url":%q}]`, srv.URL, srv.URL),
		},
		{
			desc:    "invalid url",
			content: `[{"name":"a","url":"gopher://example.com/metrics"}]`,
		},
		{
			desc:    "invalid name",
			content: fmt.Sprintf(`[{"name":"A","url":%q}]`, srv.URL),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			targetsFile := writeTargetsFile(t, tc.content)
			// The output directory already exists with content from a
			// previous run; a validation failure must not touch it.
			outDir := t.TempDir()
			sentinel := filepath.Join(outDir, "previous.json")
			if err := os.WriteFile(sentinel, []byte("keep me"), 0o666); err != nil {
				t.Fatal(err)
			}
			if code := runBatch(t, targetsFile, outDir); code != 2 {
				t.Fatalf("expected exit code 2, got %d", code)
			}
			entries, err := os.ReadDir(outDir)
			if err != nil {
				t.Fatal(err)
			}
			if len(entries) != 1 || entries[0].Name() != "previous.json" {
				t.Fatalf("output directory was modified: %v", entries)
			}
		})
	}
}

func TestRunBatchModeInvalidTargetsNoOutDirCreated(t *testing.T) {
	targetsFile := writeTargetsFile(t, `[{"name":"a","url":"not-a-url"}]`)
	outDir := filepath.Join(t.TempDir(), "out")
	if code := runBatch(t, targetsFile, outDir); code != 2 {
		t.Fatalf("expected exit code 2, got %d", code)
	}
	if _, err := os.Stat(outDir); !os.IsNotExist(err) {
		t.Fatalf("expected no output directory to be created, stat error: %v", err)
	}
}
