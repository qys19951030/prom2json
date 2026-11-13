package batch

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/prom2json"
)

func metricsHandler(body string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		fmt.Fprint(w, body)
	})
}

func readManifest(t *testing.T, dir string) []ManifestEntry {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if err != nil {
		t.Fatalf("reading manifest.json: %v", err)
	}
	var entries []ManifestEntry
	if err := json.Unmarshal(raw, &entries); err != nil {
		t.Fatalf("parsing manifest.json: %v", err)
	}
	return entries
}

func readSummary(t *testing.T, dir string) Summary {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(dir, "summary.json"))
	if err != nil {
		t.Fatalf("reading summary.json: %v", err)
	}
	var summary Summary
	if err := json.Unmarshal(raw, &summary); err != nil {
		t.Fatalf("parsing summary.json: %v", err)
	}
	return summary
}

func readFamilies(t *testing.T, path string) []*prom2json.Family {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading %s: %v", path, err)
	}
	var families []*prom2json.Family
	if err := json.Unmarshal(raw, &families); err != nil {
		t.Fatalf("parsing %s: %v", path, err)
	}
	return families
}

func testOptions(outDir string) Options {
	return Options{
		Concurrency: 4,
		Timeout:     5 * time.Second,
		Retries:     1,
		OutDir:      outDir,
	}
}

func TestRunAllSuccess(t *testing.T) {
	bodies := map[string]string{
		"alpha": "# HELP alpha_metric Alpha metric.\n" +
			"# TYPE alpha_metric counter\n" +
			"alpha_metric{code=\"200\"} 1\n" +
			"alpha_metric{code=\"500\"} 2\n" +
			"# HELP shared_metric Shared metric.\n" +
			"# TYPE shared_metric gauge\n" +
			"shared_metric 3\n",
		"mid": "# HELP mid_metric Mid metric.\n" +
			"# TYPE mid_metric gauge\n" +
			"mid_metric 1\n" +
			"# HELP shared_metric Shared metric.\n" +
			"# TYPE shared_metric gauge\n" +
			"shared_metric 2\n",
		"zeta": "# HELP zeta_metric Zeta metric.\n" +
			"# TYPE zeta_metric counter\n" +
			"zeta_metric 42\n",
	}
	// Deliberately not sorted by name to verify the manifest ordering.
	names := []string{"zeta", "alpha", "mid"}
	var targets []Target
	for _, name := range names {
		srv := httptest.NewServer(metricsHandler(bodies[name]))
		t.Cleanup(srv.Close)
		targets = append(targets, Target{Name: name, URL: srv.URL})
	}
	outDir := filepath.Join(t.TempDir(), "out")

	ok, err := Run(context.Background(), targets, testOptions(outDir))
	if err != nil {
		t.Fatalf("Run returned an error: %v", err)
	}
	if !ok {
		t.Fatal("Run reported failures although all targets should succeed")
	}

	// Result files carry the same JSON structure as single-target output.
	for _, name := range names {
		families := readFamilies(t, filepath.Join(outDir, name+".json"))
		if len(families) != 2 && name != "zeta" {
			t.Fatalf("target %s: expected 2 families, got %d", name, len(families))
		}
		if name == "zeta" && len(families) != 1 {
			t.Fatalf("target zeta: expected 1 family, got %d", len(families))
		}
		gotNames := map[string]bool{}
		for _, f := range families {
			gotNames[f.Name] = true
		}
		if !gotNames[name+"_metric"] {
			t.Fatalf("target %s: expected its own metric family, got %v", name, gotNames)
		}
	}
	alpha := readFamilies(t, filepath.Join(outDir, "alpha.json"))
	var alphaMetric *prom2json.Family
	for _, f := range alpha {
		if f.Name == "alpha_metric" {
			alphaMetric = f
		}
	}
	if alphaMetric == nil || alphaMetric.Help != "Alpha metric." || alphaMetric.Type != "COUNTER" || len(alphaMetric.Metrics) != 2 {
		t.Fatalf("unexpected alpha.json content: %+v", alpha)
	}

	entries := readManifest(t, outDir)
	if len(entries) != 3 {
		t.Fatalf("expected 3 manifest entries, got %d", len(entries))
	}
	want := []ManifestEntry{
		{Name: "alpha", Status: StatusOK, File: "alpha.json", Families: 2, Metrics: 3, Attempts: 1},
		{Name: "mid", Status: StatusOK, File: "mid.json", Families: 2, Metrics: 2, Attempts: 1},
		{Name: "zeta", Status: StatusOK, File: "zeta.json", Families: 1, Metrics: 1, Attempts: 1},
	}
	for i, w := range want {
		got := entries[i]
		if got.Name != w.Name || got.Status != w.Status || got.File != w.File ||
			got.Families != w.Families || got.Metrics != w.Metrics || got.Attempts != w.Attempts ||
			got.Error != "" || got.URL == "" {
			t.Fatalf("manifest entry %d: expected %+v, got %+v", i, w, got)
		}
	}

	summary := readSummary(t, outDir)
	if summary.TargetsTotal != 3 || summary.TargetsOK != 3 || summary.TargetsFailed != 0 {
		t.Fatalf("unexpected summary counts: %+v", summary)
	}
	if summary.MetricsTotal != 6 {
		t.Fatalf("expected 6 metrics in total, got %d", summary.MetricsTotal)
	}
	wantFamilies := []FamilyStat{
		{Name: "alpha_metric", Targets: 1},
		{Name: "mid_metric", Targets: 1},
		{Name: "shared_metric", Targets: 2},
		{Name: "zeta_metric", Targets: 1},
	}
	if len(summary.Families) != len(wantFamilies) {
		t.Fatalf("expected %d family stats, got %+v", len(wantFamilies), summary.Families)
	}
	for i, w := range wantFamilies {
		if summary.Families[i] != w {
			t.Fatalf("family stat %d: expected %+v, got %+v", i, w, summary.Families[i])
		}
	}
}

func TestRunRetryOn5xx(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if calls.Add(1) < 3 {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		fmt.Fprint(w, "# HELP flaky_metric Flaky metric.\n# TYPE flaky_metric gauge\nflaky_metric 1\n")
	}))
	defer srv.Close()
	outDir := filepath.Join(t.TempDir(), "out")

	opts := testOptions(outDir)
	opts.Retries = 2
	ok, err := Run(context.Background(), []Target{{Name: "flaky", URL: srv.URL}}, opts)
	if err != nil {
		t.Fatalf("Run returned an error: %v", err)
	}
	if !ok {
		t.Fatal("Run reported a failure although the target eventually succeeded")
	}

	entries := readManifest(t, outDir)
	if len(entries) != 1 {
		t.Fatalf("expected 1 manifest entry, got %d", len(entries))
	}
	entry := entries[0]
	if entry.Status != StatusOK {
		t.Fatalf("expected status ok, got %+v", entry)
	}
	if entry.Attempts != 3 {
		t.Fatalf("expected 3 attempts, got %d", entry.Attempts)
	}
	if got := calls.Load(); got != 3 {
		t.Fatalf("expected 3 requests against the server, got %d", got)
	}
	if _, err := os.Stat(filepath.Join(outDir, "flaky.json")); err != nil {
		t.Fatalf("expected flaky.json to exist: %v", err)
	}
}

func TestRunPartialFailure(t *testing.T) {
	okSrv := httptest.NewServer(metricsHandler(
		"# HELP good_metric Good metric.\n# TYPE good_metric gauge\ngood_metric 1\n"))
	defer okSrv.Close()
	fail500Srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer fail500Srv.Close()
	fail404Srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer fail404Srv.Close()
	outDir := filepath.Join(t.TempDir(), "out")

	targets := []Target{
		{Name: "good", URL: okSrv.URL},
		{Name: "bad500", URL: fail500Srv.URL},
		{Name: "bad404", URL: fail404Srv.URL},
	}
	opts := testOptions(outDir)
	ok, err := Run(context.Background(), targets, opts)
	if err != nil {
		t.Fatalf("Run returned an error: %v", err)
	}
	if ok {
		t.Fatal("Run reported success although two targets failed")
	}

	// Failed targets leave no result file behind.
	for _, name := range []string{"bad500", "bad404"} {
		if _, err := os.Stat(filepath.Join(outDir, name+".json")); !os.IsNotExist(err) {
			t.Fatalf("expected no result file for %s, stat error: %v", name, err)
		}
	}
	if families := readFamilies(t, filepath.Join(outDir, "good.json")); len(families) != 1 {
		t.Fatalf("expected 1 family in good.json, got %d", len(families))
	}

	entries := readManifest(t, outDir)
	if len(entries) != 3 {
		t.Fatalf("expected 3 manifest entries, got %d", len(entries))
	}
	byName := map[string]ManifestEntry{}
	for _, e := range entries {
		byName[e.Name] = e
	}
	if e := byName["good"]; e.Status != StatusOK || e.File != "good.json" || e.Metrics != 1 || e.Attempts != 1 {
		t.Fatalf("unexpected manifest entry for good: %+v", e)
	}
	// A 5xx target is retried (1 initial attempt + 1 retry).
	if e := byName["bad500"]; e.Status != StatusFailed || e.Error == "" || e.Attempts != 2 || e.File != "" {
		t.Fatalf("unexpected manifest entry for bad500: %+v", e)
	}
	// A 4xx target is not retried.
	if e := byName["bad404"]; e.Status != StatusFailed || e.Error == "" || e.Attempts != 1 || e.File != "" {
		t.Fatalf("unexpected manifest entry for bad404: %+v", e)
	}

	summary := readSummary(t, outDir)
	if summary.TargetsTotal != 3 || summary.TargetsOK != 1 || summary.TargetsFailed != 2 {
		t.Fatalf("unexpected summary counts: %+v", summary)
	}
	if summary.MetricsTotal != 1 || len(summary.Families) != 1 || summary.Families[0].Name != "good_metric" {
		t.Fatalf("unexpected summary metrics: %+v", summary)
	}
}

func TestRunConcurrencyLimit(t *testing.T) {
	var inFlight, maxInFlight atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		cur := inFlight.Add(1)
		for {
			max := maxInFlight.Load()
			if cur <= max || maxInFlight.CompareAndSwap(max, cur) {
				break
			}
		}
		time.Sleep(50 * time.Millisecond)
		inFlight.Add(-1)
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		fmt.Fprint(w, "# HELP c_metric C metric.\n# TYPE c_metric gauge\nc_metric 1\n")
	}))
	defer srv.Close()

	var targets []Target
	for i := 0; i < 6; i++ {
		targets = append(targets, Target{Name: fmt.Sprintf("t%d", i), URL: srv.URL})
	}
	outDir := filepath.Join(t.TempDir(), "out")
	opts := testOptions(outDir)
	opts.Concurrency = 2

	ok, err := Run(context.Background(), targets, opts)
	if err != nil {
		t.Fatalf("Run returned an error: %v", err)
	}
	if !ok {
		t.Fatal("Run reported failures although all targets should succeed")
	}
	if max := maxInFlight.Load(); max > 2 {
		t.Fatalf("concurrency limit violated: %d requests in flight", max)
	} else if max < 2 {
		t.Fatalf("expected requests to actually run concurrently, max in flight: %d", max)
	}
}
