package batch

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// Default values for Options fields, also used by the CLI flags.
const (
	DefaultConcurrency = 4
	DefaultTimeout     = 30 * time.Second
	DefaultRetries     = 1
	DefaultOutDir      = "out"
)

// Options configures a batch run.
type Options struct {
	// Concurrency limits the number of targets scraped at the same time.
	Concurrency int
	// Timeout bounds a single scrape attempt of one target.
	Timeout time.Duration
	// Retries is the number of retries per target on top of the first
	// attempt. Only connection errors, timeouts and 5xx responses are
	// retried.
	Retries int
	// OutDir is the directory the results, manifest.json and summary.json
	// are written to. It is created if it does not exist.
	OutDir string
	// Transport is used for the HTTP requests, nil means the default
	// transport.
	Transport http.RoundTripper
	// EscapingScheme is added as the 'escaping' parameter of the Accept
	// header, mirroring the single-target --escaping flag.
	EscapingScheme string
}

func (o *Options) withDefaults() {
	if o.Concurrency <= 0 {
		o.Concurrency = DefaultConcurrency
	}
	if o.Timeout <= 0 {
		o.Timeout = DefaultTimeout
	}
	if o.Retries < 0 {
		o.Retries = 0
	}
	if o.OutDir == "" {
		o.OutDir = DefaultOutDir
	}
}

// ManifestEntry describes the outcome of one target in manifest.json.
type ManifestEntry struct {
	Name     string `json:"name"`
	URL      string `json:"url"`
	Status   string `json:"status"`
	File     string `json:"file,omitempty"`
	Families int    `json:"families,omitempty"`
	Metrics  int    `json:"metrics,omitempty"`
	Attempts int    `json:"attempts"`
	Error    string `json:"error,omitempty"`
}

const (
	StatusOK     = "ok"
	StatusFailed = "failed"
)

// FamilyStat counts in how many successfully scraped targets a metric
// family appeared.
type FamilyStat struct {
	Name    string `json:"name"`
	Targets int    `json:"targets"`
}

// Summary is the aggregated view of a batch run, written to summary.json.
type Summary struct {
	TargetsTotal  int          `json:"targets_total"`
	TargetsOK     int          `json:"targets_ok"`
	TargetsFailed int          `json:"targets_failed"`
	MetricsTotal  int          `json:"metrics_total"`
	Families      []FamilyStat `json:"families"`
}

// Run scrapes all targets concurrently and writes one <name>.json file per
// successful target plus manifest.json and summary.json into opts.OutDir.
// Nothing is written for failed targets. It returns true if every target
// was scraped successfully.
func Run(ctx context.Context, targets []Target, opts Options) (bool, error) {
	opts.withDefaults()
	client := &http.Client{Transport: opts.Transport}

	results := make([]targetResult, len(targets))
	sem := make(chan struct{}, opts.Concurrency)
	var wg sync.WaitGroup
	for i, t := range targets {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			results[i] = scrapeTarget(ctx, client, t, opts)
		}()
	}
	wg.Wait()

	if err := os.MkdirAll(opts.OutDir, 0o777); err != nil {
		return false, fmt.Errorf("creating output directory: %w", err)
	}

	entries := make([]ManifestEntry, 0, len(results))
	summary := Summary{TargetsTotal: len(results)}
	familyTargets := map[string]map[string]struct{}{}
	for _, res := range results {
		entry := ManifestEntry{
			Name:     res.target.Name,
			URL:      res.target.URL,
			Attempts: res.attempts,
		}
		if res.err != nil {
			entry.Status = StatusFailed
			entry.Error = res.err.Error()
			summary.TargetsFailed++
		} else {
			entry.Status = StatusOK
			entry.File = res.target.Name + ".json"
			entry.Families = len(res.families)
			entry.Metrics = res.metrics
			summary.TargetsOK++
			summary.MetricsTotal += res.metrics
			if err := os.WriteFile(filepath.Join(opts.OutDir, entry.File), res.data, 0o666); err != nil {
				return false, fmt.Errorf("writing result for target %q: %w", res.target.Name, err)
			}
			for _, family := range res.families {
				if familyTargets[family] == nil {
					familyTargets[family] = map[string]struct{}{}
				}
				familyTargets[family][res.target.Name] = struct{}{}
			}
		}
		entries = append(entries, entry)
	}

	sort.Slice(entries, func(i, j int) bool { return entries[i].Name < entries[j].Name })
	summary.Families = make([]FamilyStat, 0, len(familyTargets))
	for name, targets := range familyTargets {
		summary.Families = append(summary.Families, FamilyStat{Name: name, Targets: len(targets)})
	}
	sort.Slice(summary.Families, func(i, j int) bool { return summary.Families[i].Name < summary.Families[j].Name })

	if err := writeJSON(filepath.Join(opts.OutDir, "manifest.json"), entries); err != nil {
		return false, err
	}
	if err := writeJSON(filepath.Join(opts.OutDir, "summary.json"), summary); err != nil {
		return false, err
	}
	return summary.TargetsFailed == 0, nil
}

func writeJSON(path string, v any) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return fmt.Errorf("marshaling %s: %w", path, err)
	}
	if err := os.WriteFile(path, append(data, '\n'), 0o666); err != nil {
		return fmt.Errorf("writing %s: %w", path, err)
	}
	return nil
}
