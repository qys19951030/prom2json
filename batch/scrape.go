package batch

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	dto "github.com/prometheus/client_model/go"

	"github.com/prometheus/prom2json"
)

// Accept headers duplicated from the prom2json package (they are not
// exported there) so that batch scrapes negotiate content exactly like the
// single-target mode does.
const (
	acceptHeader         = `application/vnd.google.protobuf;proto=io.prometheus.client.MetricFamily;encoding=delimited;q=0.7,text/plain;version=1.0.0;q=0.2,text/plain;version=0.0.4;q=0.1`
	acceptHeaderTemplate = `application/vnd.google.protobuf;proto=io.prometheus.client.MetricFamily;encoding=delimited;escaping=%[1]s;q=0.7,text/plain;version=1.0.0;escaping=%[1]s;q=0.2,text/plain;version=0.0.4;escaping=%[1]s;q=0.1`
)

// targetResult holds the outcome of scraping a single target, including all
// retry attempts.
type targetResult struct {
	target   Target
	data     []byte // JSON document to persist, identical to single-target output.
	families []string
	metrics  int
	attempts int
	err      error
}

// scrapeTarget fetches one target, retrying failures that are worth
// retrying. retries is the number of retries on top of the first attempt.
func scrapeTarget(ctx context.Context, client *http.Client, t Target, opts Options) targetResult {
	res := targetResult{target: t}
	for attempt := 0; ; attempt++ {
		res.attempts++
		data, families, metrics, retryable, err := fetchOnce(ctx, client, t.URL, opts)
		if err == nil {
			res.data = data
			res.families = families
			res.metrics = metrics
			res.err = nil
			return res
		}
		res.err = err
		if !retryable || attempt >= opts.Retries {
			return res
		}
	}
}

// fetchOnce performs a single HTTP request against url and converts the
// response into the JSON document prom2json would print in single-target
// mode. The returned flag retryable reports whether another attempt is
// worthwhile: connection errors, timeouts and 5xx responses are retried,
// while 4xx responses and unparsable bodies are not.
func fetchOnce(ctx context.Context, client *http.Client, url string, opts Options) (data []byte, families []string, metrics int, retryable bool, err error) {
	reqCtx, cancel := context.WithTimeout(ctx, opts.Timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, url, nil)
	if err != nil {
		return nil, nil, 0, false, fmt.Errorf("creating GET request for URL %q failed: %w", url, err)
	}
	if opts.EscapingScheme != "" {
		req.Header.Add("Accept", fmt.Sprintf(acceptHeaderTemplate, opts.EscapingScheme))
	} else {
		req.Header.Add("Accept", acceptHeader)
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, nil, 0, true, fmt.Errorf("executing GET request for URL %q failed: %w", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, nil, 0, resp.StatusCode >= 500,
			fmt.Errorf("GET request for URL %q returned HTTP status %s", url, resp.Status)
	}

	mfChan := make(chan *dto.MetricFamily, 1024)
	parseErr := make(chan error, 1)
	go func() {
		parseErr <- prom2json.ParseResponse(resp, mfChan)
	}()
	var mfs []*dto.MetricFamily
	for mf := range mfChan {
		mfs = append(mfs, mf)
	}
	if err := <-parseErr; err != nil {
		if reqCtx.Err() != nil {
			// The per-attempt deadline hit while reading the body.
			return nil, nil, 0, true, fmt.Errorf("scraping URL %q timed out: %w", url, reqCtx.Err())
		}
		return nil, nil, 0, false, fmt.Errorf("parsing response from URL %q failed: %w", url, err)
	}

	result := make([]*prom2json.Family, 0, len(mfs))
	families = make([]string, 0, len(mfs))
	for _, mf := range mfs {
		result = append(result, prom2json.NewFamily(mf))
		families = append(families, mf.GetName())
		metrics += len(mf.Metric)
	}
	data, err = json.Marshal(result)
	if err != nil {
		return nil, nil, 0, false, fmt.Errorf("marshaling metrics from URL %q as JSON failed: %w", url, err)
	}
	return append(data, '\n'), families, metrics, false, nil
}
