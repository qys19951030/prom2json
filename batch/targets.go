package batch

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"regexp"
)

// Target describes a single metrics endpoint to scrape.
type Target struct {
	Name string `json:"name"`
	URL  string `json:"url"`
}

// namePattern restricts target names so that they are safe to use as output
// file names and easy to reference in downstream tooling.
var namePattern = regexp.MustCompile(`^[a-z0-9_-]+$`)

// ValidationError describes a problem with a single entry of the targets
// file. Index is the zero-based position of the entry in the JSON array and
// Field is the offending field ("name" or "url").
type ValidationError struct {
	Index int
	Field string
	Err   error
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("targets file: entry at index %d: field %q: %v", e.Index, e.Field, e.Err)
}

func (e *ValidationError) Unwrap() error {
	return e.Err
}

// LoadTargets reads and fully validates the targets file at path. Every entry
// is checked before any scraping happens: names must be unique and match
// namePattern, and URLs must use the http or https scheme.
func LoadTargets(path string) ([]Target, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading targets file: %w", err)
	}
	var targets []Target
	if err := json.Unmarshal(raw, &targets); err != nil {
		return nil, fmt.Errorf("parsing targets file %s: %w", path, err)
	}
	seen := map[string]int{}
	for i, t := range targets {
		if t.Name == "" {
			return nil, &ValidationError{Index: i, Field: "name", Err: fmt.Errorf("must not be empty")}
		}
		if !namePattern.MatchString(t.Name) {
			return nil, &ValidationError{Index: i, Field: "name", Err: fmt.Errorf("%q contains characters outside [a-z0-9_-]", t.Name)}
		}
		if first, dup := seen[t.Name]; dup {
			return nil, &ValidationError{Index: i, Field: "name", Err: fmt.Errorf("%q duplicates entry at index %d", t.Name, first)}
		}
		seen[t.Name] = i
		if t.URL == "" {
			return nil, &ValidationError{Index: i, Field: "url", Err: fmt.Errorf("must not be empty")}
		}
		u, err := url.Parse(t.URL)
		if err != nil {
			return nil, &ValidationError{Index: i, Field: "url", Err: fmt.Errorf("%q cannot be parsed: %w", t.URL, err)}
		}
		if u.Scheme != "http" && u.Scheme != "https" {
			return nil, &ValidationError{Index: i, Field: "url", Err: fmt.Errorf("%q does not use the http or https scheme", t.URL)}
		}
	}
	return targets, nil
}
