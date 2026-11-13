// Copyright 2014 Prometheus Team
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/alecthomas/kingpin/v2"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/version"

	"github.com/prometheus/prom2json"
	"github.com/prometheus/prom2json/batch"
)

var usage = `The path or URL to metrics to convert, if omitted, defaults to read from STDIN.

Examples:

	$ prom2json http://my-prometheus-server:9000/metrics

	$ curl http://my-prometheus-server:9000/metrics | prom2json
	
`

func main() {
	cert := kingpin.Flag("cert", "client certificate file").PlaceHolder("FILE").String()
	key := kingpin.Flag("key", "client certificate's key file").PlaceHolder("FILE").String()
	skipServerCertCheck := kingpin.Flag("accept-invalid-cert", "Accept any certificate during TLS handshake. Insecure, use only for testing.").Bool()
	escapingScheme := kingpin.Flag("escaping", "Sets an escaping scheme in content negotiation. Use 'allow-utf-8' for full UTF-8 character support.").
		PlaceHolder("SCHEME").
		Enum(
			"allow-utf-8",
			"underscores",
			"dots",
			"values",
		)
	targetsFile := kingpin.Flag("targets", "Path to a JSON file listing targets to scrape in batch mode. Mutually exclusive with the METRICS_PATH | METRICS_URL argument.").PlaceHolder("FILE").String()
	concurrency := kingpin.Flag("concurrency", "Maximum number of targets scraped concurrently in batch mode.").Default("4").Int()
	timeout := kingpin.Flag("timeout", "Timeout for a single scrape attempt of one target in batch mode.").Default("30s").Duration()
	retries := kingpin.Flag("retries", "Number of retries per target in batch mode. Only connection errors, timeouts and 5xx responses are retried.").Default("1").Int()
	outDir := kingpin.Flag("out-dir", "Directory the batch mode writes results, manifest.json and summary.json to.").Default("out").PlaceHolder("DIR").String()

	kingpin.CommandLine.UsageWriter(os.Stderr)
	kingpin.Version(version.Print("prom2json"))
	kingpin.HelpFlag.Short('h')

	var input io.Reader
	var err error
	arg := kingpin.Arg("METRICS_PATH | METRICS_URL", usage).String()

	kingpin.Parse()

	if *targetsFile != "" {
		if *arg != "" {
			fmt.Fprintln(os.Stderr, "error: --targets and the METRICS_PATH | METRICS_URL argument are mutually exclusive")
			os.Exit(2)
		}
		os.Exit(runBatchMode(*targetsFile, *outDir, *concurrency, *timeout, *retries, *cert, *key, *skipServerCertCheck, *escapingScheme))
	}

	if *arg == "" {
		// Use stdin on empty argument.
		input = os.Stdin
	} else if url, urlErr := url.Parse(*arg); urlErr != nil || url.Scheme == "" {
		// `url, err := url.Parse("/some/path.txt")` results in: `err == nil && url.Scheme == ""`
		// Open file since arg appears not to be a valid URL (parsing error occurred or the scheme is missing).
		if input, err = os.Open(*arg); err != nil {
			fmt.Fprintln(os.Stderr, "error opening file:", err)
			os.Exit(1)
		}
	} else {
		// Validate Client SSL arguments since arg appears to be a valid URL.
		if (*cert != "" && *key == "") || (*cert == "" && *key != "") {
			fmt.Fprintf(os.Stderr, "%s\n with TLS client authentication: %s --cert /path/to/certificate --key /path/to/key METRICS_URL", usage, os.Args[0])
			os.Exit(1)
		}
	}

	mfChan := make(chan *dto.MetricFamily, 1024)
	// Missing input means we are reading from an URL.
	if input != nil {
		go func() {
			if err := prom2json.ParseReader(input, mfChan); err != nil {
				fmt.Fprintln(os.Stderr, "error reading metrics:", err)
				os.Exit(1)
			}
		}()
	} else {
		transport, err := makeTransport(*cert, *key, *skipServerCertCheck)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		go func() {
			if err := prom2json.FetchMetricFamiliesWithEscapingScheme(*arg, mfChan, transport, *escapingScheme); err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
		}()
	}

	result := []*prom2json.Family{}
	for mf := range mfChan {
		result = append(result, prom2json.NewFamily(mf))
	}
	jsonText, err := json.Marshal(result)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error marshaling JSON:", err)
		os.Exit(1)
	}
	if _, err := os.Stdout.Write(jsonText); err != nil {
		fmt.Fprintln(os.Stderr, "error writing to stdout:", err)
		os.Exit(1)
	}
	fmt.Println()
}

// runBatchMode executes a batch run over the targets listed in targetsFile
// and returns the process exit code: 0 if all targets were scraped
// successfully, 1 if any target failed or the results could not be written,
// and 2 if the targets file is invalid.
func runBatchMode(
	targetsFile string, outDir string, concurrency int, timeout time.Duration, retries int,
	cert string, key string, skipServerCertCheck bool, escapingScheme string,
) int {
	if (cert != "" && key == "") || (cert == "" && key != "") {
		fmt.Fprintf(os.Stderr, "%s\n with TLS client authentication: %s --cert /path/to/certificate --key /path/to/key --targets TARGETS_FILE", usage, os.Args[0])
		return 1
	}
	targets, err := batch.LoadTargets(targetsFile)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		return 2
	}
	transport, err := makeTransport(cert, key, skipServerCertCheck)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	ok, err := batch.Run(context.Background(), targets, batch.Options{
		Concurrency:    concurrency,
		Timeout:        timeout,
		Retries:        retries,
		OutDir:         outDir,
		Transport:      transport,
		EscapingScheme: escapingScheme,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		return 1
	}
	if !ok {
		fmt.Fprintln(os.Stderr, "error: at least one target failed, see manifest.json for details")
		return 1
	}
	return 0
}

func makeTransport(
	certificate string, key string,
	skipServerCertCheck bool,
) (*http.Transport, error) {
	// Start with the DefaultTransport for sane defaults.
	transport := http.DefaultTransport.(*http.Transport).Clone()
	// Conservatively disable HTTP keep-alives as this program will only
	// ever need a single HTTP request.
	transport.DisableKeepAlives = true
	// Timeout early if the server doesn't even return the headers.
	transport.ResponseHeaderTimeout = time.Minute
	tlsConfig := &tls.Config{InsecureSkipVerify: skipServerCertCheck}
	if certificate != "" && key != "" {
		cert, err := tls.LoadX509KeyPair(certificate, key)
		if err != nil {
			return nil, err
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}
	transport.TLSClientConfig = tlsConfig
	return transport, nil
}
