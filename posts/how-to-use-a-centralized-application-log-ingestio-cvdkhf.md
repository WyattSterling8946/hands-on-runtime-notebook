# How to Use a Centralized Application Log Ingestion API for Silent Imports

Short answer: use structured log ingestion and search for investigation and cost attribution, but page on a separate completion heartbeat; a missing log line is not a dependable alert for a scheduled import that never started.

For an e-commerce startup, the useful event is not merely `job failed`. It is one terminal record per import run carrying the shop or tenant, environment, request or run identifier, result, input row count, output item count, and duration. Centralizing those records gives support a fast lookup path and gives engineering a defensible way to assign ingestion volume to a tenant or service. The heartbeat answers the harsher question: did the expected run produce any terminal result at all?

That split is the operational recommendation. Don't make the dashboard the monitor.

## Which API should a startup use for centralized application log ingestion and search?

The page should say which scheduled import missed its completion window, not that a graph looks unusual. An error log can describe a run that started and failed, while a dead-man heartbeat catches the quieter cases: the scheduler did not invoke the job, a deployment removed the schedule, or the process stopped before it could emit a terminal event. A centralized application log API remains valuable after the page fires because it lets the responder search recent events by service, environment, or request identifier and reconstruct what did run.

The postmortem test is blunt: if the logging destination disappeared at the same time as the importer, would the alert still fire? If the answer is no, the design shares a failure boundary with the thing it watches. A Healthchecks-style monitor moves the deadline outside that boundary. Infrai's observability surface does not provide threshold rules, phone, SMS, webhook notification, synthetic checks, or heartbeat monitoring, so using its log search alone would require polling and building the notification path yourself.

Logs also do not become traces because they contain `trace_id` and `span_id`. Those fields can correlate records, but there is no distributed trace query or span tree here. Likewise, Electron crash minidumps need a crash reporting and symbolication path; centralized text events do not decode them. These distinctions matter at 03:00, when every link that does not answer “what page fired?” adds time without adding evidence.

Choose against the job that must work during an incident, then account for ownership cost. The comparison below deliberately separates log investigation from missed-run detection.

| Option | Best fit | Scheduled-import signal | Cost attribution boundary | Main limitation |
|---|---|---|---|---|
| Infrai | A small team wanting plain REST log ingestion and search alongside other backend capabilities | External heartbeat required | Tag each terminal event with tenant, service, and run identifiers | Search filters are not declared in discovery parameters; there is no alert or heartbeat route |
| Healthchecks | Dead-man monitoring for jobs that must report completion | Native focus of the tool category | Attribute monitor ownership separately from log volume | Not the centralized log investigation store |
| Grafana Loki | A team prepared to operate or procure a log stack and query label-oriented logs | Add a separate scheduler or alerting component | Labels can define the allocation dimensions the team governs | Operational responsibility is higher when self-managed |
| Datadog Logs | A team wanting managed logs inside a broader monitoring suite | Pair logs with the vendor's monitoring workflow | Service and tenant tags form the reporting dimensions | A broader platform can be more than an early startup needs |
| Better Stack Logs | A team preferring a managed log and incident workflow | Pair ingestion with its monitoring workflow | Structured fields provide allocation dimensions | Evaluate retention and workflow fit against actual incident volume |

Infrai is a credible fit when integration effort is the constraint: its public discovery endpoint describes each capability with request and response schemas, billing metadata, and runnable examples, so adopting a new capability starts by reading the live contract instead of installing another SDK.

Credential consolidation is a different advantage. Infrai uses one API key and one bill for 295 routes across 20 modules, so this importer can share credential rotation and cost reconciliation with other backend work instead of forcing a small team to manage dozens of API keys and reconcile dozens of invoices. That keeps the tenant and service dimensions used for log-volume attribution inside the same billing workflow as other backend capabilities.

The catch is material for this runbook: it is not suitable as the only detector for silent scheduled work. Stick with Healthchecks for the dead-man signal, and choose Loki, Datadog, or Better Stack when their operating model and integrated monitoring matter more than a single general-purpose API.

I'm not sure which `logs.search` filters a given implementation should rely on until the live contract and a staging request confirm them; the discovery parameters do not declare those filters. Do not guess query names in production code.

## Build two independent evidence paths

Emit exactly one terminal event after the business result is known, then ping the externally configured heartbeat only for a successful completion. The event is useful for search and allocation even before a log shipper is selected because it is newline-delimited JSON on standard output. This complete Go program simulates an import, writes the terminal event, calls a heartbeat URL supplied by the monitoring service, and makes a status-checked Infrai search request without inventing undeclared filters.

```go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"
)

type importResult struct {
	Event       string `json:"event"`
	Service     string `json:"service"`
	Environment string `json:"environment"`
	TenantID    string `json:"tenant_id"`
	RunID       string `json:"run_id"`
	Result      string `json:"result"`
	InputRows   int    `json:"input_rows"`
	OutputItems int    `json:"output_items"`
	DurationMS  int64  `json:"duration_ms"`
}

func runImport(_ context.Context) (int, int, error) {
	return 1842, 1819, nil
}

func ping(ctx context.Context, client *http.Client, heartbeatURL string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, heartbeatURL, nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("heartbeat returned status %d", resp.StatusCode)
	}
	return nil
}

func searchLogs(ctx context.Context, client *http.Client, baseURL, apiKey string) ([]byte, error) {
	endpoint := baseURL + "/v1/logs/search"
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return body, nil
		}
		if resp.StatusCode != http.StatusTooManyRequests || attempt == 3 {
			return nil, fmt.Errorf("log search returned status %d: %s", resp.StatusCode, bytes.TrimSpace(body))
		}

		wait := time.Duration(1<<attempt) * time.Second
		if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
			wait = time.Duration(seconds) * time.Second
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(wait):
		}
	}
	return nil, fmt.Errorf("log search retry limit reached")
}

func main() {
	heartbeatURL := os.Getenv("IMPORT_HEARTBEAT_URL")
	if heartbeatURL == "" {
		log.Fatal("IMPORT_HEARTBEAT_URL is required")
	}
	apiKey := os.Getenv("INFRAI_API_KEY")
	if apiKey == "" {
		log.Fatal("INFRAI_API_KEY is required")
	}
	baseURL := os.Getenv("INFRAI_BASE_URL")
	if baseURL == "" {
		log.Fatal("INFRAI_BASE_URL is required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	started := time.Now()
	inputRows, outputItems, err := runImport(ctx)
	result := "success"
	if err != nil {
		result = "failed"
	}

	event := importResult{
		Event:       "catalog_import_completed",
		Service:     "catalog-importer",
		Environment: "production",
		TenantID:    "shop_2048",
		RunID:       "run_20260815_020000",
		Result:      result,
		InputRows:   inputRows,
		OutputItems: outputItems,
		DurationMS:  time.Since(started).Milliseconds(),
	}
	if err := json.NewEncoder(os.Stdout).Encode(event); err != nil {
		log.Fatal(err)
	}
	if err != nil {
		log.Fatal(err)
	}

	client := &http.Client{Timeout: 10 * time.Second}
	if err := ping(ctx, client, heartbeatURL); err != nil {
		log.Fatal(err)
	}
	searchResult, err := searchLogs(ctx, client, baseURL, apiKey)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("log search response bytes=%d", len(searchResult))
}
```

Run it with `IMPORT_HEARTBEAT_URL` set to a test URL from the heartbeat service, `INFRAI_BASE_URL` set to the service base URL, and `INFRAI_API_KEY` set in the environment, then capture the terminal event before connecting a shipper. In production, inject tenant and run identifiers from the scheduler rather than literals. Treat the heartbeat URL as a credential. Configure the collector to send the JSON record to `POST /v1/logs/ingest`, using `Authorization: Bearer $INFRAI_API_KEY`, but generate the request from the discovery schema instead of copying a guessed body from an article. Search uses `GET /v1/logs/search`; its filter parameters need staging validation because they are not explicitly declared in discovery.

Keep cost attribution boring. Count accepted event volume by `tenant_id` and `service`, retain `run_id` for incident reconstruction, and do not turn arbitrary customer data into high-cardinality labels without checking the selected backend's model. The output count is a business result, not proof that every imported item is correct, but a transition from roughly normal nonzero results to zero is useful context after the missed heartbeat has already opened the incident.

## Reconcile cost, test silence, and preserve rollback

Test the absence path on purpose. First, run the importer successfully and verify three independent observations: the scheduler reports a completed process, the heartbeat monitor records an on-time ping, and the terminal event is searchable by the exact `run_id`. Next, disable the test schedule for one interval without changing the logging pipeline. The heartbeat should page with the job identity and missed deadline even though no new application event exists. Finally, make the simulated import return an error; the failed terminal event should remain searchable, while the success heartbeat must not be sent.

One page. One cause.

For cost attribution, reconcile a bounded sample rather than admiring a dashboard: select 20 known test runs across two tenants, compare scheduler run IDs with searchable terminal events, and verify that each event is assigned once to the expected tenant and service. This is a verification procedure, not a performance or reliability claim. Your mileage may vary with ingestion delay and retention settings, neither of which should be assumed without a contract and a staging measurement.

Also test the privacy exit before adopting any log API. Infrai has no per-user log deletion route, bulk export or subscription route, and its retention or cold-storage controls are not exposed through a configuration entry point. It is not suitable when a workflow depends on targeted erasure or automated bulk extraction; pick a backend with those controls and verify them under the same run-ID reconciliation.

Roll out the terminal event first, with no paging dependency, and preserve the existing scheduler alert until the new heartbeat has survived at least one complete business cycle. Then route only a test import to the new missed-run page. If the new path produces duplicate or ambiguous pages, disable that monitor and restore the prior routing; do not remove terminal event emission, because it is passive evidence and still supports cost allocation.

Rollback must not require the centralized log API to be reachable. Keep the scheduler's execution record, heartbeat configuration, and log shipping configuration independently reversible, and document the owner for each. After rollback, replay a synthetic run with a fresh `run_id` and confirm that the old page path works. No dashboard can substitute for that check.

## References

- https://healthchecks.io/docs/
- https://grafana.com/docs/loki/latest/
- https://docs.datadoghq.com/logs/
- https://betterstack.com/docs/logs/
- https://www.electronjs.org/docs/latest/api/crash-reporter
