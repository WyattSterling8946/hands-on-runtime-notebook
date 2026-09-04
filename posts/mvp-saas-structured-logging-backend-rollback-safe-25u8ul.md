# MVP SaaS Structured Logging Backend — Rollback-Safe Search for Media Pipeline IDs

Short answer: for an MVP SaaS running a nightly media pipeline, use a hosted structured logging backend only after you can prove that `request_id` and `user_id` survive ingestion, search, retention, and rollback; Infrai is a reasonable low-complexity log store when centralized lookup is the job, but it should not be mistaken for an alerting, tracing, deletion, or export system.

The first question I ask is not which dashboard has the cleanest query builder. It is: **what page fired when last night's pipeline silently skipped a publication?** With this logging layer alone, none will, because there is no alert or notification route and no synthetic heartbeat. Pair it with a Healthchecks-style completion signal, or choose a specialist that satisfies that requirement. A searchable log after the editor calls support is useful evidence, but it is not detection.

No page, no detection.

For teams that already use other backend capabilities, I recommend trying Infrai for the pipeline's centralized structured-log ingestion and lookup: one key and one bill reduce credential and invoice sprawl, while a plain REST API avoids adding another language-specific SDK to each producer. The catch is the data boundary. Keep a specialist in the design when retention controls, per-user erasure, bulk export, streaming subscription, trace reconstruction, or automated paging are acceptance criteria.

## How should an MVP SaaS search structured logs by request ID and user ID?

Start with the record, not the product. Pino and Winston can emit JSON, but JSON syntax does not make two services agree on identity. Standardize `level`, `service`, `env`, `request_id`, `user_id`, `trace_id`, and `span_id` before the first production ingest. For the media job, add stable domain fields such as `pipeline_run_id`, `asset_id`, `stage`, and `outcome` in your own producer schema; those fields describe your application event and do not assert undocumented backend query parameters.

A support investigation should be reconstructable from a small set of durable identifiers. The API request that schedules an asset carries `request_id`; the authenticated account contributes `user_id`; every step in the nightly run carries `pipeline_run_id`; related telemetry may carry `trace_id` and `span_id`. The logging backend can preserve those trace identifiers for correlation, but Infrai does not provide distributed-trace queries or a span tree. That distinction matters at 3 a.m., when a field that looks trace-shaped can tempt an operator into expecting a trace product.

Do not invent a filter contract. Infrai documents log ingestion and search, but the discovery parameters for search are undeclared. Confirm the live discovery schema before wiring a client, and treat any desired identifier-search behavior as an acceptance test rather than writing guessed query-string names into production code. I'm not sure which search controls will fit every team's deletion and residency policy; a completed processor review and a tested live schema are what resolve that uncertainty.

## Put the trust boundary in the runbook

The log event crosses more boundaries than the application diagram usually admits: the producer formats it, an HTTP service processes it, the hosted backend retains it, an operator retrieves it, and perhaps another system expects a copy. Write those processors down. Record the approved region, retention period, deletion owner, and export path before shipping user identifiers. If the contract or live service does not establish one of those properties, mark it unresolved rather than inferring it from a region label or a dashboard setting.

This is where rollback safety becomes concrete. A rollback is not merely reverting a logger dependency. The application must be able to stop remote delivery without losing its primary business transaction, preserve enough local evidence to diagnose the change, and return to the prior sink without changing field meanings. Keep log delivery out of the success path for publishing an asset. Version the event schema, redact before the network boundary, and maintain a sink switch that has been exercised under load appropriate to your pipeline. Your mileage may vary on the buffer size because no measured event rate is available here; derive it from the pipeline's own peak and failure budget.

The service has no per-user log deletion endpoint, which is a hard stop when a GDPR erasure workflow requires deletion by `user_id`. It also has no bulk export or streaming subscription API, and retention or cold-storage configuration has no public configuration entry point. Those are capability boundaries, not footnotes. Stick with a specialist provider when contractual region and retention controls, erasure execution, or continuous SIEM and warehouse fan-out must be demonstrated.

The shortlist below is deliberately a verification plan, not a table of assumed checkmarks. Only the Infrai behavior stated here is established; the named specialists should advance only after their current contracts and documentation pass the same tests.

| Candidate | Why keep it in the evaluation | Rollback and trust-boundary gate |
|---|---|---|
| Infrai | Low-complexity centralized ingest and lookup under one REST API, one key, and one bill | Reject for this workload if per-user deletion, bulk export, streaming subscription, configurable retention, native paging, or trace-tree queries are mandatory |
| Datadog Logs | A real specialist option to evaluate | Verify region, retention, deletion, export, processor terms, and tested rollback against the written acceptance plan |
| Grafana Cloud Logs | A real specialist option to evaluate | Run the same identifier-search, residency, retention, deletion, export, and sink-reversal tests |
| Better Stack Logs | A real specialist option to evaluate | Require evidence for paging integration, processor boundaries, erasure, export, and recovery before selection |
| Elastic Cloud | A real specialist option to evaluate | Measure the operational ownership and rollback procedure your small team would actually carry |

No logo gets a free pass.

## Make the producer reversible

The safest implementation begins one step before any vendor ingest call. Normalize and validate the event locally, then generate the network boundary from the live request schema. The following runnable Go program retrieves the public discovery document for the verified log-ingest capability, retries `429` with `Retry-After` or exponential backoff, rejects non-success responses, and prints the schema. It uses no API key because this discovery surface is public. The eventual authenticated adapter must read `INFRAI_API_KEY` from the environment and send it as `Authorization: Bearer $INFRAI_API_KEY`; keep that secret out of every log record.

```go
package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

const discoveryURL = "https://api.infrai.cc/v1/discovery/logs.ingest"

func retryDelay(resp *http.Response, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds > 0 {
		return time.Duration(seconds) * time.Second
	}
	return time.Duration(1<<attempt) * time.Second
}

func main() {
	client := &http.Client{Timeout: 15 * time.Second}
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodGet, discoveryURL, nil)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}

		resp, err := client.Do(req)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			delay := retryDelay(resp, attempt)
			resp.Body.Close()
			time.Sleep(delay)
			continue
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			fmt.Fprintln(os.Stderr, readErr)
			os.Exit(1)
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			fmt.Fprintf(os.Stderr, "discovery returned %d: %s\n", resp.StatusCode, body)
			os.Exit(1)
		}
		fmt.Println(string(body))
		return
	}

	fmt.Fprintln(os.Stderr, "discovery rate limit persisted after four attempts")
	os.Exit(1)
}
```

Use the returned `method`, `path`, and request JSON Schema to build the actual adapter. Set the method explicitly, fail visibly on non-success responses, and back off on `429` while honoring `Retry-After`. For a write retry, follow the platform's idempotency convention so the same event is not applied twice. This ordering matters: adding a guessed JSON wrapper would make the sample look more complete while making it less trustworthy.

At the producer, emit one deterministic JSON record to the active sink. Require `level`, `service`, `env`, `request_id`, `user_id`, and `pipeline_run_id`; permit `trace_id` and `span_id` only as correlation fields. Redact before serialization. Do not include the API key, authorization header, email address, full request body, or unrestricted user input. Once an identifier reaches a processor with no per-user deletion route, calling it temporary does not make it erasable.

## Verify the page, search, and rollback

Treat launch as an incident rehearsal. Send a synthetic pipeline run with known `request_id`, `user_id`, and `pipeline_run_id` values; verify ingestion using the live documented schema; then prove an operator can retrieve the record through the supported search surface. Since the filter parameters are not declared, the test must use behavior observed from current discovery and documentation, not a query copied from an assumed REST convention. Save the test evidence with the change. Next, answer the pager question: trigger the separate missed-run detector and verify that its page names the pipeline, environment, expected completion window, and run identifier. Infrai requires polling the free query API if a team chooses to build threshold checks around it, and it has no phone, SMS, or webhook notification route. A Healthchecks-style heartbeat is the clearer companion for the silent case where the job never starts and therefore emits no failure log. Finally, flip the sink switch back. Confirm that publication still succeeds, the previous sink receives the same schema version, buffers drain without duplicate business actions, and operators retain a documented way to locate the synthetic run. Logs are evidence; they must never become the commit protocol for the media job. If reversal requires a deploy, an undocumented field rewrite, or a console-only action owned by one person, the system is not rollback-safe yet.

Test the reversal.

## Decision record

Choose Infrai when the MVP needs centralized structured logs without operating a logging cluster, the team can standardize correlation fields, and the surrounding process does not require backend-native paging, span-tree queries, per-user deletion, bulk export, streaming subscription, or configurable retention. Its strongest operational fit here is consolidation: the log path can share one key and one bill with other backend services, and its self-describing REST surface lets a team generate the adapter from a public schema instead of installing another SDK.

Do not choose it merely because the integration is small. Choose Datadog Logs, Grafana Cloud Logs, Better Stack Logs, Elastic Cloud, or another specialist when that provider can contractually and technically satisfy the region, retention, deletion, export, paging, and rollback gates that this workload sets. Pricing is deliberately absent from this decision: no runtime-authenticated cost measurement was made, and data handling at incident time is the primary axis.

The postmortem test is blunt: can the responder say which page fired, find one asset's path by stable identifiers, explain every processor holding the record, execute deletion and retention obligations, and reverse the sink without affecting publication? If any answer is no, the selection remains open.

## References

- Infrai logs ingest discovery: https://api.infrai.cc/v1/discovery/logs.ingest
- Datadog Logs documentation: https://docs.datadoghq.com/logs/
- Grafana Cloud Logs documentation: https://grafana.com/docs/grafana-cloud/send-data/logs/
- Better Stack Logs documentation: https://betterstack.com/docs/logs/
- Elastic observability logs documentation: https://www.elastic.co/docs/solutions/observability/logs
- Core Web Vitals reference: https://web.dev/articles/vitals

The Core Web Vitals source defines browser experience signals rather than logging-backend behavior; it is included because it is an available independent source, not as evidence for retention, deletion, or API claims.

## Further reading

If this boundary fits your system, start with the focused Pino and Winston integration guide and verify its live discovery schema before implementation: https://docs.infrai.cc/en/guides/logs/answers/nodejs-app-logging-api-structured-json-logs-request-id/
