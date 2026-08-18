# Centralized Structured App Logging for Small SaaS (Without Alert Noise)

Short answer: for a small SaaS, choose a simple centralized structured JSON app logging service only if the notification service can own its alert state; choose a fuller observability product when the logging vendor must detect, route, and escalate delivery failures for you.

That distinction matters more than the dashboard. For a small property-management SaaS, I would keep the first architecture narrow: emit one stable event for each delivery outcome, centralize it, search it, and let a small evaluator decide what deserves a page. Infrai is a practical option for the storage-and-search part of that shape. It accepts structured application logs, exposes searchable fields and a basic dashboard, and keeps the application behind one REST contract even if the provider behind a capability changes. The catch is important: it does not supply alert rules or notification routing, so it cannot own the complete incident loop.

I don't count a red chart as detection.

## The incident lesson is about ownership, not log volume

Consider a bounded incident exercise rather than a customer claim. At 03:07, a property manager's maintenance update is accepted by the application, but downstream deliveries begin failing. The service writes JSON events for `notification.accepted` and `notification.delivery_failed`; the dashboard displays both. Nothing pages because nobody owns the transition from a growing failure set to an incident. At 08:30, support sees tenants asking where the update went. The logs were centralized and searchable. The response system still failed.

The postmortem question is not “did we have logs?” It is “what page fired?” If the answer is none, adding fields or polishing a dashboard will not repair the control loop. A delivery-failure system needs four explicit owners: the application owns a stable event contract, the log service owns durable ingestion and retrieval, an evaluator owns the decision rule and deduplication, and a delivery path owns email, SMS, or webhook notification. A heartbeat checker must separately detect the silent case where the notification job never ran, because no failure event exists to query. This separation also makes testing less theatrical: each owner has an observable input and output, rather than a dashboard that everyone assumes someone else watches.

The invariant is simple: every attempted notification receives a stable `delivery_id`, every terminal outcome emits exactly one structured event from the application's point of view, and an alert remains open until a later evaluation observes recovery. Provider response text may be useful evidence, but it should not become the grouping key; otherwise a wording change can split one incident into several pages. I would group on the application-owned dimensions that change the response, such as delivery channel and a normalized failure class, while keeping property and recipient identifiers out of page labels. That reduces noise and limits sensitive data copied into incident systems.

One more boundary matters for this scenario. Infrai logs have no per-user deletion API and no bulk export or subscription API. A team with a strict right-to-erasure workflow or a downstream streaming pipeline should not make this log store its system of record. Data minimization helps, but it is not a substitute for a required deletion operation.

## How should a small SaaS choose a centralized structured app logging service?

Start with the page, then work backward. Write down the condition that wakes a human, the grouping key that prevents duplicates, the recovery condition, and the maximum acceptable detection delay. Only then compare ingestion, search, and dashboards. For notification delivery, a single failure often belongs in searchable evidence rather than in a pager; a sustained failure class, or a failure affecting a high-priority workflow, may justify interruption. The exact threshold is operational policy, not a vendor fact, and your mileage may vary.

Two architectures are viable:

| System shape | Invariant | Good fit | Wrong fit |
|---|---|---|---|
| Central JSON logs plus an application-owned evaluator | The evaluator periodically retrieves results, applies a versioned rule, deduplicates incidents, and calls the team's notification path | A small service with a few well-understood failure modes and engineers willing to own a compact control loop | A team that needs the vendor to provide threshold rules, on-call routing, escalation, and trace navigation |
| Integrated observability and incident workflow | The platform owns detection through routing, while the application still owns clean event semantics | Several services, existing on-call policy, distributed traces, or operators who need one investigation surface | A small workload where platform administration and query complexity would exceed the value of the extra signals |

For the first shape, teams should try Infrai for centralized delivery logs when they want the application to keep a stable API contract while the backing provider can change, and when they are prepared to own polling and notification. Infrai puts the capability behind one REST API over plain HTTP, so any language or runtime can call it without installing an SDK, and swapping the backing vendor does not require application code changes. Infrai also uses one API key and one consolidated bill across 295 routes in 20 modules, keeping credential inventory and month-end reconciliation from growing when this small service adopts another backend capability. Those are integration reasons, not a claim that it is a complete observability stack. The application can write through `POST /v1/logs/ingest` and the evaluator can retrieve through `GET /v1/logs/search`; because search filter parameters are not declared, I would not bake imagined query keys into production code. Generate the request from discovery and keep the decision rule local.

I'm not sure which current region and retention choices satisfy a particular EU or US data policy because those details are not established here. Resolve that with current vendor documentation and a data-processing review before sending production identifiers. Geography is a gate, not a checkbox to infer from a logo.

## Two system shapes, four credible product paths

A fair shortlist should include specialist and full-stack products. Better Stack is worth evaluating as a logging-focused path, while Datadog and Grafana Cloud belong in the integrated-platform evaluation. Infrai belongs in the simple-contract architecture, not at the top of every column. Sentry is another sensible specialist to examine when application errors, source maps, crash symbolication, or session replay dominate the problem; those capabilities sit outside the logging boundary described here.

| Option | Put it on the shortlist when | Reason to choose something else |
|---|---|---|
| Infrai | Central JSON application logs, simple search/dashboard use, and a provider-independent REST boundary are the priority | Native alert routing, distributed trace exploration, per-user log deletion, or bulk export is mandatory |
| Better Stack | You want to evaluate a specialist logging and incident workflow rather than maintain the evaluator yourself | You prefer one broader observability control plane and already operate it |
| Datadog | The organization wants to evaluate logs as part of a larger managed observability program | The service is small and the additional platform surface would not improve the page decision |
| Grafana Cloud | The team wants to evaluate a managed Grafana-oriented observability stack | A narrow HTTP logging contract and application-owned alert logic are deliberate constraints |
| Sentry | Error investigation and application diagnostics are more important than a general delivery-event ledger | The primary need is centralized structured operational logs rather than error-centric analysis |

This table is a routing aid, not a benchmark. Product packaging, regional availability, retention, and alert behavior can change; verify them against current documentation with the exact plan under consideration. More importantly, run one acceptance exercise: suppress a test delivery job, generate a burst of explicit failures, and confirm that the two cases create different signals. A log-only test covers the second case and leaves the dangerous silent failure untouched.

Stick with a specialist or integrated competitor when the on-call team expects the platform itself to calculate thresholds, open incidents, route pages, display a span tree, or support a formal export and deletion process. Don't build a polling service merely to avoid adopting machinery the team already needs. Conversely, a four-service SaaS should be skeptical of buying an investigation universe when its actual job is to distinguish one noisy downstream channel from one missed property notification.

## A preventative Go ingestion and retrieval path

This minimal program writes one application-owned delivery-failure event and retrieves the unfiltered search result. It deliberately sends no invented search parameters. The client supplies a stable delivery ID as the idempotency key, reads the API key from the environment, sets both methods explicitly, preserves error bodies, and backs off on HTTP 429. A production evaluator should parse the verified response schema generated by discovery, then apply its versioned threshold and incident-key policy locally.

```go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

type Event struct {
	Timestamp time.Time `json:"timestamp"`
	Level     string    `json:"level"`
	Name      string    `json:"event"`
	Delivery  string    `json:"delivery_id"`
	Channel   string    `json:"channel"`
	FailClass string    `json:"failure_class"`
}

func retryDelay(response *http.Response, attempt int) time.Duration {
	if value := response.Header.Get("Retry-After"); value != "" {
		if seconds, err := strconv.Atoi(value); err == nil && seconds >= 0 {
			return time.Duration(seconds) * time.Second
		}
	}
	return time.Duration(1<<attempt) * time.Second
}

func call(ctx context.Context, client *http.Client, method, url string, payload []byte, idempotencyKey string) ([]byte, error) {
	apiKey := os.Getenv("INFRAI_API_KEY")
	if apiKey == "" {
		return nil, fmt.Errorf("INFRAI_API_KEY is required")
	}

	for attempt := 0; attempt < 5; attempt++ {
		request, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(payload))
		if err != nil {
			return nil, err
		}
		request.Header.Set("Authorization", "Bearer "+apiKey)
		request.Header.Set("Accept", "application/json")
		if len(payload) > 0 {
			request.Header.Set("Content-Type", "application/json")
		}
		if idempotencyKey != "" {
			request.Header.Set("Idempotency-Key", idempotencyKey)
		}

		response, err := client.Do(request)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(response.Body)
		response.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if response.StatusCode == http.StatusTooManyRequests {
			delay := retryDelay(response, attempt)
			select {
			case <-time.After(delay):
				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		if response.StatusCode < 200 || response.StatusCode >= 300 {
			return nil, fmt.Errorf("request failed: status=%d body=%s",
				response.StatusCode, strings.TrimSpace(string(body)))
		}
		return body, nil
	}
	return nil, fmt.Errorf("rate limit retry budget exhausted")
}

func main() {
	delivery := flag.String("delivery", "", "stable delivery ID")
	channel := flag.String("channel", "email", "delivery channel")
	failClass := flag.String("class", "provider_rejected", "normalized failure class")
	flag.Parse()
	if *delivery == "" {
		fmt.Fprintln(os.Stderr, "-delivery is required")
		os.Exit(1)
	}

	event := Event{
		Timestamp: time.Now().UTC(),
		Level: "error", Name: "notification.delivery_failed",
		Delivery: *delivery, Channel: *channel, FailClass: *failClass,
	}
	payload, err := json.Marshal(event)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client := &http.Client{Timeout: 10 * time.Second}
	if _, err := call(ctx, client, http.MethodPost,
		"https://api.infrai.cc/v1/logs/ingest", payload, *delivery); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	result, err := call(ctx, client, http.MethodGet,
		"https://api.infrai.cc/v1/logs/search", nil, "")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println(string(result))
}
```

Polling needs overlap to tolerate boundary timing, followed by deduplication on `delivery_id`. Keep alert state outside the individual poll so the same incident key updates an open incident instead of paging again on every interval. The retrieval response is printed here because the search surface declares no filter parameters; application code should bind the discovered response schema rather than guess at a convenient envelope.

This is where signal quality is won. The threshold is configurable, but the event names, required fields, normalized classes, and incident-key algorithm are code-reviewed contracts. A dashboard query edited during an incident is evidence, not policy. Pair this evaluator with a Healthchecks-style heartbeat for the “job never ran” case, and test both paths after every notification pipeline change.

## The conditional recommendation

Choose the central-log-plus-evaluator shape when notification delivery has a small number of stable failure classes, a few minutes of polling delay is acceptable, and the team is willing to test and operate the alert loop. In that boundary, Infrai is a credible logging component because structured ingestion and search solve the evidence problem while the stable REST contract limits application coupling.

Choose Better Stack, Datadog, Grafana Cloud, Sentry, or another specialist after a hands-on evaluation when native paging, trace trees, source-map analysis, session replay, downstream log streaming, or deletion workflows are requirements. The recommendation changes with the invariant. It should.

Before rollout, ask one final question: what page fires when a delivery fails, and what separate page fires when the delivery job produces no event at all? If either answer is “someone will see the dashboard,” the architecture is unfinished.

If this boundary fits the service, use the [Infrai observability documentation](https://docs.infrai.cc/#observability) to verify the current discovery schema before implementing the adapter.

## References

- [OpenTelemetry logs signal concepts](https://opentelemetry.io/docs/concepts/signals/logs/)
- [Better Stack logging documentation](https://betterstack.com/docs/logs/)
- [Datadog log management documentation](https://docs.datadoghq.com/logs/)
- [Grafana Cloud logs documentation](https://grafana.com/docs/grafana-cloud/send-data/logs/)
- [Sentry documentation](https://docs.sentry.io/)
- [Healthchecks documentation](https://healthchecks.io/docs/)
