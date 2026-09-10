# How to Build Cheap Centralized Logging: Self-Serve SaaS Cohort Search

Short answer: centralize structured events from Node.js Docker cron jobs, retain the cohort and experiment dimensions at ingestion, and page only on a sustained difference in user-visible outcomes; a searchable pile of strings is not an observability system.

For a small SaaS team, the useful first version is deliberately plain: each job writes one JSON event to standard output, a collector forwards it to one authenticated endpoint, and a bounded store supports exact filters for tenant cohort, experiment, job, status, and time. Keep raw logs for investigation, but derive the alert from completed-job outcomes. I've been woken by alerts that described a machine perfectly and the user impact not at all. The first question at 3 a.m. is still: what page fired?

This guide uses a tenant-cohort experiment as the concrete test. The decision is whether the treatment cohort is failing more often than control, not whether one container printed more lines. It also keeps the design cheap by controlling volume and retention rather than making a price claim that will age badly.

## How should a small SaaS centralize Node.js Docker cron job logs?

Start at the decision and work backward. Suppose a scheduled export runs for tenants in `control` and `treatment`. An operator needs to answer four questions without shell access: did the job finish, which cohort was affected, was the outcome visible to the tenant, and can the experiment be disabled independently of the job?

That requires an event contract, not a formatting preference. OpenTelemetry's logs data model separates the observed timestamp, severity, body, resource, and attributes [1]. A small implementation does not have to adopt every OpenTelemetry component on day one, but using the same categories avoids a dead end. Put stable deployment facts such as service and environment in resource fields; put query dimensions such as `tenant_cohort`, `experiment`, and `job` in attributes; keep the human explanation in the body.

Emit one completion event per attempt. Start events are useful for diagnosing stuck work, yet they cannot prove success, and counting every debug line turns a cohort comparison into a contest between noisy code paths. The completion event should carry a generated `event_id`, an `occurred_at` timestamp, `service`, `environment`, `job`, `tenant_cohort`, `experiment`, `outcome`, `duration_ms`, and a non-sensitive error class when applicable. Do not log access tokens, session identifiers, database connection strings, or sensitive personal data. OWASP's logging guidance explicitly calls out data that should usually be removed, masked, hashed, or encrypted [2].

Be strict about cardinality. `tenant_cohort=treatment` is useful; a raw tenant name in an indexed label is usually not. Retain a pseudonymous tenant identifier in the event only if incident access genuinely needs per-tenant drill-down, then enforce authorization around that field. The long paragraph here matters because this is where inexpensive systems quietly become expensive or unusable: if every request ID, tenant ID, URL, and error message becomes an indexed dimension, the index grows with values that operators rarely use, while an unstructured body forces them to scan everything. Choose a short allowlist of indexed fields, leave the rest in the stored event, and review query evidence before promoting another field.

No dashboard fixes a bad event.

## Define the signal before choosing storage

The experiment signal is a ratio over completed attempts, split by cohort. For each fixed window, calculate failed completions divided by all completions for control and treatment. Require a minimum sample count, compare equivalent job types, and suppress evaluation when telemetry is incomplete. I'm not sure what minimum count or difference is correct for your workload; historical traffic, business tolerance, and a replay against known incidents resolve that question. A threshold copied from another service does not.

The page should name the decision: `treatment export failures exceed control; disable experiment`. It should include the window, both numerators and denominators, the experiment identifier, and a link to a pre-filtered search. A page that says `log errors high` has already discarded the experiment context the responder needs.

Use three destinations with different purposes:

- Immediate search retains structured completion and error events for the period your on-call rotation commonly investigates.
- Derived counters retain cohort outcome totals longer than raw events, because trend comparison needs little storage.
- An optional archive holds compressed raw events when audit or delayed investigations require it; access should be narrower than ordinary search.

This split is the main cost control. Sampling routine success details can reduce volume, but never sample away the completion denominator independently from failures. Either retain every compact completion event or aggregate success and failure counts before sampling verbose diagnostic events. Otherwise the treatment cohort may look worse merely because its successes were discarded.

## Implement the smallest useful path

The following Go program is a runnable reference receiver and search endpoint built only with the standard library. It writes newline-delimited JSON to disk, accepts an explicit schema, redacts a small denylist defensively, and supports bounded exact-match queries. In production, place collection close to the containers so a brief network interruption does not make application work depend on the search service; buffer on disk, use TLS, rotate credentials, and set filesystem permissions appropriate to the data.

The code is intentionally one process. That is an operational choice for a small deployment, not a universal architecture.

```go
package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Event struct {
	EventID      string            `json:"event_id"`
	OccurredAt   time.Time         `json:"occurred_at"`
	Service      string            `json:"service"`
	Environment  string            `json:"environment"`
	Job          string            `json:"job"`
	TenantCohort string            `json:"tenant_cohort"`
	Experiment   string            `json:"experiment"`
	Outcome      string            `json:"outcome"`
	DurationMS   int64             `json:"duration_ms"`
	ErrorClass   string            `json:"error_class,omitempty"`
	Body         string            `json:"body"`
	Attributes   map[string]string `json:"attributes,omitempty"`
}

type Store struct {
	mu   sync.Mutex
	path string
}

func (s *Store) append(e Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	f, err := os.OpenFile(s.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer f.Close()

	if err := json.NewEncoder(f).Encode(e); err != nil {
		return err
	}
	return f.Sync()
}

func validate(e Event) error {
	if e.EventID == "" || e.OccurredAt.IsZero() || e.Service == "" || e.Job == "" {
		return errors.New("event_id, occurred_at, service, and job are required")
	}
	if e.TenantCohort != "control" && e.TenantCohort != "treatment" {
		return errors.New("tenant_cohort must be control or treatment")
	}
	if e.Outcome != "success" && e.Outcome != "failure" {
		return errors.New("outcome must be success or failure")
	}
	return nil
}

func redact(e *Event) {
	for key := range e.Attributes {
		lower := strings.ToLower(key)
		if strings.Contains(lower, "token") || strings.Contains(lower, "password") || strings.Contains(lower, "session") {
			delete(e.Attributes, key)
		}
	}
}

func authorized(r *http.Request) bool {
	want := os.Getenv("LOG_API_TOKEN")
	return want != "" && r.Header.Get("Authorization") == "Bearer "+want
}

func main() {
	store := &Store{path: "events.ndjson"}

	http.HandleFunc("/ingest", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || !authorized(r) {
			http.Error(w, "not allowed", http.StatusUnauthorized)
			return
		}
		defer r.Body.Close()

		var event Event
		decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&event); err != nil {
			http.Error(w, "invalid event", http.StatusBadRequest)
			return
		}
		if err := validate(event); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		redact(&event)
		if err := store.append(event); err != nil {
			log.Printf("append event: %v", err)
			http.Error(w, "storage unavailable", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	})

	http.HandleFunc("/search", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || !authorized(r) {
			http.Error(w, "not allowed", http.StatusUnauthorized)
			return
		}
		limit, err := strconv.Atoi(r.URL.Query().Get("limit"))
		if err != nil || limit < 1 || limit > 200 {
			limit = 100
		}

		f, err := os.Open(store.path)
		if errors.Is(err, os.ErrNotExist) {
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintln(w, "[]")
			return
		}
		if err != nil {
			http.Error(w, "search unavailable", http.StatusServiceUnavailable)
			return
		}
		defer f.Close()

		matches := make([]Event, 0, limit)
		scanner := bufio.NewScanner(f)
		for scanner.Scan() && len(matches) < limit {
			var event Event
			if json.Unmarshal(scanner.Bytes(), &event) != nil {
				continue
			}
			q := r.URL.Query()
			if q.Get("cohort") != "" && event.TenantCohort != q.Get("cohort") {
				continue
			}
			if q.Get("experiment") != "" && event.Experiment != q.Get("experiment") {
				continue
			}
			if q.Get("job") != "" && event.Job != q.Get("job") {
				continue
			}
			if q.Get("outcome") != "" && event.Outcome != q.Get("outcome") {
				continue
			}
			matches = append(matches, event)
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(matches)
	})

	log.Fatal(http.ListenAndServe("127.0.0.1:8080", nil))
}
```

Run this behind a TLS-terminating reverse proxy, with `LOG_API_TOKEN` supplied by your secret manager. A Node.js cron container should emit the same schema after work completes; the local collector can batch and forward those events. Docker documents that its default `json-file` logging driver captures container standard output and standard error, while its `local` driver performs rotation by default [3]. Configure rotation explicitly for whichever driver you use, because the local buffer is a delivery aid, not permanent retention.

The search endpoint is intentionally constrained to exact operational fields and 200 results. It is self-serve because an authorized engineer can form a cohort query without knowing which host ran the cron job. It is not suitable as a general text analytics engine, and the linear file scan will slow as retention grows. Those are design boundaries, not surprises.

## Verify the page, then rehearse rollback

Verification begins before deployment. Generate schema-valid fixtures for control success, control failure, treatment success, and treatment failure; send each through the same collector path used by containers; then assert that searches return the correct cohort and outcome. Also send an event with an unknown field, a 65 KiB body, a missing token, and an invalid cohort. The receiver should reject each one. This proves the ingestion contract and access boundary without manufacturing an incident.

Next, run a canary cron job that has no tenant side effects. Confirm that its completion event reaches centralized search, that local rotation keeps disk use bounded, and that a deliberately selected test outcome changes the derived counter but does not page production. Only after that should the alert rule be evaluated against historical windows. The useful review is not “does the graph move?” It is “would this page cause the responder to disable the treatment, and would that action protect tenants?”

Keep rollback separate from telemetry rollback. The experiment needs a kill switch that assigns new work to control without disabling completion events. If the central receiver is unavailable, jobs should continue their business work and the local collector should buffer within a hard disk limit; once that limit is reached, dropping verbose diagnostics is safer than filling the host, but preserve compact outcome counters through a separate path where possible. Alert on telemetry gaps to the team during working hours unless the missing data itself prevents a critical safety decision.

Rehearse three actions in order: disable treatment assignment, verify new completions report `control`, and compare the failure ratio after the oldest treatment attempts have cleared the window. Record the exact query beside the experiment flag. Don't make an exhausted responder reconstruct either one from a dashboard title.

Fast rollback wins.

## Know when this design stops fitting

The single-file receiver fits a small team with modest event volume, a narrow query contract, one security boundary, and tolerance for a short search-retention window. The catch is that it provides neither replicated storage nor sophisticated full-text indexing. Once multiple teams need independent retention policies, legal holds, high-volume ad hoc search, regional data controls, or a formal availability target, move the same event contract behind a purpose-built log store or managed service. Stick with an existing organization-wide platform when its access controls and on-call ownership already work; a second logging island creates another system to patch and another silence to misread.

Do not compare candidates by screenshot count. Test sustained ingest with your event distribution, query latency for the actual cohort filters, behavior during network partitions, deletion guarantees, role-based access, exportability, and the total bytes retained after indexing and replication. Signal quality remains the deciding axis: the best result is the smallest event stream that can prove whether treatment harmed tenants and tell the responder which reversible action to take.

That is enough. Centralized logging earns its keep when the page, query, and rollback all describe the same tenant outcome.

## References

1. https://opentelemetry.io/docs/specs/otel/logs/data-model/
2. https://cheatsheetseries.owasp.org/cheatsheets/Logging_Cheat_Sheet.html
3. https://docs.docker.com/engine/logging/configure/
