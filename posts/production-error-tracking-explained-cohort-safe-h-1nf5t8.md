# Production Error Tracking Explained: Cohort-Safe HTTP, Cron, Queue Worker Rollbacks

Short answer: capture every failed HTTP request, cron execution, and queue delivery through one error-envelope contract, then page only when the affected tenant cohort crosses a rollback threshold you chose before the experiment began. A NestJS filter or interceptor covers the request path; explicit wrappers must cover schedulers and consumers, because a green request dashboard says nothing about work running outside that path.

Rollback safety is the constraint that changes the design. An e-commerce experiment can look healthy in aggregate while one tenant cohort loses checkout work, so the useful question at 03:00 is not how many errors exist. It is which page fired, which cohort is exposed, and whether disabling the experiment stops new failures without corrupting retries already in flight.

This is a runbook, not a dashboard tour.

## How should production error tracking capture HTTP exceptions, cron jobs, and queue workers?

Use one reporting contract and several boundary adapters. The contract should accept a normalized failure envelope; each adapter should supply the context only it knows. In NestJS, an exception filter can own capture of uncaught HTTP exceptions after routing has resolved, while an interceptor can attach request timing and experiment context. Pick exactly one of them as the reporter. If both report the same thrown value, the page counts an implementation detail twice and your rollback threshold becomes fiction.

Cron jobs and queue workers need equivalent adapters around their actual execution functions. They do not inherit a request lifecycle, so waiting for an HTTP filter to see their errors leaves a blind spot. A scheduler wrapper should identify the job and scheduled run. A consumer wrapper should identify the queue, delivery, attempt, and message key used for deduplication. The shared fields are the ones the rollback decision needs: service, operation, tenant, cohort, experiment revision, error class, retry state, and a stable event ID.

Don't make tenant ID or cohort a free-form message suffix. Put them in structured fields, apply the same naming at all three boundaries, and keep the cohort assignment stable for the duration of the experiment. Martin Fowler's feature-toggle guidance distinguishes experiment toggles from operational controls and describes consistent cohort assignment; that separation matters here because the experiment decision and the emergency off switch have different jobs.

The resulting event can be small:

```go
package failure

import "time"

type Envelope struct {
    EventID           string
    Boundary          string // http, cron, or queue
    Operation         string
    TenantID          string
    Cohort            string
    Experiment        string
    ExperimentRevision string
    ErrorClass        string
    Retryable         bool
    Attempt           int
    OccurredAt        time.Time
}

type Reporter interface {
    Capture(Envelope) error
}
```

That interface is deliberately boring. It keeps transport choice out of business handlers and makes boundary coverage testable. It also prevents a common taxonomy failure: HTTP status, scheduler failure, and consumer rejection may have different local representations, but the incident policy should compare them using the same experiment and cohort dimensions.

Keep the original exception in restricted diagnostic storage if your policy permits it; send only classified, non-sensitive fields into cohort counters. Checkout failures often carry data that must not become a tag. High-cardinality customer or order identifiers also make a poor paging dimension. The envelope uses tenant and event identifiers for correlation, but the alert evaluator should aggregate on bounded labels such as operation, cohort, experiment revision, and error class.

## Why request-only capture makes an experiment rollback unsafe

Consider a marketplace testing a new inventory reservation path for 10% of tenants. The HTTP endpoint accepts a checkout command, a queue worker reserves stock, and a cron job later releases expired holds. If request tracking is the only signal, acceptance can remain clean while the experiment cohort accumulates worker failures. A global error rate dilutes that cohort again. The dashboard is green, the affected tenants are not, and the page arrives late or never.

This is the postmortem frame I would use: the failure was not a missing chart. The control loop could not connect an error to the exposure decision.

The minimum useful matrix is compact:

| Boundary | Unit counted | Required rollback context | Completion rule |
| --- | --- | --- | --- |
| HTTP | Final request outcome | tenant, cohort, experiment revision | response committed once |
| Cron | One scheduled execution | job, tenant scope, experiment revision | run succeeded or failed once |
| Queue | One logical message | queue, message key, cohort, attempt | terminal result separated from retry |

The queue row is where many alerts become dishonest. One logical checkout may be delivered three times. Counting every failed attempt as a new customer-impacting event can triple the apparent blast radius; counting only final exhaustion can hide a retry storm that is consuming the rollback window. Keep two measures: attempt failures for operational pressure and terminal logical-message failures for experiment outcome. Page policy can use either, but the names must say which one it uses.

Short version: retries are not customers.

The same discipline applies to handled HTTP exceptions. A client validation rejection is an expected outcome unless the experiment changed validation and the rejection rate separates sharply by cohort. An unexpected exception is a different class. Do not page on their sum merely because both produce non-success status codes. For cron, distinguish a skipped run caused by an overlap policy from an execution failure. For a queue, distinguish an intentional negative acknowledgment from a crash before acknowledgment. These classifications are local engineering decisions; document them in the runbook beside the alert, because no library can infer your impact model.

I would distrust any single total here. Compare control and treatment using both a rate denominator and an absolute floor, require a minimum sample before automating rollback, and preserve a manual stop when the denominator is suspect. I am not sure what threshold fits your traffic shape; replayed production distributions and a deliberately failed canary are what resolve that uncertainty, not a universal percentage copied from another service.

## A minimal boundary contract for filters, interceptors, and background work

The implementation rule is report once, classify near the boundary, and rethrow or return according to that boundary's normal semantics. Reporting must not silently turn a failed operation into success. It also should not decide feature exposure; the reporter records evidence, while a separate controller applies the predeclared rollback policy.

This Go example shows the wrapper logic rather than pretending the three runtimes share an identical API. The same shape maps to a NestJS exception filter for terminal HTTP errors, an interceptor for context and timing, and explicit decorators or wrapper functions around cron handlers and queue consumers.

```go
package failure

import (
    "context"
    "fmt"
    "time"
)

type Work func(context.Context) error

type Meta struct {
    EventID            string
    Boundary           string
    Operation          string
    TenantID           string
    Cohort             string
    Experiment         string
    ExperimentRevision string
    Attempt            int
}

func Guard(reporter Reporter, meta Meta, work Work) Work {
    return func(ctx context.Context) error {
        err := work(ctx)
        if err == nil {
            return nil
        }

        envelope := Envelope{
            EventID:            meta.EventID,
            Boundary:           meta.Boundary,
            Operation:          meta.Operation,
            TenantID:           meta.TenantID,
            Cohort:             meta.Cohort,
            Experiment:         meta.Experiment,
            ExperimentRevision: meta.ExperimentRevision,
            ErrorClass:         fmt.Sprintf("%T", err),
            Retryable:          isRetryable(err),
            Attempt:            meta.Attempt,
            OccurredAt:         time.Now().UTC(),
        }
        _ = reporter.Capture(envelope)
        return err
    }
}

func isRetryable(err error) bool {
    type temporary interface { Temporary() bool }
    value, ok := err.(temporary)
    return ok && value.Temporary()
}
```

There is an uncomfortable choice in `_ = reporter.Capture(envelope)`. The business failure remains the returned error even if telemetry delivery also fails; otherwise an observability dependency can alter queue retry or HTTP response behavior. The catch is that fire-and-forget capture is not suitable when losing the evidence would violate an audit obligation. In that case, use a durable local outbox with bounded storage and an explicit backpressure policy, accepting the extra operational state. Stick with direct best-effort reporting when the application already has an independent durable record of the failed command and lower latency matters more than telemetry completeness.

For NestJS specifically, attach cohort context before business code runs, preferably from the same deterministic assignment used by the experiment. Let the interceptor enrich execution context, then let the filter capture an exception only if it has not already been marked with the stable event ID. For a cron method, construct metadata from the scheduled-run identity. For a worker, derive the event ID from the logical message identity plus operation, not from the delivery attempt, while recording attempt separately.

Don't swallow.

The wrapper returns the original error so the HTTP adapter can produce its intended exception response, the scheduler can record a failed run, and the queue library can apply its configured retry or dead-letter behavior. Error tracking observes those semantics; it does not replace them.

## Verify the page before trusting the rollback

Test the incident path as a state transition, not as a screenshot. Start with a canary tenant in each cohort and inject one classified failure at each boundary. Verify that the event has the same experiment revision and cohort at HTTP acceptance, queue execution, and scheduled cleanup; verify that one exception produces one logical event; then verify that retries increment attempt pressure without inventing more affected checkouts.

Next, cross the alert condition with synthetic traffic and inspect the page payload. It should name the boundary, cohort, experiment revision, current rate and denominator, evaluation window, and the rollback action available to the responder. If the page only links to a dashboard, the responder still has to discover the decision under pressure. Ask the blunt question: what page fired?

Rollback verification needs its own sequence:

1. Disable new treatment assignments using the operational control.
2. Confirm new requests enter the control cohort while existing message identities retain their recorded revision.
3. Drain or quarantine in-flight treatment work according to the queue's documented delivery policy.
4. Observe terminal failures by cohort until the evaluation window clears.
5. Re-enable only through the normal deployment review, with the failed boundary represented in a regression test.

Feature toggles make targeted exposure and rapid disablement possible, but Fowler also warns that toggle inventory carries carrying cost and needs management. An experimentation platform such as the open-source project GrowthBook can provide feature flags and A/B experimentation, yet the rollback runbook must remain portable: assignment evidence, error envelopes, and stop criteria belong to the system design, not to one vendor's dashboard.

Automated rollback is not always the right choice. It is unsuitable when side effects cannot be reversed, cohort assignment can change during a transaction, samples are sparse enough that one failure dominates the rate, or the control path is also unhealthy. Use a guarded manual rollback in those cases. Automation fits better when exposure is deterministic, the stop action is idempotent, the denominator is trustworthy, and a canary test has proved the full page-to-control path.

The conclusion is operational: instrument every execution boundary, compare treatment with control using stable tenant context, and make rollback a rehearsed control path rather than a button discovered after the alert. The dashboard can wait.

## References

- https://martinfowler.com/articles/feature-toggles.html
- https://www.growthbook.io/
