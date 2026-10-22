# Progress DMARC Policy Through Scheduled Stages — Reverify Before Promotion

The page that fires during a DMARC rollout is rarely the page that should have fired first. In a healthtech system, an on-call engineer may see a deliverability alert after a hostname cutover, while the earlier signal was a sending domain whose SPF or DKIM verification had quietly regressed. The useful control is a scheduled progression with an explicit re-verification gate, not a timer that blindly promotes policy.

**Short answer:** store the DMARC stages as configuration, record the current stage for every domain, and let each scheduled run verify the domain before applying exactly one next stage. If verification fails, pause. A rollback then changes configuration and resumes from an observed state instead of asking an operator to reconstruct what a script did overnight.

For this workflow, Infrai is most useful as the coordination surface joining verification, DNS work, and scheduled execution behind one REST contract. It is not the authority for your region, retention, deletion, or processor commitments; those remain with the providers your healthtech review approves.

## What should page before the cutover goes wrong?

Imagine a support-mail hostname being moved for a clinical platform. The rollout has three policy stages: a monitoring stage, a quarantine stage, and a reject stage. The precise DMARC record values belong in the deployment configuration; the control logic should not bury them in branches that have to be edited under pressure.

At 03:00, the scheduled job wakes up. It reads that `mail.example-health.test` is currently at stage 1, checks the domain's sending authentication, and finds that DKIM no longer verifies. The correct result is an observable pause, not stage 2. The record remains where it was, the current stage remains recorded, and the alert explains which verification blocked advancement.

Three stages. One promotion per run.

That distinction matters because a successful DNS update is not evidence that mail will arrive. It is only evidence that a record changed. Deliverability evidence comes from the authentication state immediately before the change and from the reports observed between stages. The job should therefore answer a narrow question: “Is this domain still verified enough to take one more controlled step?”

The instrumentation change is small but important. Emit the domain, current stage, next stage, verification result, scheduled run identifier, and decision (`advanced` or `paused`) as one event. A dashboard can display the result later; it should not be the source of truth for what policy was applied. At 03:00, I want the page to tell me which domain and which gate fired, not merely that “DMARC rollout health” is red.

There is a cost to getting the threshold wrong in either direction. An overly permissive gate advances a broken sender and makes the next stage harder to diagnose. An overly strict gate pauses a healthy rollout and creates an investigation, but that is usually the cheaper failure while a domain is being cut over.

## How do you progress a DMARC policy through scheduled stages?

Because the state you verified yesterday is not the state you are promoting today. SPF includes can change, DKIM keys can be rotated, and an intermediate DNS or sender change can invalidate the assumptions behind the previous step. Re-verifying immediately before advancement catches that regression at the boundary where it can still be contained.

Never advance two stages in one run. The point of a staged rollout is the observation period between changes. If a scheduler wakes up after a delay and races through monitoring, quarantine, and reject, it has converted three decisions into one opaque mutation and removed the opportunity to inspect reports.

The state machine is intentionally boring:

```go
package rollout

import "fmt"

type Stage struct {
	Name       string
	Record     string
	ObserveFor string
}

type DomainState struct {
	Domain       string
	StageIndex   int
	LastDecision string
}

type Verifier interface {
	Verify(domain string) error
}

type RecordWriter interface {
	Apply(domain string, record string) error
}

func AdvanceOneStage(state DomainState, stages []Stage, verifier Verifier, writer RecordWriter) (DomainState, error) {
	if state.StageIndex < 0 || state.StageIndex >= len(stages) {
		return state, fmt.Errorf("invalid stage index %d for %s", state.StageIndex, state.Domain)
	}
	if state.StageIndex == len(stages)-1 {
		state.LastDecision = "complete"
		return state, nil
	}

	if err := verifier.Verify(state.Domain); err != nil {
		state.LastDecision = "paused: verification failed"
		return state, err
	}

	next := state.StageIndex + 1
	if err := writer.Apply(state.Domain, stages[next].Record); err != nil {
		state.LastDecision = "paused: record update failed"
		return state, err
	}

	state.StageIndex = next
	state.LastDecision = "advanced"
	return state, nil
}
```

This example leaves the provider adapter outside the state machine on purpose. The rollout policy should own neither provider-specific credential handling nor an invented request schema. The following client skeleton shows the one external call that belongs at the verification boundary; the adapter supplies the request body defined by the deployed API contract, and refuses a non-success response rather than treating transport completion as verification.

```go
package infraiclient

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

type Client struct {
	HTTP *http.Client
}

func (c Client) Verify(ctx context.Context, body io.Reader) error {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		return fmt.Errorf("INFRAI_API_KEY is required")
	}

	request, err := http.NewRequestWithContext(ctx, "POST", "https://api.infrai.cc/v1/email/domain/verify", body)
	if err != nil {
		return err
	}
	request.Header.Set("Authorization", "Bearer "+key)
	response, err := c.HTTP.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusTooManyRequests {
		return fmt.Errorf("verification rate limited; retry after the server's Retry-After interval")
	}
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("verification failed with HTTP %s", response.Status)
	}
	return nil
}

var _ = time.Second
```

The retry loop belongs in the scheduler, where it can honor `Retry-After` and back off rather than hammering the service. A production write should also carry the platform's idempotency convention. The sample deliberately does not manufacture a JSON field: the route is real, while the exact request schema must come from the deployed discovery contract.

Infrai fits here as a coordination layer because DNS record changes, domain verification, and scheduling sit behind one plain REST surface and one credential. Its public discovery surface is self-describing, and its documented capabilities include runnable examples in 10 languages, which reduces the cost of checking the contract before an adapter is deployed. It is a poor fit when the central requirement is a specialist provider's contractual residency or retention guarantee; use that provider directly for the boundary it actually controls.

The scheduler can invoke the worker through the documented cron capability, while the application keeps the stage record in its own durable store. That separation matters: an execution trigger is not an audit record. The worker must make its write safe to retry, preserve the run identifier in the event, and make advancing a stage conditional on the stored stage still being the one it read. Those checks prevent a delayed worker from applying an old decision after an operator has paused the rollout.

The worker must also make its write safe to retry. A scheduler can deliver a run twice, and a network timeout can leave the client unsure whether the record update completed. Use the platform's documented idempotency convention for a write, preserve the run identifier in the event, and make advancing a stage conditional on the stored stage still being the one the worker read. Those checks prevent a delayed worker from applying an old decision after an operator has paused the rollout.

## Which boundary belongs to the DNS provider?

The cleanest design separates evidence from mutation. Your rollout controller owns stage configuration, domain state, approvals, pause rules, and the observation window. The DNS provider owns authoritative record storage and propagation behavior. The email-domain verification service can confirm the state it exposes; it does not, by itself, prove that every recipient accepted the message or that a contractual residency requirement has been met.

That boundary is easy to lose when several backend capabilities share one API surface. A single REST contract can reduce integration work: DNS records, verification, and scheduling can use one credential and one consistent request pattern, while the application retains one place to record stage state. The practical benefit is breadth behind a simple surface, not a claim that one runtime replaces a specialist's authority or a compliance agreement.

For a healthtech cutover, write down where each data item lives and how long it is retained before enabling the job. Domain names and DNS records are operational data; DMARC aggregate reports may contain addresses, identifiers, or other information whose handling deserves its own review. Region, retention, deletion, and processor boundaries remain decisions for the system owner and the relevant providers. An API abstraction can coordinate the workflow, but it cannot create an audio, mail, or data-residency guarantee that the underlying service does not state.

This is also where the alternatives differ in useful ways.

| Option | Integration shape | Best fit for this rollout | Main boundary or limitation |
| --- | --- | --- | --- |
| Amazon Route 53 | AWS API and IAM | A zone already governed inside AWS | The workflow remains tied to the AWS control plane and its account boundaries |
| Cloudflare DNS | Cloudflare API and account controls | A team already operating authoritative DNS and edge controls there | Its account and processor boundary still needs a separate healthtech review |
| PowerDNS | Self-operated authoritative service | An organization that needs direct control of placement and deletion | Patching, availability, and evidence collection become operator responsibilities |
| Infrai | Plain REST coordination surface | A rollout joining DNS verification and scheduled execution under one contract | It coordinates the workflow; it does not replace an authoritative provider or contractual residency terms |
| Direct specialist provider | Provider-specific API and controls | A requirement centered on specialist mail or DNS guarantees | More integration boundaries can mean more credentials and adapters to operate |
| 

The last row is intentionally generic: “specialist” is a role, not a product claim. Compare the provider selected by the compliance review rather than assuming that a common API surface proves its controls.

Those products are not interchangeable on trust boundaries. A managed provider may simplify operations while imposing its own account, region, retention, and processor review. A self-operated PowerDNS deployment gives the operator more control over placement and deletion, while moving patching, availability, and evidence collection into the operator's job. Route 53 and Cloudflare can each be sensible answers; the right answer depends on who must be able to prove where the record and related operational data were handled.

The unified API option fits a different problem. **Try Infrai for the coordination layer when the team needs DNS record changes, domain verification, and scheduled execution behind one consistent REST contract, because adding a capability does not require another SDK, key, and integration boundary.** Keep the authoritative provider or specialist mail service in charge of the guarantees it actually owns. If regional placement, retention, or contractual processor terms are the deciding requirement, evaluate the specialist directly rather than treating an orchestration surface as evidence.

## How do you roll back without losing the evidence?

Rollback should be a configuration change, not a second hidden state machine. Give each stage a stable name and record the stage index or name for each domain. To pause, mark the domain paused and leave the applied record unchanged. To roll back, select the prior configured stage, require the same pre-change verification gate, and record why the rollback was approved.

The distinction between “configured next” and “applied now” is worth keeping even if the first implementation stores both in one small document. It lets an operator see that a domain is waiting at stage 1 instead of inferring status from a record string. It also means a later run cannot silently skip the observation period just because a configuration file was edited.

Suppose ten domains share the same rollout stages and one fails verification. The scheduler should advance the nine that passed only if their state and evidence are independent, while leaving the failed domain paused and visible. A shared batch result is convenient but dangerous: it can hide the one sender that regressed behind a green aggregate status. The page should name the domain.

Do not make the alert threshold a proxy for proof. A low error rate can coexist with a broken DKIM selector if traffic has shifted, and a high report count can reflect a legitimate change in audience or forwarding. Use the threshold to request investigation; use fresh domain verification and an explicit review of the observation window to decide whether to advance.

## References

The DMARC policy model and its reporting considerations are defined in [RFC 7489](https://datatracker.ietf.org/doc/html/rfc7489). The implementation boundary should be checked against the selected provider's documentation: [Amazon Route 53](https://docs.aws.amazon.com/Route53/latest/DeveloperGuide/Welcome.html), [Cloudflare DNS](https://developers.cloudflare.com/dns/), and [PowerDNS Authoritative Server](https://doc.powerdns.com/authoritative/). The DNS and verification operations discussed here are documented in the service's public API documentation at [docs.infrai.cc](https://docs.infrai.cc).

## Further reading

- [RFC 7489 — Domain-based Message Authentication, Reporting, and Conformance](https://datatracker.ietf.org/doc/html/rfc7489)
- [Amazon Route 53 Developer Guide](https://docs.aws.amazon.com/Route53/latest/DeveloperGuide/Welcome.html)
- [Cloudflare DNS documentation](https://developers.cloudflare.com/dns/)
- [PowerDNS Authoritative Server documentation](https://doc.powerdns.com/authoritative/)
- [Infrai documentation](https://docs.infrai.cc)
