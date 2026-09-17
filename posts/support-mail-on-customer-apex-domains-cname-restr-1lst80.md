# Support Mail on Customer Apex Domains: CNAME Restrictions and www Cutovers

A customer-support mail cutover is constrained by the longest DNS cache you do not control, so the practical choice is to keep the zone apex free of CNAME dependencies, lower relevant TTLs before the change, and move SPF, DKIM, and DMARC in stages. Use A or AAAA records at the root only when the same hostname must serve a site; put `www` behind a CNAME if convenient. Those web records do not authenticate mail. The mail path depends on MX and TXT records, plus a DKIM selector that may be either TXT or CNAME according to the signing design.

**TL;DR:** an apex CNAME is the wrong primitive for a support domain that also needs mail records, because a CNAME owner name cannot carry other data. Preserve the root for MX, TXT, A, and AAAA records, treat `www` as a separate web alias, and optimize cutover speed by controlling TTLs and sequencing changes rather than by collapsing the whole domain onto one alias.

The page I care about is not "DNS propagation in progress." What page fired? It should be a delivery symptom tied to a specific authentication result: SPF temperror, DKIM failure for a selector, DMARC policy rejection, or loss of MX reachability. A green control-plane dashboard cannot prove what recursive resolvers and receiving mail systems see.

Caches win.

## What would the postmortem actually say?

Consider a bounded incident exercise, not an invented war story. A customer-support team sends replies as `agent@example.com`, publishes a help center at `example.com`, and aliases `www.example.com` to its web edge. The team wants the cutover finished in one maintenance window. Someone proposes a CNAME at `example.com` because the web target can then move without changing customer configuration.

That proposal creates the failure before the migration begins. RFC 1034 says that if a CNAME record exists at a node, no other data should be present there. The root already needs other record types: MX for inbound mail, TXT for an SPF policy, and often A or AAAA for the website. A conventional apex CNAME therefore conflicts with the shape of the zone, regardless of whether a particular DNS control panel offers a flattening feature with CNAME-like behavior.

The incident timeline would be dull and expensive: web checks pass, the change ticket closes, and cached answers keep different receivers on different authentication states. One resolver still has the old DKIM selector response. Another sees the new SPF policy. A receiving system evaluates the message using what it can resolve at that moment, not what the deployment dashboard says should exist.

The invariant is stricter than "wait for propagation": **during every observable phase, old and new mail paths must each have a valid authentication path.** That is what the rollout must preserve.

## Why does the apex restriction matter to support mail?

SPF, DKIM, and DMARC occupy different names and answer different questions. SPF publishes policy in TXT at the envelope sender's domain and checks whether the connecting host is authorized. DKIM publishes a public key below a selector such as `s1._domainkey.example.com`; the message signature identifies that selector. DMARC publishes a TXT policy at `_dmarc.example.com` and evaluates alignment between the visible From domain and a domain authenticated by SPF or DKIM. RFC 7489 defines that alignment and the reporting mechanism.

Three records. Three failure modes.

The apex question still matters because mail and web often share the organizational domain. Putting an ordinary CNAME at the root is incompatible with co-located MX and TXT data, while an A or AAAA record can coexist with them. The `www` label has no such burden here, so a CNAME there is structurally straightforward. It can point at a web hostname without affecting authentication records below `_domainkey` and `_dmarc`.

Some authoritative DNS services synthesize A and AAAA answers at the apex from an alias-like configuration. That can be operationally useful, but it is a service-specific behavior rather than a CNAME record placed alongside MX and TXT. Evaluate it by externally observed answers, DNSSEC behavior, failure semantics, and update latency. The label in a dashboard is weak evidence. Query the authoritative servers and multiple recursive resolvers.

An A record at the apex avoids the coexistence problem but binds the customer configuration to one or more IP addresses. If those addresses change, every relevant cache must age out after the record is updated. An alias-like apex feature can decouple that lifecycle, but adds an authoritative-service dependency. The explicit trade-off is address churn against another runtime dependency. I start by asking which component owns that churn and which failure would wake someone at 3 a.m.; only then does the record type become an answer.

## Sequence the cutover around cache overlap

Propagation is not a global event with a finish time. An authoritative change becomes visible at the source, while recursive caches may continue returning an unexpired earlier answer. Negative answers can also be cached. Lowering a TTL only helps after caches holding the previous, higher TTL have expired, so changing the TTL five minutes before the migration does not create a five-minute cutover.

I would write the runbook backward from the longest old TTL and require evidence at each boundary:

1. At least one old-TTL interval before cutover, reduce TTLs only on records expected to change. Record the exact previous values and the time of the change.
2. Publish the new DKIM selector while the old selector and private key remain usable. Senders can begin signing with the new selector after its public key is observable.
3. Extend SPF authorization to cover both old and new outbound paths before traffic moves. RFC 7208 limits terms that cause DNS queries during evaluation to ten.
4. Keep DMARC policy and reporting stable while SPF and DKIM move. A policy change at the same time multiplies the plausible causes of rejection.
5. Shift sending traffic gradually, checking message headers and receiver-side authentication results. Remove old authorization only after the old path is unused and its DNS answers have aged out.
6. Restore ordinary TTLs after the rollback window closes. Short TTLs increase query load and leave less insulation from an authoritative outage, so they are a migration control, not an automatic permanent setting.

The fastest defensible cutover is bounded by the old cached state, not by how quickly an operator can click Save. Faster is possible only when earlier preparation has already created overlap.

## Make the verifier report evidence, not confidence

A preventative check should inspect the names receivers query and reject an impossible apex layout before deployment. This Go fragment models the critical checks against a resolver. It does not claim to validate delivery; it produces DNS evidence for the change record.

```go
package main

import (
    "context"
    "fmt"
    "net"
    "strings"
    "time"
)

func main() {
    domain, selector := "example.com", "s1"
    resolver := net.DefaultResolver
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()

    cname, err := resolver.LookupCNAME(ctx, domain)
    if err == nil && strings.TrimSuffix(cname, ".") != domain {
        fmt.Printf("FAIL apex resolves through CNAME: %s\n", cname)
    }

    mx, mxErr := resolver.LookupMX(ctx, domain)
    spf, spfErr := findTXT(ctx, resolver, domain, "v=spf1")
    dmarc, dmarcErr := findTXT(ctx, resolver, "_dmarc."+domain, "v=DMARC1")
    dkim, dkimErr := resolver.LookupTXT(ctx, selector+"._domainkey."+domain)
    fmt.Printf("MX count=%d err=%v\n", len(mx), mxErr)
    fmt.Printf("SPF present=%t err=%v\n", spf != "", spfErr)
    fmt.Printf("DMARC present=%t err=%v\n", dmarc != "", dmarcErr)
    fmt.Printf("DKIM TXT answers=%d err=%v\n", len(dkim), dkimErr)
}

func findTXT(ctx context.Context, r *net.Resolver, name, prefix string) (string, error) {
    records, err := r.LookupTXT(ctx, name)
    if err != nil {
        return "", err
    }
    for _, record := range records {
        if strings.HasPrefix(record, prefix) {
            return record, nil
        }
    }
    return "", fmt.Errorf("%s record not found", prefix)
}
```

This check has limits. `LookupCNAME` reports resolution behavior through the configured resolver, not authoritative zone-file intent, and a DKIM deployment may intentionally use a CNAME at the selector instead of TXT. Production verification should query authoritative servers directly as well as independent recursive resolvers, retain answers and timestamps, and parse authentication results from test messages delivered through the real path. Do not alert merely because two caches disagree during an approved overlap window. Alert when the disagreement removes all valid paths or produces receiver failures.

A useful page names the customer domain, sending pool, selector, resolver vantage point, last known-good answer, and observed authentication disposition. "DNS unhealthy" is an invitation to stare at graphs while mail queues grow.

## When this pattern does not apply

A dedicated subdomain changes the trade-off. If support mail uses `mail.example.com` or `support.example.com`, teams can isolate its SPF and DMARC policy from the organizational root, subject to DMARC policy discovery and alignment rules. It still cannot mix a CNAME with other data at the same owner name, but fewer unrelated services compete for that label.

A domain used only for HTTP, with no MX, TXT, or other required apex data, may also tolerate an apex alias mechanism. Verify actual answers and portability before depending on it. Conversely, a domain that must receive mail needs MX behavior considered explicitly; the absence of MX does not turn an apex alias into a mail architecture. RFC 5321 defines fallback handling when no MX record exists, but relying on implicit address fallback is a poor migration plan because it couples SMTP delivery to the web address.

The operational choice is plain: keep the apex able to host independent record types, use `www` as the disposable web alias, and buy cutover speed with advance TTL reduction plus overlapping authentication state. **Rollback must remain valid for at least as long as stale answers can remain valid.** That rule survives control-panel redesigns, address changes, and optimistic dashboards.

## Sources

- https://datatracker.ietf.org/doc/html/rfc1034
- https://datatracker.ietf.org/doc/html/rfc5321
- https://datatracker.ietf.org/doc/html/rfc6376
- https://datatracker.ietf.org/doc/html/rfc7208
- https://datatracker.ietf.org/doc/html/rfc7489
