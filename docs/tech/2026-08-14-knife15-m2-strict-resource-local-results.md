# Knife15 M2 Strict Resource Runbook And Local Gate Results

Date: 2026-08-14

Status: **PASS — TASK 5 COMPLETE; CANDIDATE 1 RESOURCE ADMISSION IS NEXT;
NO MAC/VPS QUALIFICATION HAS RUN**

## Outcome

The Tier-A operator transaction is now executable without weakening formal
M2. The reviewed runbook covers candidate provisioning, exact source and
binary ownership, physical IPv6 handling, one frozen workload baseline,
fresh direct/resource evidence, root-owned TUN start, smoke, paired observer,
qualification or formal action, mandatory cleanup, bundle finalization, and
ledger reduction.

The user does not start Clash, Mihomo, a separate TUIC client, or another VPN.
The runner starts the tested `mini_vpn client-tun` process. Existing `.111`
or `.27` hosts do not become candidates merely because they exist; a candidate
must first satisfy the materially distinct provider/ASN or independently
contracted route policy.

Rust production, strict `receiver_zero_intervals == 0`, UDP `3%`, TCP gap,
workload duration/rates, D16, Endpoint pacing, MTU, pool, QUIC windows, Cubic,
GSO default, and self-wake behavior are unchanged.

## Review Closure

The final concentrated review closed these locally preventable long-run
risks:

1. Predeclared route/traceroute hashes had no correct pre-run owner. They were
   removed from the input profile; the read-only preflight now owns the live
   route/traceroute files, internal checksum inventory, and immutable archive.
2. The historical `.33` reference was a full synthetic resource profile. It
   is now a minimal production reference with one exact frozen `.33` identity
   and `.77` Target, stored outside parser fixtures.
3. Provider/route evidence hashes did not prove that their contents matched
   the candidate claims. Both evidence files now use closed key/value schemas;
   helper, preflight, root runner, and ledger independently require exact
   provider, ASN, resource, Exit, Target, route class, and contract agreement.
4. External identity/capacity evidence was unbounded and its copy could race
   the initial hash. Every such file is nonempty and at most `64KiB`; preflight
   checks the hash before and after copying, and later validators retain the
   bound.
5. Reusing one `/tmp` baseline across qualification plus two formal runs was
   operationally fragile. The runbook now creates an immutable SHA-256-bound
   backup outside `/tmp`, restores only to the original safe basename, and
   replays baseline validation before a later attempt. A reboot no longer
   forces a passed candidate to change its workload contract.
6. The stable candidate identity omitted the profiled Mac physical interface.
   Qualification and formal runs could therefore have combined different
   local paths. The ledger now treats interface drift as candidate contract
   drift.

Implementation/review commits are:

- `211e7c3` — assign fresh route evidence to the preflight owner;
- `145a7aa` / `f1d5f6d` — freeze and promote the production `.33` reference;
- `5f53165` — bind evidence contents to the resource profile and ledger;
- `9791c32` — independently revalidate the binding in the root runner;
- `60a4e94` — bound inputs and recheck their post-copy bytes; and
- `9909465` — bind the stable candidate to one Mac physical interface; and
- `bcd63b4` — require `9909465` or a descendant for strict runs and ledger
  decisions.

No unresolved P0/P1 remains across candidate identity, fresh evidence,
remote observer ownership, TUN mutation ordering, cleanup, false acceptance,
false rejection, baseline persistence, or decision serialization.

## Complete Gates

```text
root all-target/harness:             725 passed; 3 ignored
main binary:                           2 passed
concurrency integration:              10 passed; 4 ignored
release build:                       PASS
established all-target Clippy:       PASS (existing warnings only)
root docs / fmt:                     PASS / PASS
release D16 batch / full-TUN:        PASS / PASS
release Endpoint 32MiB discriminator: 240.108 Mbit/s
Endpoint terminal available/live/outstanding: 61,440/0/0B
Endpoint socket would-block events:    0
vendored Quinn:                       40 passed; 3 ignored; doc 1 passed
vendored quinn-proto:                330 passed; docs 3 passed
vendored default Clippy lanes:       PASS
resource profile/preflight/ledger:   PASS / PASS / PASS
macOS runner / Exit observer:        PASS / PASS
shell / diff / provenance / secret:  PASS
code review:                         PASS; no unresolved P0/P1
```

The macOS runner's intentional hard-timeout fixture prints
`ERROR: command exceeded hard timeout of 1s`; the self-test exited zero and
ended with `passfailpassknife15 macOS runner self-test passed`.

Root provenance resolves exactly to local `third_party/quinn-0.11.11`,
`third_party/quinn-proto-0.11.16`, and `third_party/smoltcp-0.10.0`. The
standalone Quinn lane also used an explicit absolute local quinn-proto patch.

## Decision And Next Gate

Task 5 is complete. Do not start a Mac run yet: Task 6 first provisions and
reviews candidate 1, creates its exact sanitized provider/route evidence,
records server hashes and service health, and supplies the concrete runbook
exports. Only after that read-only resource admission is ready does the user
run one strict qualification. The ledger alone decides whether formal 1,
formal 2, candidate 2, or Tier B is authorized.

Runbook:
`docs/tech/2026-08-14-knife15-m2-strict-resource-macos-runbook.md`.
