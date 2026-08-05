# Knife15 M2 Full-Tunnel Quiescence Fail-Fast Local Results

Date: 2026-08-05

Status: **LOCAL RUNNER REPAIR ACCEPTED — one fresh real-Mac M2 required**

Evidence:
`docs/tech/2026-08-05-knife15-m2-full-tunnel-quiescence-failure-results.md`

## 1. Change

Commit `4ceb2ed` makes formal `m2` capture data-plane and Endpoint sample
counts immediately after full-tunnel activation, completes the unchanged
real-client preflight,
and then waits at most the existing smoke timeout (normally `50s`) for a new
globally quiescent observation. It records
`m2-full-tunnel-quiescence.txt` before the 24-hour schedule can begin.

The gate reuses the exact first-idle checkpoint ownership contract:

- a fresh data-plane sample and a fresh Endpoint sample are mandatory;
- TCP pool leases, active relays, fake-IP active ownership, and Endpoint
  live/outstanding ownership must be zero;
- registered fake IPs remain bounded to one or two and DNS drops remain zero;
- Endpoint conservation remains at or below `61,440B`.

Dirty application/system traffic returns a distinct fail-closed result and
instructs the operator to quit non-test network Apps before a fresh run.
Evidence-write failure is distinguished from a dirty snapshot. Process health
is checked separately so a stopped/unhealthy data plane is not mislabeled as
background traffic.

## 2. TDD

The first RED proved that a data-plane-only predicate incorrectly accepted a
fresh observation with Endpoint `live=1409B`. The minimal GREEN added a fresh
Endpoint sample boundary plus zero live/outstanding ownership and the existing
conservation inequality.

Fixtures now prove:

- a fresh clean snapshot passes immediately;
- stale data-plane or Endpoint samples fail closed;
- `live=1409B` fails despite otherwise clean relay/fake-IP state;
- the real-artifact shape (`active_leases=8`, `active_relays=4`, fake-IP
  `7/19`) fails after the bounded wait;
- structured evidence preserves both sample boundaries and every verdict
  value;
- evidence-write failure returns a separate status.

## 3. Scope And Review

The repair changes only the human-in-the-loop runner and runbook. It does not
alter Rust data-plane behavior, the exact 86,400-second schedule, checkpoint
or quality predicates, or any frozen D16/MTU/pool/window/chunk/Cubic/GSO/
self-wake/Endpoint value. It moves an existing invariant to a cheap early
boundary; it does not relax acceptance.

Code review verified that the gate observes samples newer than its baseline,
records evidence before returning, distinguishes evidence I/O from a dirty
machine, and runs before workload registration and the formal timeline. There
are no unresolved P0/P1 findings.

## 4. Local Gates

```text
bash syntax:                       PASS
Knife15 internal runner self-test: PASS
Knife15 external shell self-test:  PASS
Knife14 shell self-tests:          PASS
git diff --check:                  PASS
secret scan:                       PASS
code review:                       PASS; no unresolved P0/P1
```

## 5. Next Real-Mac Transaction

Pull the pushed descendant, rebuild release, quit all non-test network Apps
and every other VPN, then take exactly one fresh:

```text
m2-ipv6-check -> baseline -> direct-discriminator
-> start -> smoke -> m2 -> status -> stop
```

The new M2 pre-schedule wait should finish within about 50 seconds. If it
fails, preserve `status/snapshot/stop`; the run has avoided the 24-hour cost.
Do not tune or repeat without removing the named background traffic. M3 stays
blocked until full M2 plus cleanup acceptance.
