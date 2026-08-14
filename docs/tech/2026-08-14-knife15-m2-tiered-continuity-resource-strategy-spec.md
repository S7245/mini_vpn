# Knife15 M2 Tiered Continuity And Resource Strategy Specification

Date: 2026-08-14

Status: **ACCEPTED DIRECTION; RESOURCE-STAGE IMPLEMENTATION REQUIRED; FORMAL
M2 AND M3 REMAIN BLOCKED**

Supersedes the market-client comparison decision in the 2026-08-14 market
continuity calibration documents. Do not run the mature-client C0 trial.

## Decision

Knife15 keeps the existing strict continuity target first, but bounds how much
time may be spent asking the same standard-TUIC architecture to satisfy it.

1. **Tier A — strict:** a valid run may contain no complete one-second TCP
   receiver-zero interval. Add at most two materially different Exit-path
   resource candidates and test them in a bounded order.
2. **Tier B — low-frequency interruption:** only if both new Tier-A candidates
   fail, retain the same architecture and permit at most three isolated
   one-second receiver-zero episodes in any rolling 24 hours, with at most one
   in any rolling six hours. Consecutive zero intervals still fail.

The already-tested `.33` configuration is the failed Tier-A reference. It must
not be rerun unchanged and does not consume either of the two new candidate
slots.

This decision cancels competitor comparison. It does not rewrite the genuine
`cdbfe36` formal M2 failure and does not convert any old failure into a pass.

## Goal

Find the smallest operational architecture that delivers acceptable
long-duration TCP continuity:

- prefer strict zero-complete-second interruption when a better independent
  Exit path can supply it;
- stop spending long tests on equivalent resources or parameter tuning;
- if strict continuity is not reproducible across the bounded resource set,
  quantify and enforce a very low interruption frequency;
- preserve all existing safety, UDP, lifecycle, ownership, and cleanup gates.

## Facts Established Before This Stage

- The exact-source `cdbfe36` formal run failed strict M2 after four complete
  cycles. Five complete Target receiver-zero intervals occurred in one TCP
  flow.
- Target and Exit-to-Target service stayed healthy. The Mac-to-Exit QUIC path
  incurred loss and congestion contraction while ACK progress continued.
- The affected TCP flow was already established on one TUIC/QUIC connection.
  Standard TUIC cannot migrate those bytes to a replacement connection.
- D16, Endpoint pacing, TUN, routes, DNS, process lifecycle, observer, and
  cleanup evidence did not cause that failure.
- The current `.33` path therefore fails Tier A. Repeating it or changing
  frozen local constants is not a new architectural experiment.

## Non-Goals And Frozen Branches

- Do not compare mini_vpn with Mihomo, Clash, or a commercial VPN in this
  stage.
- Do not tune D16, MTU/PLPMTUD, pool, QUIC windows, chunk, Cubic, GSO,
  Endpoint pacing, retry, timeout, or workload constants.
- Do not count additional CPU, RAM, disk, or nominal bandwidth on an already
  unsaturated equivalent route as a new candidate.
- Do not claim that adding another pool connection can rescue an already
  established TCP stream.
- Do not implement a custom resumable transport or server in Tier A or Tier B.
- Do not weaken the existing `m2` action in place. Tier-B behavior must have a
  separately named action, evidence schema, and source-admission floor.
- M3 remains blocked until one tier is accepted with cleanup.

## Tier A — Strict Resource Gate

### Strict SLI

For every valid TCP workload result:

```text
complete_receiver_zero_intervals == 0
```

The final partial iperf interval is excluded exactly as in formal M2. All
existing M2 requirements remain active, including:

- UDP loss at or below `3%`;
- maximum TCP sender/receiver interval gap at or below `16MiB`;
- DNS and real-client checks;
- D16, Endpoint, TUN, route, DNS, process, resource, and observer health;
- exact-source evidence and complete cleanup.

### What Counts As A New Resource Candidate

A candidate must materially change the HK-Mac-to-Exit failure domain. Its
immutable manifest must identify:

- provider and account/resource identity;
- region, public IPv4, and provider ASN;
- advertised route class or product class;
- Exit TUIC endpoint and exact server configuration hashes;
- unchanged `.77` Target identity;
- Mac physical interface and route/traceroute fingerprints;
- tested source commit, release-binary hash, workload-profile hash, and
  observer version.

At least one of provider/ASN or independently contracted route class must
differ from `.33`. A same-provider same-route resize is ineligible unless the
preflight demonstrates that the old Exit was resource-saturated and the new
resource removes that exact saturation. Existing evidence does not show such
saturation.

A second Exit used only for new-flow load balancing is useful operationally
but is not sufficient evidence for established-stream continuity. The
candidate must carry the tested established flow on its materially different
path.

### Candidate Admission

Before a long run, one bounded qualification must prove:

- the manifest is complete, immutable, and secret-free;
- direct Mac-to-Target and Mac-to-candidate connectivity have no complete
  receiver-zero intervals under the derived offered load;
- Exit CPU, memory, UDP socket/drop counters, queueing, and link capacity have
  headroom above the offered profile;
- the observer matches the exact candidate TUIC port and `.77` Target;
- the release binary and tracked worktree match the admitted source;
- the existing two-cycle `m2-qualification` schedule and cleanup pass.

An invalid environment, operator mistake, power loss, or VPS outage does not
reject a candidate, but it also supplies no pass evidence. A genuine
receiver-zero, UDP, lifecycle, or safety failure rejects that exact candidate
configuration without parameter tuning.

### Acceptance And Stop Rule

For each admitted candidate:

1. run one strict qualification;
2. if it passes, run one uninterrupted 24-hour strict formal M2;
3. if that passes, run a second uninterrupted 24-hour strict formal M2 with
   the same source, binary, workload profile, resource, and server settings;
4. accept Tier A only after both consecutive formal runs and both cleanups
   pass.

Reject the candidate at its first genuine strict failure. Do not repeat it to
seek a favorable sample. Try no more than two eligible candidates after `.33`.
If neither is accepted, Tier A is exhausted and Tier B opens automatically.

Completed valid runs are immutable evidence. An unrelated later invalid run
does not erase a prior complete pass, but acceptance still requires two
consecutive valid formal passes on one candidate.

## Tier B — Low-Frequency One-Second Interruption Gate

Tier B is unavailable until the strict-attempt ledger proves that both new
eligible candidates were genuinely rejected.

### Exact Episode Definition

A **receiver-zero episode** is a maximal consecutive run of complete
one-second iperf receiver intervals containing zero bytes during an admitted
TCP result. The final partial interval is excluded.

Tier B requires all of the following:

```text
max_consecutive_receiver_zero_intervals <= 1
receiver_zero_episodes_in_any_rolling_6h <= 1
receiver_zero_episodes_in_any_rolling_24h <= 3
receiver_zero_intervals_in_any_rolling_24h <= 3
```

Thus “一天几次是极限” is fixed as **at most three isolated episodes per
rolling 24 hours**, with no clustering beyond one per rolling six hours. The
preferred value remains zero.

Three zero seconds per day correspond to:

```text
3 / 86,400 = 0.003472% zero-second budget
nonzero one-second interval ratio = 99.996528%
```

This is a workload-specific continuity SLI, not a general service-availability
claim.

All non-continuity M2 gates remain unchanged. A two-second-or-longer episode,
four episodes in a rolling day, two episodes in a rolling six hours, any UDP
loss above `3%`, any safety violation, or incomplete cleanup fails Tier B.

### Duration And Preservation Of Useful Evidence

Tier-B acceptance requires twelve valid six-hour evidence epochs, totaling 72
valid test hours, under one immutable source/binary/workload/resource profile.
At least one group of four epochs must come from one uninterrupted 24-hour TUN
and process lifetime so the long-duration lifecycle is still exercised.

Each epoch is independently sealed in the evidence ledger. An external VPS or
power interruption invalidates only the incomplete epoch; it does not discard
earlier complete valid epochs. Rolling-window evaluation spans consecutive
valid epochs and never bridges an invalid or unknown evidence gap. The
uninterrupted 24-hour requirement must still be satisfied separately.

## Architecture Boundary

The resource stage changes path diversity, not mini_vpn's data-plane
architecture. This is plausible because the selected failure is on the
Mac-to-Exit QUIC path and the current Exit itself was not saturated.

If Tier A fails twice and Tier B also fails, standard single-path TUIC has
reached its accepted limit for this product goal. The next architecture stage
must investigate an upstream that owns path diversity or resumable TCP byte
delivery across transports. Connection-pool replacement alone is insufficient
for an already-established stream.

## Decision Tree

```text
.33 strict reference already failed
  -> candidate 1 materially changes path
       -> qualification fails genuinely: reject
       -> qualification passes: strict formal twice
            -> both pass: accept Tier A, reopen M3
            -> either fails genuinely: reject
  -> candidate 2 follows the same bounded rule
       -> both strict formals pass: accept Tier A, reopen M3
       -> genuine failure: Tier A exhausted
  -> implement and run Tier B for 72 valid hours
       -> frequency and all safety gates pass: accept Tier B, reopen M3
       -> fail: open resumable/path-diverse upstream architecture stage
```

## Observability And Evidence Contract

Every candidate and epoch must preserve:

- exact UTC start/end timestamps and monotonic phase times;
- every complete receiver interval and derived episode boundaries;
- source, binary, profile, resource-manifest, server-config, and observer
  hashes;
- Mac route/interface/TUN/DNS ownership;
- D16 and Endpoint conservation/final ownership;
- QUIC cwnd, RTT, ACK, loss, congestion, PLPMTUD, generation, and path identity;
- Exit four-direction packet/byte/drop counters and bounded pcap evidence;
- Target sender/receiver summaries and cleanup result.

No reducer may infer continuity across a missing, malformed, or invalid epoch.

## System-Design Score

**9/10.** The policy has bounded experiments, precise SLIs, explicit failure
domains, immutable evidence, lifecycle coverage, and a hard architecture stop
rule. The remaining point is operational rather than conceptual: the two
actual candidate resource manifests do not exist yet. Creating and locally
validating those manifests, without silently accepting an equivalent route,
makes the stage execution-ready.
