# Knife15 M1 Eight-Hour Soak Local Implementation Results

Date: 2026-07-17

Status: **LOCAL PASS — runner implementation and review complete; real macOS
TUN M1 has not run**

## Outcome

Commit `2cca535` implements the formal Knife15 M1 eight-hour action in
`scripts/knife15-macos-soak.sh`. The implementation preserves the accepted M0
behavior and frozen data plane while adding an M1-owned profile, evidence
namespace, exact timeline, four drain checkpoints, result/SLO summary, child
deadlines, recovery evidence, and fail-closed cleanup.

No real TUN, route mutation, VPS workload, or Clash operation was performed in
this local stage. The current Mac may keep Clash-TUN running for offline gates,
but the formal M1 must be user-run only after Clash and every other VPN/TUN have
been completely disabled.

## Implemented Contract

The formal traffic budget is exactly `28,800s`:

| Segment | Seconds |
|---|---:|
| steady-a | 7,200 |
| idle-1 | 300 |
| quiet | 3,600 |
| idle-2 | 300 |
| steady-b | 7,200 |
| idle-3 | 300 |
| churn | 3,600 |
| steady-c | 6,000 |
| final drain | 300 |

The schedule produces exactly five active windows, three idle/resume
boundaries, four fresh post-drain checkpoints, 30 DNS-bearing cycles, 234
short TCP connections, 302 TCP results, 30 UDP results, and 332 phase results.
Wall-clock duration is slightly longer than eight hours because health polling,
DNS commands, transitions, and final validation are outside the traffic budget.

The runner enforces:

- direction-aware positive receiver intervals and exact result multiplicity;
- maximum TCP sender/receiver byte gap `16,777,216B`;
- maximum reverse UDP loss `3.0%`;
- at least 900 process, network, interface, and Endpoint samples;
- four fresh checkpoints with zero live/outstanding Endpoint ownership;
- Endpoint conservation at or below `61,440B`;
- bounded checkpoint RSS, FD, and thread growth;
- current-socket rebind recovery within `7,000ms`, if rebind occurs;
- no log compaction, evidence gaps, stranded D16 ownership, or incomplete
  cleanup.

Application-boundary Quinn `Stopped(0)` with a completed receiver and exact
`0/0/0` D16 ownership remains fail-visible as `REVIEW` without converting an
otherwise complete M1 SLO to FAIL. A nonzero queue, incomplete receiver, or
unclassified terminal error still fails closed.

## TDD And Review Repairs

The implementation proceeded as focused RED/GREEN slices for the public M1
contract, stage-aware controller identity, immutable profile, stage-owned
evidence, exact timeline, resource checkpoints, summary/SLO validation, and
the formal action.

The final review found and repaired the following P1 risks before acceptance:

- every foreground iperf, DNS, idle, and drain child now owns a hard deadline;
  timeout returns `124` and clears child PID state while preserving evidence;
- checkpoint capture now requires a fresh Endpoint sample generated during
  that drain, rather than accepting a previously sampled value;
- checkpoint capture immediately rejects nonzero ownership or conservation
  overflow;
- `stop` waits for workload and process termination, owned-utun disappearance,
  and route restoration before recording its final sample and cleanup event;
- negative result mutations preserve direction and place backups outside the
  formal `m1/` directory, so they cannot change evidence multiplicity;
- sender-only zero intervals remain `REVIEW`; receiver zeros remain hard
  failures.

After these repairs, code review has **no unresolved P0/P1**.

## Deterministic Evidence

The complete M1 fixture proves `302` TCP results, `30` UDP results, `30` DNS
results, `332` total phase results, four checkpoints, at least `900` samples in
each required class, and an M1 SLO PASS.

One-at-a-time negative fixtures fail closed for:

- missing, dirty, or stale checkpoints;
- RSS, FD, or thread growth beyond the specified envelope;
- TCP gap `16,777,217B`;
- UDP loss `3.000001%`;
- a receiver zero interval;
- only `899` required samples;
- incomplete rebind recovery or `7,001ms` recovery;
- log compaction;
- nonzero D16 queue ownership;
- a child exceeding its hard deadline.

The sender-only-zero fixture retains result/SLO PASS while setting internal
review to `REVIEW`, as designed. The runner self-test intentionally prints
`ERROR: command exceeded hard timeout of 1s` while exercising its negative
timeout case; the authoritative outcome is its final
`knife15 macOS runner self-test passed` line and exit status `0`.

## Local Gates

All final gates passed:

```text
bash scripts/knife15-macos-soak.sh --self-test                PASS
bash scripts/knife15-macos-soak-self-test.sh                  PASS
bash scripts/knife14h10d16-singbox-control.sh --self-test     PASS
bash -n scripts/knife15-macos-soak.sh                         PASS
cargo test --all-targets                                      PASS
cargo build --release                                         PASS
cargo clippy --all-targets                                    PASS
cargo fmt --all -- --check                                    PASS
git diff --check                                               PASS
changed-content secret scan                                    PASS
```

Clippy and compilation retained only pre-existing vendored smoltcp/project
warnings; no new changed-code warning was accepted. The implementation plan's
initial stale gate name was corrected to the existing
`scripts/knife14h10d16-singbox-control.sh` before the final gate ran.

## Accepted Stop Position

Local M1 is ready for one user-run HK macOS TUN acceptance using the exact
reviewed source. Before the run, the user must completely disable Clash-TUN and
every other VPN/proxy that owns a utun or Target/Exit/DNS route. The formal
sequence is fresh baseline, fresh 300-second direct discriminator, `start`,
`smoke`, `m1`, `status`, and `stop`.

If M1 fails, preserve `status`, `snapshot`, and `stop` evidence. Do not change
frozen product constants or immediately repeat an unchanged failure. M2 and M3
remain blocked until the real M1 bundle and cleanup evidence are reviewed with
no unresolved P0/P1.
