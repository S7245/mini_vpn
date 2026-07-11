# Knife14h10d16 Pool-Health Probe Results

Date: 2026-07-11
Code: `6209910 fix(tuic): probe idle tcp pool slots before recycle`

## Outcome

Task 11C fixed a real TCP-pool lifecycle defect, but the scoped VPS A/B did
not pass the `>150 Mbit/s` discriminator. The clean pool-2 run reached
`109/108 Mbit/s` sender/receiver while the same temporary Exit window carried
the mature sing-box control at `197.739/195.033 Mbit/s`.

The data stream used auxiliary `conn=1`, generation `1`, with
`probe_result=not_due` and `reconnect_reason=none`. Therefore destructive idle
reconnect was not involved in this failed run. It is rejected as the current
capacity root, although replacing it with evidence-based health remains the
correct product behavior.

Gate A and Gate B remain frozen.

## Code-Review Correction

Simply deleting the ten-second stale-slot reconnect would have regressed the
historical stale-slot repair: an auxiliary connection can have no Quinn close
reason and still fail its next stream open. The accepted correction therefore
uses a bounded TUIC Heartbeat/QUIC-ACK liveness probe rather than treating
elapsed time as either proof of health or proof of death.

The implementation also closes four adjacent lifecycle gaps:

- the slot mutex is acquired before lease reservation, so an exclusive close
  cannot race a new opener;
- auxiliary reconnect is followed by a bounded ready Heartbeat/ACK barrier
  before Connect;
- only a successful Connect updates last-success state, and an open failure
  invalidates the slot;
- selection evidence records slot, generation, probe result, reconnect reason,
  and last-success age without logging payload or credentials.

Primary `conn=0` remains the UDP/health connection. The change does not alter
D16 ownership, actor admission, DrainOnly, EOF, MTU, queue, or QUIC-window
behavior.

## Local TDD And Regression

RED/green tracer bullets covered idle action selection, the real Quinn
liveness probe, probe-outcome mapping, successful-open state, and runner
parsing. Local checks used only pure logic, in-memory harnesses, and Quinn UDP
loopback; no real macOS TUN test was run.

- default library: `591/591`;
- harness library: `600/600`;
- integration: `2/2`;
- concurrency harness: `10 passed`, `4 ignored`;
- default and harness `cargo check`: pass;
- suite/probe self-tests and shell syntax: pass;
- focused Rust formatting and diff-check: pass;
- clippy: the same 13 pre-existing warnings, no new warning site.

The first sandboxed full test attempt could not bind UDP sockets (`EPERM`). A
non-sandboxed rerun passed; this was host sandbox policy, not a TUN or code
failure.

## VPS Discriminator

The first control against the original `.33` Exit was incapable
(`18.349 Mbit/s` receiver), so no mini_vpn run was spent there. A temporary
same-version Cubic TUIC Exit on `.77` then established a capable window:

- direct reverse: `217.849 Mbit/s` receiver;
- mature sing-box: `197.739/195.033 Mbit/s` sender/receiver;
- client and Exit UDP socket drops: `0`.

One clean `6209910` safe1200 pool-2 reverse-first P1 then produced:

- mini_vpn: `109/108 Mbit/s` sender/receiver;
- first second: `236 Mbit/s`;
- seconds 1-5: `0`;
- seconds 5-15: mostly `188-190 Mbit/s`;
- seconds 15-20: about `2-3 Mbit/s`;
- target sender: only `259 MiB` at `109 Mbit/s`, showing upstream TUIC stream
  backpressure rather than a local TUN sink that kept receiving continuously;
- pool evidence: two selections, one generation, `probe_result=not_due`,
  `reconnect_reason=none`;
- D16 actor admitted and released `270245794B`;
- TUN TX/RX drops, actor bypass, pressure edges, send/flush errors, QUIC loss,
  congestion, and blocking discriminators: `0`.

The actor can sustain the required rate when bytes arrive: the middle ten
seconds remained near `188-190 Mbit/s`. The failure shape is instead
multi-second remote-stream starvation followed by tail collapse, upstream of
the byte-owned egress actor.

Artifact retained outside the repository:
`/tmp/mvpn_knife14h10d16_poolhealth_aux_ab_6209910_usclient_suite_20260711_225103.tar.gz`.

## Direction Correction

The next stage is not another Gate A and not another D16 tuning pass. It must
create one discriminator at the TUIC stream-service boundary that separates:

1. the sing-box target copy loop not writing useful stream bytes;
2. QUIC receiving later offsets while the ordered contiguous frontier is
   blocked;
3. a readable Quinn stream not being serviced promptly by the client task.

The tracer bullet should reproduce primary-plus-auxiliary connection service
without a macOS TUN, preserve the exact pool-2 selection/generation evidence,
and measure continuous byte-frontier progress. Only a locally reviewed change
with a falsifiable prediction may authorize another scoped capable-window A/B.
Do not change D16 queue sizes, actor cadence, MTU/PLPMTUD, broad QUIC windows,
chunk size, stale-pool timeouts, or self-wake from this result.

## VPS Cleanup

The temporary `.77` Exit was stopped, its four socket-buffer values were
restored to their original values, and its existing iperf3 service remained
active. `.27` had no remaining mini_vpn process or TUN and routed `.33` through
`eth0`. `.33` retained active sing-box/iperf3 services and its persistent high
socket-buffer settings. Temporary clean worktrees and the downloaded mature
client were removed after the artifact was pulled.
