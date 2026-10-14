# Public data-path review repair plan

The user authorized these repairs on 2026-09-04, before the two Quinn client
root combination repairs. This is a local correctness stage, not throughput,
public Knife16 cutover, owner/server implementation, or external acceptance.

Current checkpoint: the first sender implementation passed focused tests but
received an independent P2 for RESET state retention. Its original one-shot
contract below is historical and insufficient for stage acceptance. See
`2026-09-04-public-path-review-repair-local-results.md` for the exact failure
and stop. The next attempt needs a reviewed local admission/retirement budget
and user confirmation; root Rust remains gated behind public-path closure.

## Scope and ownership

- Integrator owns `client_tun.rs`, its DNS/UDP caller tests, `udp_relay.rs`
  and its private tests, repository governance, and the final evidence ledger.
- The fake-IP Writer owns `fake_ip.rs`, `dns.rs`, and their private tests.
- The TUIC Writer owns `tuic.rs`, private UDP send/reassembly children and
  their tests. No overlapping Writer edits those Modules.
- New Quinn root behavior starts only after the public-path stage is reviewed.
  Its separate narrow architecture must first close scheduler/episode custody.

All unrelated dirty files predate this task. No commit, push, host change or
external run is part of this stage.

## Failure tree and predictions

1. **Routing:** full-pool eviction overwrites a live fake-IP. A two-address
   pool with retained references must reject a third name with both maps,
   references and allocation cursor unchanged. Existing-name reuse and later
   legitimate sweep/release must still work. DNS must answer SERVFAIL rather
   than forward externally or advertise a different target.
2. **Length:** a TUIC fragment count bound does not bound a reassembled UDP
   payload to the IPv4 wire limit. The first byte beyond 65,507 must be rejected
   at both reassembly and packet construction, while the exact limit and an
   independent valid association remain usable.
3. **Progress:** awaiting QUIC uni-stream/byte credit suspends the shared TUN
   turn. A real loopback peer withholding either credit must make one UDP
   admission return in one poll; partial submission must RESET that stream,
   while unrelated traffic can still progress.

## Bounded settlement contracts

`FakeIpPool` remains the only mapping/reference owner. Allocation becomes
fallible; it does not gain eviction authority. DNS is the outward Adapter of
that rejection. Its existing response identity/question/RD rules remain intact.

IPv4 UDP payloads are bounded by `65535 - 20 - 8 = 65507` bytes. Every incomplete
reassembly entry respects that limit; at the default 256-entry cap retained
payload is at most 16,769,792 bytes, plus bounded fragment metadata and one
bounded completed output. Duplicate replacement, expiry, count eviction,
fragment-count replacement, completion and rejection preserve byte accounting.
No per-packet global inventory scan is introduced.

The TUIC Adapter owns one non-suspending stream-send attempt: no available
stream means explicit UDP pressure drop; partial/Pending/error write explicitly
resets the acquired stream; only a full write may publish FIN. There is no
application retry, queue, timer or spawned sender. The existing drop counter
records failure without synchronous per-pressure stdout. Success means complete
Packet admission to QUIC, not peer application delivery. QUIC retains its own
RESET/ACK/flow-control state; this stage does not claim whole-transport zero or
a transport memory ceiling independent of peer configuration.

## Sequence, acceptance and stop rule

For each slice: existing characterization GREEN; any needed behavior-preserving
Move with normalized-body evidence and GREEN; one focused causal RED; first
implementation GREEN; related lifecycle/limit checks; independent review.
New tests remain in private external test Modules. New production leaves stay
within 1,500 lines and test leaves within 800; oversized parents must shrink.
Only the Integrator lowers the exact governance ratchets after reviewed GREEN.

Expected pre-implementation RED is diagnosis, not a failed repair attempt.
If the implementation's GREEN test fails, stop, analyze the observed branch,
and obtain user confirmation of the next concrete modification plan before a
second repair. Do not weaken a tracer or normalize residual ownership.

The final dated result owns commands, outcomes, source paths, line changes and
review. Update compact status and learning memory at the accepted boundary,
preserving the separate necessary-only Knife16 position and external stop.
