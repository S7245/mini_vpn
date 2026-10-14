# Public-path review repairs: local evidence and review stop

Date: 2026-09-04.

Status: **PARTIAL IMPLEMENTATION; TUIC SENDER REVIEW REQUEST CHANGES.
PUBLIC-PATH STAGE NOT PROMOTED. QUINN ROOT RUST NOT STARTED.**

The user authorized the public-path blocking, wrong-routing and panic repairs,
then the two new Quinn-root combination repairs. This result owns the current
local evidence and decision-changing review failure. It is not a release or a
throughput result. The public entry remains `LegacyOwnedUpstream`; Knife16's
necessary-only R7 position and all external stop rules remain unchanged.

## Implemented and independently reviewed portions

1. `FakeIpPool::alloc` now returns `Result<Ipv4Addr, FakeIpPoolExhausted>`.
   Full capacity rejects a new name without changing either map, references,
   timestamps or allocation cursor. Existing-name lookup remains usable; only
   legitimate sweep/release can reclaim a mapping. The original full-pool
   eviction could overwrite the domain behind a referenced IP.
2. DNS maps this rejection to SERVFAIL, preserving ID, question and RD, with
   zero answers. A real full public pool is exercised through the extracted
   DNS packet-forging path; the retained target remains unchanged.
3. `build_udp_ip_packet` rejects payloads beyond the IPv4 UDP limit of 65,507
   bytes and returns `Option<Vec<u8>>`. The public downlink caller counts an
   invalid packet as a drop before injection. Exact maximum, one below, zero,
   endpoints and continued valid construction are covered. The former
   `expect` panic was reproduced on a 65,508-byte payload.
4. TUIC fragment reassembly bounds each incomplete payload to 65,507 bytes.
   Its per-entry byte count uses checked replacement arithmetic; completion,
   duplicate resize, fragment-count replacement, rejection, LRU and TTL retain
   exact ownership. The default 256-entry cap bounds payload to 16,769,792
   bytes plus bounded fragment metadata and a bounded completed output.

The independent Reviewer reported P0/P1/P2 `0/0/0` for fake-IP, DNS and packet
construction, and separately `0/0/0` for final reassembly. These portion-level
reviews do not approve the whole public stage.

Existing tests and complete implementation slices moved before behavior work:
fake-IP and DNS inline tests became private external Modules; the complete
DNS forging/hijack transaction moved to `client_tun/dns_hijack.rs`; five DNS
characterizations moved to a private leaf. UDP builder tests externalized;
complete TUIC send and reassembly implementations moved into private children
with their original characterizations. Stable caller APIs remain at their
parent seams except the intentional fallible allocation/build return types.

## Sender implementation and blocking review finding

The current sender performs one synchronous open poll and one bounded write
poll. A complete Packet may FIN; partial/Pending/error writes explicitly RESET.
Missing connection/credit becomes a counted UDP drop without per-pressure
stdout, application queue, retry or spawned sender. Real loopback tests prove
the call no longer suspends the shared caller, the peer sees complete bytes or
RESET, and unrelated connection traffic can progress.

**This implementation is not accepted.** The final independent review returned
P0/P1/P2 `0/0/1`, Request Changes at `src/tuic/udp_send.rs:93`:

- A peer can pre-grant large `MAX_STREAMS` and zero/small per-stream byte
  credit. With ACK/egress stalled, each UDP call can open another stream and
  RESET it. Zero-byte RESET metadata does not consume `MAX_DATA`.
- Vendored Quinn returns `unacked_data` on reset
  (`quinn-proto/.../streams/mod.rs:438`), but retains ResetSent/pending state
  until `reset_acked` removes it (`streams/state.rs:406`). Therefore the local
  send window does not bound this new pressure accumulation.
- The remaining bound is peer-controlled. Narrowing the result's wording to
  exclude transport memory does not close the code's local capacity gap.

Rust changes stopped after the reviewed 16-test green checkpoint. The next
attempt requires one local, bounded sender admission/retirement Authority,
including FIN/RESET custody and cross-connection residuals. Its reviewed
design and user confirmation must precede implementation. The project stop
comes from `AGENTS.md`'s failed-repair rule and `.learnings/ERRORS.md`'s
2026-09-01 explicit classification of a read-only finding as a repair-test
stop boundary. No first implementation GREEN test failed; this stop is the
independent capacity review finding.

## Exact local verification

Integrator Cargo commands used `--offline`; the TUIC Writer's commands used
the existing local dependencies without that flag. Loopback QUIC tests are local correctness
tracers, not TUN/WAN or throughput gates. Counts below describe selected tests,
not the complete library suite.

| Check | Outcome | Evidence |
|---|---:|---|
| Fake-IP original baseline / pure Move | 9 / 9 green | `/tmp/mini_vpn_fake_ip_repair/baseline.log`, `move-fake-ip.log` |
| DNS original baseline / pure Move | 5 / 5 green | same directory: `dns-baseline.log`, `move-dns.log` |
| Fake-IP causal RED / first GREEN | overwritten live route / 10 green | `exhaustion-red.log`, `exhaustion-green.log` |
| DNS SERVFAIL RED / GREEN | absent variant / 6 green | `servfail-red.log`, `servfail-green.log` |
| Final fake-IP / DNS Cargo tests | 10 / 6 green | `fake-ip-cargo-final.log`, `dns-cargo-final.log` |
| DNS forging before / after Move | 5 / 5 green | `/tmp/mini-vpn-repairs-forge-baseline.log`, `forge-move.log` with the same prefix |
| UDP builder before / after Move | 4 / 4 green | `/tmp/mini-vpn-repairs-udp-builder-baseline.log`, `/tmp/mini-vpn-repairs-builder-move.log` |
| Builder causal RED / first GREEN / boundary suite | old panic / 5 / 6 green | `/tmp/mini-vpn-repairs-builder-{red,green,final}.log` |
| Actual full-pool DNS caller | 1 green | `/tmp/mini-vpn-repairs-dns-capacity-integration.log` |
| DNS final characterization / injection counters | 5 / 1 green | `/tmp/mini-vpn-repairs-dns-caller-final.log`, `/tmp/mini-vpn-repairs-dns-injection-final.log` |
| TUIC pre-Move / pure Move | 11 / 10 green; baseline includes one extra selector test | `/tmp/mini_vpn_udp_baseline.log`, `/tmp/mini_vpn_udp_move.log`; normalized-body evidence in `/tmp/mini_vpn_udp_move_provenance.log` |
| Sender causal RED / first GREEN | single poll Pending / 4 green | `/tmp/mini_vpn_udp_pressure_{red,green}.log` |
| Reassembly causal RED | overlong single fragment incorrectly accepted | `/tmp/mini_vpn_udp_reassembly_red.log` |
| Final sender + reassembly, after test-leaf Move | 16 green | `/tmp/mini_vpn_udp_final.log` |
| Knife16 / repository test provenance | 159 / 13 green | `/tmp/mini-vpn-repairs-public-provenance.log` |
| `cargo check --offline --lib` | green | `/tmp/mini-vpn-repairs-integration-check.log` |
| Root-owned changed-file rustfmt / `git diff --check` | green | `/tmp/mini-vpn-repairs-public-fmt.log`; diff command exit 0 |
| TUIC changed-file rustfmt | green | `/tmp/mini-vpn-repairs-tuic-fmt.log` |

The TUIC final command was `cargo test --lib tuic::udp_ -- --nocapture`.
The sender RED filter was `public_udp_stream_pressure_settles`, its first
GREEN filter `tuic::udp_send::tests`, and the reassembly RED/GREEN filters
`reassemble_rejects_overlong_payload` / `tuic::udp_reassembly::tests`, all
with `cargo test --lib ... -- --nocapture`. The combined provenance command
was `cargo test --offline --test repository_test_module_provenance --test
knife16_owned_leg_provenance`.

The initial filter `client_tun::tests::dns_hijack` selected zero tests and is
not counted; the corrected forging and integration checks above are the
evidence. A temporary standalone harness's initial module path error was
corrected only in temporary harness layout, before behavior implementation.

Repository provenance retains all 162 original TUIC tests by exact moved-name
mapping plus the unchanged remaining count. It retains all 338 original
client-TUN tests across the parent and moved DNS characterization leaf. The
three extracted inline-test allowlist entries were removed.

The code-growth ratchets have **not** been promoted because the combined public
stage has an open review finding. Current observed shrink is
`client_tun.rs 16613 -> 16561`, its test index `14301 -> 14230`,
`tuic.rs 8172 -> 7989`, and its test index `5499 -> 5390`. These are observations,
not new grandfathered permissions. Lower the exact inventory only after the
pending sender repair and independent re-review are green, then run governance.

The earlier whole-project Clippy review failed on existing lint debt; this
stage does not claim all-target/all-feature Clippy clean. No unrelated lint
repair or broad suite/external acceptance was attempted.

## New Quinn root boundary

The narrow architecture in
`2026-09-04-client-root-combination-repair-architecture.md` passed independent
read-only review at P0/P1/P2 `0/0/0`, Approve. The reviewed draft SHA-256 was
`bbb106caea305614c414753aa08ecf14ffed72a1c6d5d2124a9c1878246a6725`.

It specifies complete driver-registry coverage before parking, preserved
producer cursors across bounded slices, and one retained eligible ACK-stall
decision across late final ACK and renewed replay. Root implementation and its
causal endpoint tests have **not started**, as public-path repairs come first.
The two root findings remain open. Architecture acceptance is not code evidence.

## Provenance and resumption

Preexisting dirty and untracked work is preserved. The task baseline is at
`/var/folders/l8/np2lxkgd3wg_dk5mbm9cpry40000gn/T/mini-vpn-review-repairs-erdila64`;
the TUIC Writer's pre-Move baseline is at
`/var/folders/l8/np2lxkgd3wg_dk5mbm9cpry40000gn/T/mini_vpn_udp_baseline_ko0ok4pa`.
Temporary artifacts are supporting local evidence, not durable release assets.

Resume with the current sender capacity design and same independent Reviewer;
require user confirmation before a second repair attempt. Keep a single Writer
on TUIC, and a separate later Writer on the overlapping new-root Modules.
After sender closure, promote shrink/provenance and the public result, then
execute the root design's Move-first/RED/GREEN sequence. No commit, push,
macOS TUN, VPS, WAN, throughput, M2 or completion email was performed.

The next sender proposal is in
`2026-09-04-tuic-udp-stream-retirement-capacity-amendment.md`: 256 shared
retirement slots, at most four inspections per call, an atomic 1-RTT admission
adapter, and weak tickets with a post-State-destruction token. This requires a
narrow local Quinn-fork extension; no Rust from that proposal has been written.
Architecture review and the pending explicit user confirmation are both
prerequisites to the next attempt.

The same Reviewer approved this sender design and its narrow Quinn-fork scope
at P0/P1/P2 `0/0/0`. The reviewed draft SHA-256 was
`046263ff283ae66993fd1a7ea9e9f501750ba4397e1f51236d71fba46653f061`.
Both draft-design findings (premature close retirement and early-data identity)
are closed. User confirmation is still pending; the current Rust sender's P2
remains open until implementation and code review pass.

## Stopped-tree source fingerprints

| Source | Lines | SHA-256 |
|---|---:|---|
| `src/fake_ip.rs` | 174 | `a056ad46aaa347eb69861090ba7773228f52d08018c7cafb198535f6c2ab79e5` |
| `src/dns.rs` | 109 | `582a34054b248d8842473fdfd7248c06f9f1d87478c13bae80749d2f9ba723ea` |
| `src/udp_relay.rs` | 92 | `a7620640bbfc1cb3c80b02d204503e7ffc8787f90c2193529d30fd047d6d2c20` |
| `src/client_tun.rs` | 16561 | `f9b2a960ba00cf5e629095571a6e6f11ccae3832872b6f9e14c958fef7e32529` |
| `src/client_tun/dns_hijack.rs` | 69 | `9d67b51cb1a0db1f048e111e88b159a2df790eb96b285b12ea96ab9be08bd696` |
| `src/tuic.rs` | 7989 | `8b4855d47ef310aa5de854795a247f274f72c55028bb191aad2be79cec26289f` |
| `src/tuic/udp_send.rs` | 122 | `d97489797ea226ee7d875236420aa00c90b9b11c466d921cb983932a26e10710` |
| `src/tuic/udp_reassembly.rs` | 138 | `f286278b4ec2b115db5042dcfc1057783b93172628b5576c0d2a6ebf82a64d43` |
| `tests/repository_test_module_provenance.rs` | 418 | `687c8e57723eeb9677d132af61f8183bb38391b961f6c715d874dea0d18b5363` |
