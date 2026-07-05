# Knife14bp Server Evidence Defaults Spec

Date: 2026-07-05

## Context

Knife14bo cleared the current terminal-pending branch:

- reverse-first P1 stayed low at `25.3/24.0 Mbit/s`;
- `terminal_pending_reap=0`;
- `terminal_late_remote_payload=0`;
- `pending_at_close=0`;
- `tun_tx_dropped_delta=0`;
- QUIC loss/congestion was `0/0`.

Manual follow-up showed the `.77` iperf3 sender was itself slow and bursty,
matching tunnel throughput. However, the suite's own server-evidence artifact
was incomplete because `SERVER_EVIDENCE_CHECK=1` did not imply
`EXIT_SSH_HOST` or `TARGET_SSH_HOST`. This creates an avoidable attribution
gap before the next tx-queue / receive-window branch.

## Stage Goal

Knife14bp must make the known `.27 -> .33 -> .77` acceptance topology
self-contained for server evidence:

- when `SERVER_EVIDENCE_CHECK=1`;
- and the suite is using the known Exit host `.33` and Target host `.77`;
- and the operator did not set explicit SSH destinations;
- the suite should default SSH destinations to `ubuntu@43.153.32.33` and
  `ubuntu@43.130.32.77`.

If the default VPS key path exists on the client host, the suite should also
default the evidence SSH key path to that file. Explicit env values always win.

## Non-Goals

- No mini_vpn data-plane behavior change.
- No throughput tuning.
- No change to TUIC credentials, sing-box config, or iperf3 service config.
- No storage of passwords, private keys, TUIC UUIDs, or other secrets.
- No hard-coded target behavior outside this known Knife14 acceptance topology.

## Invariants

- Defaults are only applied for `SERVER_EVIDENCE_CHECK=1`.
- Defaults are only applied for the known host IPs already used by the suite.
- Explicit `EXIT_SSH_HOST`, `TARGET_SSH_HOST`, `EXIT_SSH_KEY`, and
  `TARGET_SSH_KEY` values are preserved.
- Missing default key files must not fail the suite by themselves; the suite can
  still use SSH agent/config or report SSH failure in the evidence section.
- Evidence reports must reveal whether host/key paths are set, but never print
  secrets or private key contents.

## Acceptance

Local acceptance:

- suite self-test proves known-host defaults resolve when server evidence is
  enabled;
- suite self-test proves explicit SSH env values are not overwritten;
- suite self-test proves defaults are not applied when server evidence is off;
- `scripts/knife14b-usclient-tunnel-suite.sh --self-test` passes;
- parser self-test remains green;
- `git diff --check` passes.

VPS acceptance:

- run one reverse-first P1 evidence suite from `.27`;
- the generated bundle contains `.33` sing-box evidence and `.77` iperf3
  journal evidence without manually passing SSH host envs;
- if throughput is still low, the bundle must make the next attribution branch
  explicit: target sender stall, exit forwarding/auth issue, or local
  tx-queue/receive-window pressure.
