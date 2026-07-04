# mini_vpn Project Memory

This file is project-level memory for Codex/agent sessions. Read it before making
plans, code changes, test requests, or architecture decisions in this repository.

## Product Goal

mini_vpn is the data-plane core of a future cross-platform VPN product. The
future App is the control plane and communicates with mini_vpn through APIs. The
target App platforms are iOS, Android, macOS, and Windows.

The user-facing requirements come from `Rules.md`:

- Local devices must connect to remote servers through VPN for TCP traffic.
- Local devices must use VPN for video/live-streaming UDP traffic.
- Local devices must support high-concurrency VPN connections.
- The hard target is high concurrency, high throughput, long duration, and
  stable quality.

## Technical Direction

- Support transparent TCP and UDP data-plane behavior, not only a narrow demo.
- Support arbitrary IP/domain/port targets. Do not hard-code remote targets.
- Keep TUN, fake-IP DNS, dynamic listener ports, relay lifecycle, and TUIC/QUIC
  behavior consistent as one system.
- Current mature transport direction is TUIC v5 over QUIC, interoperable with
  mature servers such as sing-box/Mihomo.
- TCP should ride QUIC streams/TUIC Connect. UDP should ride QUIC
  datagrams/TUIC Packet where available.
- Different VPS providers, RTTs, MTUs, congestion-control choices, and server
  configs must be handled through explicit config, preflight checks,
  observability, and bounded backpressure rather than one-off assumptions.

## Engineering Bar

- System stability is more important than pretty code.
- Measure before guessing. Prefer logs, counters, harnesses, and true endpoint
  acceptance over intuition.
- Preserve lifecycle correctness before optimizing throughput.
- A single failed relay/session must not tear down unrelated sessions.
- Avoid hot-path panics and unchecked `unwrap`/`expect`.
- Avoid duplicated handshake/protocol logic when a shared abstraction already
  exists.
- Bound queues, buffers, pending downlink data, and backpressure paths.
- Keep regressions visible: TCP, UDP, TUN, fake-IP DNS, TUIC, and scripts should
  not silently break each other.

## Work Rhythm

For meaningful changes, follow this sequence:

1. Grounding: read the relevant code, docs, specs, plans, and latest logs.
2. Grill/design tree: list plausible failure branches and reject weak guesses.
3. Spec: define the exact stage goal, non-goals, invariants, and acceptance.
4. Plan: split the work into small tasks.
5. TDD: add or update focused tests/harness checks before or alongside fixes.
6. Commit per coherent task when requested by the workflow.
7. Stage code-review: review for bugs, regressions, missing tests, and
   operational risk before asking for another VPS run.
8. Stage learning: after every meaningful stage, write a short self-improvement
   summary to `.learnings/LEARNINGS.md`. If a command, test, script, or VPS run
   failed in a way that changes future behavior, also record it in
   `.learnings/ERRORS.md`.

Small related issues found during review should be fixed together before a
concentrated integration test, instead of running a full VPS suite after every
tiny edit.

## Current Agent Role And Knife14 Position

The agent's role in this repository is data-plane engineering for the
mini_vpn core: read evidence, design small testable stages, implement Rust and
script changes, review risk, run or prepare VPS acceptance, and preserve
learning memory. The agent is not owning GUI, mobile App, or backend control
plane work in this route; those belong to separate product/control-plane
sessions. Current work must stay tied to the `Rules.md` data-plane goals:
transparent TCP, UDP/live-streaming, high concurrency, high throughput, long
duration, and stable quality.

For cold-start grounding, read this file, `Rules.md`, `HANDOFF.md`, `TODO.md`,
latest `.learnings/LEARNINGS.md` / `.learnings/ERRORS.md`, and the relevant
`docs/tech/2026-*.md` files. The older numbered `docs/tech/*.md` files are
historical background; do not load all of them by default. For the current TCP
throughput branch, prioritize the 2026 Knife14 documents, especially the
US-client results, downlink/backpressure/lifecycle specs, and the latest
results documents.

Current Knife14 summary, as of 2026-07-04:

- Stale TUIC TCP pool slot diagnosis is closed. The accepted fix was
  `7c683b0` plus acceptance record `afb18f5`, with the key signal
  `tuic-tcp-pool-reconnect conn=1 reason=stale_tcp_pool_slot` and no later
  stale-slot timeout.
- Knife14ar code commit `3baf476` made downlink egress pacing close-safe by
  default: product/default egress immediate budget is no longer the rejected
  `65536`, non-empty pending backlog forces immediate flush, and close logs
  include `tun_flush_deferred`.
- Knife14ar result commit `ba9225f` recorded the failed VPS acceptance. The
  clean reverse-first window had healthy direct baselines, no clean-window QUIC
  loss/congestion, `tun_tx_dropped_delta=0`, `tun_flush_deferred=0`,
  `send_slice_zero=0`, `send_slice_errors=0`, and `tun_flush_failures=0`, but
  reverse throughput was still low (`17.2/16.2 Mbit/s`) and later closed with
  `dead_slot_reap ... pending=224765 ... tcp_state=Closed ... can_send=false`.
- Therefore the next root is not iperf3, sing-box, stale pool slots, blunt
  egress pacing, clean-window QUIC loss/congestion, TUN syscall failure, or
  clean-window TUN qdisc drops. The remaining branch is mini_vpn local TCP
  downlink lifecycle / receive-window / close-drain / terminal pending
  accounting.

Next stage bias:

- Do not keep tuning the downlink egress pacer, TUN queue length, connection
  pool, iperf3, or sing-box before resolving the lifecycle branch.
- Knife14as should first write a design tree, spec, and TDD plan that
  distinguishes "expected terminal pending after local close" from "premature
  local close or receive-window behavior that caused low reverse throughput."
- Add deterministic tests and explicit metrics/accounting for terminal pending
  bytes, such as pending bytes reaped when `tcp_state=Closed` and
  `can_send=false`, before asking for another expensive VPS throughput run.
- If this branch does not produce a coherent causal explanation after focused
  tests and one scoped acceptance run, re-evaluate the architecture instead of
  continuing suffix-by-suffix tuning.

## Stage Learning Memory

Use the `self-improving-agent` pattern for project-local memory:

- At the end of each stage, summarize what happened, the outcome, what worked,
  what failed or was rejected, the likely root cause, and the next reusable rule.
- Include relevant commit IDs, log archive paths, scripts, and acceptance
  signals when they matter.
- Keep `.learnings/LEARNINGS.md` for mini_vpn-specific stage lessons.
- Keep `.learnings/ERRORS.md` for failures that should change future debugging,
  testing, or implementation behavior.
- Do not put secrets, TUIC passwords, private keys, or one-off noisy logs into
  learning memory.
- Promote a learning into this `AGENTS.md` file or `docs/tech/` only when it
  becomes stable project guidance rather than a single-stage observation.

## Test Strategy

- Use local deterministic tests and harnesses for code-logic bugs.
- Use VPS integration suites for real timing, pressure, TUIC/QUIC behavior,
  sing-box interoperability, MTU/RTT/path effects, and service-state issues.
- Before asking the user to run a VPS suite, include a concrete one-shot test
  checklist and preflight service checks for the relevant VPS hosts, especially
  `.33` and `.77` when those hosts are part of the test.
- If a VPS service looks unhealthy, tell the user to inspect or restart it
  before running the expensive test.

## VPS Acceptance Test Access

Current acceptance hosts:

- Client VPS `.27` (`43.172.75.27`): run mini_vpn from
  `/home/ubuntu/mini_vpn`.
- Exit VPS `.33` (`43.153.32.33`): sing-box/TUIC host. Check service with
  `sudo systemctl status sing-box`; log file is `/var/log/sing-box.log`.
- Target VPS `.77` (`43.130.32.77`): iperf3 host. Check service with
  `systemctl status iperf3`; inspect logs with `journalctl -u iperf3`.

SSH access:

- From the Mac mini, use `ssh -i ~/.ssh/vpn ubuntu@<host>` for `.27`, `.33`,
  and `.77`.
- From `.27`, use `ssh -i ~/.ssh/vpn ubuntu@43.153.32.33` and
  `ssh -i ~/.ssh/vpn ubuntu@43.130.32.77`.

Operational rules:

- The agent may run smoke tests, pressure tests, and collect logs directly from
  these VPS hosts when the task calls for it.
- Do not store TUIC UUIDs, passwords, private keys, or other secrets in the
  repository, specs, learning memory, or final summaries.
- When a `.27` VPS suite may need `sudo -v`, start it in a true writable TTY
  session from the beginning and enter the sudo password only at the prompt.
  Do not rerun the same suite through non-TTY SSH after sudo has already asked
  for a password, and do not place sudo passwords in commands, scripts, docs,
  logs, repository files, reports, or summaries.
- For GitHub push from the Mac mini, do not keep retrying HTTPS origin when it
  fails with an interactive credential prompt. If SSH auth works, use a
  one-shot SSH push URL such as `git@github.com:S7245/mini_vpn.git`; ask before
  changing the repository's configured `origin`.
- sing-box logs on `.33` can grow quickly; inspect or truncate them deliberately
  when needed, and mention destructive log cleanup before doing it.

## Current Acceptance Bias

When investigating performance or reliability, prefer acceptance criteria that
show the system survives realistic pressure:

- high-concurrency TCP without event-loop stalls;
- UDP/live-streaming quality with low loss over sustained duration;
- bounded pending/downlink backlog and observable backpressure;
- no unexpected relay reaping while useful buffered data remains;
- clear metrics showing whether a bottleneck is local loop CPU, QUIC path,
  server config, VPS network, or application logic.


# Agent Behavior Guidelines
- **Model Consistency:** Keep using the currently selected advanced Codex model (such as GPT-5.5). Downgrading the model to a smaller or simplified version without my explicit authorization is strictly prohibited.
- **Quality Standard:** If you encounter difficulties, prefer using the "deep thinking" or "extended reasoning" mode. Prohibit giving incomplete code due to speed or quota constraints.


# Session & Context Management
- **Frequent Refreshes:** If the task takes too long to run or multiple repair failures occur, please automatically re-evaluate the current architecture.
- **Source of Truth:** Always refer to the project root's `AGENTS.md` and architecture design files. Do not rely on short-term memory. If the context is too large, ask me if a new session tree is needed to maintain code quality.

# Execution Plans
- **Self-Correction:** If the repair test fails, do not immediately begin random modifications. You must first analyze the reasons for the failure and output a proposed modification plan, then wait for my confirmation before proceeding to the next step.
