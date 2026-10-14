# mini_vpn Project Instructions

This file contains stable project rules. It is deliberately not a chronological
status log. Read the compact current state in `docs/current/STATUS.md`; consult
the archived project-memory snapshot only when historical provenance is needed.

## Source-of-truth order

1. `Rules.md` defines the user-facing product requirements.
2. Accepted architecture specifications and ADRs define frozen design contracts.
3. `docs/current/STATUS.md` defines the current accepted position and next gate.
4. The latest stage result named by that status file owns exact test evidence.
5. `HANDOFF.md` and `TODO.md` are compact navigation aids, not independent history.
6. `docs/archive/project-memory/` and `.learnings/archive/` are read-on-demand
   provenance. Do not load them during a normal cold start.

If two current documents disagree, stop the promotion claim, inspect the latest
accepted result, and repair the compact documents together.

## Product and technical direction

mini_vpn is the data-plane core of a future cross-platform VPN product. The App
is the control plane and will target iOS, Android, macOS, and Windows.

The data plane must:

- transparently carry arbitrary TCP targets through the VPN;
- carry UDP video/live-streaming traffic through the VPN;
- support high concurrency, high throughput, long duration, and stable quality;
- keep TUN, fake-IP DNS, dynamic ports, relay lifecycle, and transport behavior
  consistent as one system;
- retain TUIC v5/QUIC interoperability with mature peers such as sing-box and
  Mihomo, while Knife16 adds the enhanced resumable/two-leg profile;
- use QUIC streams/TUIC Connect for TCP and QUIC datagrams/TUIC Packet for the
  compatibility UDP path where available; and
- handle RTT, MTU, congestion control, provider, and server differences through
  explicit configuration, preflight checks, observability, and bounded
  backpressure rather than hard-coded targets or one-off assumptions.

The agent owns mini_vpn data-plane engineering in this repository. GUI, mobile
App, and backend control-plane implementation are out of scope unless the user
explicitly opens a separate task.

## Engineering invariants

- Stability and lifecycle correctness outrank cosmetic cleanup and throughput.
- Measure before guessing. Prefer deterministic harnesses, counters, logs, and
  true endpoint acceptance over intuition.
- One relay, flow, leg, or session failure must not tear down unrelated work.
- Avoid hot-path panics and unchecked `unwrap`/`expect`.
- Reuse shared handshake/protocol abstractions; do not duplicate frozen logic.
- Bound queues, buffers, replay, pending downlink, timers, registries, and every
  backpressure path. Preserve exact ownership on retry and shutdown.
- Keep TCP, UDP, TUN, fake-IP DNS, TUIC, scripts, lifecycle, and provenance
  regressions visible.
- Preserve unrelated dirty-worktree changes. Existing changes belong to the
  user unless the current task clearly created them.
- Do not use destructive Git cleanup or force-push without explicit authority.

## Cold-start route

For a normal new session, read in this order:

1. this `AGENTS.md`;
2. `Rules.md`;
3. `docs/current/STATUS.md`;
4. `HANDOFF.md` and `TODO.md` if continuing implementation;
5. only the current architecture/result documents linked by the status; and
6. the top current entries in `.learnings/LEARNINGS.md` and
   `.learnings/ERRORS.md` when they are relevant to the stage.

Do not load the complete archived memory, old numbered technical chronicles, or
every historical result by default. The current status must name the smallest
evidence set needed for the next decision.

## Work rhythm

For a meaningful change:

1. Ground in the relevant code, current spec/plan/result, and latest evidence.
2. Build a failure tree with falsifiable branches and predictions.
3. State the exact goal, non-goals, invariants, acceptance, and stop rule.
4. Split work into small, reversible tasks at clear interfaces/seams.
5. Add one focused red/green tracer or harness check for the changed behavior.
6. Implement without widening scope; commit coherent tasks only when requested
   or when the active workflow authorizes commit completion.
7. Review correctness, performance, lifecycle, bounded backpressure, regressions,
   missing tests, and operational risk before external acceptance.
8. Update the current result/status and project learning memory.

Small related findings should be repaired and reviewed together before an
expensive integration run. If a repair test fails, first analyze the evidence
and present a concrete modification plan; wait for the user's confirmation
before starting the next repair attempt. Do not make random follow-up changes.

### Design exit and implementation progress

For a multi-stage goal, keep one executable remaining-work checklist in the
current plan. Classify each item as a mandatory prerequisite of the first
complete running path, a later vertical slice still required by this goal, or
work explicitly assigned to a later stage. State its blocking reason,
dependencies and completion evidence. Reclassification must not remove an
acceptance obligation or redefine the goal as a prototype.

Before extending a design phase, name its exit: the real entry and call chain,
the unique resource owner, core invariants, success/rejection/pressure/
cancellation/failure/shutdown dispositions, and the next falsifiable test.
Consolidate existing contracts instead of repeatedly adding prerequisite
sidecars or reviewing wording-only status promotions. A design finding counts
as progress only when it removes a real blocker, selects an executable decision,
or produces relevant test evidence.

Once necessary design gates and exact write scope are accepted, implement the
smallest complete vertical path through real components. Use actual compilation,
deterministic lifecycle tests and call coverage to challenge the design; fake
ports, success stubs, uncalled wrappers and compatibility tests with altered
semantics do not establish that path. If a frozen contract prohibits the needed
validation, propose its smallest explicit amendment. Do not bypass a hard gate
or indefinitely extend document-only work instead.

Progress reports distinguish design decisions, actual production/test changes,
tests really executed and their results, and remaining blockers/next executable
gate. State explicitly when only documents changed and what prevents code.
Document approval, hashes, links and audit scripts are not Rust functionality
or runtime acceptance. Retain evidence but reuse inspection tools rather than
copying equivalent audit scripts for each iteration.

## Structural convergence gate

Stop local patching and deepen the architecture when any of these signals is
present:

- one correctness invariant spans three or more independently owned
  authorities or components;
- a second repair in the same failure class exposes another ownership,
  shutdown, fatal, cancellation, capacity, or rollback seam; or
- a local change cannot close success, rejection, pressure, fatal,
  cancellation, and shutdown through one auditable settlement model.

Before behavior work resumes, converge the affected control authority,
transaction state, lifecycle, and ownership ledger at one composition root.
The accepted design must:

- make invalid combinations unrepresentable where practical;
- validate duplicated physical state as a bidirectional invariant rather than
  accepting one-way marker or slot checks;
- keep every fallible operation before the commit point and make ACK or
  publication the final infallible step;
- fail closed into a sticky, non-zero quarantine when interruption cannot be
  rolled back, never normalizing an uncertain transaction to clean zero;
- inventory messages, independently summed and stored bytes, ACKs, timers,
  actors, endpoints, and session authority without a production bypass; and
- lock success, rejection, pressure, fatal, cancellation, and shutdown with
  deterministic exact-ownership tracers before further feature wiring.

This convergence belongs on bounded control/lifecycle paths. Do not route the
DATA hot path through a global root, recompute full inventories per packet, or
add per-record allocation, encoding, or global locking solely for the audit.

A local repair remains appropriate when one owner fully contains the problem,
the change neither duplicates state nor adds a bypass, and one focused tracer
can cover its complete lifecycle. Operationally: fix the bug once, repair the
seam on the second recurrence, and require structural convergence before a
third patch in the same class.

If prerequisites repeatedly expand across slices without a compilable closed
boundary, perform one whole-scope architecture review and present the genuine
architectural tradeoff to the user. Preserve ownership, bounded resources,
error retention, growth limits and independent review; do not weaken them to
force apparent progress.

## Code growth and AI navigability gate

New and changed Rust code must preserve Module Depth, Authority Locality, and a
bounded cold-read context. Line count is a structural trigger, not permission
to split one coherent transaction into shallow pass-through Modules.

The executable ratchet is `tests/code_growth_governance.rs`:

- a new production Rust file may contain at most `1,500` lines;
- a new test leaf may contain at most `800` lines, while a new
  `src/<module>/tests/mod.rs` index may contain at most `300` lines;
- every production or test file already above its limit has one exact
  grandfathered line count; the inventory is shrink-only and new paths are
  forbidden;
- when a grandfathered file shrinks, the stage Integrator lowers its recorded
  count in the same promoted change and removes the entry when the normal limit
  is reached; feature Writers report the shrink but do not edit the governance
  inventory; and
- all current-repository Rust sources are governed, including externally
  declared positive-`cfg(test)` Modules; vendored/generated/tool-worktree and
  immutable archive trees are excluded, while every `#[path]` target must close
  inside the governed inventory and new source `include!` is forbidden; and
- existing `clippy::too_many_arguments` allowances across production and test
  sources have an exact shrink-only inventory. Adding one, or hiding it behind
  `clippy::all`, `allow(warnings)`, `expect`, or `cfg_attr`, is forbidden.

For every change that touches an oversized file:

1. Locate the exact symbols, callers, tests, accepted design contract, and
   Authority owner before reading or editing. Prefer `rg`, symbol-aware tools,
   and bounded line ranges over loading the entire file during cold start.
2. State the allowed files/symbols and the change axis in the task plan. One
   Writer owns one overlapping Module; parallel agents may review read-only or
   write only disjoint Modules.
3. Put separable new behavior in a private child Module when it forms a deep
   implementation slice. Keep the external Interface at the parent seam and do
   not trade file length for re-export, `pub(super)`, parameter, or callback
   sprawl. A net-zero line swap does not authorize higher Interface or Authority
   complexity.
4. Keep one complete success, rejection, pressure, fatal, cancellation, and
   shutdown transaction local to its Authority. If extraction would split that
   settlement model, keep it together and deepen the root first.
5. Separate behavior-preserving movement from behavior changes. Freeze a
   focused characterization/provenance tracer, move one coherent slice, rerun
   it, then make the behavior change in a later green-to-green step.
6. Do not add dynamic dispatch, global coordination, per-record allocation, or
   hot-path indirection solely to satisfy a source-size limit.

New private Module roots should document, in compact module-level docs, what
they own, what they do not own, their invariants, entry symbols, and where their
tests live. Manually maintained duplicate architecture narratives are not a
substitute. Any request to exceed a normal limit changes this governance
contract and requires explicit user approval before code is written; do not add
the path to a grandfathered inventory as a workaround.

## Skill-assisted workflow

Use the project skills as engineering gates:

- `diagnose` and `tdd` for every throughput or failure-repair stage;
- `improve-codebase-architecture` and `refactoring-patterns` when no correct seam
  exists, favoring branch-by-abstraction and behavior-preserving extraction;
- `clean-architecture`, `system-design`, and `ddia-systems` for replacement
  architecture, ownership, flow-control, fault tolerance, and capacity work;
- `code-review` after each coherent implementation stage;
- `self-improving-agent` after each meaningful stage;
- the Go review/concurrency/context skills when comparing mature Go clients; and
- `git-commit`/`release-it` only at their corresponding commit or release gate.

The relevant `SKILL.md` instructions remain mandatory when a skill is selected.

## Performance architecture gate

Before a stage claims a path toward `>30 Mbit/s`, `>100 Mbit/s`, or a data-plane
architecture replacement, complete a code-level reachability review:

- convert the target to bytes/second and compare it with loop, batch, flush,
  timer, message, byte, and queue capacities;
- inventory the exact end-to-end hot path from remote TUIC read through buffering,
  local TCP/smoltcp write, `iface.poll`, `flush_tx`, and TUN egress;
- classify the change as necessary-only or plausibly sufficient for the next
  gate; never market a necessary-only fix as a throughput solution;
- audit every old pacing, credit, writer, timer, and egress path that remains
  reachable; an active legacy egress path means replacement is incomplete;
- define counters/logs that discriminate remote read, buffer growth, writer
  cadence, smoltcp/TUN drain, QUIC/path behavior, and lifecycle/close-tail;
- lock the code invariant with deterministic TDD before a VPS run; and
- stop if capacity math or reachability cannot show a plausible sufficient path.

VPS acceptance is still required for final throughput because real QUIC, kernel,
TUN, smoltcp, RTT, and scheduler behavior cannot be proven statically. A VPS run
validates an already reachable design; it does not substitute for this gate.
After a failed run, compare expected invariants with observed counters before
changing code, and do not repeat a class of tweak already shown insufficient.

## Test and acceptance strategy

- Use deterministic local tests/harnesses for code and ownership invariants.
- Do not embed new Rust test implementations in production `.rs` files. Put
  private unit-test bodies in `src/<module>/tests/mod.rs` (and child Modules
  below it), leaving only `#[cfg(test)] mod tests;` at the production seam. Use
  root `tests/` only for integration tests, public-contract tests, and repository
  provenance gates. The legacy inline-test allowlist is shrink-only: extracting
  a Module removes its entry, and adding an entry is forbidden. Crate roots are
  the path-resolution exception: `src/lib.rs` and `src/main.rs` must use distinct
  explicit `#[path = "lib/tests/mod.rs"]` / `#[path = "main/tests/mod.rs"]`
  seams (or another collision-free external layout), because a bare `mod tests;`
  in either root resolves through the shared `src/tests.*` location.
  The executable allowlist ratchet covers directly declared inline Modules
  behind positive `cfg(...test...)` expressions. Standalone test items and
  `cfg_attr`- or macro-generated test code remain forbidden here by policy and
  review; the lexical ratchet does not claim to prove their absence.
- Use true endpoints for timing, pressure, QUIC/TUIC interoperability, MTU/RTT,
  kernel, TUN, and service-state claims.
- Do not request or run a macOS TUN/VPS/WAN/throughput/M2 gate until
  `docs/current/STATUS.md` explicitly says the architecture prerequisite is
  ready. Standing machine access is authorization, not evidence of readiness.
- Before an external run, provide a one-shot checklist, exact source/binary and
  configuration provenance, service/resource preflight, observability, cleanup,
  and fail-closed verdict rules.
- Prefer acceptance that demonstrates high-concurrency TCP without event-loop
  stalls, sustained low-loss UDP, bounded backlog, no false reap with useful
  buffered data, and counters that localize the limiting subsystem.

## Current architecture position

The authoritative current position is `docs/current/STATUS.md`. As of
2026-08-26, the R7 same-clock arrival prerequisite and bounded value-only policy
kernel are locally accepted as **necessary-only** on top of R6d. Physical B
probe/hint provenance, owner/root composition, the no-oracle harness, and full
R7a-d remain open. The public TUN entry remains on the legacy upstream; Task 4,
production owner/server, second ingress, R8-R9, UDP two-leg delivery, WAN
throughput, and the M2 one-second acceptance remain open. Do not run macOS TUN,
VPS, WAN, throughput, or M2 yet.

## External access and operational safety

Current standing hosts, used only when the current stage authorizes an external
gate:

- HK macOS: `ssh -i ~/.ssh/vpn xiaoou@192.168.133.109`, worktree
  `/Users/xiaoou/mini_vpn` (not the TCC-protected Desktop clone).
- Client `.27`: `43.172.75.27`, `/home/ubuntu/mini_vpn`.
- Exit `.33`: `43.153.32.33`, sing-box service and `/var/log/sing-box.log`.
- Target `.77`: `43.130.32.77`, iperf3 service.
- Independent Exit `.111`: `43.173.101.111`; temporary TUIC UDP `8443` work is
  authorized when an accepted plan calls for it.

Use a true writable SSH TTY for interactive sudo. Never persist or echo sudo
passwords, TUIC UUIDs/passwords, private keys, or other credentials. Preserve
fail-closed cleanup. Announce destructive log truncation before doing it.

For agent-owned long Mac M2/m2-frequency actions, send the established one-time
best-effort completion notice after the runner returns with
`printf "Subject: 执行结束~" | msmtp 870941563@qq.com`, without changing the
verdict or skipping status/snapshot/stop cleanup.

Standing Git authorization permits normal non-force push of authorized mini_vpn
work to the current branch. Do not force-push, publish unrelated commits, or
change `origin` without separate authorization. If Mac HTTPS push prompts for
credentials and SSH works, use a one-shot SSH push URL without changing origin.

## Project learning memory

After each meaningful stage, keep the current reusable lesson concise in
`.learnings/LEARNINGS.md`. Record failures that change future behavior in
`.learnings/ERRORS.md`. Include useful commit IDs, artifact paths, and acceptance
signals, but never secrets or noisy one-off logs. Move superseded chronology to
`.learnings/archive/` or stage result documents instead of allowing the current
files to grow without bound.

## Agent behavior and session management

- Keep the currently selected advanced Codex model. Do not downgrade to a
  smaller/simplified model without explicit user authorization.
- Prefer deep/extended reasoning when the work is difficult; do not deliver
  incomplete code because of speed or quota pressure.
- Re-evaluate the architecture when work runs too long or repeated repairs fail.
- Use this file and accepted architecture documents instead of short-term memory.
- If context is becoming unsafe, ask whether a new session tree is needed.
- At roughly 50% context, finish the current coherent task before interrupting.
  At its boundary, recommend a new session and provide a ready-to-copy handoff
  naming the Knife/stage/task, exact accepted/stop position, required sources,
  next authorized actions, frozen non-goals, test/stop rules, and whether VPS,
  macOS TUN, commit/push, and completion notification are authorized.
