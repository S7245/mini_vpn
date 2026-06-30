# Knife14d plan — TDD task breakdown

> Companion spec: `docs/tech/2026-06-30-knife14d-downlink-reap-open-spec.md`.
> Rhythm: each task goes red -> green -> stage code-review -> commit. Push after each commit if remote is
> available.

## Task Tree

```text
T0 spec/plan
 ├─ T1 slow-open harness red test
 ├─ T2 pure TUIC async-open path
 ├─ T3 reap/open wording + invariant tests
 └─ T4 stage close gates + handoff update
```

## T0 — spec/plan

Red:

- Not applicable; this is the design commit.

Green:

- Add knife14d spec and plan.
- Record grounding and grill design-tree decisions.

Commit: `docs(knife14d): spec and plan for async tcp open`

Stage review:

- Check the spec does not claim post-14c US-client throughput results.
- Check no ADR/CONTEXT update is needed for implementation-only terminology.

## T1 — slow-open harness red test

Red:

- Add a harness scenario with two TCP flows:
  - A targets a port whose mock `open_tcp` blocks on a control signal before returning a stream.
  - B targets a normal port.
  - B must complete while A is still waiting to open.
  - After releasing A, A must complete byte-for-byte.
  - Open count must stay exactly two.
- Run only the new test and confirm it fails on the current inline pure-TUIC/default path.

Green:

- None in this task, beyond test scaffolding required to compile.

Commit: `test(knife14d): reproduce slow open blocking other flows`

Stage review:

- Verify the test is behavior-level through `run_event_loop`, not a direct private-state assertion.
- Verify the mock waits in `open_tcp`, not in the established relay path already covered by knife13.

## T2 — pure TUIC async-open path

Red:

- The T1 test is failing.

Green:

- Override `TuicUpstream::open_is_cheap()` to return `false`.
- Ensure the main-loop async-open path handles TUIC exactly like the existing Reality/Failover path.
- If needed, adjust `MockUpstream` for the new test so only the slow-open scenario forces async-open; keep
  existing harness behavior stable unless changing the default is the cleaner public behavior.
- Run:
  - the new slow-open test;
  - `stalled_tcp_uplink_does_not_block_other_flows`;
  - `cargo test`.

Commit: `fix(knife14d): move tuic tcp open off the main loop`

Stage review:

- Review stale-result, fake-IP acquire/release, and buffered-uplink ordering.
- Confirm no await with `socket_ctxs` or `sockets` borrow leaks across task boundaries.

## T3 — reap/open wording + invariant tests

Red:

- Add or extend unit tests showing:
  - active `HandshakePending` is not reaped while the socket is active;
  - inactive/closed `HandshakePending` is reaped and bumps epoch;
  - stale `HandshakeDone` after reap is ignored.

Green:

- Clarify comments and log wording from "handshake" to "async remote open" where the state is now shared
  by TUIC, Reality, and Failover.
- Keep `OpeningRemote` compatibility only for truly inline/test paths.

Commit: `test(knife14d): lock async open reap invariants`

Stage review:

- Check comments match the code after TUIC joins the async-open path.
- Check tests do not rely on smoltcp internals beyond observable socket active/closed state.

## T4 — stage close

Green:

- Run full local gates from the spec.
- Update `TODO.md` / `HANDOFF.md` with knife14d status and rerun instruction if code has landed.
- Perform final code-review over the branch diff and fix findings.

Commit: `docs(knife14d): record async open handoff`

Final acceptance request:

- Ask the user to rerun `scripts/knife14b-usclient-tunnel-suite.sh` with the same US-client environment and
  send the generated markdown/tar bundle.
