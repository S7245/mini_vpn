# Knife14bf plan - TUN-drop-aware downlink feedback

Date: 2026-07-05

## Stage Goal

Close the Knife14be observability/control gap: TUN egress drops must not remain
only a post-hoc attribution when the smoltcp tx queue is below a large
auto-scaled high watermark.

## Tasks

1. Add a parser fixture for the clean 4MiB shape:
   - no downlink high-watermark pause;
   - `send_queue_max` below high;
   - runtime TUN drop delta present;
   - new TUN feedback pause present;
   - no terminal pending/reap.
2. Add Rust unit tests for the feedback decision:
   - positive drop delta plus local pressure pauses;
   - pause remains until pressure drains to low;
   - unavailable/reset/first/zero-delta samples do not pause;
   - low-watermark drain resumes.
3. Implement runtime wiring:
   - split ordinary downlink backpressure state from global pause state;
   - sample TUN egress drops on a short interval;
   - make the feedback pause additive with ordinary backpressure;
   - log `tcp-tun-egress-feedback` transitions.
4. Run local verification:
   - focused cargo tests;
   - parser self-test;
   - suite self-test;
   - shell syntax checks;
   - `git diff --check`.
5. Record stage learning and commit/push this coherent task.

## VPS Gate

Do not run the VPS suite until local tests pass and the diff review shows that
the feedback cannot suppress `global_rx` permanently. The first VPS run should
be reverse-first only with healthy `.27`, `.33`, and `.77` preflight.

## Current Progress Estimate

- Knife14bf local design: 35%
- Knife14 overall: 65%

The phase is not complete until local tests prove the new feedback accounting
and runtime state machine are correct. Knife14 is not complete until a scoped
VPS run shows clean reverse-first no longer hides the local loss point and
throughput is stable outside the previous low band.
