# Knife14bh Stream-Gap A/B Plan

Date: 2026-07-05

## Tasks

1. Add `MINI_VPN_TUN_RX_DRAIN_BUDGET` to `TunRuntimeConfig`.
   - default `8`;
   - valid range `0..=64`;
   - `0` disables opportunistic drain;
   - log the effective value at startup.

2. Add behavior-neutral TUIC TCP stream pending diagnostics.
   - count `poll_read` pending returns per tracked stream;
   - rate-limit log output by the existing stream-gap threshold;
   - include target, connection index, stable id, stream id, pending gap,
     pending poll count, received bytes, and completed read count;
   - include pending counters in the close snapshot for post-mortem checks.

3. Update low-RTT parser.
   - include `tuic-tcp-stream-pending` in metric extraction;
   - summarize pending event count, max pending gap, and data-stream pending
     gap;
   - add `tuic_stream_read_pending` attribution when a data stream stays pending
     beyond the slow-RX floor.

4. Update US-client tunnel suite.
   - add `STOP_AFTER_REVERSE_FIRST_P1=0` default;
   - when enabled, run only the clean reverse-first P1 path, then final
     snapshots and bundle;
   - pass the drain budget env to the mini_vpn process and report it.

5. Local gates.
   - focused Rust tests;
   - parser and suite self-tests;
   - shell syntax checks;
   - `cargo test --lib` / `--features harness --lib`;
   - clippy.

6. Commit and push the diagnostic patch.

7. VPS scoped A/B.
   - default drain budget run;
   - disabled drain run;
   - parse bundles and record Knife14bh results.

## Stop Conditions

- Any failed local or VPS test must be analyzed before the next code change.
- If a fix requires changing relay architecture rather than instrumentation or
  env-gated behavior, stop for architecture confirmation.
