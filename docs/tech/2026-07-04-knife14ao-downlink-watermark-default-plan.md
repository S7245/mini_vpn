# Knife14ao plan - promote validated downlink watermarks

Date: 2026-07-04

1. Add the focused default-value regression test.
   - Assert `DownlinkBackpressureConfig::default()` is `524288/131072`.
   - First run should fail against the old `TCP_SOCKET_BUFFER_SIZE * 32/8`
     defaults.
2. Change Rust defaults.
   - Replace old computed defaults with explicit `512 * 1024` and
     `128 * 1024`.
   - Keep parser fallback, repair, and env override behavior unchanged.
3. Align the US-client suite.
   - Update help text and default exports to `524288/131072`.
   - Prefer one script-level default variable per value to avoid future drift.
4. Verify locally.
   - Run the focused test, then `cargo test --lib client_tun`.
   - Run shell syntax and suite self-test.
   - Run `git diff --check`.
5. Stage review.
   - Check for hidden behavior changes, stale docs, missing tests, and
     operational risk before any VPS rerun.
6. Learning and commit.
   - Record the default promotion and any failed command that should affect
     future work.
   - Commit a coherent task.
7. VPS acceptance decision.
   - If local review is clean, sync `.27`, explicitly source `.env`, and run
     the suite without high/low overrides to prove the product defaults are
     active.
