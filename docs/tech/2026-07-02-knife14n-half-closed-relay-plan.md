# Knife14n plan - half-closed relay state

Date: 2026-07-02

## Tasks

1. Add focused red tests:
   - post-Finish relay uses a short idle timeout;
   - `CloseWait + local_fin_sent + uplink_tx=None` is preserved;
   - remote payload acceptance does not require `uplink_tx` after local finish.
2. Implement the state-machine changes:
   - add a half-closed idle timeout constant;
   - treat closed uplink after `Finish` as expected;
   - keep remote payloads valid in the read-only relay state.
3. Verify:
   - `cargo test --lib client_tun`
   - `cargo test`
   - `cargo clippy --all-targets -- -D warnings`
   - `git diff --check`
4. Code-review:
   - no unbounded relay leaks;
   - no premature close while remote reads are progressing;
   - no regression to stale epoch / rearm guards.
