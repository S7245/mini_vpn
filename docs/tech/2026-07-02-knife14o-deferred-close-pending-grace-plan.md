# Knife14o plan - deferred close pending grace

Date: 2026-07-02

## Tasks

1. Add a red predicate test for `pending_relay_close + downlink_pending`:
   - preserve before grace;
   - reap after grace;
   - keep non-deferred inactive pending reapable.
2. Implement minimal state:
   - add a grace constant;
   - store `pending_relay_close_since_secs`;
   - reset it on rearm;
   - pass `now_secs` into the reap predicate.
3. Verify:
   - focused predicate/deferred-close tests;
   - `cargo test --lib client_tun`;
   - `cargo test`;
   - `cargo clippy --all-targets -- -D warnings`;
   - `git diff --check`.
4. Code-review:
   - byte-preservation path cannot be preempted by sweep;
   - hard reap still bounds impossible delivery;
   - stale epoch and rearm guards remain unchanged.
