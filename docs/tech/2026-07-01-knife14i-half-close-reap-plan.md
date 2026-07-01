# Knife14i plan - preserve live half-closed relay

> Spec: `docs/tech/2026-07-01-knife14i-half-close-reap-spec.md`.

## Task Tree

1. TDD guard
   - Add a focused `should_reap_slot` test for active `CloseWait` with `uplink_tx=Some` and empty pending.
   - Assert the same `CloseWait` becomes reapable after `uplink_tx` is removed.

2. Predicate fix
   - Change only the active `CloseWait` branch.
   - Preserve inactive-socket reap and stuck inline-open reap.

3. Verification
   - Run focused reap tests.
   - Run focused relay tests.
   - Run `cargo test --lib`.
   - Run clippy with harness features.
   - Run `git diff --check`.

4. Stage review and commit
   - Review byte-preservation vs slot-leak risk.
   - Commit as one knife14i stage.
