# Knife14g plan - do not reap pending downlink

> Spec: `docs/tech/2026-07-01-knife14g-reap-pending-downlink-spec.md`.

## Task Tree

1. TDD guard
   - Add a focused predicate test for active `CloseWait` with `downlink_pending`.
   - Assert it becomes reapable once pending drains.
   - Assert truly inactive sockets still reap even if pending exists.

2. Reap predicate change
   - Teach `should_reap_slot` to preserve active sockets with pending downlink.
   - Keep existing async-open and inactive-socket guards intact.

3. Verification
   - Run focused reap tests.
   - Run focused relay tests.
   - Run full `cargo test --lib`.
   - Run clippy with harness features.
   - Run `git diff --check`.

4. Stage review and commit
   - Review byte-preservation vs leak risk.
   - Commit as one knife14g stage.
