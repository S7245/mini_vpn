# Knife14h plan - concurrent relay pump

> Spec: `docs/tech/2026-07-01-knife14h-concurrent-relay-pump-spec.md`.

## Task Tree

1. TDD guard
   - Add a relay stream whose write side remains pending.
   - Make its read side produce data only after the write side has been polled.
   - Assert `run_relay` emits a `Data` event before the write timeout closes the relay.

2. Relay pump split
   - Split `RelayStream` into read/write halves.
   - Move local-to-remote writes into a dedicated writer task.
   - Send writer progress/close signals back to the parent relay task.
   - Keep one parent-owned close event to the main loop.

3. Verification
   - Run focused relay tests.
   - Run full `cargo test --lib`.
   - Run clippy with harness features.
   - Run `git diff --check`.

4. Stage review and commit
   - Review lifecycle, timeout, and task cleanup behavior.
   - Commit as one knife14h stage.
