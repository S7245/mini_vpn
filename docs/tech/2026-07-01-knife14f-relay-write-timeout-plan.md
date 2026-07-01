# Knife14f plan - bound relay upstream writes

> Spec: `docs/tech/2026-07-01-knife14f-relay-write-timeout-spec.md`.

## Task Tree

1. TDD guard
   - Add a mock relay stream whose `poll_write` never completes.
   - Assert `run_relay` does not exit before the write timeout.
   - Assert it exits after the timeout and sends `Closed { reason: "remote_write_timeout" }`.

2. Relay implementation
   - Add a relay-local write timeout constant.
   - Wrap `stream.write_all(&payload)` in `tokio::time::timeout`.
   - Preserve the existing `remote_write_failed` path for immediate write errors.

3. Verification
   - Run focused relay tests.
   - Run full `cargo test --lib`.
   - Run clippy with harness features.
   - Run `git diff --check`.

4. Stage review and commit
   - Review lifecycle/timeout behavior.
   - Commit as one knife14f stage.
