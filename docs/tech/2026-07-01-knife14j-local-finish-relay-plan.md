# Knife14j plan - propagate local finish without closing relay

> Spec: `docs/tech/2026-07-01-knife14j-local-finish-relay-spec.md`.

## Task Tree

1. TDD guard
   - Add a relay test where a local `Finish` command triggers write-half shutdown.
   - Assert remote-to-local data still reaches the main loop after that shutdown.

2. Relay command protocol
   - Replace the uplink channel payload type with a small relay command enum.
   - Keep dropped channel behavior terminal as `local_channel_closed`.

3. Main-loop finish detection
   - Track `local_fin_sent` in `SocketCtx`.
   - When an established local socket reaches `CloseWait` with no payload left, enqueue one `Finish`.
   - Keep the dirty handle active if `Finish` cannot be enqueued yet.

4. Verification
   - Run the focused new relay test.
   - Run focused relay and reap tests.
   - Run `cargo test --lib`.
   - Run clippy with harness features.
   - Run `git diff --check`.

5. Stage review and commit
   - Review half-close semantics, busy-loop risk, and dirty-set liveness.
   - Commit as one knife14j stage.
