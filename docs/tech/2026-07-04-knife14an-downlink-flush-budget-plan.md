# Knife14an plan - bounded downlink flush budget

> Spec:
> `docs/tech/2026-07-04-knife14an-downlink-flush-budget-spec.md`.

1. Add red unit tests.
   - Budget helper caps a large pending slice to the configured max.
   - Budget helper never returns more than pending bytes.
   - Parser defaults invalid, zero, below-minimum, and above-maximum values.
   - Parser accepts a 1MiB override for A/B compatibility.

2. Implement config.
   - Add `DEFAULT_DOWNLINK_FLUSH_MAX_BYTES = 256 * 1024`.
   - Add min/max constants.
   - Add `downlink_flush_max_bytes` to `TunRuntimeConfig`.
   - Parse `MINI_VPN_DOWNLINK_FLUSH_MAX_BYTES` in `from_env`.
   - Print the active budget at startup.

3. Wire the budget into downlink delivery.
   - Pass the budget from `run_event_loop` into `handle_remote_payload` and
     `process_dirty_relay` / `process_listener_activity`.
   - Change `flush_downlink` to call `send_slice` with
     `downlink_pending[..budgeted_len]`.
   - Leave unaccepted bytes in `downlink_pending`.

4. Update the US-client suite.
   - Default `MINI_VPN_DOWNLINK_FLUSH_MAX_BYTES=262144`.
   - Record and pass it into the client command.

5. Verify locally.
   - `cargo test --lib client_tun`
   - `bash -n scripts/knife14b-usclient-tunnel-suite.sh`
   - `bash scripts/knife14b-usclient-tunnel-suite.sh --self-test`
   - `git diff --check`

6. Stage review and learning.
   - Review for accidental byte drops, unbounded queues, and hidden defaults.
   - Record the local result and the exact next VPS checklist.
