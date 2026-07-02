# Knife14q plan - uplink write backpressure

## Tasks

1. Grounding
   - Use the `knife14p` suite log to confirm this is not a downlink high-water
     event: `global_rx_pressure_events=0`.
   - Confirm the failing branch is `run_relay_writer` per-payload
     `write_all` timeout: `remote_write_timeout attempted_bytes=1160`.

2. TDD
   - Replace the old expectation that a pending write closes after 5s.
   - Add/adjust tests so a pending write stays alive beyond the former
     per-write timeout and only exits through relay-level idle cleanup.
   - Keep the existing direct write-error test.

3. Implementation
   - Remove the per-payload timeout around `writer.write_all(&payload)`.
   - Keep `stop_rx` selectable while a write is pending.
   - Rename the remaining 5s cleanup budget so it describes writer stop/shutdown
     cleanup, not per-write progress.

4. Verification
   - Run focused client-tun tests first.
   - Run the full Rust test suite and clippy.
   - Run diff whitespace check.

5. Stage learning
   - Record that QUIC/TUIC stream backpressure must not be treated as a
     per-chunk failure.
   - Preserve the `knife14p` log path and follow-up observation for reverse
     downlink/half-close/reap behavior.
