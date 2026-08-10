# mini_vpn Quinn Proto Patch Manifest

Upstream crate: `quinn-proto 0.11.16`

Crates.io checksum:
`2f4bfc015262b9df63c8845072ce59068853ff5872180c2ce2f13038b970e560`

The source was copied byte-for-byte from the local crates.io package before
patching. Its unmodified library suite passed `265/265` tests.

mini_vpn changes are intentionally limited to:

- `src/config/mod.rs`: fixed endpoint pacing Parameter Object;
- `src/config/transport.rs`: default-off maximum paced-burst configuration;
- `src/lib.rs`: export the endpoint pacing Parameter Object;
- `src/connection/endpoint_pacing.rs`: pure pre-accounting byte-window,
  reservation, refund, socket-outstanding, and fake-time property tests;
- `src/connection/pacing.rs`: apply the maximum without changing refill/debt
  and characterize sub-millisecond multi-bucket refill;
- `src/connection/paths.rs`: new-path wiring and migration inheritance;
- `src/connection/stats.rs`: read-only pacing diagnostics;
- `src/connection/assembler.rs` and `src/connection/streams/mod.rs`: a
  read-only ordered receive-progress snapshot exposing the consumed prefix
  and buffered bytes beyond a missing prefix, without recovery policy;
- `src/connection/mod.rs`: pre-build reservation, traffic classification,
  per-datagram settlement, migration/cancel/detach, socket-batch attribution,
  pacing diagnostics, a read-only authenticated-packet count used by the
  Quinn live-rebind lifecycle, connection-local path-state reset, and one
  tagged successor service turn whose ACKs retain their exact send-time
  non-application-limited ownership, including authentication packets already
  in flight while excluding unrelated preexisting PLPMTUD probes, plus exact
  Endpoint-bulk ownership and per-packet application-writer
  transport-backpressure ownership for ordinary application-limited
  classification;
- `src/connection/send_buffer.rs` and `src/connection/streams/send.rs`,
  `state.rs`, and `mod.rs`: one bounded per-stream transport-write-demand bit
  plus an exclusive accepted-offset boundary with an O(1) connection
  aggregate, retained across Writable delivery and an intervening connection
  worker turn until the exact accepted prefix is cumulatively acknowledged or
  the stream becomes terminal; only packets actually carrying bytes below
  that stream's boundary inherit the demand, so cancelled writers cannot lend
  ownership to unrelated streams or later same-stream bytes;
- `src/endpoint.rs`: create one fresh service per endpoint, attach stable
  connection/path keys, and pre-account stateless responses;
- `src/tests/mod.rs` and `src/tests/util.rs`: endpoint isolation, default-off,
  real GSO accounting, socket-outcome behavior, successor-turn ACK/loss,
  preexisting authentication/PLPMTUD separation, and realistic-RTT
  service-turn, first-128KiB bounded-ordinary-loss reachability,
  Endpoint-blocked, and writer-Blocked business congestion-window ownership
  tests.

No quinn-udp, congestion-controller algorithm, MTU-discovery, crypto, loss
timer, or stream flow-control implementation is replaced. The modified suite
passes `326/326` unit tests and `3/3` doc tests.
