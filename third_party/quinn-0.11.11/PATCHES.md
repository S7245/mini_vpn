# mini_vpn Quinn Patch Manifest

Upstream crate: `quinn 0.11.11`

Crates.io checksum:
`0c1a41e437b6bbd489372cd4971de128e85c855f56c57f283d20ff016cf7c0a8`

The source was copied byte-for-byte from the local crates.io package before
patching. Its unmodified suite passed `19/19` default unit tests with `3`
ignored stress tests, one ignored `many_connections` integration test, and
`1/1` doc test.

The endpoint-pacing stage may change only:

- `Cargo.toml`: pin direct `socket2` to the accepted lockfile version `0.6.3`;
- `src/connection.rs`: thin current-waker and real socket-outcome Adapter,
  authenticated per-connection current-socket rebind generation, and a hidden
  connection-local adapter for quinn-proto's existing path-state reset;
- `src/endpoint.rs`: immediate stateless-response reservation/socket outcome,
  plus rebind-time live-connection snapshot and previous-socket retention;
- `src/lib.rs`: re-export read-only endpoint-pacing config and snapshots and
  carry internal socket-generation/authentication events, plus the read-only
  receive-stream progress handle;
- `src/recv_stream.rs`: expose one cloneable, read-only ordered-delivery
  progress adapter with no recovery policy;
- `src/tests.rs`: prove authenticated current-socket generation on live
  rebind, same-identity/same-stream delivery across a path-state reset and
  Endpoint migration, and receive-progress lifecycle;
- `examples/README.md`: remove upstream trailing whitespace so the repository
  diff-check remains clean;
- focused tests that prove default equivalence, Adapter lifecycle, socket
  `WouldBlock`, fatal error/drop cleanup, peer isolation, and stateless
  response behavior.

The normalized upstream manifest permits `socket2 >=0.5,<0.7`. Converting
Quinn from a registry dependency to a path dependency otherwise changed its
direct resolution from the accepted `0.6.3` to the already-present `0.5.10`.
The exact compatibility pin preserves the pre-vendoring dependency graph; it
does not change socket behavior or pacing policy.

Rate, burst, fairness, reservation, and token policy remain in the pinned
`quinn-proto 0.11.16` Module. No UDP socket implementation, runtime, stream,
endpoint, crypto, congestion, loss, MTU, GSO, or protocol policy is replaced.
The modified suite passes `40/40` nonignored unit tests (`3` expected ignored)
and `1/1` doc test. The one `many_connections` integration test remains
expected ignored.
