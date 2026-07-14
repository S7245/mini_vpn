# Knife14 H10d16 Product Regression Concurrency Failure

Date: 2026-07-13
Source: `a54fb171ad57dc48902a79f9d31bd08d2ac41802`

## Verdict

Task 12 step 4 stopped at the local TCP concurrency gate. The normal
harness-enabled regression passed, but the existing ignored concurrency sweep
failed its unchanged `N=1024` all-complete assertion. No UDP throughput sweep,
VPS service setup, sustained `60s` reverse flow, VPS multi-flow, live UDP,
Linux fake-IP DNS, or TUN rearm run followed the product failure.

Gate B remains accepted, but the product regression gate is **not passed** and
the final stable-`170 Mbit/s` claim remains unauthorized.

## Clean Reproduction

The run used an isolated clone at:

```text
/tmp/mini_vpn_step4_a54fb17.9L8Abb
```

The clone was clean and resolved exactly to `a54fb17`. No project source,
D16 setting, MTU, pool, QUIC window, chunk size, or self-wake setting changed.
No `MINI_VPN_*` variable was inherited by the local harness process.

The normal gate passed:

```text
cargo test --features harness -- --nocapture
lib: 609 passed, 0 failed, 2 ignored
concurrency_harness: 10 passed, 0 failed, 4 ignored
concurrent_64_all_complete: 64/64
quic_mode_highrate_downlink_intact: 300/300, zero loss
```

The explicit heavy gate was:

```text
cargo test --features harness --test concurrency_harness \
  concurrency_sweep_report -- --ignored --nocapture
```

Its results were:

| Connections | Completed | Wall | Relay segment | TCP opens |
|---:|---:|---:|---:|---:|
| 64 | 64 | 507.5 ms | 426.1 ms | 64 |
| 256 | 256 | 10.310 s | 9.815 s | 256 |
| 1024 | 733 | 120.000 s | 101.271 s | 1024 |

The `N=1024` assertion failed with `left: 733`, `right: 1024`. Many remaining
relays reached the existing `90s` idle timeout before the scenario ended.

## Failure Discrimination

The result rejects several weaker explanations:

- `tcp_opens=1024` proves all intended mock upstream opens occurred;
- the listener cap did not fire, and observed dirty/listener occupancy was far
  below the `4096` global cap;
- zero synthetic CPU burn was configured;
- the normal 64-flow, HoL-isolation, slow-open, UDP, fragmentation, D16 global
  saturation, ownership, EOF, DNS, fake-IP, and rearm tests all passed;
- the same harness historically completed `1024/1024` in about `2.3s`, with
  the relay segment reduced to about `70.8ms` after Knife2.

The active discriminator is the relay scheduler itself. Commit `8a85ce7`
added this operation inside the per-handle loop in `process_dirty_relay`:

```text
downlink_pressure_stats(dirty, socket_ctxs, sockets).total_pending
```

`downlink_pressure_stats` scans the dirty set and then iterates all sockets.
Calling it once for every handle in every dirty pass restores an
O(active-flows squared) relay cost. The current D16/default product path has
buffered-downlink disabled, and `publish_relay_read_credit_for_handle` returns
through its D16 or non-buffered branches without consuming the aggregate
pending value. The expensive scan is therefore dead work for the accepted
frozen profile.

This mechanism predicts every observed signal: opens succeed, listener
capacity remains available, relay time dominates wall time, progress slows
superlinearly from 64 to 256 to 1024, and late flows survive until idle
cleanup rather than failing at open.

## Proposed Modification Plan (Awaiting Confirmation)

1. Add a focused red test/instrumented seam proving one dirty pass does not
   perform one full aggregate-pressure scan per active handle when
   buffered-downlink is disabled. Preserve exact D16 ownership assertions.
2. Make aggregate pending computation conditional on the only consumer:
   buffered-downlink credit. For the frozen D16/non-buffered path, pass the
   unused neutral value without scanning. If the buffered path is retained,
   compute its aggregate once per pass or maintain it incrementally rather
   than rescanning all dirty handles and sockets per flow.
3. Run the focused test green, then the full `609+10` local gate, the explicit
   `64/256/1024` sweep, and the UDP throughput sweep. Require `1024/1024`, no
   ownership-cap violation, no unexpected reap, and relay cost back in the
   linear/active-set class before any VPS operation.
4. Perform a concentrated code review of all remaining
   `downlink_pressure_stats` call sites for equivalent nested scans. Do not
   change D16, MTU, pool, QUIC windows, chunk, self-wake, or lifecycle policy.
5. Only after the repaired local product gate passes, resume the still-unspent
   VPS sequence: `60s` reverse, TCP multi-flow, UDP/live-streaming, Linux
   fake-IP DNS, and TUN create/start/stop/rearm.

No implementation has been made. Confirmation is required because this is a
real product regression, not a preflight or configuration error.

## External State

No VPS was touched during this failed step. `.111` Shoes was not recreated,
`.27` did not start mini_vpn or a TUN, `.77` received no product-regression
traffic, and no macOS TUN test ran.

## Resolution Update (2026-07-13)

The user confirmed the proposed repair. The focused scan test was red then
green, and the D16/non-buffered path now skips the unused aggregate scan while
the buffered path computes it once per pass and updates it incrementally.

The clean explicit sweep now completes `64/64`, `256/256`, and `1024/1024`;
the 1024-flow case finishes in `12.294s` with `4.272s` in relay instead of
timing out at `733/1024`. Full local, harness, UDP, check, file-format, and
whitespace gates pass. The repair is therefore accepted locally.

The later VPS product sequence passed sustained reverse but stopped at a
separate forward-first QUIC/TUN/lifecycle failure. See
`2026-07-13-knife14h10d16-product-regression-repair-and-forward-failure.md`.
