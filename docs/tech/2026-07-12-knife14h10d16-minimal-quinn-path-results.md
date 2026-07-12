# Knife14 H10d16 Minimal Quinn Path Results

Date: 2026-07-12
Probe source: `8ea89256102271a4d586e363c48faa2295dc3f36`
Probe cleanup: `21dd361`

## Verdict

The test-only minimal Quinn reverse-stream probe passed from `.27` to `.111`
at `192.597 Mbit/s` receiver for 20 seconds. It used the exact Gate-aligned
transport profile: Quinn, Cubic, safe MTU 1200, the existing 8 MiB stream and
32 MiB connection/send windows, 8 MiB requested UDP socket buffers, and one
ordered bidirectional stream.

All 21 samples carried data. The minimum non-empty interval was `183.934
Mbit/s`, byte-pattern errors were zero, EOF was clean, and the client reported
zero lost packets, lost bytes, congestion events, or stream/connection data
blocking. The probe therefore clears raw UDP and the Quinn/Cubic/safe1200
ordered-stream layer as the cause of the mature TUIC `3-5 Mbit/s` burst/idle
failure.

Gate A remains frozen. This result does not execute TUIC Connect, sing-box
protocol handling, the mini_vpn pool/open/relay lifecycle, TUN, smoltcp, or the
D16 actor. The next sufficient discriminator is a direct TUIC Connect relay
that retains the exact transport profile but bypasses TUN/smoltcp/D16.

## Versioned probe

The probe is compiled only under `#[cfg(test)]`:

- bounded 32-byte request protocol with fixed-byte and duration modes;
- request limit of 2 GiB, duration limit of 30 seconds, and chunk limit of
  256 KiB;
- server-owned reverse ordered stream with fixed byte-pattern verification;
- completion ACK barrier and clean EOF assertion;
- one-second interval accounting and final Quinn connection statistics;
- real 32 MiB loopback RED/GREEN test;
- ignored, role-driven cross-host test using one identical release test
  binary on client and server.

The clean release test binary was built from the committed source on `.27`,
then copied unchanged to `.111`:

- binary: `target/release/deps/mini_vpn-f6e9da9ba27eeeb7`;
- SHA-256:
  `08ccf50aa2f70a1ecec977fb9ffc65f96f81170c789e4243e164e03a6eefba56`.

Certificate and key material were supplied to the transient `.111` server
through separate mode-0600 FIFOs. No credential or private key was stored in
the repository or evidence archives.

## Local and clean-host acceptance

The following tests passed locally and from a clean committed build on `.27`:

- request round-trip and invalid-frame rejection: `1/1`;
- real Quinn loopback, 32 MiB reverse stream and clean EOF: `1/1`;
- release test binary compilation: pass;
- product library check without the test-only module: pass;
- formatting and diff checks: pass.

The loopback receiver exceeded `170 Mbit/s`. The post-run cleanup commit also
changed probe RTT reporting from truncated milliseconds to microseconds and
bounded endpoint shutdown waiting to two seconds; it does not change product
code or the measured transfer.

## Cross-host result

Client `.27`:

```text
bytes=489095168
elapsed_ms=20315
receiver_mbps=192.597
intervals=21
zero_intervals=0
min_interval_mbps=183.934
pattern_errors=0
clean_eof=true
quinn_probe_result=PASS
```

The first interval reached `281.270 Mbit/s`; subsequent full intervals stayed
near `188-193 Mbit/s`. Client Quinn statistics were:

```text
sent_packets=118922
lost_packets=0
lost_bytes=0
congestion_events=0
tx_data_blocked=0
tx_stream_data_blocked=0
udp_rx_datagrams=418947
udp_rx_bytes=502676996
max_datagram_size=1162
```

Server `.111` sent the same `489095168` bytes in `20316ms`, or `192.594
Mbit/s`. It reported `73038381` lost bytes and `34028` congestion events while
still maintaining continuous receiver intervals. This matches the earlier raw
UDP edge near `193 Mbit/s`: Quinn adapted at the shaping boundary instead of
producing multi-second zero-rate gaps. The server loss is therefore relevant
path headroom evidence but not the TUIC burst/idle signature.

## Architecture consequence

The evidence ladder is now:

1. direct TCP baselines exceed `210 Mbit/s`;
2. raw UDP8443 is continuous at approximately `198 Mbit/s`;
3. minimal Quinn Cubic/safe1200 is continuous at `192.597 Mbit/s`;
4. mature TUIC Cubic/MTU1200 remains burst/idle at `3-5 Mbit/s`;
5. D16 local composition exceeds `170 Mbit/s` with ownership and EOF gates
   clean.

The unresolved boundary is between minimal Quinn and the product D16 reader:
TUIC Connect/sing-box stream service plus mini_vpn connection selection,
Connect open, and relay coupling. Reopening D16 ownership, actor cadence,
DrainOnly, EOF, TUN, MTU, broad windows, chunk size, self-wake, or raw VPS
tuning would skip the first untested seam.

## Next discriminator

Add one test-only direct TUIC Connect probe on `.27`:

- reuse `TuicUpstream` authentication and generic `open_tcp` against `.111`
  sing-box, with `tcp_pool=1` and the default ordered-join relay so auxiliary
  pool policy and all native/D16 readers are absent;
- bind one loopback-only TCP listener on `.27`; relay its single accepted
  socket directly to one TUIC Connect stream targeting `.77:5201` with bounded
  Tokio copy buffers, then run the ordinary iperf3 reverse client against that
  listener;
- keep TUN, smoltcp, the D16 queue/actor, and the product event loop absent;
- retain Cubic/safe1200, the 20-second reverse direction, interval reporting,
  exact source/binary hashes, and a strict `>150 Mbit/s` plus no-zero-interval
  gate;
- first RED/GREEN the loopback listener relay, bidirectional byte accounting,
  half-close/EOF, and bounded shutdown with a mock upstream; existing TUIC
  tests retain Connect framing coverage. Then run exactly one cross-host
  discriminator.

If this direct TUIC seam reproduces burst/idle, diagnose TUIC/sing-box stream
service and protocol timing. If it passes, move the next seam upward into
mini_vpn pool/open/relay integration while preserving the D16 architecture.

## Artifacts and cleanup

- client archive:
  `/tmp/mini_vpn_quinn_probe_8ea8925_client.tar.gz`
  (`1e0de7eb49adfdea74764c903264ccde904ccdd274e2a1aa04225bfa4b139135`);
- server archive:
  `/tmp/mini_vpn_quinn_probe_8ea8925_server.tar.gz`
  (`c1df41a937b2d5805c0d7bb09360aa3da94a5722a4e25a57d2da671e939f3e3d`).

The `.111` watchdog and server were stopped, UDP8443 was released, temporary
sysctls were restored, FIFO material and the remote binary were removed, and
remote build/evidence directories were deleted after verified local copy.
