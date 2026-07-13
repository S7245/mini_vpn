# Knife14 H10d16 Shoes/Modern-Quinn TUIC Results

Date: 2026-07-12
Probe source: `0f07406c07a81a373a9b3dc426c4ad0a7128400a`

## Verdict

Shoes `v0.2.7` with Quinn `0.11.9` proves that the external
`.111 -> .27` TUIC v5 path has sufficient continuous capacity for Gate A and
the `170 Mbit/s` Gate B target. The 20-second reverse probe reached
`192.665956 Mbit/s` receiver, all `20/20` intervals carried data, and the
minimum client interval was `153.099296 Mbit/s`.

This removes the remaining transport-capacity ambiguity. The earlier
`112.559 Mbit/s` Rust reference result was an old Quinn/server limitation, not
a path ceiling, and the `2-5 Mbit/s` multi-second starvation remains specific
to the tested quic-go server lineages on this external sender/path.

Composite Gate A did not run. The direct probe process exited failed because
the timed iperf data Connect ended with `Connection reset by peer`, even though
iperf produced a complete successful JSON result above the floor. Code review
found that this probe's blanket relay-error veto conflicts with the already
approved composite Gate A contract: timed capacity permits exact bounded
terminal-reset accounting, while the separate fixed-byte `64 MiB` subproof is
responsible for clean remote EOF. The gate-process correction must be TDD-locked
and confirmed before spending Gate A.

## Code-level reachability gate

Official Shoes `v0.2.7` source shows:

- TUIC v5 over Quinn `0.11.9` / quinn-proto `0.11.13`;
- direct per-Connect bidirectional copy with `32 KiB` buffers and no
  message-count staging queue or application pacing;
- `8 MiB` per-stream receive window, `20 MiB` connection receive window, and
  `16 MiB` send window;
- initial/minimum MTU `1200`, MTU discovery, GSO, and an approximately
  `8.6 MiB` requested UDP socket buffer;
- Quinn's default Cubic congestion controller;
- orderly EOF propagation through `poll_shutdown` on the QUIC send stream.

The `170 Mbit/s` target is `21.25 MB/s`. Even the `8 MiB` per-stream window
represents about `0.38s` of target data, far above the measured sub-millisecond
to low-millisecond path RTT. The source gate therefore showed a plausible
sufficient path before VPS traffic.

## Fixed experiment

- Shoes official GNU x86_64 release archive SHA-256:
  `134974a4640807bb6767bf4b35f7a71637cdbbb96b8bef622ef86cf197220aac`;
- extracted Shoes binary SHA-256:
  `160202f6744b14ba84b40c014f3754f7811642300df58df81f1399063a202147`;
- exact probe source commit `0f07406`, client Cubic/safe1200, pool 1,
  OrderedJoin, one QUIC connection, and two TUIC Connect streams;
- rebuilt Linux probe SHA-256:
  `d3210d1084546653d10e7b946e2aa13aeb5016a6456a9267902e21455f6219d1`;
- `.27` client, `.111:8443` temporary Exit, `.77:5201` target;
- one endpoint, two Tokio worker threads, zero RTT disabled;
- root-only one-shot config/certificate/key FIFOs, `--no-reload`, and no
  persistent secret-bearing file;
- temporary `.111` socket maxima `16 MiB`, defaults `1 MiB`, bounded service
  runtime, and independent restore watchdog;
- bilateral `tcpdump -i any -s128 -U` capture, port-only filter, 120-second
  budget, readiness and remaining-time assertions.

The rebuilt test binary differs from the historical
`11537685...` binary because Rust embeds the absolute clean build directory.
Source commit, Cargo lock, compiler (`rustc 1.96.1`), test target, and behavior
were fixed; all four direct-probe contract tests passed before deployment.

## Preflight and baselines

Shoes `--dry-run` consumed the in-memory/FIFO configuration and TLS material
successfully. A bounded one-second compatibility probe then authenticated,
opened both Connect streams, and received about `35.6 MB` of QUIC traffic. Its
intentionally short iperf boundary produced the same terminal reset later seen
in the official run, so it is compatibility evidence rather than a clean-close
proof.

Immediately preceding direct reverse baselines were:

- `.27 -> .77`: `231.306 Mbit/s` receiver;
- `.111 -> .77`: `221.523 Mbit/s` receiver.

## Official capacity result

Iperf JSON:

```text
sender_mbps=194.974612
receiver_mbps=192.665956
sent_bytes=493355008
received_bytes=481689600
retransmits=41138
intervals=20
zero_intervals=0
min_interval_mbps=153.099296
max_interval_mbps=288.060265
```

Client interval rates in Mbit/s:

```text
288.060,184.551,185.599,174.063,205.634,186.544,188.744,187.696,
181.403,193.987,187.706,187.695,187.683,181.404,193.987,153.099,
222.287,187.707,185.587,189.791
```

The target sender remained continuous at about `195 Mbit/s`. Its high
retransmission count and changing cwnd are consistent with the already measured
raw-path shaping/loss edge, but modern Quinn sustained useful delivery through
it without a zero client interval.

The client pcap captured `219168` packets and the Exit pcap captured `550758`;
both reported `0 packets dropped by kernel`. Client UDP receive-buffer errors
and send-buffer errors had zero delta. Exit `UdpInErrors`, `UdpRcvbufErrors`,
and `UdpSndbufErrors` all remained zero. The client host-wide `UdpInErrors`
counter increased by two while `UdpRcvbufErrors` stayed unchanged; because it
is global rather than socket-owned, it is retained as an attribution caveat,
not reported as a proven probe-socket drop.

Shoes used about `3.36s` CPU and peaked near `31 MB` memory. It remained active
with no restart. At the timed boundary Shoes logged peer closure and one stream
`connection lost`; the direct client reported one data-relay reset after the
complete iperf result.

## Code review

### P1: direct capacity probe conflates timed terminal and clean EOF

`src/tuic_direct_probe.rs` validates complete iperf JSON and then rejects any
relay error. `relay_one_connection` maps every `copy_bidirectional` error to an
undifferentiated failure, so a data-stream reset at the timed boundary vetoes
capacity even when every interval and the aggregate floor passed.

That is stricter than the accepted composite Gate A design in
`7a7ca04`/`f1627bc`:

- A-capacity is timed and may terminate with an exact bounded reset cause;
- A-clean is fixed-byte and must terminate through clean remote EOF with every
  queue/tail counter at zero.

The direct probe should expose terminal causes separately instead of silently
relaxing all resets or using its timed stream as a clean-close proof.

### No product-path regression found

This discriminator bypasses TUN, smoltcp, D16 readers/queues/actor/EOF,
auxiliary pool selection, and the product event loop. Its `192.666 Mbit/s`
result provides no evidence for changing the byte-owned architecture, MTU,
broad Quinn windows, chunk size, self-wake, or pool policy.

## Proposed correction before Gate A

1. RED/GREEN a pure direct-probe gate decision that distinguishes successful
   timed data-stream reset from control-stream failure and mid-window failure.
   Accept it only for capacity after complete iperf JSON, `>150 Mbit/s`, and
   every interval nonzero; retain the terminal cause in the result.
2. Keep fixed-byte/clean-close decisions strict: any reset, missing EOF, or
   nonzero tail remains a failure.
3. Replay the captured Shoes JSON plus terminal record through the corrected
   decision test. Do not repeat the 20-second Shoes discriminator.
4. After confirmation, redeploy the same Shoes release/profile and run one
   composite Gate A from clean committed source on `.27`: timed A-capacity,
   quiet barrier, then fixed `64 MiB` A-clean. Gate B remains frozen until both
   subproofs pass.

## Artifacts

- client bundle:
  `/tmp/mini-vpn-shoes-client.tar.gz`
  (`87ef38a50e7a1bc91fc5e6031e23fb845f05d21ccc6d283fc40842fbcbfe7e90`);
- server bundle:
  `/tmp/mini-vpn-shoes-server.tar.gz`
  (`b25a26826d18b49c356283767e125765a644ecddf7e71ea3ca971006d0f8ca8c`);
- target journal:
  `/tmp/mini-vpn-shoes-target.log`
  (`61a45bc3dff1900c6270e2a1c0cfafb27a174f31deeea9185c6b3b1705acb9e3`).

The server journal was UUID/password-redacted and pattern-scanned before
packaging. Bundles contain no configuration payload, environment file,
credential, certificate, private key, or sudo input.

## Cleanup

- Shoes, capture units, and restore timer are stopped; `.111:8443` is free;
- `.111` socket maxima/defaults are restored to `212992`;
- all runtime FIFOs, remote binaries, source trees, pcaps, and logs were removed
  after verified local collection;
- `.77` iperf3 remains active;
- no macOS TUN test ran and no product source changed.
