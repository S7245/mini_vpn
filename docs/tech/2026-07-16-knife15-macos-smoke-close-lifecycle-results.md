# Knife15 macOS Smoke Close-Lifecycle Results

Date: 2026-07-16

Status: **ROOT CAUSE ACCEPTED; LOCAL REPAIR AND BOUNDED RUNNER PASS; CLEAN
SHENZHEN REPLAY REQUIRED**

## Evidence And Operation

The synchronized clean rearm bundle is:

- `/tmp/mini_vpn_knife15_macos_20260716_100056.tar.gz`;
- SHA-256:
  `baee8f2fd961057e379202ec85688e575f5774d654f83c63f679ba1c2753cdaa`;
- source: `c50613c`;
- runner SHA-256:
  `c323f25fa504668cef3a15ada73ea402661f7191576cc83c9f329e6537f03810`;
- release binary SHA-256:
  `125f4cbcd83d34889b3d4b46d20d0f1e791b181c3a1568769d1ba06726bc09e1`.

The user operation was correct. Start requested at `10:00:56Z` and became
ready on `utun5` at `10:00:57Z`. Smoke started at `10:01:15Z`. After it failed
to complete, the user interrupted it; the runner recorded forward failure at
`10:19:30Z`, and snapshot/stop/cleanup completed at `10:20:39Z`.

The Target route remained on `utun5` and the Exit route remained on physical
`en0`. A separate `curl ipinfo.io` still reporting Shenzhen is expected:
Knife15 installs a target-only route and does not replace the Mac's default
public egress. No route, password, cleanup, or operator error caused the hang.

## Exact Local Failure

The first smoke relay opened normally and its writer accepted the exact `37B`
iperf control request. For the following `11.045s`, the relay showed:

- `rx_bytes=0`;
- `rx_stream_frames=0`;
- `pending_cause=connection_rx_no_stream_frames`;
- thousands of endpoint self-wake polls without remote stream progress.

The terminal sequence then was:

1. the D16 downlink queue became closed and installed a provisional
   `remote_eof` close;
2. `finish_deferred_relay_close_if_drained` called `socket.close()`, queuing a
   local FIN and moving smoltcp from `Established` toward `FinWait1`;
3. the relay's later `Closed(remote_read_failed)` event reached
   `handle_relay_closed` in the same epoch;
4. the old handler immediately rearmed the slot, and `rearm_socket` called
   `socket.abort()` before `iface.poll + flush_tx` could deliver the FIN.

Local iperf3 therefore received neither FIN nor RST. Its own requested traffic
duration could not start or finish, and the old smoke wrapper had no child
deadline, so it waited until manual interruption. The empty
`tunnel-forward.json` is consistent with a foreground iperf process that never
reached structured completion.

This is a product lifecycle defect, not slow Shenzhen capacity. It is also
independent of endpoint pacing: endpoint conservation remained within
`61,440B`, there was no pacing block, and no TUN flush/pump/resource growth
signal selected a local pressure branch.

## Packet-Direction Discriminator

The bounded Exit capture
`/tmp/knife15-shenzhen-clean-rearm-20260716T0751Z.first.pcap` contained 289
packets between `18:00:57.573` and `18:09:48.609 +0800`. It showed
bidirectional UDP `8443` on the fresh Shenzhen source port `63665`, including
handshake/recovery bursts around `18:00:57`, `18:02:58`, and `18:03:28`, plus
later small heartbeat exchanges.

This rejects a permanent blackhole tied only to the old M0 five-tuple. The Exit
sing-box log also recorded authentication timeouts, and ICMP intermittently
lost one or two of three probes while the local gateway stayed clean. The WAN/
QUIC path can still be lossy or transiently unstable; this result does not
accept transport stability or M0. It only proves packet direction is no longer
the blocker for repairing the independently demonstrated local close bug.

## RED To GREEN Repair

Implementation commit `9f68435` changes two owned seams.

First, `handle_relay_closed` detects an existing same-epoch
`pending_relay_close` and coalesces the later event instead of aborting the
socket. The slot stays `Closing`, remains dirty, and continues through the
existing bounded close-drain state machine. If the provisional cause was
`remote_eof` and the later event is non-clean, the stored close is upgraded so
final diagnostics preserve `remote_read_failed`. Epoch guards, tail draining,
and bounded grace behavior remain unchanged.

Second, each standalone smoke iperf command now has a hard timeout equal to
`duration+30s`. The helper captures output, sends TERM then KILL if required,
returns status `124`, and leaves the TUN running for status/snapshot/stop
evidence. The manifest records the timeout contract.

The deterministic TDD seam uses the complete in-memory TUN path with a peer
smoltcp TCP client and an injected D16 reader failure after the control write.
Before the repair it ended at:

```text
state=Established active=true may_recv=true
```

After the repair, the same local client observes EOF/reset within the
two-second acceptance bound. The timeout helper's self-test also proves that a
30-second child is terminated at one second with status `124` under the actual
macOS Bash 3.2 environment.

## Local Gates And Review

All required local gates pass:

- focused D16 remote-failure, relay-close, deferred-close, and remote-EOF
  tests;
- all-target library tests: `640 passed`, `3 ignored`;
- main tests: `2 passed`;
- `cargo check --all-targets --features harness`;
- `cargo build --release`;
- `cargo clippy --all-targets --features harness` with only established
  project/vendored warnings;
- Knife15, Knife14 low-RTT, US-client, and sing-box-control shell suites;
- Bash syntax, `cargo fmt --all`, and diff checks.

Focused code review found no unresolved P0/P1. No endpoint pacing, D16, MTU,
pool-size, QUIC-window, chunk, Cubic, GSO, self-wake, workload-rate, or receiver
SLI constant changed.

## Next Clean Shenzhen Gate

The source, runner, and release binary changed, so the accepted historical
baseline `/tmp/mini_vpn_knife15_macos_baseline_20260716_064427` and direct
directory `/tmp/mini_vpn_knife15_macos_direct_20260716_071534` cannot supply
formal provenance for the repaired binary.

The next sequence is:

1. synchronize commit `9f68435` to the Shenzhen Mac and build release;
2. run a fresh physical baseline with mini_vpn/TUN off;
3. run a fresh 300-second direct continuity discriminator;
4. only on direct PASS, run one start and bounded smoke;
5. only on smoke PASS, enter formal M0; preserve any failure with status,
   snapshot, and stop.

Slow rates remain environment measurements and have no minimum threshold. A
smoke command failure now ends within its hard bound; it must be diagnosed from
the preserved evidence and cannot authorize frozen-constant tuning. M1 remains
blocked until repaired-source M0 and an independent rearm both pass.
