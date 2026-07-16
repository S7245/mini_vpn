# Knife15 macOS M0 Shared QUIC Timeout Results

Date: 2026-07-16

Status: **M0 FAILED; OPERATION CORRECT; SHARED ENDPOINT/UDP-PATH
DISCRIMINATOR ACTIVE**

## Evidence And Provenance

The synchronized Shenzhen bundle is:

- `/tmp/mini_vpn_knife15_macos_20260716_072536.tar.gz`;
- SHA-256:
  `dd8a28f0055f6d13b15204a1300824d82b5e67a95889975b551b2450a07ec525`;
- source: `c50613c`;
- runner SHA-256:
  `c323f25fa504668cef3a15ada73ea402661f7191576cc83c9f329e6537f03810`;
- release binary SHA-256:
  `125f4cbcd83d34889b3d4b46d20d0f1e791b181c3a1568769d1ba06726bc09e1`.

The archive checksum is exact and its regular-file structure is complete.
The manifest binds the accepted 1KiB baseline and 300-second direct result by
their exact hashes. Target and Exit were on physical `en0`; current-HK live
state is excluded from the copied Shenzhen evidence.

The user operation was correct:

- start requested at `07:26:07Z`, ready on `utun5` at `07:26:09Z`;
- smoke ran from `07:26:11Z` to `07:26:56Z` and passed;
- M0 was prepared and started at `07:26:57Z`, inside the direct freshness
  window;
- cycle 1 forward completed its 300-second client wait and failed at
  `07:31:58Z`;
- the user preserved the failed TUN, took a manual snapshot, and stopped it;
- cleanup completed at `07:36:48Z`.

This was not a password delay, stale direct result, wrong source/binary,
baseline mismatch, missing smoke, premature stop, or operator error.

## Exact Failure

Cycle 1 forward requested 300 seconds at the exact half-baseline rate,
`11,218,349 bit/s`. The client JSON contains:

- `Broken pipe` while sending the final control message;
- `10,485,760B` sent at only `0.279617 Mbit/s` averaged over 300 seconds;
- 300 client intervals, 292 of them zero;
- no structured server output.

The Target iperf service recorded only the early payload and then zero
delivery before reporting that the client unexpectedly closed. The failure is
therefore real transport interruption, not the prior sender-versus-receiver
observer mistake.

Both TUIC TCP pool connections shared one Quinn endpoint and failed together:

- conn0 carried the quiet iperf control stream;
- conn1 carried the forward data stream and accepted `5,242,917B` from D16;
- conn1 stopped receiving QUIC progress, remained pending for about `37.4s`,
  and ended `ConnectionLost(TimedOut)`;
- conn0 and conn1 both reported `closed=TimedOut`;
- later connection and UDP-driver reconnect attempts on the same live endpoint
  repeatedly hit the five-second handshake timeout.

The Exit sing-box service remained active with zero restarts. Its log records
both M0 TUIC-to-Target opens and no service-side crash or intentional close.
The Target iperf service remained active and recorded an unexpectedly closed
client. This rejects a `.33` or `.77` service restart.

## Negative Internal Discriminators

The failure is not attributable to the accepted endpoint pacing conservation
or local TUN resource branches:

- endpoint conservation stayed at or below `61,440B` and ended
  `61,440/0/0B` available/live/outstanding;
- endpoint `would_block=0`, maximum pacing delay `47us`;
- TUN pump queue high-water `118/500`, zero full waits and zero pump errors;
- TUN/local egress flush failures were zero;
- process FD and thread counts were flat;
- physical interface errors were zero;
- no log compaction or watchdog health failure occurred.

The preceding smoke did expose a stressed UDP/QUIC path: conn1 accumulated
`7,446` lost packets, `9,985,352` lost bytes, and `1,169` congestion events
while smoke still completed. During M0, Exit ICMP remained mostly available
but intermittently lost one of three probes. ICMP availability does not prove
UDP `8443` continuity.

## Open Branches

The bundle narrows the failure to the shared endpoint/UDP path but does not
yet distinguish two falsifiable branches:

1. the Shenzhen network/NAT or an upstream UDP classifier blackholed the old
   UDP five-tuple after the high-volume smoke;
2. mini_vpn's shared Quinn endpoint stopped receiving or servicing UDP, so
   every connection and reconnect on that endpoint failed together.

Changing pacing constants, MTU, pool size, QUIC windows, chunking, Cubic, GSO,
or the receiver SLI cannot distinguish these branches and remains frozen.

## Next Discriminator And Stop Rule

A bounded packet capture is running on `.33` for the Shenzhen host and UDP
`8443`. Run one fresh-process start/smoke/stop rearm without M0.

- If a new UDP source port completes start/smoke, the old endpoint/five-tuple
  was the failure scope. Use the capture to determine whether late old-port
  client packets reached `.33` and whether replies left `.33` before selecting
  endpoint rebind versus network-path handling.
- If the fresh endpoint also fails, compare `.33` ingress/egress packets. No
  ingress proves the client-to-Exit UDP path; ingress without replies points to
  Exit handling; bidirectional `.33` traffic with client timeout points back
  to the client receive/service path.
- Do not repeat formal M0 or modify frozen data-plane constants until this
  discriminator is classified.

M1 remains blocked. A fresh formal baseline/direct pair will be required only
after the timeout branch has a reviewed resolution.
