# Knife14x spec - explicit QUIC flow-control windows

## Grounding

- Project goal from `AGENTS.md`: mini_vpn must become a stable high-throughput
  VPN data-plane core, not a single-path demo.
- Test bundle:
  `/tmp/mvpn_knife14w2_usclient_suite_20260703_112033.tar.gz`.
- Tested commit: `95bfe47`.
- The suite now reaches the full tunnel test. Cargo discovery worked, `.33`
  reachability and `.77:5201` direct iperf were healthy, and the suite completed.
- Direct `.77` preflight receiver was about 281 Mbit/s, while tunnel forward P1
  was 2.83 Mbit/s receiver and reverse P1 was 314 Kbit/s receiver.
- Relay writer coalescing is active: local writer payloads are now often
  64KiB-class instead of MSS-sized. The new blocker is long upstream write wait:
  `tcp-local-write-pressure` showed 110 events, with close diagnostics reaching
  `local_write_wait_max_us` in the seconds-to-tens-of-seconds range.

## Problem

The client QUIC transport currently keeps quinn's default stream/data windows
except for idle, keepalive, MTU, congestion controller, and uni-stream count.
quinn 0.10 defaults are tuned around roughly 100ms and 100Mbit/s per stream. A
VPN tunnel over variable VPS paths needs explicit flow-control headroom, or
pressure can surface as long `write_all` waits that are hard to distinguish from
server-side windows or congestion-control limits.

## Goal

Make client-side QUIC stream/data/send windows explicit and VPN-sized, and log
the selected values at TUIC startup so the next live run can attribute remaining
write pressure correctly.

## Non-goals

- Do not change TUIC protocol framing, TCP relay lifecycle, or downlink pending
  behavior.
- Do not make TCP connection pooling the default.
- Do not change the default congestion controller in this stage.
- Do not tune the sing-box/server-side flow-control window from the client.

## Invariants

- Keep idle timeout, keepalive, initial MTU, and congestion-control selection
  behavior unchanged.
- Bound memory by setting a finite connection receive window when increasing
  per-stream receive window and bidi-stream allowance.
- Keep the existing `MINI_VPN_TUIC_CC` override path for later Cubic/BBR A/B.
- Startup logs must print the actual window values used by the client.

## Acceptance

- Unit tests verify the transport config contains the intended bidi/uni/window
  values.
- Local gates pass: focused quic tests, full tests, clippy, and diff check.
- Next VPS run should show whether `tcp-local-write-pressure` improves. If it
  does not, the remaining blocker is likely peer/server receive window,
  congestion control, or path loss rather than the client's default transport
  window.
