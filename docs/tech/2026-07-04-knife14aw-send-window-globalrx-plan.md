# Knife14aw plan - send-window and global_rx queue diagnostics

Date: 2026-07-04

## Stage Goal

Make the next low-throughput reverse run self-attributing enough to decide
whether the remaining limiter is smoltcp send-window closure, relay queue
backlog, local TUN egress, or terminal post-close accounting.

## Design Tree

1. Change reap/close-drain behavior now.
   Rejected. Knife14av shows terminal pending, but a closed/no-send socket
   cannot deliver those bytes. Changing reap without knowing when the socket
   lost send capacity risks preserving undeliverable buffers rather than fixing
   throughput.

2. Tune TUN queue, watermarks, egress pacer, or TUIC pool again.
   Rejected for this slice. Prior Knife14 runs already rejected those as first
   moves for the clean reverse-first symptom.

3. Add behavior-neutral receive-window and queue diagnostics.
   Selected. This is the smallest patch that can tell whether low reverse
   throughput starts before terminal close and where bytes are waiting.

## Tasks

1. Record Knife14av failed VPS result.
2. Add a `TcpSendWindowSnapshot` sampled during `flush_downlink`.
3. Aggregate and print send-window fields in `tcp-downlink-flush`.
4. Add close-time socket queue/window fields to `tcp-handle-close`.
5. Record relay `global_rx` channel used/max capacity in live/close diagnostics.
6. Extend the low-RTT probe parser and self-test for the new fields.
7. Run local regression gates.
8. Commit and push.
9. Update `.27` and rerun the scoped reverse-first acceptance.

## Regression Checks

- Existing terminal pending and pending-at-close parsing still works.
- Old log compatibility: missing new fields summarize as zero.
- No new high-cardinality log line type is added; existing TCP diag lines carry
  the added fields.
- The tunnel data path remains behavior-neutral: no queue sizes, sleeps,
  watermarks, close predicates, or socket operations change.
