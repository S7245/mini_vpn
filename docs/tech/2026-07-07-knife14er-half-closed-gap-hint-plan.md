# Knife14er Half-Closed Gap Hint Plan

Date: 2026-07-07

## Design Tree

- Server/path branch: rejected for this stage. Knife14eq preflight baselines
  were healthy on both `.27 -> .77` and `.33 -> .77`.
- QUIC loss/MTU branch: rejected for this stage. safe1200 was active and QUIC
  loss/congestion/blocking stayed zero.
- Local pressure branch: rejected for this stage. Knife14eq had no pending or
  headroom debt and read credit stayed at the old `65536` floor.
- Half-closed read-gap branch: selected. Knife14eq had late remote data after
  local finish, but the relay gap-hint predicate disabled ACK/window hints when
  `writer_done=true`.

## TDD Plan

1. Flip the focused relay gap-hint test so data-bearing half-closed streams
   must still be eligible for ACK/window hints.
2. Keep existing guards for no remote data, tiny control streams, paused read
   credit, and rate limiting.
3. Implement the smallest behavior change: remove `writer_done` as a hard
   suppressor from the data-stream gap-hint predicate.
4. Run focused relay-gap/local-finish/downlink-credit gates.

## Remote Plan

1. Sync to `.27`, excluding `.env`, `.git`, `target`, and tar archives.
2. Run `.27` focused tests and release build.
3. Preflight `.33` sing-box, `.77` iperf3, and `.27` readiness.
4. Run one scoped safe1200 reverse-first P1.
5. Pull and parse the bundle.
6. If the suite fails, stop at evidence and write the next modification plan
   before making another code change.
