# Knife14bl TUN Egress Pressure Plan

Date: 2026-07-05

## Design Tree

1. Hidden server-side loss or target sender issue.
   - Rejected by Knife14bk: target iperf3 journal showed the sender itself at
     the low tunnel rate.
2. sing-box auth/time/config issue.
   - Rejected for the clean window: `.33` service was active, clocks were NTP
     synced, and current TUIC inbound/direct outbound opens appeared without
     current fail-auth evidence.
3. QUIC congestion/loss issue.
   - Rejected for the clean window: loss/congestion/blocking deltas were zero.
4. Terminal pending reap issue.
   - Rejected for Knife14bk P1: `terminal_pending_reap=0`.
5. Local TUN egress pressure reducing the effective TCP receive window.
   - Still open and now strongest: `send_queue_max=1048576`,
     `may_recv_false=7301`, `tun_tx_dropped_delta=6070`, and target sender rate
     matched the low tunnel receiver rate.

## Tasks

1. Add a unit test proving a pending backlog no longer forces immediate flush
   when local send queue pressure is at or above high watermark.
2. Implement pressure-aware immediate-flush decision without lowering the global
   immediate-byte default.
3. Update startup text if it currently claims pending always forces immediate
   flush.
4. Run focused local tests and suite self-tests.
5. Update learning memory.
6. Commit and push the coherent behavior patch.
7. Sync to `.27`, run the reverse-first P1 VPS acceptance, parse bundle, and
   decide whether the branch is fixed or needs architecture review.
