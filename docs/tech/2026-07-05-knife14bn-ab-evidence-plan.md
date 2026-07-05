# Knife14bn A/B Evidence Plan

Date: 2026-07-05

## Design Tree

1. Knife14bm no-data-stream shape is a `3d06bea` regression.
   - Plausible because Knife14bl on `09bb67c` reached about `130 Mbit/s`.
   - Needs same-window A/B evidence before changing behavior code.
2. Knife14bm no-data-stream shape is run-to-run stream/send-side variance.
   - Plausible because earlier Knife14bj/bi runs showed similar low-byte stream
     starvation while local egress was quiet.
   - Needs target sender and `.33` evidence for both commits.
3. Knife14bl pressure branch remains the next valid branch.
   - Plausible if either commit returns to useful throughput followed by
     TUN/downlink pressure, drops, and close pending.
4. Sing-box auth/config/time problem.
   - Rejected unless a current `fail auth`, startup failure, time sync issue, or
     missing TUIC outbound line appears in the A/B artifacts.

## Tasks

1. Create local git bundles for `09bb67c` and `3d06bea`.
2. Copy each bundle to `.27`, switch `.27` to the target commit, and build
   `target/release/mini_vpn`.
3. Before the first expensive run, check `.33` sing-box health, `.77` iperf3
   health, and time sync on the involved VPS hosts.
4. Run the scoped reverse-first P1 suite for `09bb67c` with:
   `RUN_REVERSE_FIRST_P1=1`, `STOP_AFTER_REVERSE_FIRST_P1=1`,
   `BUILD_RELEASE=0`, and `SERVER_EVIDENCE_CHECK=1`.
5. Without unrelated changes, switch `.27` to `3d06bea`, build, and run the
   same scoped suite.
6. Pull both bundles to the Mac, extract them, and parse the tunnel report,
   target iperf3 journal, `.33` sing-box evidence, and attribution summary.
7. Record the classification and next branch in a results document.
8. Update learning/error memory, commit, push, and report detailed stage and
   overall Knife14 progress.
