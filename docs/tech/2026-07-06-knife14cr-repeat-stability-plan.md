# Knife14cr Repeat Stability Plan

Date: 2026-07-06

## Stage Plan

1. Keep code unchanged from Knife14cq commit `d85f1ff`.
2. Preflight `.33` for active service and current-window `fail auth`.
3. Run the same `.27 -> .33 -> .77` reverse-first P1 suite:
   `RUN_REVERSE_FIRST_P1=1`, `STOP_AFTER_REVERSE_FIRST_P1=1`, `P=1`,
   `DURATION=30`, MTU `1200`, `SERVER_EVIDENCE_CHECK=0`, explicit `.33`
   exit SSH variables.
4. Pull and parse the bundle locally.
5. Record whether the repeat preserves stable high throughput and clean
   pending/terminal/reap/drop/QUIC surfaces.
6. Update learning memory and commit/push the repeat result.

## Parse Checklist

- iperf sender/receiver Mbit/s
- interval profile and `throughput_shape`
- `timer_active_flow_attempts`
- TUN drops and backpressure
- pending at close, terminal pending reap, terminal late remote payload
- egress-at-close class
- send-slice/TUN flush errors
- QUIC loss/congestion/blocking
- `.27` cleanup and `.33 fail auth` current-window check
