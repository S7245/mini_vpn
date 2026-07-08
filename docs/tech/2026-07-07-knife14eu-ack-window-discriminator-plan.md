# Knife14eu ACK/window discriminator plan

Date: 2026-07-07

## Plan

1. Add relay diagnostic counters for local finish timing and split read gaps
   into pre-finish and post-finish windows.
2. Update relay live/close log formatting and deterministic unit tests.
3. Update the low-RTT attribution parser only if it needs new fields for
   summary classification.
4. Run focused local tests, full local gates, and remote focused gates.
5. Run exactly one scoped VPS discriminator suite:
   - `MINI_VPN_TUIC_MTU_MODE=safe1200`
   - `RUN_REVERSE_FIRST_P1=1`
   - `STOP_AFTER_REVERSE_FIRST_P1=1`
   - `EXIT_TO_TARGET_IPERF_CHECK=1`
   - explicit `EXIT_SSH_HOST` and `EXIT_SSH_KEY`
6. Parse the bundle and decide:
   - pre-FIN gap: investigate TUIC stream wakeup / TUN ACK drain;
   - post-FIN gap: design bounded FIN deferral A/B;
   - pressure returns: revert or adjust the Knife14et progress-aware staging
     change;
   - environment unhealthy: stop before code changes.

## Stop rule

If this stage fails to produce a clear classification after the VPS run, stop
and report why. Do not run another threshold-tuning loop in the same area.
