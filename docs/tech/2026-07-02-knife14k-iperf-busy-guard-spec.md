# Knife14k spec - guard iperf busy sweeps

> Date: 2026-07-02 | Branch: `codex/knife14d-downlink-reap-open`
> Input bundle: `/tmp/mvpn_knife14h_usclient_suite_20260702_092633.tar.gz`.
> Companion plan: `docs/tech/2026-07-02-knife14k-iperf-busy-guard-plan.md`.

## Grounding

Knife14j changed the test surface in the intended direction:

- the standalone P1 no longer blocks the full sweep;
- `tcp-relay-write-half-closed ... reason=local_finish` appears in the client log;
- reverse TCP now completes P1/P2/P4/P8 with tens to hundreds of MB delivered.

The full forward sweep is still polluted:

- forward P1 exits `0`, but the client log later reports one `remote_write_timeout`;
- forward P2 and P4 immediately return `iperf3: error - the server is busy running a test`;
- forward P8 exits `124` with no transfer.

Those P2/P4/P8 rows are not clean tunnel measurements. The target iperf3 daemon is still busy from the previous
run, so the suite cannot distinguish data-plane failure from target-service cleanup lag.

## Problem

`scripts/knife14b-lowrtt-probe.sh` runs every P value back-to-back. If target iperf3 reports "server is busy", the
probe records the failure and immediately moves to the next P. One busy response can therefore cascade through the
rest of the sweep and make the report look like multiple tunnel failures.

## Design

Add a small iperf busy guard inside the low-RTT probe:

- capture each iperf command output while still streaming it into the report;
- if the output contains the iperf3 busy message, sleep and retry the same command a bounded number of times;
- make retry count and wait interval configurable via environment;
- include those knobs in the suite launcher so bundled reports are self-describing.

This guard does not hide real tunnel errors. Non-busy failures keep their original exit code and output.

## Non-Goals

- No data-plane change.
- No change to mini_vpn relay timeouts.
- No remote service restart automation.
- No claim that `remote_write_timeout` is fixed.

## Acceptance

- `bash -n` passes for both probe scripts.
- A dry-run fake iperf command can emit "server is busy" once and prove the retry path keeps going.
- Future reports label busy retries in the markdown before the metrics section.
