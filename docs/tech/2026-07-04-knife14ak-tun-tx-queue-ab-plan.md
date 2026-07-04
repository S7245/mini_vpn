# Knife14ak plan - TUN tx queue A/B attribution

> Spec:
> `docs/tech/2026-07-04-knife14ak-tun-tx-queue-ab-spec.md`.

## T1 - Harness knob

- Add optional `TUN_TX_QUEUE_LEN` to
  `scripts/knife14b-usclient-tunnel-suite.sh`.
- Validate that the value is a positive integer when set.
- After TUN discovery and target-route setup, run:
  `sudo ip link set dev "$TUN_IF" txqueuelen "$TUN_TX_QUEUE_LEN"`.
- Record `ip link show "$TUN_IF"` after setup.
- Keep default behavior unchanged when the variable is unset.

## T2 - Report actual queue length

- Parse the actual queue length from `ip link show "$TUN_IF"`.
- Add the requested and actual queue length to the MTU / Probe Plan section.
- Preserve the existing TUN drop attribution summary in each probe.

## T3 - Local verification and stage review

- Run:
  - `bash -n scripts/knife14b-usclient-tunnel-suite.sh`;
  - `git diff --check`.
- Review for:
  - no product data-plane behavior change;
  - no persistent host network tuning;
  - no secret exposure;
  - no accidental change to stale-slot, MTU, or qdisc defaults.

## T4 - Commit and push harness task

- Commit the coherent harness/docs slice after local verification passes.
- Push with the SSH GitHub remote if HTTPS push still cannot authenticate.

## T5 - Scoped VPS A/B sample

- On `.27`, pull the pushed branch.
- Source `.evn` quietly:
  `set -a; source ./.evn >/dev/null 2>&1; set +a`.
- Run a scoped suite with:
  - `SUITE_TAG=knife14ak_tunqlen5000`;
  - `TUN_TX_QUEUE_LEN=5000`;
  - `MINI_VPN_TUIC_CC=bbr`;
  - `MINI_VPN_TUIC_TCP_POOL=1`;
  - `RUN_REVERSE_FIRST_P1=1`;
  - `PARALLEL_SET=1`;
  - `DURATION=30`;
  - direct exit-to-target preflight enabled.
- Copy the bundle back to `/tmp/mini_vpn/` on the Mac.

## T6 - Learning and next decision

- Compare qlen=5000 against the previous qlen=500 bundle:
  `/tmp/mini_vpn/mvpn_knife14aj_tun_drop_env_usclient_suite_20260704_112101.tar.gz`.
- Record the result in `.learnings/LEARNINGS.md`.
- Record any failed command or misleading branch in `.learnings/ERRORS.md`.
- Decide the next product branch:
  - paced/budgeted TUN flush if larger qlen improves drops/throughput;
  - downlink watermark tuning if pending pressure dominates;
  - QUIC/path attribution if TUN drops disappear but throughput stays low.
