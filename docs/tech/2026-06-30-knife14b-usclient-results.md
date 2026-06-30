# 刀14b US-client results — low-RTT path is clean; next is downlink/backpressure + MTU/MSS

日期：2026-06-30

## Source

- User bundle: `/tmp/conn/mvpn_knife14b_usclient_suite_20260630_142932.tar.gz`
- Local extraction used for analysis: `/tmp/conn_suite_20260630_142932/`
- New helper script used by the user: `scripts/knife14b-usclient-tunnel-suite.sh`

## Environment

- Client: Ubuntu VPS, `43.172.75.27`, US Santa Clara, 200M
- TUIC Exit: `43.153.32.33`, sing-box TUIC on UDP `8443`, 200M
- iperf Target: `43.130.32.77:5201`, 200M
- Route shape during suite:
  - Target: `43.130.32.77 dev tun0 src 10.0.0.1`
  - Exit: `43.153.32.33 via 172.26.0.1 dev eth0`

## What Passed

- The suite completed with `status: COMPLETED`.
- TUIC authentication and startup are good:
  - `已连接 TUIC 出口 43.153.32.33:8443`
  - `UDP relay 数据面就绪`
  - `TUIC UDP 驱动已就绪`
- Traffic really entered the tunnel:
  - iperf local endpoint is `10.0.0.1`.
  - Target route is via `tun0`.
  - Exit route is not recursively routed into `tun0`.
- The low-RTT / same-region environment removes the earlier high-RTT WAN-path ambiguity. Prior direct
  baselines from this environment were roughly in the 150-200M class, so the tunnel result is now the
  suspect, not the physical path.

## Key Numbers

| Probe | Direction | Result |
|---|---|---:|
| MTU 1500, P1 | forward | receiver `476 Kbit/s` |
| MTU 1500, P1 | reverse | client receiver `2.06 Mbit/s` while remote sender reports `141 Mbit/s` |
| MTU 1200, P1 | forward | receiver `29.0 Mbit/s` |
| MTU 1200, P1 | reverse | client receiver `2.06 Mbit/s` while remote sender reports `141 Mbit/s` |
| MTU 1200, full | forward P1 | receiver `33.2 Mbit/s` |
| MTU 1200, full | forward P2 | failed with `iperf3: error - unable to receive results` |
| MTU 1200, full | P4/P8 and reverse sweeps | failed with `Connection reset by peer` |

The client log repeatedly shows:

- `写入上游流失败: Custom { kind: ConnectionReset, error: Stopped(0) }`
- `远端服务器关闭了车厢 SocketHandle(...)`

## Reading

1. **MTU matters.** Lowering TUN MTU from 1500 to 1200 changes forward P1 from sub-1M to roughly 30M.
   That makes MTU/MSS handling a first-class next task, not a footnote.
2. **Reverse/downlink is the current hard failure.** A single reverse flow is already pathological:
   the remote sender reports `141 Mbit/s`, but the client receives only `2.06 Mbit/s`.
3. **Connection pool is not the next knife.** Pooling can only help if a single TUIC/QUIC connection is
   the ceiling. Here P1 reverse is already broken and P2+ resets; adding more connections would hide the
   mechanism before we understand it.
4. **The next work is inside TCP relay downlink/backpressure and MTU/MSS.** Existing LoopProfiler lines
   show reverse runs dominated by relay time, while poll is low. This points at relay scheduling,
   socket write readiness, pending-buffer behavior, or upstream stream reset handling before any pool work.

## Next Knife: 14c

Recommended scope: **knife14c — TCP downlink/backpressure instrumentation + MTU/MSS fix**.

Tasks:

1. Add focused TCP relay metrics:
   - per-handle `downlink_pending` current/high-water bytes;
   - remote-read bytes into `global_rx`;
   - bytes accepted by `tcp_socket.send_slice`;
   - TUN/device `flush_tx` calls and failures;
   - `global_rx` / relay channel pressure;
   - stream close/reset reason with handle and byte counters.
2. Fix or expose MTU handling:
   - make Linux TUN MTU configurable or set the acceptance path to the chosen value;
   - align smoltcp `DeviceCapabilities.max_transmission_unit` with the actual TUN MTU;
   - add TCP MSS clamping or equivalent advertised-MSS control if needed.
3. Rework downlink backpressure only after metrics identify the mechanism:
   - ensure partial `send_slice` progress is preserved;
   - avoid spinning a dirty handle when the TCP socket cannot accept bytes;
   - make reset/error logging directional and actionable.
4. Extend `scripts/knife14b-usclient-tunnel-suite.sh` or add a 14c variant so the user can rerun one
   command and upload a complete markdown/tar bundle.

Exit criteria:

- Forward P1 at MTU 1200 does not regress.
- Reverse P1 no longer collapses to ~2M.
- P2 no longer breaks iperf control/result exchange.
- The report contains enough per-handle counters to decide whether connection pool is still relevant.

## Worktree Note

At handoff time, `scripts/knife14b-usclient-tunnel-suite.sh` existed as an untracked helper script. It is
useful and should be reviewed/committed or folded into the next scripted acceptance work before relying
on a clean worktree.
