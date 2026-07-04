# Knife14aj spec - closed pending downlink and TUN egress pressure

## Grounding

- Latest code commit tested: `34d5cb7`.
- Prior stale TUIC TCP pool slot fix is accepted and closed:
  `7c683b0` plus `afb18f5`.
- The pool=4 startup/auth failure reproduced before restarting `.33`:
  `/tmp/mini_vpn/mvpn_knife14ai_pool4_startup1_20260704_091032.log`.
- Restarting sing-box on `.33` made pool=4 startup pass:
  `/tmp/mini_vpn/mvpn_knife14ai_pool4_startup_after_singbox_restart_20260704_091141.log`.
- The post-restart scoped bundle is:
  `/tmp/mini_vpn/mvpn_knife14ai_pool4_after_singbox_restart_usclient_suite_20260704_091224.tar.gz`.
- Direct preflights in that bundle were healthy:
  - Client `.27 -> .77`: 277 Mbit/s receiver.
  - Target `.77 -> .27`: 280 Mbit/s receiver.
  - Exit `.33 -> .77`: 283 Mbit/s receiver.
  - Target `.77 -> .33`: 284 Mbit/s receiver.
- Reverse-first P1 through the tunnel was still low:
  - iperf sender: 24.8 Mbit/s.
  - iperf receiver: 23.6 Mbit/s.
- Attribution after restart moved away from sing-box auth and QUIC loss:
  - `local_write_pressure: events=0`.
  - `global_rx_pressure: events=0`.
  - `downlink_backpressure: pause_edges=2 resume_edges=2
    max_pending_bytes=2149256`.
  - QUIC loss/congestion and blocked-frame deltas were zero.
  - The label was `local_downlink_backpressure`.
- The final close diagnostic included a live pending drop:
  `dead_slot_reap state=Relaying pending=2111595 ... tcp_state=Closed
  active=false can_send=false`.
- mini_vpn reported successful TUN writes, but the Linux interface showed local
  queue loss after the run:
  - `tun_flush_tx_failures=0`.
  - `tun0` TX dropped increased to `1423`.
- A pool=1 comparison had the same closed-pending shape around 2 MiB, but
  `tun0` TX dropped was `0`. That means closed-pending reap and kernel/TUN
  egress pressure must be measured separately instead of collapsed into one
  label.

## Problem

Knife14u intentionally made inactive, not-send-capable pending downlink
immediately reapable:

```text
downlink_pending != empty && !active && !can_send => reap now
```

That rule avoided stale slots after knife14t made pending grace too broad.
Fresh grounding on smoltcp `0.10.0` confirms that `can_send=false` in
`TcpState::Closed` is not temporary transmit-buffer pressure: `can_send()` is
`may_send() && !tx_buffer.is_full()`, and `may_send()` is only true in
`Established` and `CloseWait`.

The latest post-restart run shows the immediate reap branch firing while roughly
2 MiB of downlink was still buffered, with no `send_slice` errors and no TUN
write failures. That tail is not itself drainable once the local TCP socket is
Closed. The stronger throughput clue is that TUN TX drops were visible only
through `ip -s link show tun0`, not in the per-probe attribution summary.

The next stage needs to distinguish two conditions that currently look too
similar in the report:

- truly undeliverable pending from a dead local socket, where immediate reap is
  correct and should stay locked by tests;
- earlier local/TUN egress pressure that can make the receiver close with tail
  pending left behind, where TUN drop attribution is the next useful signal.

## Goal

Make closed-pending downlink lifecycle and TUN egress pressure measurable and
bounded:

- add focused tests around inactive closed sockets with pending downlink;
- preserve the knife14u stale-flow protection for pending that has no recent
  drain/progress evidence;
- preserve immediate reap for `Closed`/not-send-capable pending even if recent
  pending metadata exists, because smoltcp cannot send from that state;
- expose TUN RX/TX drop deltas in the scoped throughput reports;
- keep acceptance focused on reverse/downlink pressure, not stale-slot or
  sing-box-auth diagnosis.

## Non-Goals

- Do not continue stale TUIC TCP pool slot diagnosis.
- Do not treat the pool=4 auth/startup failure as a mini_vpn data-plane bug
  after it was cleared by restarting sing-box.
- Do not change TUIC authentication, TUIC framing, QUIC congestion control,
  TCP pool selection, MTU defaults, or iperf3 configuration in this stage.
- Do not hide low throughput by only increasing TUN qdisc length, socket
  buffers, or downlink watermarks.
- Do not add unbounded pending buffers or unbounded grace windows.

## Design Tree

1. Stale TCP pool slot is still the root cause.
   Rejected. The accepted full reverse signal already showed
   `stale_tcp_pool_slot`, and the post-restart pool=4 run had no stale
   reconnects.

2. The low throughput is just iperf3 or direct VPS path weakness.
   Rejected for this stage. Direct `.27`, `.33`, and `.77` baselines were
   around 277-284 Mbit/s in the same bundle.

3. The pool=4 startup failure proves sing-box is the current throughput root
   cause.
   Rejected. Restarting `.33` fixed startup/auth. After startup passed, the
   remaining low throughput still showed local downlink backpressure and TUN
   TX drops.

4. Revert knife14u and always grace inactive pending.
   Rejected. Knife14u was added because broad inactive pending grace left stale
   flows alive across probes and harmed throughput.

5. Grace `Closed && !can_send` pending when recent progress metadata exists.
   Rejected after code grounding. smoltcp cannot send from `Closed`, so this
   would delay cleanup without delivering the pending bytes.

6. Lock undeliverable closed-pending behavior plus TUN drop attribution.
   Selected. This keeps the stale-flow cleanup that knife14u restored, while
   making kernel/TUN loss visible in the report so the next throughput fix aims
   at the pressure source instead of the terminal tail.

## Invariants

- `Listening` slots are never reaped.
- Active relays with pending downlink are preserved.
- Pending downlink remains bounded by existing high/low watermarks and by a
  finite grace.
- A closed and not-send-capable relay with pending must be reapable even if
  pending metadata is recent; stale pollution must not come back.
- An inactive but still send-capable relay with recent progress metadata keeps
  the existing bounded grace.
- Progress must mean bytes were accepted or pending decreased, not merely that
  pending grew.
- Reap diagnostics must continue to include pending size, TCP state, `active`,
  `can_send`, and flush counters.
- Suite summaries must show TUN drop deltas without storing secrets or full
  noisy service logs.

## Acceptance

Local acceptance:

- Focused `client_tun` tests first cover:
  - stale inactive pending with no progress metadata is reaped;
  - inactive closed pending with recent progress is still reaped immediately
    when `can_send=false`;
  - inactive send-capable pending with recent progress still gets bounded
    grace;
  - active pending behavior remains unchanged.
- Script tests or self-tests cover parsing TUN RX/TX drop deltas from
  `ip -s link show` output.
- `cargo test --lib client_tun` passes.
- `bash -n scripts/knife14b-lowrtt-probe.sh` passes.
- `bash -n scripts/knife14b-usclient-tunnel-suite.sh` passes.
- `git diff --check` passes.

VPS acceptance:

- Preflight `.33` sing-box and `.77` iperf3 before the expensive run.
- Pool=4 startup smoke on `.27` reaches tunnel ready. If it fails with TUIC auth
  after `.33` has been running for a long time, restart/check sing-box before
  attributing the failure to mini_vpn.
- Run the same scoped reverse-first P1 with direct exit-to-target preflight.
- The report contains per-probe TUN RX/TX drop deltas.
- A low reverse run may still end with
  `dead_slot_reap ... pending>0 ... can_send=false`, but the report must make it
  clear whether prior TUN egress drops or other pressure happened before that
  terminal cleanup.
- If throughput remains low, the report must attribute it to a visible bounded
  pressure signal: TUN drops, downlink backpressure, local write pressure,
  global RX pressure, QUIC loss/congestion, or sender-side backpressure.
