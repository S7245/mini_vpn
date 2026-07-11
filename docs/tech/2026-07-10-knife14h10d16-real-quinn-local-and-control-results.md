# Knife14 H10d16 Real-Quinn Local and Same-Window Control Results

Date: 2026-07-10

## Outcome

The ordered-stream, byte-ownership, actor-exclusivity, bounded TUN RX, and EOF
architecture is closed locally at commit `1bf1f78`. Gate A was not run because
the same-window mature sing-box control did not meet the `>150 Mbit/s`
capability precondition. Gate B remains frozen.

## Local Real-Quinn Closure

Three progressively deeper loopback tests use a real Quinn bidirectional
stream:

1. `TuicNativeOrderedReader` sustained ordered progress;
2. `run_d16_native_reader` sustained the RAII reservation and readiness-only
   leased byte queue;
3. the full `run_event_loop` path crossed Quinn, D16 ownership, the actor,
   smoltcp, a bounded TUN ring, a second smoltcp stack, and clean EOF.

The full-path test transfers `32 MiB` and requires:

- receiver capacity at least `170 Mbit/s`;
- at most 24 payload packets per flush;
- zero modeled TUN drop;
- zero actor-bypass admission;
- remote EOF only with zero owned/pending/inflight tail.

An early repeat timed out after all `32 MiB` were already received and the
generator socket had entered `CloseWait`. Both TUN rings were empty, QUIC had
zero loss and congestion, but the test sink retained a prior `34507B`
owned/inflight snapshot. The lifecycle observation was refreshed only during
local-egress service windows; a control-only dirty-relay pass could complete
EOF without another observation.

The fix extracts the existing D16 harness snapshot and refreshes it after each
`process_dirty_relay` pass when the sink explicitly requests harness
observations. Production sinks return before aggregation. No queue, permit,
actor, EOF, socket, timer, MTU, window, chunk, or self-wake behavior changed.

Stable local evidence:

- full real-Quinn path: `30/30` repeats;
- normal library: `585/585`;
- harness library: `594/594`;
- concurrency harness: `10 passed`, `4` existing ignored;
- cargo checks, fmt, diff-check, and both script self-tests: pass;
- clippy: exit `0`, existing style warnings only.

## Same-Window Capability Check

The VPS services and exit socket-buffer prerequisite were healthy:

- Exit sing-box: active;
- Target iperf3: active;
- Exit `rmem_max/wmem_max`: `16 MiB`;
- Exit defaults: `1 MiB`.

Sequential direct reverse baselines were healthy:

- client `.27 -> .77`: `217.430 Mbit/s` receiver;
- exit `.33 -> .77`: `218.688 Mbit/s` receiver.

The mature sing-box control used a target-only TUN route through `.33`, kept
`.33` itself on `eth0`, and temporarily calibrated `.27` default UDP buffers to
`16 MiB`. The actual sing-box UDP socket reported `16 MiB` receive/send buffers
and drop `0`. The `20s`, reverse, `P=1` result was:

- sender: `3.093 Mbit/s`;
- receiver: `1.363 Mbit/s`;
- one-second receiver intervals were burst/idle, with many zero intervals.

An earlier calibrated control in the same investigation reached only
`14.207 Mbit/s`, also with `16 MiB` buffers and drop `0`. The temporary FIFO,
helper, TUN interface, and sysctl override were removed; `.27` defaults were
restored to `212992` bytes. No credential or persistent sing-box config was
written.

A later same-shape retry confirmed the condition rather than clearing it:

- sequential direct reverse baselines: `218.898 Mbit/s` from `.27` and
  `211.140 Mbit/s` from `.33`;
- sing-box sender: `4.508 Mbit/s`;
- sing-box receiver: `1.182 Mbit/s`;
- client UDP receive/send buffers: `16 MiB`, drop `0`;
- most one-second intervals: `0 Mbit/s`.

The next useful external discriminator is one explicitly approved restart of
the active `.33` sing-box service followed by the same mature control. Repeating
the unchanged low control or editing D16 code has no additional diagnostic
value.

Remote artifacts:

```text
/tmp/mini_vpn_h10d16_same_window_control/
/tmp/mini_vpn_h10d16_same_window_control_retry/
/tmp/mini_vpn_h10d16_same_window_control_window2/
```

## Decision

The local D16 path has plausible and stable `170+ Mbit/s` capacity, but the
current external TUIC window cannot validate it because the mature control is
below the Gate A floor while both direct legs remain above `200 Mbit/s`.

Do not change D16 architecture, packet budgets, MTU/PLPMTUD, QUIC windows,
pooling, chunk size, or self-wake from this result. Before Gate A, require a
fresh same-window mature sing-box reverse P1 control above `150 Mbit/s` with
comparable socket buffers and zero drops. Then deploy `1bf1f78` from an isolated
worktree and run exactly one `20s` reverse-first P1 Gate A. Gate B starts only
after that run is clean.
