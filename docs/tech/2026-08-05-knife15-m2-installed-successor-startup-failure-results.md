# Knife15 M2 Installed-Successor Startup Failure Results

Date: 2026-08-05

Status: **M2 FAILED — auxiliary replacement stop rule fired**

## Immutable evidence

```text
b0d3815e493e0706fe76930f11f7d932c6cc8164b9729cd5bc7749d872733fc9
/tmp/mini_vpn_knife15_macos_20260805_111712.tar.gz

/tmp/mini_vpn_knife15_macos_direct_20260805_111112
```

The archive, `.sha256`, extracted bundle, and manifest agree. The xiaoou Mac
ran exact source `f570353daa3906543bffcd7dafb03aef1d18b36b`, runner
`2a57206e...`, and release binary `fb812d89...`.

Pre-TUN evidence was healthy:

- baseline forward/reverse receivers: `34.319 / 52.001 Mbit/s`, no complete
  receiver-zero interval;
- 300-second direct receiver: `17.151374 Mbit/s`, no sender or receiver zero;
- start, smoke, IPv6 `safe_absent`, full-tunnel, controlled drain, DNS, and
  real-client preflights: PASS.

M2 then completed seven full mixed cycles, seven DNS checks, and seven real
HTTPS checks. Across those complete cycles, maximum reverse-UDP loss was
`1.797529%`, below the frozen `3%` SLI. Cleanup later restored the physical
routes/DNS and removed the process/TUN ownership.

## Exact failure

Cycle 8 `tcp-forward` ran for the complete 300 seconds. Both endpoints
ultimately transferred the exact `643,563,520B`, the local sender reported
`17.161515 Mbit/s` and zero retransmits, and the Target receiver reported
`17.151728 Mbit/s`. Only the first complete Target interval failed:

```text
0.000000s -> 1.001055s: 0B
```

This is not a command tail. In the same first local interval the sender had
already written `2,228,224B`, so the application/TUN path admitted data while
initial remote delivery waited behind the shared QUIC connection's work.

The selected connection was the already installed auxiliary successor:

```text
conn1 id=33232107536 generation=2
replacement installed earlier in 170ms
cycle-8 control stream=160, data stream=161
```

Immediately before both opens:

```text
conn0 generation1: active=2, degraded, black_holes=0 -> 9,
                   cwnd=1,796,462B, RTT=169ms, not admitted
conn1 generation2: active=8/10, qualified in its current busy epoch,
                   black_holes=60 -> 60, cwnd=25,850B, RTT=168ms, admitted
```

Control stream 160 received its first byte in `170ms`, proving open/auth/remote
handling progress. Data stream 161 remained an uplink-only stream until clean
EOF, as expected, and its D16 writer ultimately wrote `643,563,557B`; one
writer poll waited `2,551,045us`. During the phase conn1's PLPMTUD black-hole
count later advanced, but that evidence arrived after the failed first Target
interval.

## Healthy controls and ownership

Every network sample from `13:00:09Z` through the `13:05:06Z` failure kept:

- Target route `utun4`, Exit route `en0`, physical `en0/192.168.133.1`;
- Exit `3/3`, `0%` loss at about `162ms`;
- gateway `3/3`, `0%` loss;
- physical and utun interface errors at zero, with both interfaces advancing;
- process PID stable at `15` FDs and `11` threads.

Endpoint pacing stayed conservative with zero socket would-block events. Its
samples remained within `61,440B`, and the failed relays closed with D16
queued/leased/reserved ownership all zero. The repaired false generic
recovery did not recur: there were zero endpoint rebinds and zero
`trigger=no_rx ... udp_active=false` actions.

## Decision

This is the exact stop rule from the auxiliary-generation architecture: an
installed successor produced a complete Target receiver-zero interval while
the physical controls, TUN, D16, Endpoint conservation, and process remained
healthy. Auxiliary replacement is retained as bounded lifecycle protection,
but rejected as sufficient initial-stream service.

Do not repeat unchanged and do not tune selector scores, cwnd/RTT thresholds,
pool size, MTU, windows, chunk, Cubic, GSO, Endpoint constants, recovery
bounds, workload, or SLI. The selected next stage is Quinn new-stream startup
scheduling with an exact bounded service contract.
