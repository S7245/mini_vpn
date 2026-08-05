# Knife15 M2 Full-Tunnel Quiescence Failure Results

Date: 2026-08-05

Status: **ENVIRONMENT PREREQUISITE EXPOSED — data-plane architecture retained**

## 1. Artifact And Prerequisites

The exact-source `798c1a5` artifact is:

```text
/tmp/mini_vpn_knife15_macos_20260805_015011.tar.gz
SHA-256 386ecafcfb56316020b4b9970d17de004e7b1cee136d98f85f2f7e69d15a5770
```

Its manifest binds source `798c1a544d40d15bf63520312937de2dabd54015`,
runner `0c561dd7...`, and binary `e87dd6a7...`. The paired baseline
`/tmp/mini_vpn_knife15_macos_baseline_20260805_013527` passed at
`45.914/26.363 Mbit/s`; the 300-second direct discriminator
`/tmp/mini_vpn_knife15_macos_direct_20260805_013951` passed at
`22.908 Mbit/s` with zero sender and receiver intervals. Start, smoke, the
IPv6/full-tunnel gate, and the real-client preflight also passed. This is not
an operator command, source, binary, direct-path, or preflight failure.

## 2. Workload Result

M2 ran from `01:51:04Z` through the first idle checkpoint at `06:10:10Z`:

- the complete four-hour `steady-a` window passed;
- 17 complete mixed cycles plus cycle 18 forward passed;
- all 154 phase results were structurally valid: 137 TCP and 17 UDP;
- Target receiver-zero intervals were `0`;
- maximum TCP sender/receiver gap was `10,354,688B`, below `16MiB`;
- maximum reverse-UDP loss was `0.604884%`, below `3%`;
- Endpoint conservation max/final was `61,440B / 61,403/0/0B`;
- Exit/gateway controls, physical interface, routes, D16, resources, and
  final cleanup remained valid.

At the end of the 600-second `idle-1` drain, the unchanged checkpoint
predicate correctly failed:

```text
endpoint=61403/0/0B
active_relays=4
fake_ip_active=7
fake_ip_registered=19
dns_dropped=0
```

The checkpoint requires zero relay/fake-IP ownership and only the one or two
registered entries produced by the controlled probe. It must not be weakened.

## 3. Decisive Traffic Evidence

The four live relays at the failed checkpoint belonged to non-test Mac
traffic. Exact log ownership identifies:

```text
37-courier.push.apple.com:5223
mtalk.google.com:5228
filehelper.weixin.qq.com:443
api2.cursor.sh:443
```

The Mac therefore had Apple/Google push, WeChat, and Cursor traffic during the
formal full tunnel. The runbook said not to browse and to disable other VPNs,
but did not explicitly require quitting all network-using Apps. The runner
also had no equivalent gate before its first four-hour active window. This is
a dedicated-test environment prerequisite and late-observer defect, not a
mini_vpn data-plane regression and not command misuse.

## 4. Architecture Stop Rule

This artifact positively exercises the bounded auxiliary-generation design:

```text
replacement-start slot=1 predecessor_id=42560471056 generation=1
  active=6 black_hole_anchor=192 black_holes_current=193
replacement-installed slot=1 predecessor_id=42560471056
  successor_id=42560514064 generation=2 handshake_ms=689 active=6
```

New traffic used the authenticated successor and all 17 complete mixed cycles
remained receiver-positive. The old predecessor remained drain-only because
an existing `mtalk.google.com:5228` stream still owned it. That is bounded,
expected preservation of an existing flow; it is not a missing migration or
reset. The architecture-rejection discriminator—an installed-successor
healthy-control complete receiver-zero interval—did not occur.

## 5. Selected Repair And Next Action

Formal M2 must prove global full-tunnel quiescence immediately after
activation and its real-client preflight, before starting the exact 86,400s
schedule. The runner will reuse the existing smoke hard timeout (normally
`20 + 30 = 50s`) and require fresh data-plane and Endpoint observations with:

```text
active_leases == 0
active_relays == 0
fake_ip_active == 0
1 <= fake_ip_registered <= 2
dns_dropped == 0
endpoint_live == 0
endpoint_outstanding == 0
endpoint_available + endpoint_live + endpoint_outstanding <= 61,440
```

Failure remains fail-closed with structured evidence and leaves the owned TUN
and full tunnel available for `status/snapshot/stop`. No duration, SLO,
workload, D16, MTU, pool, QUIC window, chunk, Cubic, GSO, self-wake, Endpoint
pacing value, or data-plane code changes. After the reviewed repair is pushed,
take one fresh transaction only after quitting every non-test network App. M3
remains blocked pending complete M2 plus cleanup acceptance.
