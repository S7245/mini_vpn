# Knife15 M2 Successor Install Revalidation macOS Qualification Results

Date: 2026-08-13

Status: **PAIRED QUALIFICATION PASS_NON_ACCEPTANCE; ONE FRESH FORMAL M2
REOPENED; M3 BLOCKED**

Mac artifact:
`/tmp/mini_vpn_knife15_macos_20260813_095649.tar.gz`

Mac SHA-256:
`db8297c6a3bad48626fcfd2949c93914b01d4093fbf198de77dc76b612d0ac02`

Exit artifact:
`/tmp/mini_vpn_knife15_exit_target_observer_20260813_095739.tar.gz`

Exit SHA-256:
`9dc4c864cbf4d4d9d13f006fe1f5f62cf5cf891555c6d1bb7321ec3f55c854e7`

Exact source: `d91f20592ca2e40bca26ba5162fae0dcbacd092a`.

Formal source-floor gate: `4130ab9`.

## Qualification Verdict

The immutable Mac artifact is `PASS_NON_ACCEPTANCE`, as required for the
two-cycle discriminator. It completed:

- baseline `19.549/52.870 Mbit/s` forward/reverse receiver throughput;
- bounded 300-second direct forward `9.771 Mbit/s` with no zero interval;
- smoke and every controlled full-tunnel/IPv6/observer preflight;
- two cycles, eight phases, two DNS checks, and two real-client checks;
- six TCP and two UDP results with zero invalid files;
- zero Target receiver-zero intervals;
- maximum TCP sender/receiver gap `5,767,168B`, below `16MiB`;
- maximum UDP loss `2.548546%`, below `3%`; and
- owned route/DNS/TUN/process cleanup.

Three client sender intervals were zero: one in the 300-second cycle-2
forward flow and two in its 10-second short forward flow. Every corresponding
Target receiver interval remained nonzero. The paired observer also retained
TUIC traffic in every sampled second of both windows. These are bounded
sender-side scheduling/supply gaps, not Target receiver discontinuity.

## Decisive Replacement Branch

The qualification reached the repaired branch rather than proving only
regression cleanliness:

```text
predecessor generation=1 current_cwnd=235,811B
transaction handoff floor=235,811B
successor first-turn readiness=26,338B
successor final proof=427,953B
proof rounds=5 sent/acked/lost=414,587/414,587/0B
successor generation=2 installed in 1,946ms
```

The exact `c06a9d0` install CAS therefore observed a live same-path successor
whose current cwnd still covered the typed proof while holding the slot mutex.
No replacement failed, no `stale_cwnd` request recurred, and mini_vpn issued
no connection-local path reset or Endpoint rebind. The old predecessor's
bounded gap-ACK counter reached 26 before retirement; the successor remained
at zero through the rest of qualification.

This directly validates first-turn readiness/final-handoff separation,
transaction overwrite, current-service inheritance, and proof-to-install
revalidation under a real HK-to-US path. It does not replace the formal
24-hour duration gate.

## Ownership And Close Tails

The three `Stopped(0)` remote writes correspond to smoke and the two timed
10-second forward close tails. Each exact relay closed with D16
queued/leased/reserved `0/0/0B`; handle close recorded zero pending/reap/drop,
zero `send_slice` errors, and zero TUN flush failures. The reviewed immutable
terminal replay passes.

Endpoint conservation stayed within `61,440B`. The final sample was
available/live/outstanding `61,402/0/0B`; abandoned datagrams/bytes and socket
would-block were zero. Interface errors were zero. RSS grew by `34,224KiB`;
FDs and threads had zero net growth. Cleanup ended with the process dead,
owned routes and DNS restored, and secret scan PASS.

## Paired Exit Evidence

The exact v2 observer started four seconds before admission and froze 14
seconds after Mac cleanup. It captured `10,442,111` packets with exactly zero
kernel drops. Final nftables counts were:

```text
Target ingress/egress: 3,032,671 / 1,536,611 packets
TUIC ingress/egress:   1,922,172 / 2,716,915 packets
```

The synchronized local bundle matches the independent remote `sha256sum`;
gzip/tar integrity and all 28 internal `SHA256SUMS` entries pass. Observer
state and its nftables table are absent after bundling; sing-box is active
with zero restarts. Text evidence contains no credential-shaped value.

## Decision

Do not repeat qualification. Pull/rebuild the pushed reviewed descendant and
take exactly one fresh formal transaction while the HK Mac, `.33` Exit, and
`.77` Target can remain uninterrupted for about 25 hours:

```text
m2-ipv6-check -> baseline -> direct-discriminator -> start -> smoke
-> fresh .33 observer start -> m2 -> status -> stop
```

The formal runner owns automatic observer freeze/bundle on terminal paths.
Sync both final bundles. Any formal receiver-zero, SLI breach, safety mismatch,
or cleanup failure remains a failure; do not tune or repeat unchanged. M3
remains blocked until formal M2 and cleanup pass.

Post-qualification review also raised the formal source floor from production
commit `c06a9d0` to runner-gate commit `cce3bf8`. A focused RED proved that the
former could still run its older preflight script; GREEN rejects `c06a9d0`
and accepts `cce3bf8`. This changes no production code or frozen behavior.
