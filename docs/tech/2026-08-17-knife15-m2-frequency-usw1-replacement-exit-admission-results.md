# Knife15 M2 Tier-B US-West Replacement Exit Admission Results

Date: 2026-08-17

Status: **PASS; NO TUN, OBSERVER, OR TIER-B EPOCH STARTED**

## Scope

This stage admits the Alibaba US-West replacement Exit as the immutable
Tier-B resource before the first six-hour epoch. It does not reopen Tier A,
add a third Tier-A candidate, tune a frozen value, change Rust production
code, or spend a Tier-B traffic epoch.

The original candidate-1 ECS had been destroyed after Tier A was sealed. No
Tier-B epoch existed, so the retained EIP could be attached to one replacement
ECS and frozen as a new Tier-B identity without bridging evidence across
instances.

## Frozen identity

```text
candidate_id=tierb-alibaba-usw1-r1
provider=alibaba-cloud
resource_id=i-rj9c5rn1psf504mf1zo2
region=us-west-1
availability_zone=us-west-1b
instance_class=ecs.c8i.large
public_ipv4=47.89.211.4
private_ipv4=172.18.188.19
route_contract_id=eip-rj9hj9g6dbtxwwxfqmw0t
asn=45102
host_key_ed25519=SHA256:km4qtBz/r+jNuPv4WKoCP3LoIyw1U6nfRNMIWpW9K24
server_binary_sha256=4ea794fddcb2ad84532adeab979a9b0d7b2052822bb3439dfb321c33c941da19
server_config_sha256=0c48b68369253dfa427a953f7f05a0945a9a7023073713b2a10423390d288c40
server_unit_sha256=259fa4ee0a29fb863bd1597eba9392bd9767c679c1e28b8b1ee91ce1ea01c84d
provider_identity_sha256=4d24f9e0ae327c3657555a96485d03c8820d27c4e42a1145792effb085ae6355
route_identity_sha256=66118cd71c558ddba3391950ebd258d8d1955d8ff722c66a335f27bdc29a82ea
```

Strict `ssh-keyscan` from the HK Mac reproduced the out-of-band ED25519
fingerprint. Key-only strict SSH succeeded. The live systemd fragment is
`/usr/lib/systemd/system/sing-box.service`; an earlier check of the historical
`/etc/systemd/system` path was rejected and replaced by `FragmentPath`
authority.

## Security-group and real-protocol admission

The final ingress contract is only:

- HK `119.13.90.246/32` to TCP 22;
- HK `119.13.90.246/32` to UDP 8443.

The user removed the prior two extra SSH sources and public TCP 443 rule. The
paired runtime discriminator then passed:

```text
non-HK direct SSH: exit=255, Connection closed
HK strict SSH:     PASS
```

An arbitrary short UDP datagram had earlier produced no guest packet and was
not accepted as TUIC authority. The final exact no-TUN mini_vpn probe fetched
the server credential into HK process memory only, emitted no credential, and
completed the frozen handshake/auth/Connect path:

```text
tuic_tcp_sink_probe target=43.130.32.77:5201 requested_duration_secs=1 \
elapsed_ms=1001 bytes=0 read_mbps=0.000 reads=0 first_rx_ms=none \
max_read_gap_ms=0 eof=false
```

A temporary accept-only guest nftables counter observed `25 packets / 11,843B`
on UDP 8443. Its table was deleted and absence rechecked immediately after the
probe. `sing-box` remained active with `NRestarts=0` and `ExecMainStatus=0`.

## Agent isolation and reboot authority

Alibaba Security Center Agent Protection initially rejected local removal and
left `aegis.service` enabled plus `AliSecGuard` loaded. The user removed this
exact instance from Agent Protection. The unit was then stopped/disabled and
the Exit rebooted.

Post-reboot authority at boot time `2026-08-17 14:44:14Z` was:

```text
aegis.service:          not-found / inactive
Aegis unit paths:       0
Aegis processes:        0
AliSecGuard modules:    0
```

The reboot also retained:

- exact ED25519 host key and server binary/config/unit hashes;
- `sing-box` active, zero restarts, UDP 8443 listener present;
- Target `43.130.32.77:5201` reachable;
- zero global IPv6 addresses;
- UTC and synchronized NTP;
- `apt-daily` and `apt-daily-upgrade` timers disabled;
- `aliyun.service` disabled/inactive;
- 2 vCPU, 3,499 MiB memory, and about 35,865 MiB free root storage.

## Local gates and cleanup

Sequential observer/controller, resource profile/preflight, frequency reducer,
and continuity-ledger **self-tests**, plus Python compile, diff, and
changed-diff secret gates pass. The per-run resource preflight is deliberately
not claimed here: it requires the new committed source plus fresh direct and
candidate-profile evidence and remains part of the next run preparation. The
HK worktree remained clean at source `2b38d077da72` before this admission
documentation. The one-time probe script and temporary guest nftables table
are absent. No TUN, route, DNS, observer, or Tier-B epoch ownership began.

## Decision

Replacement Exit control-plane, host-isolation, security-group, and real TUIC
admission pass. Freeze `tierb-alibaba-usw1-r1` and its exact identities for all
twelve Tier-B epochs. Next, commit/push this identity closure, sync/build the
HK Mac, take fresh baseline/direct/resource evidence, and start the first
four-epoch/24-hour `m2-frequency` run. M3 remains blocked until twelve valid
epochs and the immutable ledger emit `TIER_B_ACCEPTED`.
