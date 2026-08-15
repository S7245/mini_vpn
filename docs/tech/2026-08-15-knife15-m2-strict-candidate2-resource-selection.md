# Knife15 M2 Strict Candidate 2 Resource Selection

Date: 2026-08-15

Status: **SELECTION REVIEWED; USER AWS CONSOLE CREATION REQUIRED; NO
CANDIDATE-2 TRAFFIC YET**

## Outcome

Alibaba candidate 1 is immutably rejected by its first genuine formal strict
failure. The bounded Tier-A policy therefore permits one final resource
candidate. Following the accepted provider order after ordinary Tencent was
found ineligible, candidate 2 is AWS Lightsail in Asia Pacific (Tokyo),
`ap-northeast-1`.

Tokyo is selected instead of Oregon or Singapore to change both provider/ASN
and geography while balancing the two fixed legs: HK Mac to Exit and Exit to
the US-west Target. A direct HK-Mac probe of AWS regional API endpoints showed
stable TCP connect times of approximately `72..74ms` to Tokyo, `51..56ms` to
Singapore, and `199..206ms` to Oregon. Singapore is slightly closer to the
Mac, but Tokyo is materially closer to the fixed US-west Target; live
candidate admission, not these pre-allocation probes, remains authoritative.

AWS documents Lightsail availability in Tokyo and bills instance bundles
hourly up to their monthly price. The compute-optimized 4-GiB IPv4 bundle
provides 2 vCPUs, 4 GiB memory, 160 GB storage, and a 5-TB transfer allowance.
It is selected to avoid the standard bundle's CPU-credit variable during a
roughly 55-hour possible qualification/formal sequence.

## Exact Creation Contract

Create exactly one Lightsail instance with these values:

| Field | Required value |
| --- | --- |
| Provider/product | AWS Lightsail virtual server |
| Region | Asia Pacific (Tokyo), `ap-northeast-1` |
| Availability Zone | any offered Tokyo zone; record the exact zone |
| Platform | Linux/Unix |
| Blueprint | OS only, Ubuntu 24.04 LTS |
| Plan | Compute optimized Large, 2 vCPU / 4 GiB, public IPv4 |
| Storage/transfer | 160 GB SSD / 5 TB allowance |
| Instance name | `knife15-m2-candidate2-aws-tokyo` |
| Static IP name | `knife15-m2-candidate2-aws-tokyo-ip` |
| Addressing | attach one Lightsail static public IPv4 directly to the instance |
| SSH key | upload the public key derived from `~/.ssh/vpn`; never upload the private key |
| Inbound TCP | port 22 from the current HK public IPv4 `/32` |
| Inbound UDP | port 8443 from the current HK public IPv4 `/32` |
| Other inbound | none; remove default HTTP/HTTPS rules |
| IPv6 | disabled for this dedicated strict test resource |

Do not create a load balancer, CDN, database, container service, distribution,
snapshot schedule, additional disk, or DNS zone. Do not enable TCP 8443; TUIC
uses UDP. Do not use a burstable Standard plan as a silent substitute. If the
exact compute-optimized 4-GiB IPv4 bundle or Ubuntu 24.04 blueprint is absent
in Tokyo, stop before purchase and report the available choices.

The current HK public IPv4 observed by candidate-1 evidence is
`119.13.90.246`. Recheck it immediately before saving firewall rules; use the
then-current exact `/32` and never `0.0.0.0/0`.

## Required Nonsecret Handoff

After creation and static-IP attachment, return only:

- the static public IPv4;
- the exact Availability Zone;
- the instance and static-IP names;
- an SSH command using the uploaded key and Ubuntu login user; and
- the ED25519 host-key fingerprint collected from the Lightsail web console:

```bash
sudo ssh-keygen -lf /etc/ssh/ssh_host_ed25519_key.pub
```

Do not return AWS account IDs, access keys, private keys, TUIC UUIDs,
passwords, certificate private keys, or billing identifiers.

## Agent-Owned Admission

After the handoff, the agent will, without another Mac-operation request:

1. match `ssh-keyscan` to the out-of-band fingerprint before trusting the
   host;
2. prove the allocated public provider/ASN is materially different from both
   Tencent AS132203 and Alibaba AS45102;
3. install the exact reviewed sing-box binary/config/certificate service and
   bounded observer dependencies;
4. make SSH key-only, suppress automatic maintenance only for the dedicated
   test window, and verify reboot-stable service identity;
5. prove Target reachability, UDP 8443 guest ingress, a real no-TUN TUIC
   handshake/auth/Connect, capacity/headroom, and fail-closed cleanup;
6. create one immutable candidate-2 profile and run exactly one strict
   qualification;
7. if qualification passes, run at most two consecutive strict formal M2s;
   stop at the first genuine quality failure.

Production Rust, source `b4244a7`, workload, strict SLI, observer, Target,
D16, Endpoint pacing, QUIC settings, and every frozen value remain unchanged.

Official AWS references checked on 2026-08-15:

- <https://docs.aws.amazon.com/lightsail/latest/userguide/understanding-regions-and-availability-zones-in-amazon-lightsail.html>
- <https://docs.aws.amazon.com/lightsail/latest/userguide/amazon-lightsail-bundles.html>
- <https://docs.aws.amazon.com/lightsail/latest/userguide/amazon-lightsail-faq-data-transfer-allowance.html>
- <https://docs.aws.amazon.com/lightsail/latest/userguide/amazon-lightsail-faq-networking.html>
