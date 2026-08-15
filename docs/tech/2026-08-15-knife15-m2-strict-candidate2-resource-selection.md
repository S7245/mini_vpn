# Knife15 M2 Strict Candidate 2 Resource Selection

Date: 2026-08-15

Status: **ALIBABA TOKYO SUBSTITUTE CREATED; HOST/IMAGE IDENTITY VERIFIED;
SERVICE AND TRAFFIC ADMISSION PENDING**

## Outcome

Alibaba candidate 1 is immutably rejected by its first genuine formal strict
failure. The bounded Tier-A policy permits one final resource candidate. The
user explicitly selected Alibaba Cloud Tokyo instead of the previously
reviewed AWS Lightsail Tokyo resource after reviewing the weaker
provider-diversity tradeoff.

Candidate 2 is therefore one Alibaba Cloud ECS plus directly attached EIP in
Japan (Tokyo), `ap-northeast-1`. This changes the frozen `.33` Tencent
AS132203 provider/ASN, Exit IPv4, resource, region, and live route. It does not
change provider/ASN relative to Alibaba candidate 1: both are AS45102. That
limitation is part of the evidence and must not be described as provider
diversity.

The substitution is still structurally eligible against the frozen `.33`
reference under the reviewed resource-profile rule
`distinct_provider_and_asn`. Candidate execution remains serialized: the
ledger may admit candidate 2 only because candidate 1 is already rejected. A
genuine candidate-2 quality failure exhausts Tier A and opens the separately
specified Tier-B implementation; it does not authorize an AWS retry.

## Exact Allocated Identity

| Field | Observed or required value |
| --- | --- |
| Candidate ID | `candidate2-alibaba-tokyo` |
| Provider/product | Alibaba Cloud ECS plus EIP |
| Provider/ASN | Alibaba Cloud / AS45102 |
| Region | Japan (Tokyo), `ap-northeast-1` |
| Availability Zone | `ap-northeast-1c` |
| Instance ID | `i-6weckus0r7voaarxz2k3` |
| Instance type | `ecs.c8ine.large`, x86_64, 2 vCPU / 4 GiB class |
| Image | Ubuntu 24.04 LTS, `ubuntu_24_04_x64_20G_alibase_20260720.vhd` |
| System disk | 40 GiB class |
| VPC | `vpc-6wecmya2bem2iq46rv0k6` |
| vSwitch | `vsw-6weqbkdl4osubcf0lbnyn` |
| Private IPv4 | `172.20.158.70` |
| Direct EIP | `8.211.176.98` |
| Route class | `public-internet` |
| Route contract ID | `alibaba-eip-8.211.176.98` |
| EIP metering/cap | required contract: pay by data transfer, 200 Mbit/s peak; live headroom pending |
| TUIC ingress | required contract: UDP 8443 from current HK public IPv4 `/32`; live ingress pending |
| SSH ingress | required contract: TCP 22 from current HK public IPv4 `/32` only |
| IPv6 | no global IPv6 address |
| ED25519 host key | `SHA256:r7JYHgl+fH36CCVXrb7fc5ADsg1fnM9Z8j6KowZZYYM` |

The user collected the host-key fingerprint out of band from the Alibaba web
console. A fresh `ssh-keyscan` produced the exact same fingerprint before the
old rebuilt-host entry was replaced. Strict SSH then proved the same host key,
Ubuntu 24.04 image ID, region, zone, instance ID/type, VPC/vSwitch, private
address, EIP metadata, and external source address.

The first system disk was Ubuntu 22.04. It was rejected before service
deployment or test traffic, rebuilt with Ubuntu 24.04, and assigned a new
out-of-band-verified host key. No candidate attempt or Tier-A slot was consumed
by the rejected image.

## Frozen Resource Contract

Keep the allocated instance, EIP, region/zone, instance type, image, disk,
VPC/vSwitch, and bandwidth cap unchanged through qualification and any formal
attempts. Do not add a load balancer, CDN, shared-bandwidth product, NAT
gateway, managed database, automatic snapshot, additional disk, or IPv6.
Do not enable TCP 8443; TUIC uses UDP.

The EIP's 200-Mbit/s pay-by-data-transfer peak is best effort rather than
guaranteed. It is only nominally sufficient for the frozen sub-100-Mbit/s
workload. The live admission gate must independently prove Target
reachability, usable path headroom, CPU/memory/socket/drop headroom, and a
real external TUIC handshake/authentication/Connect before TUN ownership.

Production Rust, exact test source/binary, workload, strict SLI, observer,
Target, D16, Endpoint pacing, QUIC settings, and every frozen value remain
unchanged. Candidate credentials may be unique, but they must remain only in
the root-owned server configuration and test process environment; never put
them in the repository, evidence, ledger, or messages.

## Agent-Owned Admission

The agent owns the remaining sequence without another operator command:

1. provision the exact reviewed sing-box version, test certificate shape,
   bounded observer dependencies, and key-only SSH;
2. suppress Alibaba guest agents and automatic maintenance only for the
   dedicated test window, then prove reboot-stable host/service identity;
3. create sanitized provider and route identity records that state both the
   `.33` diversity and the candidate-1 same-ASN limitation;
4. prove Target `43.130.32.77:5201`, guest UDP 8443 ingress, service hashes,
   observer lifecycle, and a real no-TUN TUIC handshake/authentication/Connect;
5. create one immutable candidate-2 profile and run exactly one strict
   qualification;
6. if qualification passes, run at most two consecutive strict formal M2s;
   stop at the first genuine quality failure.

If qualification or either formal run has a genuine strict quality failure,
reject candidate 2 and implement Tier B. Environment/operator/cloud
invalidations remain decision-neutral but must be diagnosed and sealed; they
do not authorize resource or frozen-value tuning.

Official Alibaba references checked on 2026-08-15:

- <https://www.alibabacloud.com/help/en/ecs/user-guide/regions-and-zones>
- <https://www.alibabacloud.com/help/en/ecs/user-guide/compute-optimized-instance-families>
- <https://www.alibabacloud.com/help/en/eip/product-overview/limits>
- <https://www.alibabacloud.com/help/en/eip/pay-as-you-go/>
- <https://www.alibabacloud.com/help/en/eip/bind-an-eip-to-a-cloud-resource>
