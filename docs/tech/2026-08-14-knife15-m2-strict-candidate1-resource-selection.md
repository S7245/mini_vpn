# Knife15 M2 Strict Candidate 1 Resource Selection

Date: 2026-08-14

Status: **SELECTION REVIEWED; RESOURCE NOT YET CREATED OR ADMITTED**

## Outcome

The existing `.111` and `.27` hosts are not eligible Knife15 Tier-A resource
candidates:

- `.33` (`43.153.32.33`), `.111` (`43.173.101.111`), and `.27`
  (`43.172.75.27`) are currently announced by AS132203 and identify as Tencent
  network resources. Changing their historical role from Client to Exit does
  not change the provider/ASN failure domain.
- `.111` also presented a new SSH ED25519 host key relative to the pinned
  `known_hosts` entry. Do not remove or replace the old entry without an
  out-of-band fingerprint from the current instance owner.
- `.27` closed the current SSH connection before a read-only probe completed.
  Neither availability problem changes the eligibility decision.
- The current development Mac routes these addresses through `utun1024` while
  Clash is active. Those routes are not candidate evidence. The formal
  candidate route/traceroute must be captured on the HK Mac, on its physical
  interface, with every external VPN/TUN stopped.

No candidate is unbounded. Ordinary Tencent CVM is still ineligible because it
does not change the failed Tencent/AS132203 public-Internet failure domain.
Tencent Anycast Internet Acceleration (AIA) could qualify as an independently
contracted route, but it is a separate bandwidth-capped, 95th-percentile-billed
acceleration product rather than an ordinary new VPS. It is not the default
candidate because its exact account availability and purchase-page cost are
not yet bounded.

Following the user's provider preference after removing the ineligible
ordinary-Tencent option, the selected candidate-1 product is Alibaba Cloud ECS
plus an EIP in US (Silicon Valley), `us-west-1`, subject to exact
post-allocation ASN and route admission. AWS Lightsail becomes the provisioning
fallback and, after a genuine candidate-1 quality rejection, the preferred
candidate 2. Expected provider/ASN diversity is not evidence: the assigned EIP
must be checked and the candidate is rejected before TUN ownership if its
actual provider/ASN is equivalent to the `.33` reference.

## Exact Creation Contract

Create exactly one resource with these stable values:

| Field | Required value |
| --- | --- |
| Provider/product | Alibaba Cloud ECS plus EIP |
| Region | US (Silicon Valley), `us-west-1` |
| Zone | any available `us-west-1` zone; record the exact allocated zone |
| Platform | Linux, VPC |
| Image | Ubuntu 24.04 LTS |
| Instance type | `ecs.c8i.large`, 2 vCPU, 4 GiB |
| System disk | 40 GiB ESSD, PL0 or higher |
| Instance name | `knife15-m2-candidate1-usw1` |
| EIP name | `knife15-m2-c1-usw1-eip` |
| Addressing | IPv4 EIP attached directly to this ECS instance |
| EIP metering | pay-by-data-transfer |
| EIP peak bandwidth | exactly 200 Mbit/s |
| SSH key | upload the RSA public key derived from `~/.ssh/VPN-test.pem` |
| Inbound TCP | port 22 from the HK test operator's stable public IPv4 `/32` |
| Inbound UDP | port 8443 from the HK test operator's stable public IPv4 `/32` |
| Other inbound | none |

Do not add a load balancer, CDN, shared-bandwidth product, NAT gateway, managed
database, or automatic snapshot. Do not open TCP 8443; TUIC uses UDP 8443.
Keep the instance and EIP attached until the candidate is accepted or rejected
and every paired artifact is sealed. Do not silently substitute another
instance family, region, bandwidth cap, or billing mode: first record the
purchase-page availability and estimated price.

Before opening the create-instance page, derive only the public half of the
existing RSA key on the Mac:

```bash
ssh-keygen -y -f "$HOME/.ssh/VPN-test.pem" > \
  /tmp/knife15-alibaba-usw1.pub
ssh-keygen -lf /tmp/knife15-alibaba-usw1.pub
```

Upload `/tmp/knife15-alibaba-usw1.pub`; never upload or paste
`~/.ssh/VPN-test.pem`. The agent will use that existing private key only from
the Mac filesystem.

The selected compute-optimized plan is deliberate: it avoids a burst-credit
CPU variable. The instance has substantially more internal-network capacity
than the 200 Mbit/s Internet EIP. Alibaba documents the pay-by-data-transfer
EIP cap as best effort rather than guaranteed. That is acceptable only because
the frozen M2 offered load is below 100 Mbit/s and the read-only admission gate
must independently prove path, throughput, CPU, queue, socket, and drop
headroom before the candidate consumes a slot. The cap must remain fixed for
qualification and both formal attempts.

The 200 Mbit/s pay-by-data-transfer EIP is selected instead of a high fixed
bandwidth purchase to keep a roughly 55-hour experiment economically bounded.
Only outbound Internet data is charged under this mode. Capture the console's
exact instance, EIP, and estimated-transfer prices before purchase; price is
not part of technical admission and must not be guessed from this document.

Official references checked on 2026-08-14:

- <https://www.alibabacloud.com/help/en/ecs/user-guide/regions-and-zones>
- <https://www.alibabacloud.com/help/en/ecs/user-guide/compute-optimized-instance-families>
- <https://www.alibabacloud.com/help/en/ecs/user-guide/network-bandwidth/>
- <https://www.alibabacloud.com/help/en/eip/product-overview/limits>
- <https://www.alibabacloud.com/help/en/eip/pay-as-you-go/>
- <https://www.tencentcloud.com/jp/document/product/213/12523>
- <https://www.tencentcloud.com/ko/document/product/644/12628>
- <https://www.tencentcloud.com/ko/document/product/684/67493>
- <https://docs.aws.amazon.com/lightsail/latest/userguide/amazon-lightsail-bundles.html>
- <https://docs.aws.amazon.com/lightsail/latest/userguide/amazon-lightsail-faq-data-transfer-allowance.html>

## Out-of-Band Identity Handoff

Before the agent first connects, use Alibaba Cloud ECS Workbench/VNC on the new
instance and collect only these nonsecret values:

```bash
printf 'instance_name=%s\n' 'knife15-m2-candidate1-usw1'
printf 'eip_resource=%s\n' 'knife15-m2-c1-usw1-eip'
printf 'availability_zone=%s\n' '<copy exact zone from ECS console>'
printf 'ssh_ed25519_fingerprint=';
sudo ssh-keygen -lf /etc/ssh/ssh_host_ed25519_key.pub | awk '{print $2}'
```

Return the EIP, instance name, EIP resource name, Availability Zone, and SSH
ED25519 fingerprint. Never return an Alibaba account ID, AccessKey ID/secret,
private SSH key, TUIC UUID, or TUIC password.

## Agent-Owned Admission After Creation

After the identity handoff, the agent will:

1. compare `ssh-keyscan` with the out-of-band fingerprint before trusting the
   host and add only the exact new host key;
2. verify the actual public IPv4 ASN and provider, rejecting AS132203 or an
   equivalent resource before any test slot is consumed;
3. patch the OS, install only the observer dependencies, copy the exact
   reviewed sing-box `1.13.14` binary/config/certificate material without
   printing credentials, and install a bounded systemd service;
4. verify UDP 8443, `.77:5201`, server/config hashes, CPU/memory/socket/drop
   headroom, and fail-closed cleanup;
5. create the exact sanitized provider and route identity records and the
   immutable candidate profile;
6. run the read-only resource preflight from the HK Mac while Clash and every
   other VPN/TUN are off;
7. only then provide the concrete qualification commands from the reviewed
   strict-resource runbook.

Candidate 1 is not admitted by this document. No long Mac run, ledger attempt,
or Tier-A candidate slot begins until all six admission steps pass.
