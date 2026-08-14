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

The selected candidate-1 product is Amazon Lightsail in `us-west-2` (Oregon),
subject to exact post-allocation ASN and route admission. It materially changes
the cloud provider and is expected to change the announcing ASN, but expectation
is not evidence: the assigned static IPv4 must be checked and the candidate is
rejected before TUN ownership if its actual ASN is still AS132203.

## Exact Creation Contract

Create exactly one resource with these stable values:

| Field | Required value |
| --- | --- |
| Provider/product | Amazon Lightsail |
| Region | US West (Oregon), `us-west-2` |
| Platform | Linux/Unix, OS-only |
| Image | Ubuntu 24.04 LTS |
| Plan | Compute Optimized, 4GB RAM, 2 vCPU, public IPv4 |
| Instance name | `knife15-m2-candidate1-usw2` |
| Static-IP name | `knife15-m2-c1-usw2-ip` |
| Addressing | dual-stack instance with an attached static IPv4 |
| SSH key | upload the RSA public key derived from `~/.ssh/VPN-test.pem` |
| Inbound TCP | port 22 from the HK test operator's stable public IPv4 `/32` |
| Inbound UDP | port 8443 from IPv4 clients |
| Other inbound | none |

Do not install an application blueprint, load balancer, CDN, managed database,
or automatic snapshot. Do not open TCP 8443; TUIC uses UDP 8443. Keep the
instance and static IPv4 attached until the candidate is accepted or rejected
and every paired artifact is sealed.

Lightsail's import API requires an RSA public key, while `~/.ssh/vpn` is
ED25519. Before opening the create-instance page, derive only the public half
of the existing RSA key on the Mac:

```bash
ssh-keygen -y -f "$HOME/.ssh/VPN-test.pem" > \
  /tmp/knife15-lightsail-usw2.pub
ssh-keygen -lf /tmp/knife15-lightsail-usw2.pub
```

Upload `/tmp/knife15-lightsail-usw2.pub`; never upload or paste
`~/.ssh/VPN-test.pem`. The agent will use that existing private key only from
the Mac filesystem.

The selected compute-optimized plan is deliberate. The general-purpose 4GB
plan has burstable CPU behavior, which would introduce another long-duration
resource variable. AWS currently lists the compute-optimized 4GB/2-vCPU public
IPv4 bundle at USD 42/month with 5TB transfer and on-demand hourly billing up
to that monthly maximum. At roughly 55 hours for qualification plus two formal
runs, compute cost is approximately USD 3.20 before tax, assuming the resource
is deleted after evidence closure and transfer stays within the bundle.

Official references checked on 2026-08-14:

- <https://docs.aws.amazon.com/lightsail/latest/userguide/amazon-lightsail-bundles.html>
- <https://docs.aws.amazon.com/lightsail/latest/userguide/understanding-regions-and-availability-zones-in-amazon-lightsail.html>
- <https://docs.aws.amazon.com/lightsail/latest/userguide/amazon-lightsail-frequently-asked-questions-faq-billing-and-account-management.html>
- <https://docs.aws.amazon.com/lightsail/latest/userguide/lightsail-create-static-ip.html>
- <https://docs.aws.amazon.com/lightsail/latest/userguide/understanding-firewall-and-port-mappings-in-amazon-lightsail.html>

## Out-of-Band Identity Handoff

Before the agent first connects, use the Lightsail browser terminal on the new
instance and collect only these nonsecret values:

```bash
printf 'instance_name=%s\n' 'knife15-m2-candidate1-usw2'
printf 'static_ip_resource=%s\n' 'knife15-m2-c1-usw2-ip'
token="$(curl -fsS -X PUT \
  -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' \
  http://169.254.169.254/latest/api/token)"
printf 'availability_zone=%s\n' "$(curl -fsS \
  -H "X-aws-ec2-metadata-token: $token" \
  http://169.254.169.254/latest/meta-data/placement/availability-zone)"
printf 'ssh_ed25519_fingerprint=';
sudo ssh-keygen -lf /etc/ssh/ssh_host_ed25519_key.pub | awk '{print $2}'
```

If the metadata query fails, read the Availability Zone from the Lightsail
instance page instead. Return the static IPv4, instance name, static-IP
resource name, Availability Zone, and SSH ED25519 fingerprint. Never return an
AWS account ID, access key, secret key, private SSH key, TUIC UUID, or TUIC
password.

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
