# Knife15 M2 Tier-B Frequency macOS Runbook

Date: 2026-08-17

Status: **LOCAL GATES AND REPLACEMENT EXIT ADMISSION PASS; FIRST FOUR-EPOCH
RUN NOT STARTED**

This is the only active Tier-B procedure. It does not reopen Tier A, add a
third candidate, tune a frozen value, or change mini_vpn production code. The
HK Mac is agent-operated through
`ssh -i ~/.ssh/vpn xiaoou@192.168.133.109`, using the clean
`/Users/xiaoou/mini_vpn` clone. The user does not open Clash, another TUN, or a
separate TUIC client.

## Fixed decision and resource

- Use the replacement Alibaba US West Tier-B Exit, `47.89.211.4`, for all
  twelve epochs. The original candidate-1 ECS was destroyed after Tier A was
  sealed but before any Tier-B epoch began. The directly attached EIP and
  route contract survived; the replacement ECS, host key, server
  configuration, and candidate ID are therefore frozen as a new Tier-B
  resource identity before the first epoch. This is not a third Tier-A
  candidate or an unchanged strict retry. Do not use rejected Tokyo candidate
  2, whose first UDP phase reached `6.164314%`.
- One run requests four exact six-hour epochs. Three valid four-epoch runs
  produce 72 valid hours and independently satisfy the uninterrupted 24-hour
  process/TUN lifetime requirement.
- The exact source, release binary, preserved baseline/workload contract,
  candidate, server binary/config, observer, Exit/Target, and stable
  provider/route identity remain unchanged across all three runs.
- Each run still takes a fresh 300-second direct discriminator and fresh
  resource preflight. Those proofs are validated inside that run but do not
  masquerade as stable resource identity.
- A genuine Tier-B continuity or existing safety failure stops evaluation. An
  unrelated infrastructure interruption invalidates only the incomplete
  epoch; already sealed epochs remain useful and the next run records an
  explicit evidence gap.
- Only the immutable ledger may emit `TIER_B_ACCEPTED`. M3 remains blocked
  before that result.

Candidate anchors:

```text
candidate_id=tierb-alibaba-usw1-r1
provider=alibaba-cloud
resource_id=i-rj9c5rn1psf504mf1zo2
region=us-west-1
public_ipv4=47.89.211.4
asn=45102
route_class=public-internet
route_contract_id=eip-rj9hj9g6dbtxwwxfqmw0t
tuic_port=8443
target_ipv4=43.130.32.77
target_iperf_port=5201
server_binary_sha256=4ea794fddcb2ad84532adeab979a9b0d7b2052822bb3439dfb321c33c941da19
server_config_sha256=0c48b68369253dfa427a953f7f05a0945a9a7023073713b2a10423390d288c40
host_key_ed25519=SHA256:km4qtBz/r+jNuPv4WKoCP3LoIyw1U6nfRNMIWpW9K24
provider_identity_sha256=4d24f9e0ae327c3657555a96485d03c8820d27c4e42a1145792effb085ae6355
route_identity_sha256=66118cd71c558ddba3391950ebd258d8d1955d8ff722c66a335f27bdc29a82ea
```

## One-time source and Tier-A admission setup

Complete and push all Task-9 review changes before selecting the test source.
On the Mac, fetch that clean commit, detach it, build once, and record it as
`TEST_SOURCE_COMMIT`. Do not advance it between runs.

Copy only the reviewed compact Tier-A files to a mode-700 directory on the
Mac; giant Tier-A captures are not runtime inputs:

```text
knife15-tier-a-ledger-004.json
evaluation-004.json
```

Their exact SHA-256 values must be:

```text
96e50cd21bc9cc277596d82e90d4c7fc0a5856c24e05f94d58ab8b3dec7b1989
8a95308964bb464fcaef1686eaebc7627839f269b174960365d0be4c0c2d8733
```

The runtime exports are:

```bash
export M2_FREQUENCY_TIER_A_LEDGER="$HOME/knife15-evidence/tier-a-exhaustion/knife15-tier-a-ledger-004.json"
export M2_FREQUENCY_TIER_A_ARTIFACT_ROOT="$HOME/knife15-evidence/tier-a-exhaustion"
export M2_FREQUENCY_EPOCHS=4
```

Run all self-tests before exporting `M2_BASELINE_DIR`; the runner deliberately
tests neutral baseline selection.

## Immutable public environment

Export the standard target and candidate fields from the strict-resource
runbook, substituting the candidate anchors above. In particular:

```bash
export TARGET=43.130.32.77
export DNS_TARGET=8.8.8.8
export DNS_NAME=example.com
export IPERF_PORT=5201
export DURATION=20
export PARALLEL=1
export METRICS_SECS=30
export SAMPLE_SECS=30

export M2_CANDIDATE_ID='tierb-alibaba-usw1-r1'
export M2_CANDIDATE_PROVIDER='alibaba-cloud'
export M2_CANDIDATE_RESOURCE_ID='i-rj9c5rn1psf504mf1zo2'
export M2_CANDIDATE_REGION='us-west-1'
export M2_CANDIDATE_IPV4='47.89.211.4'
export M2_CANDIDATE_ASN='45102'
export M2_CANDIDATE_ROUTE_CLASS='public-internet'
export M2_CANDIDATE_ROUTE_CONTRACT_ID='eip-rj9hj9g6dbtxwwxfqmw0t'
export M2_CANDIDATE_TUIC_PORT=8443
export M2_SERVER_BINARY_SHA256='4ea794fddcb2ad84532adeab979a9b0d7b2052822bb3439dfb321c33c941da19'
export M2_SERVER_CONFIG_SHA256='0c48b68369253dfa427a953f7f05a0945a9a7023073713b2a10423390d288c40'
export M2_PROVIDER_IDENTITY_EVIDENCE="$HOME/knife15-evidence/resource-identities/tierb-alibaba-usw1-r1-provider-identity.txt"
export M2_ROUTE_IDENTITY_EVIDENCE="$HOME/knife15-evidence/resource-identities/tierb-alibaba-usw1-r1-route-identity.txt"
export EXIT_SSH_HOST='root@47.89.211.4'
export EXIT_SSH_KEY="$HOME/.ssh/vpn"
export M2_EXIT_SERVER_CONFIG_PATH='/etc/sing-box/config.json'

export TEST_SOURCE_COMMIT="$(git rev-parse HEAD)"
export M2_EVIDENCE_HOME="$HOME/knife15-evidence/$M2_CANDIDATE_ID/$TEST_SOURCE_COMMIT"
mkdir -p "$M2_EVIDENCE_HOME"
chmod 700 "$M2_EVIDENCE_HOME"
```

TUIC UUID/password remain only in the live shell environment. Never write,
print, archive, message, or commit them. Provider/route evidence remains the
reviewed sanitized closed-form files outside the repository.

## Per-run preparation

For every run:

1. prove candidate host key, `sing-box` active/zero restarts, exact binary and
   config hashes, UDP 8443 listener, Target `43.130.32.77:5201`, clock,
   capacity, observer absence, and the exact security-group contract: only
   TCP 22 and UDP 8443 from HK `119.13.90.246/32`, with no public 443 or other
   SSH source; prove `aegis.service` disabled/inactive and no `AliSecGuard`
   module after reboot;
2. prove the Mac has no other VPN/TUN, stays on power and one physical network,
   and has an active bounded `caffeinate -dimsu` owner;
3. safely record and disable physical IPv6 exactly as section 2 of the strict
   resource runbook; restore the recorded mode only after cleanup;
4. run build, frequency controller, runner, reducer, ledger, resource, and
   observer self-tests from a clean tracked worktree;
5. run `baseline` only for run 1, copy that exact directory outside `/tmp`,
   and reuse it for runs 2 and 3;
6. run one fresh `direct-discriminator` against the preserved baseline;
7. generate a fresh candidate profile bound to the exact source, release
   binary, fresh direct manifest, physical interface, provider/route evidence,
   and reviewed server/observer hashes;
8. run the nonsudo resource preflight and immediately move `OUT_DIR` into
   `M2_RESOURCE_PREFLIGHT_DIR`, then `unset OUT_DIR`;
9. require the fresh direct result to remain under the runner's 15-minute age
   limit when `m2-frequency` begins.

Before starting the observer, require `command -v msmtp` or
`test -x /opt/homebrew/bin/msmtp` to pass and require the user's msmtp config
to exist. Missing email configuration is not a test failure, but it must be
known before entrusting progress visibility to the completion notice.

The exact profile-generation and IPv6 procedures are sections 2–5 of
`docs/tech/2026-08-14-knife15-m2-strict-resource-macos-runbook.md`. Do not use
its historical candidate-2 values or strict `m2` action.

## Start, smoke, observer, and detached owner

Create and attach one persistent macOS `screen` TTY; do not run the controller
as a child of the SSH connection itself:

```bash
screen -S knife15-frequency-01
cd /Users/xiaoou/mini_vpn
```

Inside that screen, export the already-reviewed environment, authenticate
`sudo -v` once without storing the password, and execute:

```bash
sudo -E bash scripts/knife15-macos-soak.sh start
sudo -E bash scripts/knife15-macos-soak.sh smoke

export TUIC_PORT=8443
export OBSERVER_TIMEOUT_SECS=93600
bash scripts/knife15-exit-target-observer.sh start
bash scripts/knife15-exit-target-observer.sh status

sudo -n -v
export KNIFE15_FREQUENCY_CONTROLLER_LOG="$M2_EVIDENCE_HOME/controller_run01_$(date -u '+%Y%m%d_%H%M%S').log"
test ! -e "$KNIFE15_FREQUENCY_CONTROLLER_LOG"
bash scripts/knife15-m2-frequency-controller.sh \
  >"$KNIFE15_FREQUENCY_CONTROLLER_LOG" 2>&1
export KNIFE15_FREQUENCY_CONTROLLER_RC=$?
printf 'controller_rc=%s\ncontroller_log=%s\n' \
  "$KNIFE15_FREQUENCY_CONTROLLER_RC" "$KNIFE15_FREQUENCY_CONTROLLER_LOG"
```

Detach with `Ctrl-A`, then `D`; do not send `Ctrl-C`. A later SSH session may
inspect it with `screen -ls` and
`tail -n 80 "$KNIFE15_FREQUENCY_CONTROLLER_LOG"`. Keep the screen until its
controller exits and evidence is synchronized.

The versioned controller:

- executes only `m2-frequency`;
- maintains the same-TTY sudo ticket every 45 seconds while the root workload
  owns the 24-hour run;
- immediately after `m2-frequency` returns, success or failure, executes the
  user-requested best-effort notification equivalent to
  `printf "Subject: 执行结束~" | msmtp 870941563@qq.com`;
- bounds that notification to 15 seconds and ignores failure/timeout for
  evidence verdicts;
- always attempts `status`, `snapshot`, and `stop` in that order;
- after Mac cleanup, classifies the Exit observer state and freezes/bundles it
  only if the runner did not already do so; an unknown observer status makes
  the controller fail rather than leak ownership silently;
- reports failure if workload or cleanup failed.

The controller log lives outside the run directory. The SSH link is not the
lifetime owner. Do not press Ctrl+C, change network, start another VPN, or add
unrelated load.

## Evidence collection and ledger transaction

After the controller exits, verify IPv6 restoration, no owned utun/routes/DNS,
no observer/nftables ownership, no live workload/caffeinate leak, and one Mac
plus one Exit archive with their SHA-256 sidecars. Sync both archives to the
immutable local evidence root.

For a four-epoch bundle, run `seal-frequency-epoch` four times with the same
Mac/Exit pair, sequential ledger sequence numbers, and run indices `0`, `1`,
`2`, `3`. Append each record into a new ledger filename; never overwrite an
older ledger. Then run `evaluate-frequency` with artifact verification.

Expected states are:

```text
after 4 epochs:  TIER_B_PENDING, max same lifetime = 4
after 8 epochs:  TIER_B_PENDING
after 12 epochs: TIER_B_ACCEPTED only if every rolling/safety/cleanup gate passes
```

If a run stops mid-epoch, seal only earlier epochs having exact seal events,
paired observer coverage, complete archives, and cleanup. Never manufacture a
terminal boundary or bridge the missing period. If the failure is a genuine
quality/safety failure rather than unrelated infrastructure, stop Tier B and
open the path-diverse/resumable-upstream architecture stage.
