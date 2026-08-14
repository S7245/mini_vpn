# Knife15 M2 Strict Resource macOS Runbook

Date: 2026-08-14

Status: **CANDIDATE 1 PROVISIONED; DO NOT START UNTIL ITS CLOUD SECURITY GROUP
ADMITS HK-MAC UDP 8443 AND THE LIVE TUIC PROBE PASSES**

This is the only active Knife15 Tier-A Mac procedure. The old `.33` formal M2
runbook and market-client comparison are historical and must not be executed.
The user does **not** open Clash, Mihomo, a TUIC client, or another VPN: the
`mini_vpn client-tun` process started by this runner is the tested client.

The HK Mac is now agent-operated over
`ssh -i ~/.ssh/vpn xiaoou@192.168.133.109`; the user does not run these command
blocks. Use the clean `/Users/xiaoou/mini_vpn` clone. Do not use
`/Users/xiaoou/Desktop/mini_vpn`: macOS Desktop privacy denies SSH access and
that original directory remains user-owned. Privileged actions use a real SSH
TTY and an interactive `sudo` prompt; never persist the login password.

## Fixed Decision Policy

- `.33` is the already-rejected strict reference and is not a candidate.
- At most two new resources may be attempted, in order.
- Candidate 2 cannot start until candidate 1 has a valid quality rejection.
- Each candidate gets one valid strict qualification and, only after it
  passes, two valid consecutive strict formal runs.
- Qualification takes about 45 wall-clock minutes including setup. Each
  formal run needs about 25 uninterrupted wall-clock hours.
- A genuine quality/safety/lifecycle failure rejects the candidate without
  tuning or favorable-sample repetition. Power, operator, evidence, or VPS
  invalidation neither passes nor rejects it.
- Keep one exact source, release binary, workload contract, candidate
  identity, server binary/configuration, and observer version throughout one
  candidate. A fresh direct/resource profile is still required for each run.
- Only the ledger may emit `TIER_A_ACCEPTED` or `TIER_A_EXHAUSTED`.

## 0. Agent-Owned Resource Provisioning Gate

Do not hand this run to the Mac operator until all of the following exist:

1. A new Exit whose provider and ASN both differ from `.33`, or whose route
   class and independently contracted route identity both differ. A CPU/RAM/
   bandwidth resize on an equivalent route is not eligible. Existing `.111`
   is not accepted merely because it exists.
2. The candidate runs the same reviewed sing-box TUIC service shape, has the
   test certificate installed, listens on the selected UDP port, and can
   reach `.77:5201`.
3. Exact server binary and `/etc/sing-box/config.json` SHA-256 values are
   recorded. The configuration itself and its credentials are never copied
   into evidence.
4. Two sanitized regular files are supplied outside the repository:
   `provider-identity.txt` and `route-identity.txt`. They identify provider,
   resource, region, ASN, route class, and contract reference without account
   numbers, tokens, keys, passwords, or UUIDs. They use the exact closed forms
   below; their values must match the reviewed candidate profile byte for
   byte:

   ```text
   schema=knife15-m2-provider-identity-v1
   candidate_id=<candidate ID>
   provider=<provider>
   resource_id=<resource ID>
   region=<region>
   public_ipv4=<Exit IPv4>
   asn=<integer ASN>
   ```

   ```text
   schema=knife15-m2-route-identity-v1
   candidate_id=<candidate ID>
   public_ipv4=<Exit IPv4>
   target_ipv4=43.130.32.77
   route_class=<route class>
   route_contract_id=<independent contract ID>
   ```

   Unknown, missing, duplicate, or mismatched claims are
   rejected before TUN ownership. Hash equality alone is not admission.
5. The candidate's stable values below have been reviewed and supplied to the
   operator. Repository fixture IPs/hashes are parser tests and cannot be used
   as a real candidate.

VPS setup and health checks do not use the Mac TUN and may be performed before
the user session. Do not purchase or count a second candidate until the first
candidate's ledger state is `rejected`.

Candidate 1 is Alibaba Cloud ECS `i-rj9cabfprph7x3sard3z`, EIP
`47.89.211.4`, region/zone `us-west-1/us-west-1b`, AS45102, with EIP contract
`eip-rj9hj9g6dbtxwwxfqmw0t`. Its security group must allow inbound
`UDP/8443` from the current HK Mac public IPv4 `119.13.90.246/32`. Do not use
`0.0.0.0/0`. Recheck the Mac public IPv4 immediately before changing the rule;
if it changed, use the new exact `/32`. A local listener or successful SSH is
not proof of this rule: the resource preflight's live TUIC handshake is the
decisive admission.

## 1. One Clean Test Source Per Candidate

On the HK test Mac, quit Clash completely and disable Clash-TUN and every
other VPN/proxy/TUN. Keep the Mac on power, prevent sleep, and avoid network
switching or heavy unrelated traffic. Slow HK bandwidth is not itself a bug;
the baseline derives the frozen offered load.

Open a fresh terminal:

```bash
cd /Users/xiaoou/mini_vpn

git fetch origin
git switch codex/knife14d-downlink-reap-open
git pull --ff-only origin codex/knife14d-downlink-reap-open
git status --short
git merge-base --is-ancestor 0a1cf1c HEAD && \
  echo 'PASS: strict resource source accepted'
export TEST_SOURCE_COMMIT="$(git rev-parse HEAD)"

unset M0_BASELINE_DIR M0_DIRECT_DIR M1_BASELINE_DIR M1_DIRECT_DIR
unset M2_BASELINE_DIR M2_DIRECT_DIR M2_RESOURCE_PREFLIGHT_DIR
unset BASELINE_OUT_DIR DIRECT_OUT_DIR OUT_DIR

export TARGET=43.130.32.77
export DNS_TARGET=8.8.8.8
export DNS_NAME=example.com
export IPERF_PORT=5201
export DURATION=20
export PARALLEL=1
export METRICS_SECS=30
export SAMPLE_SECS=30
```

`git status --short` must print nothing. Keep `TEST_SOURCE_COMMIT` unchanged
until this candidate is accepted or rejected; do not pull between its
qualification and formal runs.

Export only the reviewed candidate values supplied by the agent:

```bash
export M2_CANDIDATE_ID='candidate1-alibaba-usw1'
export M2_CANDIDATE_PROVIDER='alibaba-cloud'
export M2_CANDIDATE_RESOURCE_ID='i-rj9cabfprph7x3sard3z'
export M2_CANDIDATE_REGION='us-west-1'
export M2_CANDIDATE_IPV4='47.89.211.4'
export M2_CANDIDATE_ASN='45102'
export M2_CANDIDATE_ROUTE_CLASS='public-internet'
export M2_CANDIDATE_ROUTE_CONTRACT_ID='eip-rj9hj9g6dbtxwwxfqmw0t'
export M2_CANDIDATE_TUIC_PORT=8443
export M2_SERVER_BINARY_SHA256='4ea794fddcb2ad84532adeab979a9b0d7b2052822bb3439dfb321c33c941da19'
export M2_SERVER_CONFIG_SHA256='9aa397471060d1ef11afea858ee2a7550c426a2e133cfee1d2468c55425f33d8'
export M2_PROVIDER_IDENTITY_EVIDENCE='/tmp/knife15-m2-candidate1-alibaba-usw1-provider-identity.txt'
export M2_ROUTE_IDENTITY_EVIDENCE='/tmp/knife15-m2-candidate1-alibaba-usw1-route-identity.txt'

export EXIT_SSH_HOST="root@$M2_CANDIDATE_IPV4"
export EXIT_SSH_KEY="$HOME/.ssh/vpn"
export M2_EXIT_SERVER_CONFIG_PATH='/etc/sing-box/config.json'

export MINI_VPN_TUIC_SERVER="$M2_CANDIDATE_IPV4:$M2_CANDIDATE_TUIC_PORT"
export MINI_VPN_TUIC_UUID='REPLACE_WITH_UUID'
export MINI_VPN_TUIC_PASSWORD='REPLACE_WITH_PASSWORD'
export MINI_VPN_TUIC_SNI='example.com'
export MINI_VPN_TUIC_CA_PATH='certs/dev/ca-cert.pem'
```

UUID/password remain only in this shell environment. Never paste them into a
profile, evidence file, bundle message, commit, or ledger.

Record the nonsecret candidate anchor outside both the repository and `/tmp`:

```bash
export M2_EVIDENCE_HOME="$HOME/knife15-evidence/$M2_CANDIDATE_ID/$TEST_SOURCE_COMMIT"
mkdir -p "$M2_EVIDENCE_HOME"
chmod 700 "$HOME/knife15-evidence" \
  "$HOME/knife15-evidence/$M2_CANDIDATE_ID" "$M2_EVIDENCE_HOME"
printf 'candidate_id=%s\nsource_commit=%s\n' \
  "$M2_CANDIDATE_ID" "$TEST_SOURCE_COMMIT" | \
  tee "$M2_EVIDENCE_HOME/candidate-anchor.txt"
chmod 600 "$M2_EVIDENCE_HOME/candidate-anchor.txt"
```

The anchor contains no credential and must not be executed with `source` or
`eval`. On a later terminal, read the two values, export them explicitly, and
require `git rev-parse HEAD` to equal the recorded commit. If the branch has
advanced, use `git switch --detach "$TEST_SOURCE_COMMIT"`; do not merge, pull,
or rebuild from a different commit inside this candidate sequence.

## 2. Disable Physical IPv6 Safely

Derive the physical interface and enabled network service from the candidate
Exit route; do not hard-code `Wi-Fi` even if the interface is `en0`:

```bash
export M2_PHYSICAL_IF="$(route -n get "$M2_CANDIDATE_IPV4" | \
  awk '/interface:/ {print $2; exit}')"
export M2_NETWORK_SERVICE="$({ networksetup -listnetworkserviceorder || true; } | \
  awk -v interface="$M2_PHYSICAL_IF" '
    /^\([0-9]+\) / {
      service = $0
      sub(/^\([0-9]+\) /, "", service)
      disabled = 0
      next
    }
    /^\(\*\) / { service = ""; disabled = 1; next }
    $0 ~ /Device: [^)]+\)$/ {
      device = $0
      sub(/^.*Device: /, "", device)
      sub(/\)$/, "", device)
      if (!disabled && service != "" && device == interface) {
        matches++
        selected = service
      }
    }
    END { if (matches != 1) exit 1; print selected }
  ')"
export M2_IPV6_MODE_BEFORE="$(networksetup -getinfo "$M2_NETWORK_SERVICE" | \
  awk -F': ' '$1 == "IPv6" {print $2; exit}')"

printf 'interface=%s\nservice=%s\nipv6_before=%s\n' \
  "$M2_PHYSICAL_IF" "$M2_NETWORK_SERVICE" "$M2_IPV6_MODE_BEFORE"
```

All values must be nonempty and the original mode must be `Automatic`. If it
is `Off`, `Manual`, `Link-local`, missing, or ambiguous, stop and preserve the
output; do not guess a restoration mode.

```bash
export M2_IPV6_RECORD="/tmp/mini_vpn_knife15_m2_ipv6_before_$(date -u '+%Y%m%d_%H%M%S').txt"
printf 'service=%s\ninterface=%s\nmode=%s\n' \
  "$M2_NETWORK_SERVICE" "$M2_PHYSICAL_IF" "$M2_IPV6_MODE_BEFORE" | \
  tee "$M2_IPV6_RECORD"
chmod 600 "$M2_IPV6_RECORD"

sudo networksetup -setv6off "$M2_NETWORK_SERVICE"
sleep 5
networksetup -getinfo "$M2_NETWORK_SERVICE" | grep '^IPv6'
bash scripts/knife15-macos-soak.sh m2-ipv6-check
```

Require `IPv6: Off` and `PASS: M2 IPv6 route check`. Before `start`, any
failure uses the pre-start restoration in section 9. After `start`, restore
IPv6 only after `status/snapshot/stop` completes.

## 3. Build And Local Mac Gates

```bash
cargo build --release
/usr/bin/python3 -I scripts/knife15-m2-resource-profile.py --self-test
/usr/bin/python3 -I scripts/knife15-m2-continuity-ledger.py --self-test
bash scripts/knife15-m2-resource-preflight.sh --self-test
bash scripts/knife15-macos-soak.sh --self-test
bash scripts/knife15-macos-soak.sh preflight
```

The runner self-test intentionally prints
`ERROR: command exceeded hard timeout of 1s`; it passes only when the last
line includes `knife15 macOS runner self-test passed`. These commands start no
TUN. On failure, preserve output and restore IPv6 through section 9.

Start one global, bounded sleep inhibitor before baseline and keep its exact
PID in this shell through cleanup. This is mandatory for agent-operated
non-TTY traffic; `ttyskeepawake` does not cover an SSH command without a TTY.

```bash
export KNIFE15_CAFFEINATE_SECS=100800
export KNIFE15_CAFFEINATE_LOG="/tmp/mini_vpn_knife15_caffeinate_${M2_CANDIDATE_ID}_$(date -u '+%Y%m%d_%H%M%S').log"
nohup /usr/bin/caffeinate -dimsu -t "$KNIFE15_CAFFEINATE_SECS" \
  >"$KNIFE15_CAFFEINATE_LOG" 2>&1 &
export KNIFE15_CAFFEINATE_PID=$!
sleep 2
kill -0 "$KNIFE15_CAFFEINATE_PID"
pmset -g assertions | grep 'PreventUserIdleSystemSleep'
```

Require `PreventUserIdleSystemSleep` to be `1`. The runner independently
enforces the assertion at baseline, direct, start, qualification, and formal
entry. If the process exits or the assertion disappears, the attempt is
environment-invalid and must not start or continue.

## 4. Candidate Workload Baseline

Run this section once for the candidate's first valid qualification:

```bash
bash scripts/knife15-macos-soak.sh baseline
export M2_BASELINE_DIR='/tmp/mini_vpn_knife15_macos_baseline_REPLACE_TIMESTAMP'
```

Baseline normally takes 40–90 seconds. It must pass both directions. Freeze
this exact directory and reuse it for the candidate's qualification and both
formal runs; that keeps all derived offered rates identical. If no valid
qualification has been sealed, an invalid baseline/environment may be
discarded and captured again. Once qualification is valid, never replace its
baseline for that candidate.

`/tmp` may be cleared by a reboot, so immediately preserve the exact baseline
bytes outside `/tmp` before the first direct discriminator:

```bash
export M2_BASELINE_NAME="$(basename "$M2_BASELINE_DIR")"
export M2_BASELINE_ARCHIVE="$M2_EVIDENCE_HOME/${M2_BASELINE_NAME}.tar.gz"

test ! -e "$M2_BASELINE_ARCHIVE" && \
  test ! -e "$M2_BASELINE_ARCHIVE.sha256"
COPYFILE_DISABLE=1 tar -C /tmp -czf \
  "$M2_BASELINE_ARCHIVE" "$M2_BASELINE_NAME"
shasum -a 256 "$M2_BASELINE_ARCHIVE" | \
  tee "$M2_BASELINE_ARCHIVE.sha256"
chmod 400 "$M2_BASELINE_ARCHIVE" "$M2_BASELINE_ARCHIVE.sha256"

M2_BASELINE_DIR="$M2_BASELINE_DIR" \
  bash scripts/knife15-macos-soak.sh baseline-check
```

Keep the printed archive path and digest with the candidate evidence. Before a
later attempt, first use the still-present `/tmp` directory if its replay
passes. If it is absent after a reboot, restore only from this verified
archive to the original exact basename:

```bash
shasum -a 256 -c "$M2_BASELINE_ARCHIVE.sha256"
test ! -e "/tmp/$M2_BASELINE_NAME"
tar -tzf "$M2_BASELINE_ARCHIVE"
tar -C /tmp -xzf "$M2_BASELINE_ARCHIVE"
export M2_BASELINE_DIR="/tmp/$M2_BASELINE_NAME"
M2_BASELINE_DIR="$M2_BASELINE_DIR" \
  bash scripts/knife15-macos-soak.sh baseline-check
```

The listing must contain only the one recorded baseline directory and its
regular evidence files. Never restore over an existing path. The runner
revalidates the forward/reverse JSON and every fresh direct result binds their
exact hashes; a checksum or replay failure invalidates the environment and
does not authorize a newly measured baseline after qualification was sealed.

## 5. Fresh Direct Evidence And Run-Bound Candidate Profile

Repeat this section for qualification, formal 1, formal 2, and any retry that
was explicitly classified `invalid`:

```bash
bash scripts/knife15-macos-soak.sh direct-discriminator
export M2_DIRECT_DIR='/tmp/mini_vpn_knife15_macos_direct_REPLACE_TIMESTAMP'
```

The direct discriminator takes a little over five minutes and must pass with
zero complete receiver intervals. Immediately create the run-bound candidate
profile; the following block refuses to replace an existing path and writes
no credential:

```bash
export M2_RESOURCE_CANDIDATE_PROFILE="/tmp/mini_vpn_knife15_${M2_CANDIDATE_ID}_profile_$(date -u '+%Y%m%d_%H%M%S').json"

/usr/bin/python3 -I - "$M2_RESOURCE_CANDIDATE_PROFILE" <<'PY_PROFILE'
import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path


def required(name):
    value = os.environ.get(name, "")
    if not value or "REPLACE" in value:
        raise SystemExit(f"missing exact environment value: {name}")
    return value


def sha256(path):
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


output = Path(sys.argv[1])
if output.exists() or output.is_symlink():
    raise SystemExit(f"refuse to replace profile: {output}")
direct_manifest = Path(required("M2_DIRECT_DIR")) / "manifest.txt"
profile = {
    "schema": "knife15-m2-resource-profile-v1",
    "candidate_id": required("M2_CANDIDATE_ID"),
    "provider": required("M2_CANDIDATE_PROVIDER"),
    "resource_id": required("M2_CANDIDATE_RESOURCE_ID"),
    "region": required("M2_CANDIDATE_REGION"),
    "public_ipv4": required("M2_CANDIDATE_IPV4"),
    "asn": int(required("M2_CANDIDATE_ASN")),
    "route_class": required("M2_CANDIDATE_ROUTE_CLASS"),
    "route_contract_id": required("M2_CANDIDATE_ROUTE_CONTRACT_ID"),
    "provider_identity_evidence_sha256": sha256(
        required("M2_PROVIDER_IDENTITY_EVIDENCE")
    ),
    "route_identity_evidence_sha256": sha256(
        required("M2_ROUTE_IDENTITY_EVIDENCE")
    ),
    "tuic_port": int(required("M2_CANDIDATE_TUIC_PORT")),
    "target_ipv4": required("TARGET"),
    "target_iperf_port": int(required("IPERF_PORT")),
    "server_binary_sha256": required("M2_SERVER_BINARY_SHA256"),
    "server_config_sha256": required("M2_SERVER_CONFIG_SHA256"),
    "observer_sha256": sha256("scripts/knife15-exit-target-observer.sh"),
    "source_commit": subprocess.check_output(
        ["git", "rev-parse", "HEAD"], text=True
    ).strip(),
    "client_binary_sha256": sha256("target/release/mini_vpn"),
    "workload_profile_sha256": sha256(direct_manifest),
    "mac_interface": required("M2_PHYSICAL_IF"),
    "prior_saturation_proven": False,
    "prior_saturation_evidence_sha256": "",
    "replacement_capacity_proven": False,
    "replacement_capacity_evidence_sha256": "",
}
output.write_text(
    json.dumps(profile, sort_keys=True, separators=(",", ":")) + "\n",
    encoding="utf-8",
)
PY_PROFILE

export M2_RESOURCE_REFERENCE_PROFILE="$PWD/scripts/knife15-m2-reference-33.json"
/usr/bin/python3 -I scripts/knife15-m2-resource-profile.py validate \
  "$M2_RESOURCE_CANDIDATE_PROFILE"
/usr/bin/python3 -I scripts/knife15-m2-resource-profile.py compare \
  --reference "$M2_RESOURCE_REFERENCE_PROFILE" \
  --candidate "$M2_RESOURCE_CANDIDATE_PROFILE"
```

The comparison must emit `"eligible":true`. Then run the nonsudo, read-only
resource preflight:

```bash
export OUT_DIR="/tmp/mini_vpn_knife15_resource_${M2_CANDIDATE_ID}_$(date -u '+%Y%m%d_%H%M%S')"

bash scripts/knife15-m2-resource-preflight.sh run
export M2_RESOURCE_PREFLIGHT_DIR="$OUT_DIR"
```

Require `PASS: distinct Knife15 M2 resource preflight completed` and preserve
its directory, tar path, and SHA-256 line. It verifies live physical routes,
traceroutes, remote health/headroom, exact sing-box hashes/listener, and a real
one-second TUIC handshake/authentication/Connect probe to `.77:5201`, plus
every profile/source/binary/direct/observer identity, without changing routes
or starting TUN. A security-group or authentication failure therefore stops
before `start`. Continue promptly: the M2 action requires the fresh direct
result to be no older than 15 minutes.

## 6. Start, Smoke, Observer, And The Authorized Action

Preflight the exact action window:

```bash
git status --short
test "$(git rev-parse HEAD)" = "$TEST_SOURCE_COMMIT" && \
  echo 'PASS: candidate source unchanged'
sudo -v
sudo -E bash scripts/knife15-macos-soak.sh start
sudo -E bash scripts/knife15-macos-soak.sh smoke
export TUIC_PORT="$M2_CANDIDATE_TUIC_PORT"
export OBSERVER_TIMEOUT_SECS=93600
bash scripts/knife15-exit-target-observer.sh start
```

Require empty `git status`, the source PASS, smoke PASS, and a healthy fresh
observer. Run exactly one action authorized by the ledger state:

Qualification (about 30 minutes of workload, about 45 minutes overall):

```bash
sudo -E bash scripts/knife15-macos-soak.sh m2-qualification
```

Formal 1 or formal 2 (86,400-second schedule; reserve about 25 hours):

```bash
sudo -E bash scripts/knife15-macos-soak.sh m2
```

Do not press `Ctrl+C`, close the terminal, start another VPN, change the
physical network, browse, or deliberately add load. A formal success remains
pending until cleanup.

## 7. Mandatory Cleanup And Observer Finalization

After the action returns, whether PASS or ERROR:

```bash
sudo -E bash scripts/knife15-macos-soak.sh status || true
```

If it returned ERROR or status is not clearly clean, also run:

```bash
sudo -E bash scripts/knife15-macos-soak.sh snapshot || true
```

Then always run:

```bash
sudo -E bash scripts/knife15-macos-soak.sh stop
```

For qualification, manually finalize the still-owned observer after Mac
`stop`:

```bash
bash scripts/knife15-exit-target-observer.sh status || true
bash scripts/knife15-exit-target-observer.sh freeze
bash scripts/knife15-exit-target-observer.sh bundle
```

Formal `m2` automatically freezes/bundles its admitted observer. After Mac
`stop`, inspect once and manually finalize only if state is still active:

```bash
OBSERVER_STATUS="$(bash scripts/knife15-exit-target-observer.sh status 2>&1 || true)"
printf '%s\n' "$OBSERVER_STATUS"
if grep -Fxq 'status=active' <<<"$OBSERVER_STATUS"; then
  bash scripts/knife15-exit-target-observer.sh freeze
  bash scripts/knife15-exit-target-observer.sh bundle
fi
```

Preserve and synchronize the Mac and Exit tar bundles plus both checksum
files. Do not retry or start another candidate until their exact contents are
classified and sealed in the ledger.

After Mac/observer evidence is safely finalized, stop only the recorded global
sleep inhibitor and verify the assertion is gone:

```bash
if ps -p "$KNIFE15_CAFFEINATE_PID" -o command= | \
  grep -Fq "/usr/bin/caffeinate -dimsu -t $KNIFE15_CAFFEINATE_SECS"; then
  kill "$KNIFE15_CAFFEINATE_PID"
fi
wait "$KNIFE15_CAFFEINATE_PID" 2>/dev/null || true
pmset -g assertions | grep 'PreventUserIdleSystemSleep' || true
```

## 8. Ledger Classification

The user supplies only the bundle paths and SHA-256 values. The agent copies
both bundles into one artifact directory, performs content/provenance review,
and invokes `seal-attempt`. The evidence class is not chosen from the terminal
word `ERROR` alone:

- `pass / strict_pass` requires every strict SLI, safety, observer, result,
  and cleanup gate;
- `quality_failure` requires complete paired evidence and one exact
  `receiver_zero`, `udp_loss`, `safety_failure`, or `lifecycle_failure` reason;
- environment/operator/power/VPS/evidence invalidation is audit-only and does
  not alter candidate state.

After each seal, evaluate the complete ledger:

```bash
/usr/bin/python3 -I scripts/knife15-m2-continuity-ledger.py evaluate \
  /ABSOLUTE/PATH/knife15-tier-a-ledger.json \
  --artifact-root /ABSOLUTE/PATH/PAIRED_BUNDLES \
  --output /ABSOLUTE/PATH/NEW_RESULT.json
```

An existing result path is never overwritten. Continue only as follows:

- `awaiting_formal`, formal passes `0`: run formal 1;
- `awaiting_formal`, formal passes `1`: run formal 2;
- `TIER_A_ACCEPTED`: stop Tier A and reopen M3 by a separate result;
- candidate `rejected`: stop it; only then may candidate 2 be provisioned;
- `TIER_A_EXHAUSTED`: do not run a third resource; implement Tier B first.

## 9. Restore IPv6

Before `start`, restore immediately after preserving any failed output. After
`start`, restore only after Mac `stop` finishes:

```bash
if [ -z "$M2_NETWORK_SERVICE" ] || \
  [ "$M2_IPV6_MODE_BEFORE" != 'Automatic' ]; then
  echo 'ERROR: refusing ambiguous IPv6 restoration; inspect M2_IPV6_RECORD' >&2
else
  sudo networksetup -setv6automatic "$M2_NETWORK_SERVICE"
  sleep 5
  networksetup -getinfo "$M2_NETWORK_SERVICE" | grep '^IPv6'
fi
```

Require `IPv6: Automatic`. If `stop` reports owned route/DNS cleanup failure,
do not manually change routes/DNS and do not start Clash. Preserve
`status/snapshot` and wait for evidence review.

## 10. Next Valid Attempt On The Same Candidate

For formal 1, formal 2, or an explicitly invalid retry:

1. read—not execute—the saved candidate anchor, explicitly export the exact
   values, and require the checked-out `HEAD` to equal `TEST_SOURCE_COMMIT`;
2. use the same Mac/network, release binary, credentials, candidate identity,
   server binary/config, and verified/restored frozen `M2_BASELINE_DIR`;
3. repeat IPv6 disable, section 5 fresh direct/profile/resource preflight,
   fresh `start -> smoke -> observer`, the one ledger-authorized action,
   cleanup/finalization, bundle sync, and IPv6 restoration;
4. never rerun after a valid quality failure; and
5. never change workload, server, route contract, code, or frozen constants
   inside a candidate sequence.
