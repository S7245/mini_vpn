# Knife15 M2 Market Continuity Calibration macOS Runbook

Date: 2026-08-14

Status: **READY FOR ONE MATURE-TUIC C0 TRIAL; THIS IS NOT FORMAL M2**

This runbook measures whether the existing strict one-second receiver-zero
event is specific to mini_vpn or is also observable through a mature TUIC
client on the same path. The first trial is six cycles and 84 minutes of active
traffic; reserve about 90 minutes including preflight, probes, and evidence
finalization.

Do not run the 25-hour formal M2, change mini_vpn production code, or tune any
frozen value during this calibration. The harness observes an already-running
external client. It never reads its configuration, stops it, or owns its
routes/DNS. `PASS_WITH_EVENTS` means valid evidence was collected; it is not a
formal M2 pass.

## Fixed C0 Identity

| Item | Required value |
| --- | --- |
| Mac/path | HK Mac, one unchanged physical interface |
| Mature client | Mihomo/Clash TUIC v5, exact version recorded |
| TUIC Exit | `43.153.32.33:8443` |
| Target | `43.130.32.77:5201` |
| SNI | `example.com` |
| UDP relay | native |
| Congestion control | Cubic |
| IP lane | explicit IPv4 Target/DNS/Exit evidence |
| Observer | one fresh `.33` Exit observer, started by the harness |
| Source floor | reviewed commit `3474dff` or a descendant |

The mature client must trust the same CA and use the same TUIC UUID/password
as mini_vpn. Enter those values only in the client; never paste them into test
output or send them with a bundle. Mihomo's current TUIC fields are documented
at <https://wiki.metacubex.one/en/config/proxies/tuic/>. Prefer importing the
CA or pinning its SHA-256 fingerprint. Do not silently replace CA verification
with `skip-cert-verify`.

Do not alter IPv6 for this calibration. Every decisive address is explicit
IPv4, and the harness verifies the exact Target, DNS, Exit, and public-egress
routes. The formal-M2 IPv6 procedure does not apply here.

## 1. Prepare A Clean Reviewed Source

Keep every VPN/TUN client off, including Clash-TUN and mini_vpn. Open a fresh
terminal and run:

```bash
cd /Users/liushan/Documents/Personal/Languages/Rust/mini_vpn

git fetch origin
git switch codex/knife14d-downlink-reap-open
git pull --ff-only origin codex/knife14d-downlink-reap-open
git status --short
git rev-parse --short HEAD
git merge-base --is-ancestor 3474dff HEAD && \
  echo 'PASS: market calibration source accepted'

python3 scripts/knife15-market-iperf-summary.py --self-test
bash scripts/knife15-market-continuity.sh --self-test
bash scripts/knife15-exit-target-observer.sh --self-test
```

`git status --short` must print nothing. All three self-tests must end in their
own `passed` line. Do not create the immutable profile until the final source
is pulled, and do not pull, edit, or rebuild the scripts after creating it.

## 2. Create One Direct Baseline And Frozen Profile With VPN Off

The established baseline command has a legacy release/source preflight, so it
still needs the local mini_vpn TUIC environment even though it does not start
mini_vpn. Replace only the credential placeholders locally:

```bash
export TARGET=43.130.32.77
export IPERF_PORT=5201
export DURATION=20
export PARALLEL=1
export METRICS_SECS=30
export SAMPLE_SECS=30
export DNS_TARGET=8.8.8.8
export DNS_NAME=example.com

export MINI_VPN_TUIC_SERVER='43.153.32.33:8443'
export MINI_VPN_TUIC_UUID='REPLACE_WITH_UUID'
export MINI_VPN_TUIC_PASSWORD='REPLACE_WITH_PASSWORD'
export MINI_VPN_TUIC_SNI='example.com'
export MINI_VPN_TUIC_CA_PATH='certs/dev/ca-cert.pem'

cargo build --release

printf 'Target interface: '
route -n get "$TARGET" | awk '/interface:/ {print $2; exit}'
printf 'Exit interface: '
route -n get 43.153.32.33 | awk '/interface:/ {print $2; exit}'

bash scripts/knife15-macos-soak.sh baseline
```

Before `baseline`, both printed interfaces must be the same physical interface
(normally `en0`), never `utun*`. The baseline normally takes 40–90 seconds.
Export the exact PASS directory printed by the command:

```bash
export BASELINE_DIR='/tmp/mini_vpn_knife15_macos_baseline_REPLACE_TIMESTAMP'

profile_output="$(bash scripts/knife15-market-continuity.sh profile \
  "$BASELINE_DIR")"
printf '%s\n' "$profile_output"
export PROFILE_FILE="$(printf '%s\n' "$profile_output" | \
  awk -F= '$1 == "profile" {print $2}')"

bash scripts/knife15-market-continuity.sh verify-profile "$PROFILE_FILE"
sed -n '1,40p' "$PROFILE_FILE"
```

If baseline reports receiver-zero or either command fails, stop. Do not enable
the mature client and do not manufacture a profile from that baseline. Slow
bandwidth alone is acceptable: the profile derives its long-phase rates at
half the direct receiver rate and short-phase rates at four-fifths.

## 3. Manually Enable The Mature TUIC Client

Configure exactly one Mihomo/Clash TUIC v5 node with:

```yaml
type: tuic
server: 43.153.32.33
port: 8443
uuid: REPLACE_LOCALLY
password: REPLACE_LOCALLY
sni: example.com
ip-version: ipv4
udp-relay-mode: native
congestion-controller: cubic
```

Also configure verified trust for `certs/dev/ca-cert.pem` using the client's CA
or certificate-fingerprint facility. Do not include the YAML or credentials in
the evidence. Leave MTU, windows, retries, stream limits, and all other client
values at their defaults.

Enable the client's TUN/full-tunnel mode and select only this node. mini_vpn
and every other VPN/proxy must remain off. The Exit itself must bypass the TUN,
while Target and `8.8.8.8` must traverse it.

Derive, inspect, and export the actual interfaces rather than guessing them:

```bash
export TUIC_EXIT_IPV4=43.153.32.33
export EXPECTED_EXIT_IPV4=43.153.32.33
export TUIC_PORT=8443

export PHYSICAL_IF="$(route -n get "$TUIC_EXIT_IPV4" | \
  awk '/interface:/ {print $2; exit}')"
export EXPECTED_VPN_IF="$(route -n get "$TARGET" | \
  awk '/interface:/ {print $2; exit}')"

printf 'physical_if=%s\nvpn_if=%s\n' "$PHYSICAL_IF" "$EXPECTED_VPN_IF"
route -n get "$TARGET"
route -n get "$TUIC_EXIT_IPV4"
route -n get "$DNS_TARGET"
curl -4 --fail --silent --show-error --max-time 15 https://api.ipify.org
echo
```

Continue only when `PHYSICAL_IF` is a physical interface, `EXPECTED_VPN_IF` is
`utunN`, Target/DNS use that same `utunN`, Exit uses the physical interface,
and public egress is exactly `43.153.32.33`.

## 4. Export Sanitized Trial Identity And Run Read-Only Preflight

Set the exact client version shown by the App/core. It must not contain a UUID,
password, token, or other credential. `CLIENT_BINARY` is optional; export it
only if the actual core is available as a real non-symlink file.

```bash
export CLIENT_KIND=mature
export CLIENT_LABEL=mihomo-tuic
export CLIENT_VERSION='Mihomo REPLACE_WITH_EXACT_VERSION'
# export CLIENT_BINARY='/absolute/path/to/the/actual/mihomo-core'

export EXIT_SSH_HOST='ubuntu@43.153.32.33'
export EXIT_SSH_KEY="$HOME/.ssh/vpn"

bash scripts/knife15-market-continuity.sh preflight
```

Preflight is read-only and normally takes seconds. It must print
`PASS: market external-client preflight`. A failure directory is evidence of a
route/client/VPS/source mismatch; repair that exact cause before C0. Do not run
formal `start`, `smoke`, `m2`, `status`, `snapshot`, or `stop` for this lane.

## 5. Run The 90-Minute C0 Trial

Keep the Mac awake, plugged in, and on the same physical interface. Do not
change the selected proxy, DNS, routes, source, or profile while this runs.
Normal background macOS traffic may remain, but avoid deliberate large
downloads or streaming.

```bash
bash scripts/knife15-market-continuity.sh run
```

The harness runs six cycles and continues through quality events. It starts,
binds, freezes, and bundles one fresh Exit observer automatically. Do not start
an observer manually. Do not press `Ctrl+C` unless the machine or network must
be taken down; an interrupt deliberately makes the trial `INVALID` and still
attempts bounded evidence finalization.

Valid completion prints either `market_status=PASS_NO_EVENTS` or
`market_status=PASS_WITH_EVENTS`, plus:

```text
bundle=/tmp/mini_vpn_knife15_market_....tar.gz
sha256=...
observer_bundle=/tmp/mini_vpn_knife15_exit_target_observer_....tar.gz
observer_sha256=...
```

Both PASS statuses mean the calibration evidence is valid. They do not accept
formal M2. An `INVALID` result also creates the best available Mac bundle and
leaves the external client operator-owned. There is no market-runner
`status/snapshot/stop` sequence; do not use the formal runner's commands.

Only after the command prints its final bundle lines may you manually disable
the mature client. If the command reports a pre-existing observer before
traffic, do not blindly stop it; preserve the error and ask for inspection.

## 6. Verify, Copy, And Classify Evidence

On the test Mac, use the exact printed paths:

```bash
export MAC_BUNDLE='/tmp/mini_vpn_knife15_market_REPLACE.tar.gz'
export OBSERVER_BUNDLE='/tmp/mini_vpn_knife15_exit_target_observer_REPLACE.tar.gz'

cat "$MAC_BUNDLE.sha256"
shasum -a 256 "$MAC_BUNDLE"

scp -i "$EXIT_SSH_KEY" \
  "$EXIT_SSH_HOST:$OBSERVER_BUNDLE" /tmp/
scp -i "$EXIT_SSH_KEY" \
  "$EXIT_SSH_HOST:$OBSERVER_BUNDLE.sha256" /tmp/
cat "$OBSERVER_BUNDLE.sha256"
shasum -a 256 "$OBSERVER_BUNDLE"

bash scripts/knife15-market-continuity.sh summarize \
  "mature=$MAC_BUNDLE"
```

The calculated hashes must exactly match the printed/sidecar hashes. Copy the
Mac bundle, its `.sha256`, the Exit bundle, and its `.sha256` to the validation
machine without renaming them, then provide both exact paths and SHA-256s.

C0 classification is exact:

- `CALIBRATE_PRODUCT_SLI`: the mature client also had at least one complete
  receiver-zero interval. Stop; do not spend time on C1 or select a custom
  protocol. Design a market-calibrated gap-frequency/recovery SLI next.
- `RUN_MATCHED_C1`: mature C0 had no receiver-zero interval. Stop and prepare
  the matched `mature -> mini_vpn -> mature -> mini_vpn` C1 only after this
  evidence is reviewed.
- `NO_DECISION`: at least one identity, integrity, role, or evidence check was
  invalid. Repair only that cause and repeat only the invalid trial.

`MINI_VPN_DIFFERENTIAL` and `EXTEND_MATCHED_DURATION` require matched C1
evidence and cannot be produced from the first mature C0 alone.
