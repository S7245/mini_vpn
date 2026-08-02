# Knife15 M2 macOS HITL Runbook

Date: 2026-07-30

Status: **READY — implementation commit `4dac87c` or a descendant required**

This is the only reviewed M2 sequence. It runs a controlled IPv4 full tunnel
for 24 hours and temporarily changes the active physical network service DNS.
`stop` is mandatory even after a successful `m2`.

## Before Opening The Test Terminal

1. Use the HK Mac and its normal physical network.
2. Quit Clash completely and disable Clash-TUN.
3. Disable every other VPN, proxy app, and manually created TUN.
4. Prevent sleep and power loss; keep the Exit and Target VPSs powered.
5. Do not browse, change Wi-Fi/Ethernet, alter DNS, or start another VPN until
   Knife15 `stop` finishes.
6. M2 is an IPv4-only gate. The dedicated physical network service must have
   IPv6 temporarily disabled before baseline. Restore immediately after a
   pre-start failure; once `start` is invoked, restore only after `stop`, using
   the exact procedure below.

Slow HK bandwidth is not itself a bug. The baseline derives offered rates, and
M2 judges continuity, UDP loss, lifecycle, resources, routes, and cleanup.

## 1. Fresh Terminal, Source, And Environment

Run:

```bash
cd /Users/liushan/Documents/Personal/Languages/Rust/mini_vpn

git fetch origin
git switch codex/knife14d-downlink-reap-open
git pull --ff-only origin codex/knife14d-downlink-reap-open
git status --short
git rev-parse HEAD
git merge-base --is-ancestor 4dac87c HEAD && echo 'PASS: M2 source accepted'

unset M0_BASELINE_DIR M0_DIRECT_DIR
unset M1_BASELINE_DIR M1_DIRECT_DIR
unset M2_BASELINE_DIR M2_DIRECT_DIR
unset BASELINE_OUT_DIR DIRECT_OUT_DIR OUT_DIR

export TARGET=43.130.32.77
export DNS_TARGET=8.8.8.8
export DNS_NAME=example.com
export IPERF_PORT=5201
export DURATION=20
export PARALLEL=1
export METRICS_SECS=30
export SAMPLE_SECS=30

export MINI_VPN_TUIC_SERVER='43.153.32.33:8443'
export MINI_VPN_TUIC_UUID='REPLACE_WITH_UUID'
export MINI_VPN_TUIC_PASSWORD='REPLACE_WITH_PASSWORD'
export MINI_VPN_TUIC_SNI='example.com'
export MINI_VPN_TUIC_CA_PATH='certs/dev/ca-cert.pem'
```

`git status --short` must print nothing, and the source check must print PASS.
Replace only the UUID/password placeholders; do not send those values or paste
them into a bundle.

## 2. Identify And Temporarily Disable Physical IPv6

M2 must not run while global IPv6 can bypass the IPv4 TUN. First derive the
physical interface used by the TUIC Exit and the one enabled macOS network
service that owns it:

```bash
export M2_PHYSICAL_IF="$(route -n get 43.153.32.33 | awk '/interface:/ {print $2; exit}')"
export M2_NETWORK_SERVICE="$({ networksetup -listnetworkserviceorder || true; } | awk -v interface="$M2_PHYSICAL_IF" '
  /^\([0-9]+\) / {
    service = $0
    sub(/^\([0-9]+\) /, "", service)
    disabled = 0
    next
  }
  /^\(\*\) / {
    service = ""
    disabled = 1
    next
  }
  $0 ~ /Device: [^)]+\)$/ {
    device = $0
    sub(/^.*Device: /, "", device)
    sub(/\)$/, "", device)
    if (!disabled && service != "" && device == interface) {
      matches++
      selected = service
    }
  }
  END {
    if (matches != 1) exit 1
    print selected
  }
')"

printf 'M2 physical interface: %s\nM2 network service: %s\n' \
  "$M2_PHYSICAL_IF" "$M2_NETWORK_SERVICE"
bash scripts/knife15-macos-soak.sh m2-ipv6-check || true
networksetup -getinfo "$M2_NETWORK_SERVICE" | grep '^IPv6'
export M2_IPV6_MODE_BEFORE="$(networksetup -getinfo "$M2_NETWORK_SERVICE" | \
  awk -F': ' '$1 == "IPv6" {print $2; exit}')"
```

Both derived values must be nonempty. On the current HK lane the physical
interface is expected to be `en0`, but do not hard-code a service name such as
`Wi-Fi`: use the printed service that owns the actual Exit route.

The `networksetup` output must report `IPv6: Automatic`. If it reports Manual,
Link-local, Off, is missing, or the service derivation is empty/ambiguous, stop
and preserve the output; do not guess how to restore it.

Record the original mode and disable IPv6 for only that service:

```bash
if [ -z "$M2_PHYSICAL_IF" ] || [ -z "$M2_NETWORK_SERVICE" ] || \
  [ "$M2_IPV6_MODE_BEFORE" != 'Automatic' ]; then
  echo 'ERROR: exact physical service and original IPv6 Automatic mode are required' >&2
else
  export M2_IPV6_RECORD="/tmp/mini_vpn_knife15_m2_ipv6_before_$(date -u '+%Y%m%d_%H%M%S').txt"
  printf 'service=%s\ninterface=%s\nmode=%s\n' \
    "$M2_NETWORK_SERVICE" "$M2_PHYSICAL_IF" "$M2_IPV6_MODE_BEFORE" | \
    tee "$M2_IPV6_RECORD"
  chmod 600 "$M2_IPV6_RECORD"

  sudo networksetup -setv6off "$M2_NETWORK_SERVICE"
  sleep 5
  networksetup -getinfo "$M2_NETWORK_SERVICE" | grep '^IPv6'
  bash scripts/knife15-macos-soak.sh m2-ipv6-check
fi
```

The service must now report `IPv6: Off`, and the exact runner check must end
with `PASS: M2 IPv6 route check`. It uses the same
`2001:4860:4860::8888` probe and four-state classifier as formal M2. A known
`not in table` line with no interface is `safe_absent` even on macOS versions
that return status zero; `lo0`/`utun*` is `safe_tunnel`; a physical interface
or unknown outcome fails closed. If the check fails, preserve its complete structured
output, restore IPv6 immediately with the pre-start branch in section 8, and
do not take a baseline.

Keep `M2_NETWORK_SERVICE`, `M2_IPV6_MODE_BEFORE`, and `M2_IPV6_RECORD` exported
in this terminal. Disabling IPv6 may briefly reset the physical link, so all
baseline/direct evidence must be collected after this step.

## 3. Build And Offline Gates

```bash
cargo build --release
bash scripts/knife15-macos-soak.sh --self-test
bash scripts/knife15-macos-soak.sh preflight
```

The self-test intentionally prints one
`ERROR: command exceeded hard timeout of 1s` line. It passes only if it later
prints:

```text
knife15 macOS runner self-test passed
```

`preflight` must end in PASS and changes no route or DNS state.
If build, self-test, or preflight fails, no Knife15 TUN exists; preserve the
output and immediately use the pre-start restoration branch in section 8.

## 4. Fresh Baseline

```bash
bash scripts/knife15-macos-soak.sh baseline
```

This normally takes about 40–90 seconds. Copy the directory from:

```text
PASS: direct baseline complete: /tmp/mini_vpn_knife15_macos_baseline_...
```

Then export it, for example:

```bash
export M2_BASELINE_DIR='/tmp/mini_vpn_knife15_macos_baseline_REPLACE_WITH_ACTUAL_TIMESTAMP'
```

If baseline fails, stop here. No TUN was started; send the baseline directory.
Restore IPv6 using the pre-start branch in section 8. Low throughput alone is
acceptable, but receiver discontinuity is not.

## 5. Fresh 300-Second Direct Discriminator

```bash
bash scripts/knife15-macos-soak.sh direct-discriminator
```

It takes a little over five minutes. It must print:

```text
PASS: 300s direct Target receiver continuity discriminator completed
```

Export its `direct_dir`, for example:

```bash
export M2_DIRECT_DIR='/tmp/mini_vpn_knife15_macos_direct_REPLACE_WITH_ACTUAL_TIMESTAMP'
```

Continue immediately. M2 must consume this evidence within 15 minutes.
If the direct discriminator fails, no TUN was started; preserve its directory
and restore IPv6 using the pre-start branch in section 8.

## 6. Start, Smoke, And M2

```bash
sudo -v
sudo -E bash scripts/knife15-macos-soak.sh start
sudo -E bash scripts/knife15-macos-soak.sh smoke
caffeinate -dimsu sudo -E bash scripts/knife15-macos-soak.sh m2
```

Do not press `Ctrl+C`, close the terminal, start Clash, or change the network.
The `m2` command has exactly 24 hours of planned traffic/drain time plus DNS,
HTTPS, health, and transition overhead. Reserve about 25 hours.

During M2, the Mac's public IPv4 should be the Exit VPS. That is expected.
The runner blocks a routable physical IPv6 path instead of claiming a
dual-stack leak-free result.

If `m2` returns PASS, it will say cleanup acceptance is pending. That is not
the final acceptance; continue to the next section.

## 7. Mandatory Status And Stop

After `m2` returns, successful or failed:

```bash
sudo -E bash scripts/knife15-macos-soak.sh status
sudo -E bash scripts/knife15-macos-soak.sh stop
```

`stop` restores DNS/routes, stops the owned TUN, writes final acceptance, scans
secrets, creates one immutable bundle, and prints its SHA-256.

Send both lines:

```text
<sha256>  /tmp/mini_vpn_knife15_macos_....tar.gz
bundle=/tmp/mini_vpn_knife15_macos_....tar.gz
```

Also synchronize the bundle to the analysis Mac as before.

## 8. Restore The Physical Service IPv6

Restore the exact service whose original mode was recorded as `Automatic`.

If failure occurs before invoking `start`, there is no Knife15 TUN or owned
route/DNS state. Restore immediately after preserving the failure output. If
`start` was invoked, first complete the documented
`status -> snapshot -> stop` path; restore only after `stop` has cleaned the
run and printed its immutable bundle path and checksum.

For either branch, run:

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

The service must report `IPv6: Automatic`. This restoration is required after
success and every failure. Once `start` has been invoked, it must never occur
while the Knife15 TUN or M2 full-tunnel ownership is active. If the original
terminal was lost, use the protected `M2_IPV6_RECORD` file to identify the
service and mode; do not source or `eval` that file.

## Failure Procedure

If `start`, `smoke`, or `m2` fails, do not retry and do not tune any value.
Run:

```bash
sudo -E bash scripts/knife15-macos-soak.sh status || true
sudo -E bash scripts/knife15-macos-soak.sh snapshot || true
sudo -E bash scripts/knife15-macos-soak.sh stop
```

After `stop` completes, restore IPv6 using section 8. Do not restore it before
`stop`, because a physical IPv6 route appearing during M2 is itself a safety
failure.

If `stop` itself reports an owned route/DNS cleanup mismatch, do not start
Clash or another VPN. Preserve the terminal output and run `status` again so
the ownership conflict can be reviewed safely. Do not manually delete routes,
change DNS, or create an ad-hoc archive. A dead owned utun may have caused
macOS to reap its interface routes; current descendants recognize only an
exact return to the recorded physical interface/gateway and release the stale
markers without issuing a delete. Every other mismatch remains fail-closed.

## Expected Total Time

```text
IPv6 inspect/disable        about 1 minute
build/self-test/preflight   about 1–3 minutes
baseline                    about 40–90 seconds
direct discriminator        a little over 5 minutes
start + smoke               about 1–2 minutes
M2                          about 25 wall-clock hours
status + stop + bundle      about 1–3 minutes
IPv6 restoration           about 1 minute
```
