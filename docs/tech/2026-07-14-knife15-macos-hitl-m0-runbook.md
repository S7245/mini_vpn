# Knife15 macOS HITL M0 Runbook

Date: 2026-07-14
Status: **M0 repeated a real receiver interruption; 300s physical direct
continuity gate required before another TUN M0**

## Purpose And Authority Boundary

This runbook runs the formal Knife15 target-only macOS M0 after the short
evidence-loop qualification. It may run on the HK development Mac or the
dedicated Shenzhen Mac. A low-bandwidth cross-region result is not an H10d16
capacity failure; the capable Linux/VPS lane remains the absolute throughput
reference.

The macOS user runs every command, including every `sudo` command. The agent
does not create the TUN or mutate macOS routes. The reviewed runner adds only
explicit host routes for the iperf target and optional DNS target. It does not
install a default route or change system DNS.

## Preconditions

- Use a clean checkout containing `scripts/knife15-macos-soak.sh`.
- Install `cargo`, `iperf3`, `jq`, and the normal mini_vpn build dependencies.
  macOS must also provide `dig` for periodic fake-IP DNS checks and its normal
  BSD `ping` for same-window Exit/gateway controls.
- Keep the five TUIC credential/configuration values local. Never paste UUID,
  password, private keys, or credential-bearing environment output into chat.
- Use a certificate-only CA file. The runner rejects private-key material.
- Exit any existing VPN/proxy that routes the TUIC Exit or target through a
  `utun` interface. The runner refuses a recursive setup.
- Keep the Mac awake, connected to power, and on a stable network for the
  formal two-hour workload below.
- The Target iperf3 service must run with JSON output enabled. The current
  `.77` service is qualified with `iperf3 -s --json --forceflush`; `start`
  verifies the structured receiver result before changing routes.

## 1. Build And Export Local Configuration

From the repository root:

```sh
cargo build --release

export MINI_VPN_TUIC_SERVER='REPLACE_WITH_EXIT_IPV4:8443'
export MINI_VPN_TUIC_UUID='REPLACE_WITH_UUID'
export MINI_VPN_TUIC_PASSWORD='REPLACE_WITH_PASSWORD'
export MINI_VPN_TUIC_SNI='REPLACE_WITH_SERVER_NAME'
export MINI_VPN_TUIC_CA_PATH="$PWD/REPLACE_WITH_CA_CERTIFICATE.pem"

export TARGET='43.130.32.77'
export DNS_TARGET='8.8.8.8'
export DNS_NAME='example.com'
export DURATION='20'
export PARALLEL='1'
```

Do not use shell angle-bracket placeholders such as `<password>`; replace the
quoted `REPLACE_WITH_...` strings locally.

## 2. Prove The Existing Route Is Safe

```sh
EXIT_HOST="${MINI_VPN_TUIC_SERVER%:*}"
route -n get "$EXIT_HOST" | awk '/interface:/ {print "exit " $0}'
route -n get "$TARGET" | awk '/interface:/ {print "target " $0}'
```

Both interfaces must be a physical/non-TUN path such as `en0`; neither may be
`utun*`. If either is `utun*`, exit the current VPN/proxy and repeat this step.
Do not delete another VPN's routes manually.

## 3. Validate The Runner, Baseline, And Direct Continuity

```sh
bash scripts/knife15-macos-soak.sh --self-test
bash scripts/knife15-macos-soak.sh preflight
```

Stop here if self-test or preflight fails. The one required repaired-observer
short validation completed on exact source `5e8846f` in bundle
`/tmp/mini_vpn_knife15_macos_20260715_084113.tar.gz` (SHA-256
`85dd7a66...`). It proved semantic numeric physical counters, meaningful byte/
rate deltas, zero physical errors, complete network controls, and clean
teardown. Do not repeat that gate unless the observer changes again.
Summary-only follow-up commit `2c8030a` separates canonical terminal-relay
counts from raw/derived diagnostic log matches; it does not change the
observer and therefore does not require another short TUN validation.

For reference, the accepted short-gate sequence was:

```sh
sudo -v
sudo -E bash scripts/knife15-macos-soak.sh start
sudo -E bash scripts/knife15-macos-soak.sh smoke
sudo -E bash scripts/knife15-macos-soak.sh stop
```

Now take a fresh direction-aware baseline on the dedicated Shenzhen Mac:

```sh
bash scripts/knife15-macos-soak.sh baseline
```

Stop here if direct forward or direct reverse fails. Preserve the printed
baseline directory. The repaired baseline requests structured output from both
ends and validates the Target receiver for forward and the local receiver for
reverse. An older baseline without this evidence is intentionally rejected.

Export the exact fresh directory printed by `baseline`; do not reuse the
earlier short-qualification or pre-repair baseline:

```sh
export M0_BASELINE_DIR='/tmp/mini_vpn_knife15_macos_baseline_REPLACE_WITH_TIMESTAMP'
```

Before starting any TUN, run the exact 300-second physical-path discriminator:

```sh
bash scripts/knife15-macos-soak.sh direct-discriminator
```

This is a non-root direct iperf3 test; it does not create a TUN or change
routes. It uses one forward stream at exactly 50% of the fresh forward Target
receiver baseline. PASS requires 300 complete positive Target receiver
seconds, a complete 300-second result, unchanged physical Target/Exit routes,
and matching source, runner, release-binary, baseline, and result hashes.

If it fails, do not run `start`: the physical path did not meet M0's receiver
continuity SLI. Do not tune mini_vpn or relax the SLI. Preserve the printed
direct directory and retry only with a new baseline/direct pair in a more
suitable network window.

If it passes, export its exact printed directory:

```sh
export M0_DIRECT_DIR='/tmp/mini_vpn_knife15_macos_direct_REPLACE_WITH_TIMESTAMP'
```

Proceed immediately to section 4. Formal M0 accepts only a matching direct
PASS completed within the previous 15 minutes and copies that evidence into
the final bundle. The controller then derives sustained TCP/UDP rates at `50%`
of the same-direction receiver baseline and short TCP bursts at `80%`.

## 4. Run The Formal 2-Hour M0 Workload

```sh
sudo -v
sudo -E bash scripts/knife15-macos-soak.sh start
sudo -E bash scripts/knife15-macos-soak.sh smoke
caffeinate -dimsu sudo -E bash scripts/knife15-macos-soak.sh m0
sudo -E bash scripts/knife15-macos-soak.sh status
sudo -E bash scripts/knife15-macos-soak.sh stop
```

`start` must report the new `utun`, target route, non-recursive Exit route,
and evidence directory. Before it changes any route or starts mini_vpn, it now
requires a fresh one-second direct Target transaction with structured JSON
receiver evidence to complete. A busy or incorrectly configured Target blocks
rearm without changing local state. `smoke` rechecks forward TCP, reverse TCP,
and fake-IP DNS before the expensive run. `m0` takes approximately two hours
plus command setup overhead and keeps the Mac awake through `caffeinate`.

The frozen M0 timeline contains `6,780s` of active work, one `300s` idle-drain
window, and one `120s` final drain. Each full cycle contains capped forward and
reverse TCP, reverse UDP at a `1160B` application payload, six alternating
short TCP connections, and fake-IP DNS. The controller validates every iperf
JSON interval, byte evidence, UDP loss field, and DNS answer. It identity-
tracks traffic, idle, and final-drain children, checks process/target/Exit and
lossless-log health at least every two seconds, and records an independently
verified idle/resume/final-drain timeline. Forward quality is judged from the
Target receiver intervals returned in `server_output_json`; reverse quality is
judged from the local receiver intervals. Sender-only zero intervals remain
visible in the summary and trigger review without prematurely ending M0.

Every periodic snapshot also pings the TUIC Exit directly outside the utun,
pings the current physical gateway, and records physical-interface packet,
byte, error, and derived bit-rate fields. `m0` refuses to start unless the
start/smoke samples are complete and recent. During the workload it verifies
the watchdog and a recent valid control at least every two seconds; final PASS
requires a valid Exit and physical-interface control for every process sample.
ICMP total loss is preserved as valid path evidence with unknown RTT. Missing
or unparseable controls are not silently converted to PASS.

`m0` also revalidates `M0_DIRECT_DIR`. Missing, stale, shortened, modified, or
provenance-mismatched direct evidence fails closed. Because this validation
occurs after the user-created TUN exists, use the failure cleanup sequence
below if the 15-minute window was missed.

On a successful M0, run `status` and then `stop`; no manual `snapshot` is
needed. `stop` first terminates any identity-verified M0 controller, then terminates
mini_vpn, removes only routes still owned by this run, scans for secret-shaped
material, and produces a `.tar.gz` bundle plus SHA-256.

If `start`, `smoke`, or `m0` fails, do not tune any frozen setting. The M0
failure leaves the TUN running for evidence. Run:

```sh
sudo -E bash scripts/knife15-macos-soak.sh status || true
sudo -E bash scripts/knife15-macos-soak.sh snapshot || true
sudo -E bash scripts/knife15-macos-soak.sh stop
```

The last `stop` is the emergency cleanup and evidence-finalization command.
Run `status` and `snapshot` only before that `stop`. Once `stop` has published
a valid archive/checksum pair, the evidence is immutable: later `snapshot`,
`event`, or overwrite attempts are refused, and a repeated `stop` only prints
the already-finalized path and checksum without changing either file.

## 5. Prove Stop/Re-create/Rearm

After the first `stop` prints its bundle and checksum, create a fresh TUN and
prove the client rearms. No second two-hour workload is required:

```sh
sudo -E bash scripts/knife15-macos-soak.sh start
sudo -E bash scripts/knife15-macos-soak.sh smoke
sudo -E bash scripts/knife15-macos-soak.sh stop
```

The second `start` first waits for the direct Target readiness gate to pass,
then must create one new utun, keep the Exit outside it, and pass TCP/DNS
again. Do not repeatedly race `start` while the Target reports busy. The
second `stop` must again restore the target and DNS host routes and produce a
separate sanitized bundle.

## 6. Return Only Safe Evidence

Return these items for review:

- the self-test PASS line;
- the preflight output;
- the direct baseline directory and forward/reverse summary;
- the direct-discriminator directory and result hash;
- the first `start`, `smoke`, `m0`, and `status` outputs;
- both bundle paths and SHA-256 values printed by the two `stop` commands.

Do not return exported environment variables, UUID, password, CA/private-key
contents, shell history, or credential-bearing process/environment dumps. The
agent can inspect a bundle that remains on the shared HK Mac by its local path.

## Qualification Decision

The macOS lane has qualified target-only lifecycle, clean rearm, and the
repaired physical observer. Exact source `239acba` completed two mixed cycles,
then cycle 3 forward produced three complete Target receiver zero-throughput
seconds despite exact final byte delivery. User operation, ownership, TUN,
relay lifecycle, and resource regressions are rejected; bulk-connection QUIC
loss/congestion remains selected. The old 20-second baseline could not prove
300-second direct receiver continuity, so the new physical direct gate must
pass before another TUN M0.
Formal M0 passes only after the two-hour bundle shows `m0_status: complete`, zero phase
and health failures, one completed idle/resume/final-drain sequence, every
endpoint conservation sample at or below `61,440B`, zero final live and
outstanding ownership, bounded resource envelopes, no unexplained TUN errors,
complete same-window network controls, no lossy log compaction, and clean stop.
The fresh re-create/smoke/stop bundle must independently prove rearm and
cleanup. M0 does not complete M1, M2, or M3.

If the direct gate passes and the immediately following M0 repeats a receiver
interruption with the internal invariants clean, stop repeating this M0 shape
and classify the single-bulk-QUIC transport path as an architecture failure.
Begin a connection-health isolation/failover design stage with a mature-client
control. Result:
`docs/tech/2026-07-15-knife15-macos-m0-direct-continuity-discriminator-results.md`.
