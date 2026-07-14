# Knife15 macOS HITL M0 Runbook

Date: 2026-07-14
Status: **Runner locally verified; real TUN not yet executed**

## Purpose And Authority Boundary

This runbook qualifies the Knife15 target-only macOS evidence loop before any
2-hour soak. It may run on the HK development Mac or the dedicated Shenzhen
Mac. A low-bandwidth cross-region result is not an H10d16 capacity failure; the
capable Linux/VPS lane remains the absolute throughput reference.

The macOS user runs every command, including every `sudo` command. The agent
does not create the TUN or mutate macOS routes. The reviewed runner adds only
explicit host routes for the iperf target and optional DNS target. It does not
install a default route or change system DNS.

## Preconditions

- Use a clean checkout containing `scripts/knife15-macos-soak.sh`.
- Install `cargo`, `iperf3`, and the normal mini_vpn build dependencies.
- Keep the five TUIC credential/configuration values local. Never paste UUID,
  password, private keys, or credential-bearing environment output into chat.
- Use a certificate-only CA file. The runner rejects private-key material.
- Exit any existing VPN/proxy that routes the TUIC Exit or target through a
  `utun` interface. The runner refuses a recursive setup.
- Keep the Mac awake and connected to power for a later soak. The first run
  below is only a short M0 qualification.

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

## 3. Run The Non-Root Gates

```sh
bash scripts/knife15-macos-soak.sh --self-test
bash scripts/knife15-macos-soak.sh preflight
bash scripts/knife15-macos-soak.sh baseline
```

Stop here if self-test, preflight, direct forward, or direct reverse fails.
Preserve the printed baseline directory. A failed direct/control path makes
throughput attribution invalid and does not authorize constant tuning.

## 4. Run The User-Controlled Target-Only TUN Qualification

```sh
sudo -v
sudo -E bash scripts/knife15-macos-soak.sh start
sudo -E bash scripts/knife15-macos-soak.sh smoke
sudo -E bash scripts/knife15-macos-soak.sh status
sudo -E bash scripts/knife15-macos-soak.sh stop
```

`start` must report the new `utun`, target route, non-recursive Exit route,
and evidence directory. `smoke` exercises forward TCP, reverse TCP, and the
optional DNS route. `stop` terminates only identity-verified runner processes,
removes only routes still owned by this run, scans for secret-shaped material,
and produces a `.tar.gz` bundle plus SHA-256.

If `start` or `smoke` fails, do not tune any frozen setting. Run:

```sh
sudo -E bash scripts/knife15-macos-soak.sh status || true
sudo -E bash scripts/knife15-macos-soak.sh snapshot || true
sudo -E bash scripts/knife15-macos-soak.sh stop
```

The last `stop` is the emergency cleanup and evidence-finalization command.

## 5. Return Only Safe Evidence

Return these items for review:

- the self-test PASS line;
- the preflight output;
- the direct baseline directory and forward/reverse summary;
- the `start`, `smoke`, and `status` outputs;
- the bundle path and SHA-256 printed by `stop`.

Do not return exported environment variables, UUID, password, CA/private-key
contents, shell history, or credential-bearing process/environment dumps. The
agent can inspect a bundle that remains on the shared HK Mac by its local path.

## Qualification Decision

This short run proves runner/profile/routing/cleanup behavior only. A valid
bundle with exact H10d16 startup fingerprint, endpoint conservation, working
TCP/DNS paths, and clean restoration authorizes preparation of M0's 2-hour
workload. It does not by itself complete M0, M1, M2, or M3.
