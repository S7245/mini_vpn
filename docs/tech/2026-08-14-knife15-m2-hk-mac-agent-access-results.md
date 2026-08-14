# Knife15 M2 HK Mac Agent Access Results

Date: 2026-08-14

Status: **REMOTE ACCESS AND NONPRIVILEGED READINESS PASS; CANDIDATE 1 STILL
NOT CREATED OR ADMITTED**

## Standing Access

The user authorized the agent to execute code, operate the Knife15 macOS TUN
workflow, and collect/analyze logs on the HK test Mac without asking the user
to run shell commands.

```text
SSH: ssh -i ~/.ssh/vpn xiaoou@192.168.133.109
Host identity: agent.local
macOS: 26.4.1 (25E253), arm64
Agent worktree: /Users/xiaoou/mini_vpn
Physical interface: en0
Physical gateway: 192.168.133.1
```

Never place the Mac login password in commands, scripts, documentation,
evidence, logs, shell history, or project memory. Privileged TUN actions must
start in a writable SSH TTY and satisfy `sudo` only at its interactive prompt.

## Desktop TCC Boundary

The user identified `/Users/xiaoou/Desktop/mini_vpn` as the existing project
directory. SSH can stat that directory but macOS privacy control denies reading
its contents with `Operation not permitted`. The directory was not modified.

Rather than weaken Full Disk Access or ask the user to operate the terminal, a
fresh single-branch clone was created at `/Users/xiaoou/mini_vpn`. This path is
outside Desktop's TCC boundary and is the only agent-owned acceptance worktree.

## Exact Readiness Evidence

- HTTPS clone and branch access: PASS.
- Source: `a16f661147ac22b3abe64cab677c7839b8e1366b`.
- Tracked worktree: clean.
- Rust: `rustc 1.97.1 (8bab26f4f 2026-07-14)`.
- Cargo: `cargo 1.97.1 (c980f4866 2026-06-30)`.
- Release build: PASS.
- Release SHA-256:
  `5e946af25ac05e74fa54d607fe4a67d7b22a1305c3b0a2e042fffb3d3cbf3032`.
- Complete Knife15 runner self-test: PASS. Its expected hard-timeout branch
  printed `ERROR: command exceeded hard timeout of 1s`, followed by the final
  `passfailpassknife15 macOS runner self-test passed` marker.
- Homebrew `iperf3` exists at `/opt/homebrew/bin/iperf3`; remote commands must
  prepend `/opt/homebrew/bin` and `~/.cargo/bin` to `PATH`.
- Noninteractive `sudo -n` is unavailable, as expected. No sudo policy or
  system state was changed.
- Routes to `.33` and `.77` both use `en0` through `192.168.133.1`.
- Four inactive/background `utun` interfaces exist, but no Clash, Mihomo,
  sing-box, mini_vpn, WireGuard, OpenVPN, or Tailscale process was found and
  neither tested route uses a `utun`.
- The remote `~/.ssh/vpn` fingerprint exactly matches the controlling Mac's
  key: `SHA256:pl88hGkkMKvDQduM27xAiShN+7Is+21bP0NsrjEhmK8`.

## Next Action

Do not run baseline, direct, start, smoke, qualification, or formal M2 yet.
First create the selected AWS Lightsail candidate and return its nonsecret
static IPv4, Availability Zone, and out-of-band SSH host-key fingerprint.
The agent will bootstrap the server with the existing RSA key, add the shared
ED25519 public key, provision the exact service, and then run resource admission
from this HK Mac.

Candidate contract:
`docs/tech/2026-08-14-knife15-m2-strict-candidate1-resource-selection.md`.
