# Knife14w suite spec - cargo discovery before VPS preflight

## Grounding

- Project goal from `AGENTS.md`: VPS integration runs are expensive evidence and
  should fail early with actionable environment diagnostics.
- Test bundle:
  `/tmp/mvpn_knife14w_usclient_suite_20260703_111144.tar.gz`.
- Tested commit: `1144fc2`.
- The suite ran as `uid=0(root)` and failed before `.33/.77` service preflight,
  before tunnel startup, and before any data-plane traffic.
- Failure: `BUILD_RELEASE=1` but `cargo` was not in root's `PATH`.

## Problem

The suite uses `command -v cargo` for release builds. On the Ubuntu client VPS,
`cargo` can be installed by rustup under `/home/ubuntu/.cargo/bin/cargo`, while
the suite may be launched from a root shell or with a root PATH. That makes a
valid checkout look unbuildable and burns a full test cycle without exercising
the code under test.

## Goal

Make the suite resolve cargo from common rustup locations and record the exact
binary path used before running `cargo build --release`.

## Non-goals

- Do not change tunnel behavior, data-plane code, VPS service checks, or iperf
  parameters.
- Do not install Rust automatically or mutate the VPS environment.
- Do not bypass `BUILD_RELEASE=1`; the suite should still build the current
  checkout unless the user explicitly sets `BUILD_RELEASE=0`.

## Invariants

- If `CARGO` is set and executable, it wins.
- If cargo is in `PATH`, use that.
- If root PATH misses rustup, try `$HOME/.cargo/bin/cargo`,
  `/home/ubuntu/.cargo/bin/cargo`, and `/root/.cargo/bin/cargo`.
- If no executable cargo is found, fail before tunnel setup with a fix command
  that tells the user how to export `CARGO` or update `PATH`.

## Acceptance

- `bash -n scripts/knife14b-usclient-tunnel-suite.sh` passes.
- Local diff check passes.
- Next VPS run should reach the existing `.33/.77` service preflight when cargo
  exists in `/home/ubuntu/.cargo/bin/cargo`, even if the suite runs as root.
