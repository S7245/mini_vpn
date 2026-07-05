# Knife14bk Server-Side Evidence Plan

Date: 2026-07-05

## Tasks

1. Add optional target SSH configuration to the US-client suite:
   - `SERVER_EVIDENCE_CHECK`;
   - `TARGET_SSH_HOST`, `TARGET_SSH_PORT`, `TARGET_SSH_KEY`;
   - target SSH host-key policy and known-hosts path.
2. Add a server-evidence artifact per probe:
   - capture client-side probe start/end UTC and epoch;
   - capture bounded `.33` sing-box lines for TUIC/outbound/fail-auth/error;
   - capture `.77` iperf3 journal between `start-5s` and `end+10s`.
3. Keep evidence optional and no-secret:
   - if SSH host variables are empty, write skipped/manual instructions;
   - print whether keys are set, not key paths in summaries unless already in
     command context;
   - redact UUID/password-like tokens from sing-box logs.
4. Extend suite self-test:
   - help text advertises server-evidence knobs;
   - server-evidence defaults stay off.
5. Run local gates:
   - suite self-test;
   - shell syntax;
   - `git diff --check`.
6. Commit/push script and docs.
7. Rerun one scoped reverse-first P1 with server evidence enabled.

## Risk Controls

- Do not truncate remote logs.
- Do not require target SSH unless `SERVER_EVIDENCE_CHECK=1` is explicitly set.
- Do not put secrets in docs, reports, learning memory, or final summaries.
- Keep the patch in the acceptance script only; no Rust behavior changes.

## Local Implementation Status

- Added opt-in per-probe server evidence artifacts to
  `scripts/knife14b-usclient-tunnel-suite.sh`.
- Artifact capture is read-only: `.33` uses bounded sing-box log grep with
  redaction; `.77` uses a bounded iperf3 journal window.
- Target SSH remains optional. When unset, the suite records a skipped target
  journal section instead of failing ordinary smoke runs.
- Local gates passed: suite self-test, shell syntax, and `git diff --check`.
