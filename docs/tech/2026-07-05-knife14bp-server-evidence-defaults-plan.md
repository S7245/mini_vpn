# Knife14bp Server Evidence Defaults Plan

Spec: `docs/tech/2026-07-05-knife14bp-server-evidence-defaults-spec.md`

## Design Tree

1. Missing evidence due to unset SSH destinations.
   - Evidence: Knife14bo had `SERVER_EVIDENCE_CHECK=1`, but `.33` and `.77`
     collection was skipped.
   - Fix: default known SSH destinations when server evidence is enabled.
2. Missing evidence due to missing key path.
   - Evidence: `.27` uses the project VPS key path for SSH to `.33/.77`.
   - Fix: default the key path only when a file exists, preserving explicit env
     values and allowing SSH config/agent fallback.
3. Authentication or service failures during collection.
   - Evidence would appear in bounded server evidence once collection actually
     runs.
   - Do not pre-classify this as a mini_vpn data-plane failure.

## Tasks

1. Add pure shell default helpers.
   - Resolve default SSH host for known `.33`/`.77`.
   - Resolve default key path using a test-overridable candidate path.
2. Add suite self-tests first.
   - Known host + evidence enabled applies defaults.
   - Explicit values are preserved.
   - Evidence disabled applies no defaults.
3. Implement the default application before reports/preflight.
   - Apply after `EXIT_HOST` is derived from `MINI_VPN_TUIC_SERVER`.
   - Keep the generated report clear about resolved host/key presence.
4. Run local gates.
   - `scripts/knife14b-usclient-tunnel-suite.sh --self-test`
   - `scripts/knife14b-lowrtt-probe.sh --self-test`
   - `git diff --check`
5. Update learning memory and commit.
   - This is one script/documentation commit.
6. Run one VPS evidence acceptance.
   - Source `.27` `.env`.
   - Use `SERVER_EVIDENCE_CHECK=1` without explicit SSH host envs.
   - Confirm the bundle includes `.33/.77` evidence.

## Stop Conditions

- If self-test requires real network access, stop and refactor into pure helper
  tests.
- If VPS evidence shows current `.33` `fail auth`, inspect sing-box
  service/time/config before treating throughput as a mini_vpn regression.
- If VPS evidence is complete and still shows low `.77` sender throughput with
  clean terminal/drop counters, move to tx-queue-only receive-window
  instrumentation rather than another close-drain patch.
