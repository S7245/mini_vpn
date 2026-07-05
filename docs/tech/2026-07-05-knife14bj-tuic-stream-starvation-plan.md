# Knife14bj TUIC Stream Starvation Plan

Date: 2026-07-05

## Tasks

1. Upgrade `scripts/knife14b-usclient-tunnel-suite.sh` auth failure diagnosis:
   - `.33` service active/status and UDP 8443 listener;
   - sing-box config check;
   - no-secret TUIC inbound summary;
   - no-secret exact UUID/password/SNI/ALPN match booleans;
   - `.27` and `.33` UTC epoch/time delta;
   - certificate subject/issuer/dates/fingerprint only.
2. Add suite self-test coverage for the auth-diagnosis generated script/report
   shape so it cannot regress into printing secrets.
3. Review TUIC stream read/relay code for missing behavior-neutral counters.
4. If needed, add one narrow diagnostic patch and parser self-test.
5. Run local gates.
6. Commit/push the auth-diagnosis patch separately from any Rust diagnostic
   patch.
7. Run one scoped VPS reverse-first suite after preflight.
8. Parse and record whether the starvation branch is:
   - `.33` auth/config/time;
   - sing-box/target send starvation;
   - mini_vpn stream polling/wakeup;
   - QUIC stream flow-control;
   - local TCP/TUN receive-window behavior.

## First Coherent Task

Start with the suite auth diagnosis. It is behavior-neutral, directly addresses
the `.33 fail auth` concern, and improves future failed-startup bundles without
touching mini_vpn data-plane code.

## Risk Controls

- Do not print UUID/password or derived hashes.
- Prefer remote `python3 -` via SSH stdin for exact comparisons, because nested
  jq filters and greps previously caused quoting errors or secret exposure risk.
- Keep `.33` log reads bounded and time-windowed.
- Do not truncate `/var/log/sing-box.log` unless explicitly approved.
