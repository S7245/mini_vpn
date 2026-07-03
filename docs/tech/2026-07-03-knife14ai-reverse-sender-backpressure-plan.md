# Knife14ai plan - reverse sender backpressure attribution

> Spec: `docs/tech/2026-07-03-knife14ai-reverse-sender-backpressure-spec.md`.

## T1 - Red self-test

- Add a low-RTT probe self-test fixture based on the knife14ah reverse-first
  iperf shape.
- Assert the summary contains `iperf_sender_mbps`.
- Assert the attribution contains `reverse_sender_backpressured`.

Expected initial result: self-test fails before parser implementation.

## T2 - Parser implementation

- Add sender Mbps parsing without removing `iperf_receiver_mbps`.
- Detect reverse TCP probes from iperf output.
- Add conservative reverse sender backpressure attribution when local pressure,
  QUIC pressure/loss, and stale reconnect labels are absent.

## T3 - Verification and review

- Run shell syntax and self-tests.
- Run `git diff --check`.
- Review the diff for parser-only scope, backward compatibility, and secret
  hygiene.

## T4 - Learning and commit

- Record the stage outcome in `.learnings/LEARNINGS.md`.
- Record any failed command or misleading assumption in `.learnings/ERRORS.md`.
- Commit and push the coherent task.
