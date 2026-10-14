# mini_vpn

mini_vpn is the data-plane core of a future cross-platform VPN product.
This repository owns the Rust transport, TUN, TCP/UDP, DNS, and lifecycle work;
the App and backend control plane are separate work.

## Start here

- [Product requirements](Rules.md)
- [Engineering and safety rules](AGENTS.md)
- [Current accepted position and next gate](docs/current/STATUS.md)
- [Implementation handoff](HANDOFF.md) and [remaining work](TODO.md)

Follow the source-of-truth order in AGENTS.md. Current STATUS and its named
accepted results own progress and test evidence; this README is navigation.

## Current boundaries

The compatibility path retains TUIC v5/QUIC interoperability. The public TUN
entry still uses LegacyOwnedUpstream; the enhanced resumable/two-leg profile
does not replace that entry merely because its local tests pass.

P3, Case C, R7, and Task 4 remain incomplete. The
[compatibility recovery execution guide](docs/tech/2026-09-14-compatibility-recovery-execution-guide.md)
describes the N0–N5 route; this cleanup does not activate N0, retire P3
obligations, authorize external acceptance, or claim throughput readiness.
Consult STATUS and an explicitly accepted run plan before any external run.

## Historical notes

The previous README contained early server/yamux instructions that no longer
describe the current tree. Its exact text is preserved in the
[2026-09-18 snapshot](docs/archive/project-memory/2026-09-18/README.full.md).
The [early assessment](ASSESSMENT.md) is historical, not the current roadmap.
