# Knife15 M2 Checkpoint Log-Tail Consistency Spec

Date: 2026-08-17

Status: **APPROVED FOR LOCAL TDD; FORMAL RETRY BLOCKED UNTIL REVIEWED GATES PASS**

## Problem

Candidate-2 Formal 1 reached the first 600-second idle boundary after 17
complete cycles and then failed before writing the first checkpoint row. The
sleep completed, the exact post-idle process/network sample was healthy, no
health-failure event exists, and replaying the completed archive satisfies the
checkpoint envelope.

The checkpoint reads `mini_vpn.log` while mini_vpn appends metrics. Two readers
used different completion authority for the same data-plane record:

- `m2_data_plane_envelope` counted only a record containing all required
  numeric fields;
- `m2_controlled_tcp_replay_envelope` counted any tail beginning with
  `📊 数据面:`, including an incomplete concurrent append.

The resulting transient `replay_samples != data_samples` fails the checkpoint
without identifying a data-plane or resource violation. The active-lease
reader had the same class of risk: a trailing `active_leases=` fragment could
hide the latest complete numeric record.

## Goal

Make every checkpoint reader use complete-record authority so an append in
progress cannot create a false mismatch. Preserve fail-closed behavior for a
complete malformed record and preserve all M2 quality, workload, resource,
lifecycle, and cleanup thresholds.

## Invariants

1. A data-plane record is counted by both envelope and replay readers only
   after the shared required numeric prefix is complete.
2. A trailing empty `active_leases=` fragment is ignored in favor of the
   latest complete numeric record.
3. A complete nonnumeric active-lease record remains invalid and fail-closed.
4. No Rust production code, release binary, Endpoint/D16/TUN behavior, M2
   duration, phase, rate, SLI, frozen constant, server, observer, or resource
   identity changes.
5. Candidate-2 qualification remains immutable. A Formal retry may cross this
   evidence-runner-only repair only when the release binary hash, workload
   contract, resource identity, server binary/config, observer, profile helper,
   and resource preflight runner remain exact.

## Source-Contract Bridge

The strict ledger normally binds an exact source and runner across
qualification and formal attempts. This is correct by default. For this one
reviewed evidence-reader repair, the ledger may treat exactly these two runner
hashes as one compatibility class:

- qualified runner:
  `c55dc940d974539f98e2f449387159965972249dcda476fad451c44d3466e192`;
- repaired runner:
  `8b0d0c9ed220075481f4459dbfbde7d8ef88738d9ca6f2afc802b65d96f25a86`.

The bridge is valid only with the already-contracted client release SHA-256
`5e946af25ac05e74fa54d607fe4a67d7b22a1305c3b0a2e042fffb3d3cbf3032`.
Every other source/runner change remains candidate-contract drift. Source-tree
validation must still prove that each artifact's runner hash is the blob at
its recorded commit.

## Acceptance

- RED fixtures reproduce both incomplete-tail mismatches.
- GREEN fixtures pass while complete malformed ownership remains rejected.
- Ledger RED/GREEN proves only the exact runner pair plus exact binary can
  bridge; arbitrary source or runner drift remains rejected.
- Runner/ledger/profile/preflight/observer self-tests, shell syntax,
  diff/secret checks, exact release hash, and concentrated code review pass.
- The Mac and candidate Exit pass fresh environment/resource preflight before
  the next formal run.

## Stop Rules

- Any Rust binary hash or workload-contract drift stops the retry.
- Any unexpected local regression stops execution until repaired and reviewed.
- A genuine Formal quality failure remains decision-bearing; this bridge may
  not reclassify or repeat it.
