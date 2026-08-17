# Knife15 M2 Tier-B Epoch Ledger Local Results

Date: 2026-08-17

## Result

Task 8 local implementation and review pass. A separately named
`m2-frequency` action now owns Tier-B execution; strict `m2` and
`m2-qualification` reject Tier-B admission inputs and retain the zero
receiver-interval policy. No Rust production path, transport setting, MTU,
pool, QUIC window, Endpoint capacity, or frozen traffic rate changed.

Tier-B admission is pinned to the accepted Tier-A exhaustion ledger/evaluation
SHA-256 pair `96e50cd2.../8a953089...` and to implementation source floor
`15c9e47`. One run may seal one through four exact six-hour epochs. Twelve
epochs and at least one contiguous four-epoch process/TUN lifetime are required
for acceptance.

## Preserved gates

Each epoch preserves the existing `3%` UDP loss ceiling, `16MiB` TCP gap
ceiling, DNS and real-client checks, D16 terminal ownership, Endpoint
conservation at `61,440B`, physical-interface and recovery safety, live
resource binding, paired Exit observer coverage, and final cleanup. At least
675 process, network, and Endpoint samples are required per epoch, matching the
existing 24-hour coverage ratio.

Only complete receiver-zero intervals may continue into the Tier-B reducer.
Command failures, malformed/missing receiver evidence, UDP loss above `3%`,
and every other safety failure still stop immediately.

## Immutable evidence behavior

- TCP results are confined to their exact epoch directory.
- UTC and monotonic clocks must both prove an exact six-hour epoch.
- A parent run ending `failed` or `interrupted` may preserve only epochs with
  an exact seal event and complete paired Mac/Exit evidence.
- A later infrastructure failure does not discard earlier sealed epochs, but
  an unknown gap is explicit and is never bridged.
- One Exit capture cannot be reused across multiple Mac runs.
- Fresh direct/resource preflight evidence is validated per run. Cross-run
  identity compares the stable provider/route/server/resource contract, so a
  fresh direct hash cannot manufacture resource drift.
- Source, release binary, normalized workload contract, server binary/config,
  observer, Exit, Target, ports, and stable resource identity remain exact
  across all twelve epochs.

## Review repairs before long execution

Concentrated review repaired four locally preventable long-run risks:

1. ordinary schedule failure could leave the parent status `running` and make
   already sealed epochs unusable;
2. signal status `interrupted` was not admitted by the archive replay;
3. result paths were safe but not bound to the owning epoch;
4. the full resource-profile hash included the fresh-direct hash and would
   falsely reject the second 24-hour batch as identity drift.

Full fake Mac/Exit archive replay now covers the interrupted-parent case,
resource archive and provider/route bindings, exact seal metadata, cleanup,
packet-capture coverage, and result materialization. Focused tests also prove
strict/Tier-B input isolation, receiver-zero-only continuation, unchanged UDP
failure, signal evidence, Exit non-reuse, and 11/12-epoch pending/acceptance.

## Gates

PASS:

- continuity-ledger Python compile and self-test;
- Tier-B frequency reducer self-test;
- macOS runner syntax, full self-test, and wrapper self-test;
- resource profile/preflight and Exit observer self-tests;
- market runner/reducer regression self-tests;
- stable resource-identity CLI probe;
- `git diff --check` and focused secret scan;
- concentrated code review with no unresolved P0/P1.

Task 9 is next: close the source-floor commit, write the macOS detached
controller/runbook with once-only best-effort email, repeat the complete local
gate, check the chosen Exit/Mac environment, and only then start the first
four-epoch/24-hour run. M3 remains blocked pending twelve valid epochs and
immutable ledger acceptance.
