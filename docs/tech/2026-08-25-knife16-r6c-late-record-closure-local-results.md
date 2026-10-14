# Knife16 R6c Late-Record Closure Local Results

Date: 2026-08-25

Status: **LOCALLY ACCEPTED AS A NECESSARY-ONLY TEST/HARNESS SLICE; TASK 4,
PRODUCTION REACHABILITY, WAN, THROUGHPUT, AND M2 ARE NOT ACCEPTED**

Architecture contract:
`docs/tech/2026-08-25-knife16-r6c-late-record-closure-architecture-spec.md`.

## Outcome

R6c closes the local late-record matrix around one already committed A-to-B
replacement. The existing production-shared ownership and classification
contracts required no production runtime behavior change; this stage adds and
deepens deterministic tests and test helpers which prove those contracts at
the controller, fixed Quinn actor, composition, owner, replay, and Target
boundaries.

Three deliberately different paths are now distinguished instead of being
collapsed into one generic “stale A” policy:

1. A decodable DATA, application ACK, CLOSE, or RESET arriving through the
   client-side retired physical A actor is inert for either claimed wire
   generation. It cannot construct a client-side session event, reach the
   active reducer, release replay, or disturb current B.
2. During the asymmetric owner-B/client-A publication window, a real physical
   A uplink can still construct the old owner-A event, but the B-current owner
   reducer rejects it with exact
   `LegGenerationMismatch { current: B, actual: A }`. The Target and owner
   state remain unchanged.
3. An A-generation DATA, ACK, CLOSE, or RESET carried by current physical B is
   physically classified by B and then rejected with exact
   `FrameGenerationMismatch { current: B, actual: A }`. It cannot become an
   accepted reducer effect or recovery observation.

Retired ATTACH acceptance, generation status, and probe ACK are rejected
before active policy. A byte-identical retired-A probe ACK cannot consume an
armed current-B probe; only the same ACK on B's exact actor can do so.

Actor-local DELIVERED is also closed as a separate ownership fact. A and B are
deliberately advanced until both own pending ordinal `3`, both physical
DELIVERED writers are blocked, and A is retired. Releasing A clears only A's
pointer-identical receipt while B remains pending and unarmable. Releasing B
then clears only B, after which B alone can arm and accept its exact probe ACK.
Equal numeric ordinals therefore do not cross actor-private pending maps.
Neither DELIVERED advances resumable application ACK, changes Target state, or
releases replay.

The nonzero replay/Target tracer holds late A DELIVERED across B commit, keeps
one nine-byte replay extent live, rejects the retired and cross-generation
matrix, applies owner recovery state through current B exactly once, and then
requires the owner reducer's Target-backed B application ACK. Only that ACK
emits `ReplacementRecovered` from `0` to `9` and releases the replay range.

Both cleanup boundaries are covered. Once clean retired join begins, A is not
dequeued or semantically classified and no DELIVERED is fabricated. For a
terminal-bound post-loss A, an ordered probe already queued before the actor's
first failure is drained under the pointer-identical retained failure; B
recovery continues, and only its Target-backed application ACK completes the
transaction. Final A and B message/byte ownership is zero.

## Exact scope and decomposition

- The primary DATA/ACK/CLOSE/RESET, probe, DELIVERED, join, first-failure,
  replay, and Target proofs cross real localhost Quinn actors.
- The production active actor queue cannot mint post-acceptance
  ATTACH_ACCEPTED or ATTACH_GENERATION_STATUS. Those two defensive rows use
  the production-shared `EncodedDelivery -> LegIo -> controller` seam; no raw
  test-only production injection API was added.
- The terminal-bound actor already returns its retained first failure before
  dequeue or classification. One genuinely ordered probe therefore represents
  that category-independent boundary; the complete record-category policy is
  proved before it.
- Unknown, duplicate, or wrong-route DELIVERED remains an actor-local protocol
  failure. R6c does not weaken that existing production rejection path or
  relabel corruption as benign retired traffic.

## Review repairs and rejected false evidence

Concentrated adversarial review found and repaired the following test-oracle
or specification defects before acceptance:

- the first live-Target fixture consumed queued owner recovery frames as raw
  transport cleanup, which could make Target continuity vacuous; the accepted
  fixture retains them until B is installed and applies the exact expected
  record count through current B;
- endpoint ownership alone could become zero while a record was still between
  transport and composition; the recovery helper now requires the expected
  record count plus queue bytes, actor bytes, waiting inbound bytes, message
  permits, and delivery reservations all at zero;
- a zero-flow fixture initially violated a nonzero-flow assertion; zero flows
  are now allowed only when Target establishment is explicitly disabled;
- whole actor snapshots were initially compared across physically validated
  rejected input, even though header counters must advance; the accepted
  oracle compares exact header deltas and every ownership field separately;
- normal clean retired A was incorrectly expected to wait for B's application
  ACK. That restriction belongs only to terminal-bound post-loss cleanup; the
  accepted proof uses the later exact `ReplacementRecovered` event as the
  recovery witness;
- a single held A ordinal did not prove actor-private isolation. The final
  tracer holds equal numeric A/B ordinals simultaneously and releases them
  independently;
- the specification's unqualified “retired A cannot construct SessionEvent”
  conflicted with the separately required owner-B/client-A rejection path. It
  now names the client-side `RetiredClientLeg` precisely.

A short Cargo filter combined with `--exact` executed zero tests and was
rejected. A later standalone composition-module filter also executed zero
tests because the production-shared composition tests intentionally live in
the Quinn actor test module. All accepted focused and repeat evidence names a
real nonzero test.

## Accepted local validation

| Gate | Result |
|---|---|
| Six focused R6c controller/real-Quinn lifecycle tracers | each `1/1` |
| Fixed Quinn actor/composition test module | `72/72` |
| Focused client controller | `19/19` |
| Focused `LegIo` / standby / supervisor / session / TCP | `28/28`, `27/27`, `46/46`, `8/8`, `42/42` |
| All targets/all features | library `1200 passed; 3 ignored`; main `2/2`; concurrency `10/10; 4 ignored`; provenance `130/130`; protocol `25/25`; session API `1/1` |
| Explicitly ignored concurrency sweeps | `4/4` |
| Real-smoltcp fake adapter | `2/2`; core tracer `100/100` |
| Six critical R6c tracers on the final all-feature libtest | each `100/100` |
| Strict production-library Clippy | PASS with only the four established repository allowances |
| Development/release all-target/all-feature checks and release build | PASS |
| Rustdoc | PASS with the established unrelated warnings |
| Isolated vendored Quinn with exact local proto provenance | `47 passed; 3 ignored`; integration `1 ignored`; doctest `1/1` |
| Isolated vendored quinn-proto | `330/330`; doctests `3/3` |
| Root rustfmt, `git diff --check`, targeted Quinn/proto generated artifacts | PASS |
| Added-line and untracked-file secret-shape scan | PASS |

The final all-feature libtest is
`target/debug/deps/mini_vpn-802b66297ce9f029`, SHA-256:

```text
03084d0527015015dcc6d6735040882d7e57ae747246a3206e6c45c07dcd5038
```

The six `100/100` sets are the controller late-record matrix, real
Target/replay late-record matrix, retired-A/current-B probe-ACK isolation,
equal-ordinal actor-private DELIVERED isolation, terminal-bound prequeued
record cleanup, and clean retired-A join while B continues exchanging.

Final implementation-source hashes:

```text
0f1bb77de23b01d8fbc59370cb75d452dc2dfc1c02ac471683cd3cdf94960c76  src/owned_upstream/client_controller.rs
dc224db155c23f1418a77dfa7947effb49bd4e2fb9797006cdfda63f8feade56  src/owned_upstream/quinn_leg_actor.rs
f34fdaf8825748829664f32fcf35003022fd27d9a086d9f79c1574c3e26209b7  tests/knife16_owned_leg_provenance.rs
```

Independent adversarial and final source reviews report unresolved
P0/P1/P2 **0/0/0**.

No TUN, VPS, WAN, throughput, M2, M3, user traffic, or external resource was
used by this stage.

## Remaining boundary and next step

R6c is a lifecycle coverage closure, not a production runtime repair and not
evidence that the one-second M2 failure is solved. The real TUN entry still
selects the legacy upstream, and no production composition root/session
nursery owns and continuously drives the two-leg path.

The only next R6 step is R6d: derive and enforce the checked aggregate
`<=500ms` recovery-component capacity sum, then install the production
composition root/nursery with bounded startup, cancellation, join, retry, and
shutdown ownership. That remains necessary-only until R7 reverse-only
no-oracle switching, authenticated hints/rejections/races, R8 fault coverage,
R9 real-smoltcp parity, UDP two-leg delivery, enhanced owner/Target, local
capacity, two-ingress qualification, and final macOS M2 acceptance all pass.

Do not run macOS TUN, VPS, WAN, throughput, or M2 yet.
