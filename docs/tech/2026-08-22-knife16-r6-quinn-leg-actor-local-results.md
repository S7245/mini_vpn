# Knife16 R6 Fixed Quinn Leg Actor Slice-A Local Results

Date: 2026-08-22; final review repair: 2026-08-24

Status: **LOCAL NECESSARY SLICE ACCEPTED; R6A–D PRODUCTION COMPOSITION,
RETIRED CLEANUP, AND CAPACITY CLOSURE, R7A–D, R8–R9, UDP, TUN, VPS, WAN,
THROUGHPUT, TASK 4, M2, AND M3 NOT ACCEPTED**

## Outcome

One production-shared fixed Quinn actor now owns one enhanced resumable leg's
private or shared Endpoint relationship, full-1RTT connection, live
exporter-derived attach binding, sole `LegIo`, outbound FIFO, exact live byte
permits, independent cancellation lane, one root, and four fixed children.
The actor mints its non-cloneable joined witness only after the real root and
all children have completed, the exact connection has closed, both its
Endpoint-map removal and normal ConnectionDriver completion are witnessed
after driver-held references are dropped, actor ownership is zero, and a
client-owned private Endpoint has reached idle, released its final handle, and
completed its EndpointDriver/UDP-socket lifetime normally.

This closes the physical actor seam that the 2026-08-20 client replacement
transaction lacked. It does not install that actor in the real TUN path, does
not let the controller clear a retired slot, and does not exercise a
sub-second blackout. It therefore does not prove or claim that the M2
one-second continuity failure is fixed.

Architecture contract:
`docs/tech/2026-08-20-knife16-r6-quinn-leg-actor-architecture-spec.md`.

## Implemented vertical

### Authenticated construction and provenance

- `QuinnLegActorFactory::connect` always constructs the Client role;
  `QuinnLegActorListener::admit` returns either address `RetrySent` or an owned
  `QuinnLegActorBootstrap`, whose `start` always constructs Owner. Production
  exposes no combined full-bootstrap `accept`. Callers cannot pass a Quinn
  Endpoint, Connection, TLS config, verifier, exporter callback,
  `AttachTransportBinding`, endpoint role, task, or actor incarnation.
- Client CA material is parsed into a redacted crypto config at configuration
  construction; server certificate/key material is parsed once at listener
  bind. Both configs require an explicit checked nonzero bootstrap timeout.
- Both roles require TLS 1.3 and the dedicated
  `mvpn-resumable-leg-v1` ALPN, disable early data and QUIC DATAGRAM, derive
  the frozen 32-byte TLS exporter binding from the live connection, and
  complete the role/leg/policy-bound HMAC confirmation before publication.
- `QuinnLegActorTransportGate` has one private factory mint. `LegIo` then
  consumes its own private `LegIoAuthenticatedTransportGate` to mint the sole
  `(LegIo, LegTransportReporter)` pair. Test-only deterministic constructors
  remain separately `cfg(test)`.
- Source provenance locks the one exporter call, exact label/context and
  confirmation prefixes, both live-binding consumers, factory gate mint,
  `LegIo` gate mint, sole actor/listener joined mint sites, the unique typed
  inbound mint, absence of a production naked extractor, production
  admission/start split, test-only combined accept helper, absolute deadlines,
  and the single-use vendored Quinn drain authority.

### Fixed framing and bounded ownership

- Two long-lived unidirectional streams carry fixed 18-byte DATA and DELIVERED
  records. Stream magic, version, kind, role, leg, route, direction, lane,
  ordinal, flags, and payload bounds fail closed.
- Record reads use a cancellation-safe counted `RecvStream::read` loop rather
  than `read_exact`, which discards partial-progress information on error. Only
  a zero-byte end at a record boundary is benign `PeerClosed`/`Reset`; any
  partial header or any body end/reset is fatal `truncated actor record`.
- Ordered completion occurs only after the peer's composition owner accepts
  the classified record and its independently reserved DELIVERED record is
  positively written. QUIC write completion is not application delivery.
- Message and byte groups remain separate: outbound queue, actor outbound,
  inbound bytes, composition handoff, and ACK message/byte ownership each have
  checked nonzero bounds. The accepted checked envelope remains:

  ```text
  messages <= Qm + Om + Im + Cm + Am + 2
  bytes    <= Qb + Ob + Ib + Ab + Hd + F + Ha
  tasks    == 1 root + 4 fixed children
  ```

- ACK message permits follow a record through reservation, channel queue, and
  writer in-flight ownership. Inbound payload bytes are not allocated until
  byte, composition, inbound-channel, ACK-message, and ACK-byte admission is
  held. The returned opaque `QuinnLegActorInbound` retains its `Ib` and `Cm`
  permits through composition use/discard; its sole extractor is `cfg(test)`.
  Cancellation drops every partial reservation without acknowledging the
  record.
- One coalesced readiness epoch wakes pressure waiters without an unbounded
  event queue. The final review also binds readiness waiting to actor cancel,
  so a child failure occurring after waiter registration returns the shared
  first cause instead of hanging behind an unchanged epoch.

### Lifecycle and shared listener

- The retained actor owner holds one outer `JoinSet`; its root owns and drains
  the DATA reader/writer and DELIVERED reader/writer `JoinSet`. No task handle
  or generic future escapes.
- Cancellation closes new port mutations, uses non-lossy replacement on a
  pressure-independent watch lane, hands the inbound receiver to the root,
  drains queued ownership, preserves the shared first failure, closes the
  connection, waits for zero ownership, and consumes the exact Endpoint-map
  plus normal-ConnectionDriver dual witness before the root can return.
- A cancelled `join(&mut self)` future leaves the nursery and observed outcome
  retryable. Drop aborts fail closed and cannot mint joined.
- Semantic transport terminal classification and structural task completion
  are separate. A cleanly awaited `PeerClosed`/`Reset` may be carried by the
  joined result, while any child panic/`JoinError` remains structural failure,
  even after an earlier semantic first cause; it can never mark the listener
  registration clean or mint actor joined.
- All root exits share one cleanup epilogue. A semantic or structural failure
  is retained while connection drain, private Endpoint idle/final-handle drop,
  EndpointDriver completion, and zero-ownership checks are still attempted;
  the first error cannot skip later resource cleanup.
- The server listener has a checked live-actor registry. One slot remains live
  until the server owner and all five root/child termination guards are gone.
  Every guard retires its termination counter before releasing its last slot
  reference, so reused capacity cannot overlap an old counted guard.
  A live actor blocks shutdown without closing the shared Endpoint; an
  uncleanly dropped actor writes sticky poison immediately, but shutdown can
  return that poisoned verdict only after real task termination and exact
  listener EndpointDriver/socket release.
- Server actor A can cleanly join while B continues a real ordered exchange on
  the same Endpoint; A join observes exactly one remaining open connection,
  and B join observes zero. Listener shutdown is an
  `Accepting -> Closing -> Joined` transaction: cancelling a waiter after
  close leaves it retryable, while a completed transaction mints its private
  joined witness once and rejects later calls.
- Unvalidated Quinn `Incoming` values are retried before registry reservation.
  A validated admission freezes one absolute deadline and returns an owned
  bootstrap, allowing later admission while `start` proceeds independently.
  Dropped bootstraps and cancelled `start` callers remain tracked until the
  exact unpublished Quinn connection has been abandoned, removed from the
  Endpoint map, and completed its normal ConnectionDriver; only then is the
  listener slot released. Endpoint-only or driver-only completion, panic,
  abort, I/O failure, or driver loss fails closed.
- Endpoint-map idle is not EndpointDriver or socket completion. Clients take a
  consuming `EndpointDriverDrain` before synchronous connect, while listener
  shutdown moves its final Endpoint and the same single-use witness into one
  cancellation-retryable registered task. Both drop the final Endpoint before
  awaiting normal driver completion. Listener `Joined` therefore returns only
  after the UDP address is immediately rebindable.
- The per-connection normal driver witness is sent only after explicit Drop of
  the `ConnectionDriver` future and its held `ConnectionRef`/socket Arcs.
  Driver I/O error, panic, abort, or runtime loss drops the sender and fails
  the combined witness closed.

## TDD and review findings

The accepted tree includes focused RED/GREEN locks for the following defects:

1. ACK capacity originally counted the reserved channel value but not the
   delivery writer's dequeued in-flight record. With `Am=1`, the RED observed
   two live ACK records. A separate ACK-message semaphore now follows the
   entire ownership lifetime.
2. Dropping a server owner originally released its listener slot before the
   aborted root and children terminated. The slot is now an `Arc` lease shared
   by the registration and all five task guards.
3. A start failure after registry publication poisoned the listener despite
   publishing no actor. All fallible construction now precedes the one
   reservation-publication boundary; the same listener then accepts a valid
   retry.
4. Listener shutdown loaded the live count twice when constructing its error.
   One acquire snapshot now determines both the branch and carrying value.
5. A readiness waiter registered before child failure could wait forever
   because failure cancelled the actor without advancing the readiness epoch.
   The waiter now selects the independent cancel lane and re-reads the exact
   owner error.
6. A semantic first cause could mask a later child panic and let structural
   cleanup mint `Joined`. Structural child `JoinError` is now retained
   independently through drain/zero/close and always rejects joined/clean
   listener authority.
7. Root exit could land between the progress error check and watch
   subscription, and lossy watch sends could leave ports asleep. Terminal
   publication now replaces the watch value and progress uses
   check/subscribe/recheck.
8. A naked inbound extraction could release `Ib`/`Cm` before composition use.
   Production now exposes only the permit-owning envelope; the test-only
   consumer releases both permits after its closure returns.
9. The first split listener still reserved before address validation and
   restarted a relative timeout in `start`. Address Retry now precedes
   reservation and validated admission owns one absolute deadline.
10. Failed bootstrap returned its slot after `Connection::close` but before
    Quinn removed the connection from the Endpoint. The restricted vendored
    seam now abandons only unpublished attempts and consumes both the exact
    Endpoint-map removal ACK and normal ConnectionDriver-completion witness
    before releasing the slot.
11. Client CA loading occurred during connection setup. Client and server TLS
    inputs are now preloaded, redacted, and deleted-file regression tests prove
    later admissions perform no certificate-file I/O.
12. Cancelling `Bootstrap::start` could otherwise detach cleanup from the
    caller. A listener-counted inner task now owns bootstrap cleanup to
    completion; Drop follows the same tracked rule.
13. `RecvStream::read_exact` erased whether a reset followed a partial actor
    record. A counted, cancellation-safe read loop plus a real Quinn tracer now
    proves partial header/body reset is fatal truncation rather than a benign
    transport terminal.
14. A successfully published actor previously dropped its Quinn connection
    without awaiting the exact close tail. The single-use drain authority now
    crosses only the final infallible publication seam; the root uses ordinary
    close, never unpublished abandon, and awaits both exact witnesses before
    server registration can become clean.
15. Listener shutdown set a consumed boolean before its first await. Cancelling
    that future made the retained listener permanently return `ListenerClosed`.
    Explicit Accepting/Closing/Joined state makes the same close transaction
    retryable and still mints Joined once.
16. A bootstrap termination guard released its last live-slot reference before
    decrementing the termination count. Reuse could transiently exceed the
    checked `max_live * 6` peak. Guard retirement now linearizes before slot
    reuse while sticky poison still precedes both.
17. A client failure waiter directly owned the only Endpoint/socket cleanup
    future. Cancelling the waiter could discard that authority. Cleanup is now
    registered on the construction runtime before its first await; the caller
    may lose only its waiter.
18. Endpoint map idle was treated as full Endpoint teardown. A retained
    Endpoint clone proved that the EndpointDriver and UDP socket were still
    live. A consuming, non-cloneable driver witness now follows final Endpoint
    Drop on client and listener paths.
19. Listener shutdown could be cancelled while awaiting EndpointDriver
    completion. The exact release task and verdict now live in the listener
    state, so retry awaits the same task; a same-address rebind test proves the
    socket is gone before `Joined` returns.
20. `match driver.await` could keep the completed `ConnectionDriver` temporary
    alive through the match arm and send its normal ACK first. The accepted
    helper awaits through `&mut`, explicitly drops the driver, and only then
    sends; a synchronous Drop probe and source-order provenance lock the fact.
21. `Endpoint::connect` can fail synchronously before any `ConnectionDrain`
    exists. Client construction now owns the EndpointDriver witness before the
    call, and an IPv4-Endpoint/IPv6-remote tracer proves the same local UDP
    address can be rebound immediately after the factory returns its error.
22. A late root error could return before every remaining close-tail check.
    One unified epilogue now preserves the carrying error while attempting all
    connection, Endpoint, EndpointDriver, and ownership cleanup in order.
23. Caller cancellation after server actor publication but before claiming the
    success handoff could drop an untracked live actor. A runtime-bound handoff
    guard now cancels and joins any unclaimed actor; a structural failure still
    poisons listener shutdown.

Negative peer-policy and `LegId` agreement failures, address Retry without
slot use, expired admission deadline, dropped/cancelled bootstrap, a full
listener registry, join retry, first-failure/port linearization, root inbound
drain, cancellation under blocked DATA write, fatal child handoff, capacity
overflow, listener-shutdown cancellation, real partial-record reset,
dual-witness actor close, and exact source mint tests are also locked.

Independent actor, lifecycle, and documentation review lanes plus final main
review covered lifecycle/cancellation, bounded
backpressure, listener isolation, source provenance, first-failure
linearization, TUN/TCP/UDP regression risk, hot-path panic shape, and missing
tests. All discovered findings above are repaired; final source reviews report
unresolved Slice-A P0/P1/P2 `0/0/0`.

## Accepted local validation

- fixed Quinn actor: `46/46`; critical shared-listener, partial-read,
  cancellation, handoff, Endpoint/socket, and driver-Drop tracers: each
  `100/100` where repeated;
- LegIo `28/28`, controller `10/10`, standby `20/20`, supervisor `46/46`;
- root all-target library `1156/1156`, `3` ignored; main `2/2`; typed
  provenance `128/128`; protocol `25/25`; public session API `1/1`;
- one non-isolated root attempt, incorrectly run beside two vendored builds,
  rejected two scheduler-sensitive D16 capacity gates at `115.8/110.0
  Mbit/s`; exact isolated reruns passed, including `328.639 Mbit/s` on the
  forward D16/TUN batch tracer with zero ring drops/residual/EOF tail, and the
  subsequent isolated full suite produced the accepted counts above;
- concurrency harness with `--features harness`: `10/10`, `4` ignored, plus
  the four ignored sweeps explicitly `4/4`;
- retained real-smoltcp adapter floor: `2/2`, then the same built binary's
  exact tracer `100/100`;
- development and release `--all-targets --all-features` checks and release
  build: PASS;
- strict production-library Clippy with only the four documented legacy
  allowances: PASS;
- rustdoc: PASS with the existing 15 unrelated warnings;
- isolated vendored Quinn with the exact absolute local quinn-proto `0.11.16`
  path: `47/47`, `3` ignored, doctest `1/1`; vendored quinn-proto `330/330`,
  docs `3/3`;
- `cargo fmt --all -- --check`, `git diff --check`, source provenance,
  changed-secret-shape scan, and generated vendor-artifact cleanup: PASS.

Final implementation and provenance-source SHA-256 values:

```text
111b9034453023e095c961938920fd5230f40319b3e21e41b798493f501f5d02  src/owned_upstream/quinn_leg_actor.rs
5dcbbcdeb7c7520f3baf61c0443b44f14acaff3be9bdc4a7a2260e53a211ac6e  src/owned_upstream/leg_io.rs
f169d23ea47e03626306b3304411a4d271952ff229b83246eb187c625dedae20  src/owned_upstream/leg.rs
52ca4a986e7b1f633d7726b1ddbb19da714073315e92eb42d5eefe5f737d4758  src/owned_upstream/mod.rs
b4a12721f76d425fe8720993eb9e413583c0b9118e3838f013286719b647b535  tests/knife16_owned_leg_provenance.rs
35decc0351c264c68e1092bcf9750712af68956dcf521043f8ef370834f9187e  third_party/quinn-0.11.11/src/connection.rs
436b979763bb8d05cef2ee01b5744f48ae74d61bceb49dd1c52569a80d258987  third_party/quinn-0.11.11/src/endpoint.rs
e67c6dcc63e49e45b40c310251def2b23028f784cf49da8e698f48fcf5cc8e4f  third_party/quinn-0.11.11/src/lib.rs
0e9ac1aacff54383f8e013b096a124a4232fda7e0bfc8b6bdf9d9e621cb864eb  third_party/quinn-0.11.11/src/tests.rs
999225ec989f3ff2d205273cb83a8458dd37a30a1dd6e230aff095b7e6c21df4  third_party/quinn-proto-0.11.16/src/connection/mod.rs
```

No macOS TUN, VPS, WAN, enhanced-path throughput acceptance, blackout, M2, M3,
or user-traffic test was run or consumed by this result. Historical local
D16/TUN capacity regressions were rerun only as root-suite isolation gates;
they do not make the enhanced actor production-reachable or accept M2.

## Remaining blockers and exact next vertical

Slice A is a necessary physical ownership seam, not a sufficient production
data plane:

- `client_tun` still selects `LegacyOwnedUpstream`; no non-test composition
  root owns a controller together with these actors;
- `QuinnLegActorJoined` is not yet bound to the exact controller retired slot,
  so successful actor join cannot clear that slot;
- bounded Incoming admission/start separation and absolute bootstrap deadlines
  are locally closed, but no production composition nursery owns their
  join/retry/deadline policy; a caller-side timeout is not clean join evidence;
- R6a–d still lack production actual-close/silent-failure composition, exact
  retired cleanup, and checked recovery-capacity closure;
- R7a–d remain entirely open: no-oracle reverse-only failure behavior,
  authenticated switch hints, the rejection matrix, and trigger-race
  arbitration are not implemented;
- no enhanced server session/Target owner, UDP two-leg engine, R8 fault
  matrix, R9 real-smoltcp replacement parity, or local `>170 Mbit/s` gate
  exists.

Next implement one tracer-bullet transaction that binds this exact actor
incarnation and joined witness to the controller's retired slot. It must prove
that cleanly joining retired A clears only A, emits no false `LegLost`, and
does not disturb active B. Then add the bounded real-loopback R6
actual-close/silent-failure lifecycle composition and capacity closure. This
does not cover R7; its reverse-only no-oracle, authenticated-hint, rejection,
and trigger-race gates remain separate. Do not run TUN, VPS, WAN, throughput,
or M2 before R6–R9 pass their required local gates.
