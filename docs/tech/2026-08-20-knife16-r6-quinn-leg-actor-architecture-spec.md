# Knife16 R6 Fixed Quinn Leg Actor Architecture Specification

Date: 2026-08-20; final review repair: 2026-08-24

Status: **SLICE A LOCALLY ACCEPTED 2026-08-22; NECESSARY-ONLY; R6A–D
PRODUCTION COMPOSITION, RETIRED CLEANUP, AND CAPACITY CLOSURE, R7A–D, R8–R9,
TUN, VPS, WAN, THROUGHPUT, TASK 4, AND M2 NOT ACCEPTED**

## Purpose

The accepted R6 client transaction can retire a logical predecessor and keep
its exact state bounded, but it cannot clear the retired slot. The originally
missing fact was physical: no production-shared actor owned one authenticated
leg's QUIC connection, encoded bytes, cancellation lane, root task, and
structured children through a successful real await. Slice A now supplies that
actor seam, but the composition owner still does not consume its joined fact.

This slice introduces that physical ownership module.  It does not connect the
real TUN entry, replace standard TUIC, implement the enhanced owner runtime, or
claim that the one-second M2 continuity failure is fixed.

The accepted implementation and validation record is
`docs/tech/2026-08-22-knife16-r6-quinn-leg-actor-local-results.md`.  The final
review additionally closed fatal-readiness, cancellation, and resource-release
races: a progress waiter selects the independent cancel lane as well as the
coalesced readiness epoch; cleanup ownership is registered before caller
cancellation; and `Joined` is withheld until the last owned Endpoint is
dropped and its normal EndpointDriver/socket completion is witnessed.

## Scope

Slice A must prove one fixed, production-shared Quinn leg actor through a real
`127.0.0.1` connection:

1. a dedicated enhanced-protocol ALPN completes a full 1-RTT handshake;
2. the actor derives the exact attach transport binding from that live TLS
   connection rather than accepting a caller-created binding;
3. one bounded opaque encoded submission reaches the peer actor's bounded
   ingress byte-for-byte;
4. ordered delivery completes only after the peer admits that message and
   returns its application-level delivery acknowledgement;
5. cooperative cancellation is independent of data-lane pressure;
6. the root and every fixed child are truly awaited; and
7. an exact joined fact is minted only after successful task await, the exact
   QUIC connection is closed, its Endpoint-map removal and normal
   ConnectionDriver completion are both witnessed after the driver's held
   references are dropped, and actor-owned queue/byte ownership is zero.

Endpoint ownership is role-specific. A client actor owns its private Endpoint,
so its clean join closes it, awaits `Endpoint::wait_idle_or_driver_lost()`,
drops the final Endpoint handle, and consumes a non-cloneable witness of normal
EndpointDriver completion. An accepted server actor never owns or closes the
shared listener Endpoint; the listener supervisor performs the same
idle/drop/driver-witness transaction only during whole-listener shutdown. One
server leg exiting must not interrupt another accepted leg on the same
Endpoint. Endpoint-map idle alone is not EndpointDriver or UDP-socket release.

The next slice may bind that exact joined fact to the controller's retired
slot. Slice A deliberately does not clear the slot yet, does not close the
remaining R6a–d production actual-close/silent-failure and capacity gates,
and does not implement any R7a–d reverse-only no-oracle, authenticated-hint,
rejection, or trigger-race behavior. R8–R9 remain open as well.

## Non-goals

- no `client_tun` composition switch;
- no replacement of `LegacyOwnedUpstream` or standard sing-box TUIC;
- no public enhanced-protocol server or Target-socket owner;
- no controller retired-slot clear;
- no UDP path switching;
- no throughput constant or `>170 Mbit/s` claim;
- no macOS TUN, VPS, WAN, blackout, M2, or M3 execution;
- no generic wrapper around a caller-provided `Future`, `JoinHandle`, Quinn
  `Endpoint`, Quinn `Connection`, or `AttachTransportBinding`.

## Module and dependency direction

The policy modules remain transport-independent:

```text
resumable session / ClientSessionController / LegIo policy
                         ^
                         | opaque submissions and exact facts
                         |
             fixed Quinn leg actor adapter
                         ^
                         |
               Quinn / rustls / Tokio / UDP socket
```

`resumable`, `leg`, `leg_io`, and `client_controller` must not learn Quinn or
Tokio types.  The outer actor adapter depends inward on their opaque
submission, terminal, and later join interfaces.  A future composition root
will own both the controller and runtime actor owners; the controller itself
will never own a Tokio `JoinHandle` or await while mutably borrowed.

The actor is a deep module.  Its interface exposes configuration, bounded
submission/ingress ports, cancellation, and an exact joined result.  TLS,
framing, byte permits, ACK correlation, task structure, socket close, and
Quinn idle mechanics stay inside its implementation.

## Factory interface contract

The concrete factory is responsible for constructing the authenticated
transport.  Conceptually it provides:

```text
QuinnLegActorListener::bind(server_config)
QuinnLegActorListener::admit(leg, limits, peer_policy)
    -> RetrySent | QuinnLegActorBootstrap
QuinnLegActorBootstrap::start()
QuinnLegActorFactory::connect(client_config, leg, limits, peer_policy)
```

The project-owned checked configuration may contain addresses, certificate
paths, expected owner identity, device principal, an explicit nonzero maximum
live server-actor registry count, an explicit checked bootstrap timeout, and
checked queue limits. Client CA material is parsed when client configuration is
constructed; server certificate/key material is parsed when the listener is
bound. Async admission performs no certificate-file I/O. Configuration
must not contain a Quinn/rustls config, custom verifier, session cache,
prebuilt endpoint, connection, exporter callback, attach binding, future, or
task handle.  `AttachPeerPolicy` is caller policy; it is not actor identity.
The independent actor-incarnation identity is allocated only inside the
factory.

Endpoint role is not caller input: `connect` always constructs the Client
endpoint role and a validated admitted bootstrap always starts Owner. Passing
or swapping a raw `LegEndpointRole` is impossible at this API.

Success returns closed ownership for exactly one actor incarnation:

```text
StartedQuinnLegActor {
    one closed LegIo + sole-outbound-queue + encoded-sender port,
    bounded inbound receiver,
    non-cloneable actor control capability,
    nursery-owned actor-join observation
}
```

The exact concrete names may change during the first RED/GREEN cycle, but the
ownership split and forbidden inputs may not.  There is no unrestricted
`into_parts` that lets siblings mix one actor's LegIo, queue, sender, receiver,
or runtime identity with another actor.

## TLS and binding

- Dedicated ALPN: `mvpn-resumable-leg-v1`.
- The factory constructs rustls/Quinn configs internally, fixes TLS 1.3,
  performs ordinary certificate-chain and SNI verification, and disables
  early data.  Neither endpoint accepts a custom verifier or resumption/0-RTT
  policy from the caller.
- The client waits for the ordinary 1-RTT `Connecting` result; it does not use
  `into_0rtt`.
- Both peers inspect rustls handshake data and require the exact ALPN.
- Both peers export 32 bytes from the live connection using this frozen input:

  ```text
  label   = b"EXPORTER-mini-vpn-resumable-leg-v1"
  context = b"mini-vpn/r6/quinn-leg/attach-binding/v1\0"
            || owner_identity[32]
            || device_principal[16]
  ```

  The context is identical on both roles; direction, socket address, and leg
  label are deliberately excluded so both ends derive the same channel
  binding.  The actor incarnation remains a separate process-local identity.
- `TlsExporterBinding`, `AttachAlpn`, expected `OwnerIdentity`, and established
  `DevicePrincipal` form `AttachTransportBinding` inside the factory.
- The factory then mints the sole `LegIo`, `LegTransportReporter`, and outbound
  queue for that connection.  No sibling caller can bless a caller-supplied
  binding as authenticated transport ownership.

`LegIo` exposes one actor-only constructor with the same signature in test and
release builds.  It consumes an unforgeable gate whose sole mint is private to
this fixed factory and always returns `(LegIo, LegTransportReporter)`.  The
legacy deterministic constructor remains separately named and `cfg(test)`;
the actor implementation contains no `cfg` branch for authenticated endpoint
construction.

Before either actor bundle becomes visible, the fixed actor bootstrap performs
a production binding-agreement prelude.  The client opens exactly one
bidirectional stream and the server accepts exactly one.  The initial client
transport config permits zero remotely initiated bidirectional streams; the
initial server config permits exactly one.  Immediately after accepting that
prelude, and before finishing either half of it, the server sets its desired
remote-bidirectional limit to zero so closing the prelude cannot replenish
another peer-opened stream.  Actor DATA/DELIVERED later use only the fixed
unidirectional streams.  Each side writes one fixed 32-byte
HMAC-SHA256 tag and calls `finish`, then `read_exact(32)`, requires EOF, and
constant-time verifies the peer tag before closing the prelude stream.  Tags
are keyed by the 32-byte exporter over:

```text
b"mini-vpn/r6/quinn-leg/binding-confirm/v1\0"
|| sender_role[1]
|| leg_id[1]
|| owner_identity[32]
|| device_principal[16]
|| b"mvpn-resumable-leg-v1"
```

Role codes are frozen as Client `0x01` and Owner `0x02`; leg codes use the
canonical `LegId` encoding.  The peer role is fixed and opposite, both LegId
values and policy bytes must agree, and both confirmations must verify before
LegIo/queue/task ownership escapes.  This is a channel-binding agreement, not
device authentication; signed attach remains authoritative.  A
source/provenance gate locks the sole exporter call and exact label/context,
while negative peer-policy and mismatched-LegId tests prove disagreement fails
before actor publication.

TLS authenticates the server certificate and binds the live channel.  The
configured owner/device values are expected attach policy, not a claim that
the server has mutually authenticated the device at TLS.  The existing signed
attach transaction remains the authority that authenticates the device and
must consume the exporter-derived binding in the provenance test.

The development CA/server certificate may be used only by the localhost test.
The private-key contents must never enter logs, snapshots, or evidence.

## Fixed wire framing

The dedicated ALPN still uses a bounded explicit record envelope.  `Hd` and
`Ha` are both the exact 18-byte header below; there is no padding or
implementation-defined encoding:

```text
offset  width  field
0       1      version: 0x01
1       1      kind: DATA=0x01, DELIVERED=0x02
2       1      flags: bit0 ORDERED, all other bits zero
3       1      leg: A=0x01, B=0x02
4       1      direction: ClientToOwner=0x01, OwnerToClient=0x02
5       1      lane: Control=0x01, Data=0x02
6       8      actor-local ordinal, unsigned big-endian, nonzero
14      4      payload length, unsigned big-endian
```

The first implementation supports only:

- `DATA`: opaque already-encoded protocol bytes; bit 0 of `flags` is the
  `ORDERED` bit; and
- `DELIVERED`: the exact ordered DATA ordinal admitted by the peer actor, with
  the original DATA leg, direction, and lane repeated in the header even
  though the DELIVERED stream itself travels in the reverse QUIC direction.

DATA payload length is exactly `1..=F`.  DELIVERED has flags `0` and payload
length exactly `0`; it is the sole legal header-only record.  Unknown flag bits
are rejected.  The reader rejects unknown version/kind/route/lane, illegal
length, payload above the configured maximum, duplicate delivery
acknowledgement, wrong original lane, and ordinal not pending in the same actor
incarnation.  Only ordered DATA produces DELIVERED.  The sender allocates an
ordinal only after local count/byte admission succeeds, and an ordinal is never
reused within an incarnation.

Before admitting ordered DATA to its bounded inbound channel, the peer
atomically holds both the exact inbound message/byte ownership and a reserved
slot/bytes in the independent ACK lane.  The queued inbound value carries that
ACK reservation. DELIVERED is not sent merely because the actor channel
accepted the value: only `StartedQuinnLegActor::recv_classified` committing the
exact bytes through the actor-private LegIo ingress gate consumes the
reservation and queues DELIVERED. It returns an opaque
`QuinnLegActorInbound` envelope that retains the exact inbound-byte (`Ib`) and
composition-message (`Cm`) ownership through the composition consumer's use or
discard; no production naked extractor exists in Slice A. This matches the
deterministic transport's receiver-dequeue delivery boundary.
QUIC acknowledgement, stream write completion, or local actor-channel
submission alone is not delivery authority.

Both transport configs initially permit exactly two remotely initiated
unidirectional streams.  Each peer opens exactly two long-lived
unidirectional streams and accepts exactly two.  Every stream
starts with this fixed 8-byte preface:

```text
b"MVL1" || version=1 || kind(DATA=1, DELIVERED=2)
        || sender_role(Client=1, Owner=2) || leg_id(A=1, B=2)
```

Duplicate kind, wrong role/leg/version/magic, a third stream, or EOF before the
preface is terminal protocol failure.  The initial remote-unidirectional limit
of two is part of the handshake transport config, rather than a later best-
effort reduction, so a peer cannot race-open a third stream while bootstrap
registers the first two.  Once registered, the desired remote-unidirectional
limit is set to zero and the original two remain open until cancellation; no
replacement credit is issued.  The DATA and DELIVERED record parsers run only
on their matching stream kind.

QUIC DATAGRAM is disabled in both directions (`datagram_receive_buffer_size`
is `None` and `datagram_send_buffer_size` is zero).  The factory also derives
finite `stream_receive_window`, connection `receive_window`, and `send_window`
values by checked arithmetic from the binding-confirm bytes, two fixed stream
prefaces, `Hd + F`, and `Ha`.  These transport windows are independent from,
and never substituted for, the exact user-space permits below.  Configuration
overflow or a value that cannot carry one maximum legal record fails before
Endpoint construction.

## Bounded ownership

Let:

- `Lom`, `Lob`, `Lim`, `Lib` be LegIo's monotonic lifetime outbound/inbound
  message/byte budgets (accounting limits, not live ownership);
- `Qm`, `Qb` be the sole outbound FIFO live message/byte bounds;
- `Om`, `Ob` be actor outbound message/byte bounds, including writer in-flight
  and ordered messages awaiting delivery acknowledgement;
- `Im` be actor inbound-channel live message slots through dequeue, while
  `Ib` is the exact inbound byte ownership retained through the admitted
  value's lifetime at the composition owner;
- `Cm` be composition-handoff message permits retained by each admitted value
  after dequeue, independently bounding its fixed metadata ownership;
- `Am`, `Ab` be the independent delivery-ACK message/byte bounds, including
  the control writer in-flight record;
- `F` be maximum encoded frame bytes; and
- `Hd` be the fixed DATA envelope header bytes; and
- `Ha` be the fixed header-only DELIVERED record bytes.

The checked user-space bound per leg is:

```text
messages <= Qm + Om + Im + Cm + Am + 2 partial readers
bytes    <= Qb + Ob + Ib + Ab + (Hd + F) + Ha
tasks    == 1 root + 4 fixed children
            (DATA rx/tx + DELIVERED rx/tx)
```

`Om >= 1`, `Im >= 1`, `Cm >= 1`, `Am >= 1`, `Ob >= F`, `Ib >= F`, and `Ab`
fits at least `Am` DELIVERED records; additionally `Am >= Im`.  Count-only bounds are
insufficient.  Actor byte permits are acquired before a submission or inbound
payload allocation becomes owned. A failed `try_send` releases the exact
permit and returns typed retryable pressure without consuming controller
ownership.

These capacity groups are never conflated.  `Lom/Lob/Lim/Lib` limit total
work over the leg lifetime; `Qm/Qb`, `Om/Ob`, `Im/Ib`, `Cm`, and `Am/Ab` limit
simultaneously live ownership.  The first tracer sends two equal-size frames,
so its lifetime outbound budget is at least two messages and twice their
encoded bytes even though actor outstanding capacity remains one message and
one frame's bytes.

For ordered inbound DATA, after validating the header the reader obtains the
exact byte permit first, then a composition `Cm` permit, inbound `Im` message
slot, and reserved ACK-channel send permit before allocating/reading and
admitting the payload.  Failure or cancellation before all four are held
admits nothing and acknowledges nothing.  Composition dequeue releases the
`Im` channel slot, retains both the `Ib` byte and `Cm` message permit in the
returned admitted value, and consumes the reserved ACK send permit
synchronously to queue the ACK before returning.
There is no await after dequeue.  Thus `Im = 1` and `Cm = 1` still permit the
next header to reach and block first on the prior value's held `Ib`, without
letting two payload allocations or admitted metadata objects coexist.
DATA and DELIVERED use separately identified QUIC unidirectional streams with
fixed rx/tx children; DATA cannot borrow ACK channel capacity or occupy the ACK
stream.  ACK permits release only after the full DELIVERED record is positively
written.  QUIC connection-level buffering/flow-control remains separately
observed and is not included in this exact user-space bound.

For ordered outbound DATA, the actor retains its byte permit and completion
token until the exact `DELIVERED` record arrives.  Pressure retry never marks
the token.  Unordered ownership may release after the full positive stream
write.  Kernel UDP and QUIC internal buffers are observed separately and are
not misreported as exact user-space ownership.

Capacity release and matching DELIVERED advance one coalesced readiness epoch.
The transport-independent waiter uses check/register/recheck around that
monotonic epoch, so a release between the failed flush and registration cannot
lose the wakeup.  The notifier owns O(1) state; releases do not enqueue an
unbounded notification stream.

This slice does not claim the bounds are sufficient for 100 or 170 Mbit/s.
At 100 Mbit/s, 5 ms already represents 62,500 application bytes; real sizing
belongs to the later measured capacity slice.

## Task and cancellation lifecycle

The factory spawns one fixed root.  Before any port escapes, that root is
registered in an outer, bounded actor nursery implemented with a `JoinSet`;
the root owns and completely drains its fixed DATA-rx, DATA-tx, DELIVERED-rx,
and DELIVERED-tx child `JoinSet`.  No bare `JoinHandle` is dropped or returned.
The factory never accepts caller-defined task code.

Cancellation proceeds as follows:

1. close new submission admission;
2. signal cooperative cancel on a lane independent of data pressure;
3. stop/drain fixed actor-owned queues according to their exact ownership
   result without marking undelivered records delivered;
4. finish or reset the framed stream deliberately;
5. await every fixed child and then the root;
6. close the exact Quinn connection and await both its Endpoint-map removal ACK
   and normal ConnectionDriver completion after the driver-held connection and
   socket references have been dropped;
7. for a client-owned private Endpoint, close it, await
   `Endpoint::wait_idle_or_driver_lost()`, drop the final Endpoint handle, and
   consume normal EndpointDriver/socket completion; for a server actor, leave
   the shared listener Endpoint to the listener supervisor; and
8. verify actor-owned message/byte counters are zero before minting joined.

The returned closed actor bundle owns the non-cloneable cancel capability and
the outer nursery.  Its `join(&mut self)` future borrows that retained owner;
cancelling the caller future therefore leaves the nursery available for a
later join retry. The root itself, not `join`, performs child draining,
per-leg connection close and dual-witness wait, private client-Endpoint
close/idle/final-handle drop/EndpointDriver witness, and the zero-ownership
check before it returns `RootExit::Clean`. Consequently the
only await in `join` is `outer_nursery.join_next()`: once that poll returns the
root result, the method stores the outcome and synchronously transitions the
owner to Joined before returning the exact fact.  There is no cancellation
point between consuming the root outcome and persisting it.  Dropping the
whole owner closes admission, signals cancel, and drops the nursery, whose
abort-on-drop semantics prevent root/child detachment; this failure path is
deliberately not a clean await and can never mint joined.  Channel close,
`JoinHandle::is_finished`, timeout, abort, terminal report, weak-reference
loss, or deterministic wire cleanup likewise cannot mint joined.

Semantic transport termination and structural task completion are independent.
A cleanly awaited `PeerClosed`/`Reset` may be carried in the joined outcome,
but root panic, child panic, structural `JoinError`, nonzero residual ownership,
or ALPN/exporter failure returns a carrying failure and mints no ordinary
joined fact. A structural child failure remains fatal even when an earlier
semantic first cause already exists. The local acceptance harness bounds every
join and idle wait. Cancelling a caller-side join deadline retains the nursery
for retry and is not join evidence; dropping the whole owner remains an
unclean abort path. Slice A does
not yet define the production composition owner's deadline policy.
Server-listener shutdown has its own carrying result and cannot be substituted
for a per-leg actor join.

Each accepted server bundle, like the client bundle, is the unique owner of its
outer root nursery.  `QuinnLegActorListener` owns no duplicate root handle and
cannot cancel or reap a per-leg task.  Instead it keeps a bounded registry of
incarnation-bound clean-join obligations.  A successful server `join(&mut
self)` records the exact clean outcome; an actor-bundle Drop records an
unclean/poisoned outcome after its nursery aborts fail-closed.

`admit` first receives one Quinn `Incoming`. An unvalidated remote address is
sent Retry and returns `RetrySent` without reserving a registry slot. A
token-validated retry freezes one absolute bootstrap deadline, reserves one
slot, and returns an owned `QuinnLegActorBootstrap`; `start()` may then run
concurrently with further admission. A full registry returns typed pressure
only after address validation. Publication transitions the slot to `Live`. A
successful bundle join first makes the bundle inert, awaits the exact
connection dual witness, then atomically records `Live -> Clean` and releases
the live slot. Drop immediately writes one sticky listener-level Unclean
outcome. The five termination guards then retire their counters and final slot
references; listener shutdown may return the unclean verdict only after those
tasks and Endpoint resources are cleaned. Clean and unclean history therefore
remain O(1) and cannot exhaust a long-lived listener.

Each root and each of its four fixed child futures also holds a registry-owned
termination guard.  On an unclean actor-bundle Drop, the outer and inner
JoinSets abort fail-closed; the guards notify only when those futures are
actually dropped. A termination guard retires its counter before its last slot
reference, so reclaimed capacity can never overlap the old guard count. This
abort-completion observation can never mint a per-leg
joined fact, but it gives listener cleanup a bounded, awaitable proof that no
task remains detached.

Admitted-but-unpublished bootstrap cleanup is tracked separately. `start()`
registers an inner bootstrap task before awaiting it, so cancellation of the
outer caller cannot detach uncounted cleanup. Dropping a bootstrap likewise
starts tracked cleanup. Timeout, authentication rejection, prepublication
start failure, or drop immediately abandons only that unpublished protocol
connection and awaits a consuming, single-use Quinn dual witness: both the
exact Endpoint map-removal ACK and normal ConnectionDriver completion. Either
witness alone is insufficient; panic, abort, I/O failure, or driver loss fails
closed. Client construction additionally takes the one EndpointDriver witness
before synchronous `Endpoint::connect`; every failure path registers cleanup,
drops the last Endpoint, and awaits normal driver/socket completion even when
no per-connection witness was minted. The slot is released only after its
applicable facts, and the termination counter retires before the slot becomes
reusable; ordinary graceful QUIC closing delay is not charged as reusable
capacity. Panic or runtime loss marks the listener unclean rather than claiming
cleanup.

The same non-cloneable witness crosses the successful publication seam only
after all fallible actor construction and the final absolute-deadline check.
The published root uses ordinary cooperative close and awaits both facts; it
must never call the unpublished `abandon` authority. Thus actor `Joined` and a
server live-slot release cannot race the exact connection's close tail.

`shutdown(&mut self)` returns a carrying `ListenerShutdownBlocked` without
closing the Endpoint while any live actor bundle still owns a nursery. Once
no live bundle exists, it stops admission and closes the shared Endpoint. The
state transition is `Accepting -> Closing -> Joined`: cancellation at either
await retains `Closing`, and a later `shutdown()` resumes the same transaction
rather than consuming the sole witness. For cleanly joined actors it awaits
`wait_idle_or_driver_lost` and may mint clean listener joined.
For an uncleanly dropped actor it additionally awaits the root and all four
child termination guards plus Endpoint idle-or-driver-loss observation, then
returns a distinct `ListenerShutdownUnclean` result and mints no clean listener
fact. After map
idle, the listener moves its final Endpoint and consuming driver witness into
one registered cleanup task; cancellation retains that task for the next
shutdown call. Only after the task drops the Endpoint and observes normal
EndpointDriver/socket completion may `Joined` be minted, and the released UDP
address is rebindable before return. Thus an unclean actor poisons the verdict,
not resource cleanup forever. A per-leg
joined fact cannot close that Endpoint, and listener idle cannot stand in for
a missing per-leg clean join. A focused lifecycle test proves server leg A can
join while server leg B continues to exchange bytes on the same listener
Endpoint.

## Exact actor identity

An independent process-local actor-incarnation allocation is created in the
same factory transaction as the exact leg seal.  It is not serialized on the
wire and is never supplied by the caller.  Runtime owner, closed transport
port, inbound receiver, and later controller control half share only that
opaque identity.  An inbound DELIVERED record is accepted only by the same
incarnation's reader and pending-ordinal table.  Leg id, generation, route,
endpoint address, certificate, peer policy, and attach binding are not
sufficient substitutes.

The joined fact is non-cloneable, has no public or test constructor, and is
minted at exactly one source location immediately after the successful real
per-leg await/connection-close/zero-ownership checks (plus private Endpoint
idle, final-handle Drop, and normal EndpointDriver completion for a client
actor). Slice A only exposes this fact for review; the controller does not
consume it until the next vertical.

## First tracer bullet

The single first test is:

```text
real_loopback_actor_marks_ordered_delivery_only_after_peer_admission_and_mints_join_only_after_clean_await
```

It uses a Tokio current-thread runtime and real `127.0.0.1:0` Quinn endpoints.
The test substitutes only the localhost peer; it uses the same fixed actor
loop and factory as non-test code. Its `cfg(test)` `accept` helper expands only
to the production `admit -> Bootstrap::start` transaction and loops past
`RetrySent`; production exposes no full-bootstrap `accept`. Client connect and
that test helper run concurrently with `tokio::join!`. Every connect,
admission/start, receive, join, and listener shutdown future is under a bounded
local timeout.

The test config separates the four capacity domains explicitly: LegIo
lifetime outbound/inbound budgets are each at least two messages and twice the
encoded frame size; the sole FIFO, actor outbound, actor inbound, and ACK lanes
each own one live message with their corresponding exact byte bound, and one
`Cm` permit bounds the admitted composition handoff.  The
factory does not accept a caller endpoint role: `connect` is Client and
an admitted bootstrap is Owner by construction.

Sequence:

1. bind the dedicated-ALPN server and concurrently connect plus admit/start
   client and server actors; publication proves full 1-RTT, exact ALPN, exporter-derived
   binding agreement, and factory-private actor incarnations;
2. through the closed client port's factory-produced LegIo and sole outbound
   queue, enqueue one valid ordered protocol frame and call the real queue
   `try_flush` path; no test code can construct `EncodedLegSubmission`;
3. prove stream submission alone leaves the queue's ordered wait pending, then
   wait for a read-only snapshot showing one validated header, one queued
   inbound message, its exact owned bytes, and one reserved DELIVERED slot;
   while that stable admission exists, prove the receipt is still false and
   only then capture the coalesced readiness epoch;
4. dequeue the exact peer-admitted bytes, then wait until both the receipt and readiness
   epoch have advanced, then register `wait_for_progress` with the deliberately
   stale pre-delivery epoch and prove it returns immediately; finally prove the
   ordered queue advances exactly once;
5. keep the first admitted inbound value alive so its exact inbound byte permit
   remains owned, enqueue and flush a second ordered frame, wait for an O(1)
   read-only snapshot proving its header was validated and its exact bytes are
   waiting on the held inbound permit, and only then prove the second frame
   remains undelivered under real inbound pressure;
6. request cooperative cancel on the independent lane without releasing that
   pressure and prove neither cancel nor task-exit observation can construct a
   joined fact;
7. borrow both retained actor owners concurrently, prove the server join stays
   pending while admitted ownership remains live, release that value, keep the
   second ordered wait false, await every fixed task, close both exact
   connections, idle the private client Endpoint, and obtain both per-leg
   joined facts;
8. assert zero actor-owned messages/bytes, then shut down and idle the shared
   server listener Endpoint separately.

Test code may construct a legal protocol `Frame`/`LegControlFrame` input and
inspect the closed port's queue disposition.  It may not mint an opaque
submission or ordered token, split/mix raw LegIo/queue/sender parts, replace the
actor loop, provide a future/handle, mint terminal, mint joined, bypass TLS or
binding agreement, release a byte permit directly, or mark delivery.

The existing compile RED that directly minted an opaque submission is rejected
evidence and must be rewritten.  The accepted first RED is the missing closed
factory/port/nursery interface exercised through real LegIo and its sole queue.
After that interface compiles, any handshake, binding-agreement, framing,
pressure, readiness, delivery, or join failure is a behavior RED and must be
diagnosed before broadening the slice.

## Failure branches and discriminators

1. Handshake/ALPN failure: no binding, LegIo, reporter, task, or socket
   ownership escapes.
2. Exporter/config/binding-agreement/bootstrap-timeout failure: the client
   abandons the unpublished attempt, closes/idles its private Endpoint, and
   the server abandons only its failed unpublished connection, awaits exact
   driver drain, then releases the listener slot; no actor authority is minted.
3. Outbound count/byte pressure: opaque submission remains retryable at the
   caller; counters and ordered state remain unchanged.
4. Stream write failure: the reporter may emit the exact terminal fact, but
   no delivery or joined fact is inferred.
5. Inbound bound pressure: stream reading backpressures within `F + Hd`; DATA is
   not acknowledged before bounded admission.
6. Cancel under data pressure: the independent cancel lane still wakes the
   fixed root and children.
7. Capacity/DELIVERED release: the single readiness epoch advances and wakes
   all current waiters without accumulating one notification per release.
8. Panic/JoinError or a caller-side join/idle deadline: actor owner remains
   failed or uncleared; joined is absent.  Server listener idle is a distinct
   shutdown result.
9. Wrong/stale actor fact in the next slice: exact-return and no controller
   mutation.  Slice A records identity now but does not pretend to test the
   future consumer.

## Stop rules

Stop and report instead of forcing GREEN if any implementation requires:

- caller-provided Quinn endpoint/connection/binding/future/JoinHandle;
- caller-provided Quinn/rustls config, verifier, exporter callback, or actor
  incarnation;
- a test-only actor loop or synchronous joined constructor;
- a test-only raw `EncodedLegSubmission`/ordered-token mint or unrestricted
  LegIo/queue/sender `into_parts`;
- detached root or child tasks;
- abort, timeout, terminal, channel close, or `is_finished` as join proof;
- unbounded message, byte, ACK, child, or pending-delivery ownership;
- QUIC DATAGRAM enablement, post-handshake-only stream limiting, or unbounded
  QUIC connection/stream flow-control windows;
- cancel sharing the pressured data lane;
- server per-leg code closing or idling the shared listener Endpoint;
- a pressure result without a coalesced check/register/recheck readiness seam;
- delivery completion at enqueue or QUIC write rather than peer admission;
- a dependency from policy/controller modules to Quinn or Tokio;
- reuse of standard TUIC framing/ALPN as if sing-box understood the enhanced
  protocol; or
- a production, blackout, throughput, Task-4, M2, or M3 claim from this local
  slice.

## Acceptance for Slice A

- the initial exact RED and the focused lifecycle REDs become GREEN through
  the production-shared actor factory;
- peer-policy and mismatched-`LegId` cases fail before publication, and source
  provenance locks the exact ALPN/exporter/binding-confirmation inputs and
  sole joined mint, typed Ib/Cm envelope, split production admission/start,
  test-only combined accept helper, and single-use Quinn drain authority;
- address Retry consumes no actor slot; an absolute admitted-bootstrap
  deadline cannot restart at `start`; dropped/cancelled start ownership remains
  tracked until the exact failed connection has both left the Endpoint map and
  completed its normal driver;
- server actor A can join while actor B continues on the same shared listener
  Endpoint, and A's server join observes only B remaining; after B joins, the
  listener has zero open connections before shutdown;
- listener shutdown with a live actor returns carrying `Blocked` without
  closing the Endpoint; dropping a pressured server actor then makes shutdown
  await the root plus four child termination guards and exact
  EndpointDriver/socket cleanup, return `ListenerShutdownUnclean`, and mint
  neither per-leg nor listener joined;
- cancellation after listener close or during EndpointDriver wait can be
  retried through the same registered cleanup task to the sole Joined witness,
  the UDP address is rebindable before return, and a later third call is
  rejected;
- an IPv4 client Endpoint synchronously rejecting an IPv6 remote still drops
  its EndpointDriver/socket before returning, even though no ConnectionDrain
  exists;
- real Quinn partial-header and partial-body resets are fatal truncated
  records; only a zero-byte end at a record boundary is benign transport end;
- the same built test binary passes 100 consecutive runs;
- actor, LegIo, controller, standby, and supervisor focused suites pass;
- all-target check and release check pass;
- strict production-library Clippy, rustfmt, rustdoc, diff, provenance, and
  secret-shape checks pass;
- independent actor, lifecycle, and documentation reviews report no unresolved
  P0/P1 in Slice A;
  and
- results and failures are written to project memory with the necessary-only
  claim preserved.

Only after Slice A is accepted may the next tracer bind exact actor cancel/join
authority to the controller's retired slot and prove that clearing retired A
does not emit `LegLost` or disturb active B.
