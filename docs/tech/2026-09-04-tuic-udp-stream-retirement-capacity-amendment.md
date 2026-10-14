# TUIC UDP stream admission and exact retirement capacity

Date: 2026-09-04.

Status: **DESIGN AND IMPLEMENTATION INDEPENDENTLY ACCEPTED; PUBLIC LOCAL GATES CLOSED.**

Execution update (2026-09-05): original Reviewer Popper closed implementation
and its final authorized format-only delta at P0/P1/P2 `0/0/0`, Approve.
Public local gates and the four exact shrink entries are closed. Exact commands,
source fingerprints and preserved failure/confirmation history are in
`2026-09-05-public-path-capacity-repair-local-results.md`. The implementation
contract is unchanged; review clarified the scan-coverage statement below for
early-return versus full-pressure calls. Both Quinn client-root repairs remain
frozen and P2T stays with the coordinating task; this supersedes only the
historical automatic root-repair sequence below.

This amends only the sender capacity contract in
`2026-09-04-public-path-review-repair-plan.md`. The earlier 16-test one-shot
sender checkpoint did not close the independent RESET-capacity P2; the dated
result owns closure of that finding and final acceptance.

## Problem and goal

The current one-shot sender can repeatedly open and RESET streams while a peer
withholds byte credit or ACKs. Quinn immediately restores `unacked_data` after
RESET, yet retains stream storage until acknowledgement. Its send window is
therefore not an independent local bound for this new admission pattern.

Close signals also do not prove memory retirement. `close_reason`, Closing,
Draining, and the existing `ConnectionDrain` endpoint/driver witness can all
precede destruction of protocol storage retained by other application handles.
Weak upgrade failure alone is insufficient: it can occur while the last strong
owner is still running the value's destructor.

The goal is one local Authority that bounds every UDP-created stream, including
FIN and RESET tails across connection generations, while each UDP call remains
non-suspending. It must eventually reuse capacity after proven retirement and
must not keep a closed connection alive merely to observe its destruction.

Non-goals: throughput acceptance, public Knife16 cutover, a new sender actor,
TCP lifecycle replacement, forced transport disposal, handshake redesign,
transport-wide zero ownership, and external tests.

## Authority and capacities

`TuicUpstream` owns one private sender settlement Authority implemented in
`tuic/udp_send.rs`. It owns a startup-allocated fixed array of **256** optional
retirement tickets and one persistent scan cursor. There is no second count,
global byte ledger, free-slot registry, or queued application Packet.

One slot is reserved before a stream can be opened. Each accepted Packet is
already bounded to **67,584 bytes (66 KiB)**. At most 256 UDP-created stream
episodes can remain unretired, independently of peer MAX_STREAMS/MAX_DATA:

`256 * 67,584 = 17,301,504 bytes (16.5 MiB)` maximum submitted Packet bytes,
plus 256 fixed ticket records and transport metadata attributable to those
bounded episodes. This is the sender's outstanding admission bound, not a
process-wide or all-QUIC allocation ceiling.

Only the stream branch takes this Authority's private nonblocking `try_lock`.
Native datagrams retain their current path. Lock pressure causes one counted
UDP drop. Each call examines at most **four** rotating slots for vacancy or
retirement, preserving the cursor across calls; no full inventory is recomputed
per packet. If none of the four is reusable, the packet is dropped even if a
later slot might be reusable. During continuous full pressure, 64 unsuccessful calls that each inspect four
occupied slots cover every slot. Calls that find a reusable slot return early;
across calls that actually enter the slot scan, a full rotation takes at most
256 calls. This deliberately bounded admission policy is necessary-only and makes no
throughput promise.

## Narrow Quinn fork additions

The proposed fork changes are confined to `quinn`'s `connection.rs`,
`send_stream.rs` if needed for type construction, its public export seam, and
private tests. There is no `quinn-proto` mutation or forced stream disposal.

1. Add a synchronous `Connection::try_open_uni_1rtt_with_retirement` adapter.
   Under the existing single Quinn connection-state lock, it checks connection
   error and `inner.is_handshaking()` **before** allocating a stream. During
   handshaking it returns unavailable without opening anything. When established
   and uni credit is available, it opens one stream and returns that exact
   `SendStream` paired with its restricted retirement ticket. It never creates a
   Notify waiter and never awaits. The 1-RTT check and stream allocation share
   the lock, preventing a separate precheck/open interpretation.
2. A retirement ticket contains a private `Weak<ConnectionInner>`, the exact
   stream ID minted by that adapter, and a clone of the connection's destruction
   token. It exposes only a synchronous retirement query. It cannot recreate
   `Connection`, open streams, send bytes, or invoke handshake/0-RTT behavior.
3. Allocate one destruction token per connection, backed by `Arc<AtomicBool>`.
   A final sentinel field in `ConnectionInner`, declared **after `state` and
   `shared`**, stores `true` with Release ordering only when those earlier
   fields and their protocol buffers have finished destruction. The ticket
   reads this flag with Acquire ordering. Add a deterministic destructor-window
   tracer proving weak-upgrade failure can precede this token.

Do not assume current TUIC publication has awaited 0-RTT acceptance: the current
`try_0rtt` path discards its acceptance future. The new atomic adapter therefore
drops only stream-mode UDP during QUIC handshaking; native datagrams and the
existing authentication path remain as they are. No early-data stream is opened
and then discarded as an unsupported admission attempt.

## Exact retirement predicate

A ticket may release its slot in exactly these cases:

- The connection value's final destruction token is observed `true` with
  Acquire ordering; or
- Weak upgrade succeeds and, under that exact connection's state lock, the
  existing **synchronous proto** `send_stream(id).stopped()` returns
  `Err(ClosedStream)`, meaning the send-map key is absent.

The latter is a physical absence query, not the asynchronous Quinn
`SendStream::stopped()` API. `Ok(Some(STOP_CODE))` and `Ok(None)` retain the
slot. In particular, the proto method returns `Ok(None)` for an allocated but
uninitialized map entry, so absence is not confused with the current progress
sampler's uninitialized-state error. The ticket's 1-RTT identity prevents an
early-data stream-ID reuse ambiguity.

If Weak upgrade fails while the destruction token is still false, retain the
slot. A close flag, generation change, RESET success, acknowledged data without
FIN retirement, endpoint removal, driver completion, or async stopped result
cannot release it. A temporary upgrade uses a bare `Arc`; it never wraps it in
`ConnectionRef` or changes Quinn's separate public-handle `shared.ref_count`.
Drop the state-lock guard before dropping this temporary Arc.

## One-call transaction and interruption

After finding a vacant/retired slot, the synchronous adapter either rejects
without stream allocation, or returns one stream plus its ticket. The UDP
Authority immediately stores the ticket in that slot and places the stream in
an in-scope guard before any write. These steps allocate no new queue storage
and have no callback or await boundary.

The guard performs one bounded write poll. A full write may call FIN; any prefix,
Pending, write error, or failed FIN attempts RESET. The slot remains charged in
every case. Only after FIN or RESET settlement does the guard release its strong
stream handle, leaving the **weak-only ticket** in the slot. The guard's Drop
attempts RESET when settlement was interrupted, so Rust unwinding cannot
implicitly FIN a partial Packet. Successful FIN must disarm that reset guard.

No timer, per-packet future, retry, sender task, retained payload queue, or ACK
wait is introduced. Cancellation before the public future's first poll admits
nothing; its active poll has no suspension point. A poisoned Authority preserves
its occupied slots and fails further stream admissions closed; it must not
clear poison or normalize uncertain custody to vacant slots.

All tickets, including old-generation tickets, share the same 256 slots. Normal
finish/reset immediately releases the sender's strong handles, so old tickets
cannot keep the old transport alive. If another TCP handle still retains that
connection's buffers, the ticket correctly remains charged. After real stream
retirement or complete connection destruction, later bounded scans recover the
slot. This does not manufacture successful recovery while another owner is
still retaining the charged resource; it also does not leave self-retaining
dead slots or require rebuilding the adapter.

Dropping the Authority ends future admission and drops only its weak tickets.
It does not claim that another owner's transport is destroyed or that the whole
process has reached zero. The in-scope guard, if any, settles its own stream
before releasing strong ownership.

## Required deterministic traces

Keep the current 16 UDP tests, including complete Packet+FIN, partial and
zero-credit RESET, lock/closed/oversize rejection, native fallback, and exact
fragment ownership. Add vertical traces against actual loopback Quinn:

1. On a current-thread runtime, complete the handshake, grant more than 256 uni
   streams, set zero/small stream byte credit, and perform more than 256 public
   UDP calls without yielding to ACK processing. Admission must remain bounded
   to 256 stream IDs. The next real `open_uni` stream ID provides an independent
   physical creation count; it is a fixture probe, not a production bypass.
   Include zero-byte reset metadata and partial-byte reset cases.
2. Let actual RESET or FIN acknowledgements retire streams. Subsequent calls
   must recover slots; retaining a STOP_SENDING result or partial byte ACK must
   not recover them. Verify native datagram progress during pressure.
3. Close an old connection while retaining another real application handle.
   Replace the sender's current connection and continue sending: old tickets
   still consume the same global 256-slot bound. After releasing that handle
   and finishing endpoint/driver teardown, the destruction token releases the
   old slots and the existing adapter resumes admission.
4. Hold the connection-value destructor before its final sentinel with a
   deterministic test barrier. Weak upgrade must already fail while the ticket
   remains unretired; only releasing the barrier/token permits reuse.
5. Exercise the atomic handshaking rejection and established admission paths;
   the rejected case consumes no stream ID or ticket. Exercise interruption
   after ticket custody but before normal FIN/RESET, preserving occupied custody
   and ensuring no partial FIN or poison-to-zero shortcut.

Test physical stream IDs, live send-map absence, exact ticket occupancy, bytes,
and final destructor ordering independently. Do not substitute scalar-equal
mock counters for the real transport's retained state. Tests may scan full
inventories; production packet admission may not.

## Gate and stop rule

The same independent Reviewer approved the architectural draft with SHA-256
`046263ff283ae66993fd1a7ea9e9f501750ba4397e1f51236d71fba46653f061`, reporting
P0/P1/P2 `0/0/0`, Approve. Both design P2 findings were closed. This approval
covers only the local admission/retirement design and the narrow Quinn-fork
scope specified here; it does not approve the current sender implementation,
authorize Rust, or promote the public stage. The later gate wording clarifies
that review and user confirmation may proceed in parallel without relaxing
either prerequisite.

The same independent Reviewer must approve this architecture and its explicit
Quinn-fork scope, and the user must confirm the next attempt, before Rust
resumes. These two necessary gates may proceed in parallel; neither substitutes
for the other. No Rust follows merely from the original task authorization or
this draft. Once both gates pass: preserve the current baseline, add one causal RED,
implement the bounded transaction, run focused GREEN and lifecycle/provenance
checks, then return to the same Reviewer. A failed implementation test or new
ownership review finding stops the next repair attempt again.

Only after sender closure may the Integrator promote the public-path result
and shrink ratchets, then start the separately reviewed Quinn-root repair plan.
No commit, push, VPS, macOS TUN, WAN, throughput, or M2 run is authorized here.
