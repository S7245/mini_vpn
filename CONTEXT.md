# mini_vpn

A learning-oriented VPN: a local client intercepts traffic through a TUN device and a userspace TCP/IP stack, then tunnels it over TLS + Yamux to a remote proxy server, which connects out to the real destination on the client's behalf.

## Language

**Upstream**:
The remote proxy/relay server the client tunnels through (e.g. the US server). The client reaches it over a **Transport**, and the Upstream connects out to the Target on the client's behalf.
_Avoid_: relay-server, gateway (when meaning the proxy box)

**Transport**:
How the client carries its tunnel to the **Upstream**. Today: **TUIC over QUIC** (the data plane — TCP relay streams plus a UDP datagram plane). A second transport, **VLESS over REALITY over TCP**, is the anti-censorship fallback for when QUIC is degraded or blocked.
_Avoid_: protocol (overloaded), connection (a transport can span reconnects)

**TUIC TCP pool path service**:
The current send-side service estimate of one TUIC TCP pool slot, represented by that QUIC path's congestion window divided by its RTT. It is a point-in-time placement input for service-normalized admission when every admitted candidate is busy and known, and remains the equal-load tie-break inside the bounded fallback. It is not a bandwidth promise, a health verdict, or permission to change congestion-control constants. Unknown or idle evidence falls back to the pool's stable lease-aware ordering.
_Avoid_: connection speed, bandwidth score, priority (the estimate is transient transport state, not a configured class of service)

**TUIC TCP pool service-normalized admission**:
The new-open placement rule applied after forward qualification when every admitted candidate is busy and has known current path service. It compares exact `active lease ownership * RTT / cwnd`, so ownership demand is evaluated against the current service available to it; lower normalized load wins. Equal normalized load falls back to lower raw ownership, then greater path service, then stable index. Idle, Unknown, and all-degraded cases retain the existing bounded fallback. The comparison is exact rational ordering with no float, threshold, multiplier, timer, Target rule, or config knob, and it cannot mutate an existing connection, stream, or payload.
_Avoid_: bandwidth score (path service remains a transient placement hint), weighted tuning (there is no configured weight), connection health (forward qualification owns negative evidence), affinity (no Target cohort is inferred)

**TUIC TCP pool forward qualification**:
Whether a TCP pool slot has added a Quinn PLPMTUD black-hole detection since the start of its current nonzero TCP-lease ownership epoch. A slot is qualified while the monotonic count stays at its epoch anchor and degraded after it advances; exact per-slot lease zero begins a new epoch. Qualification controls only new Target relay admission while the pool is busy. It never closes, reconnects, migrates, retries, or promises bandwidth, and an all-degraded pool retains a bounded least-active fallback.
_Avoid_: connection health (qualification is one exact placement discriminator, not a complete health verdict), blacklist (zero ownership starts a new epoch), MTU tuning (the observed counter is used without changing MTU policy)

**TUIC TCP pool generation replacement**:
A bounded lifecycle handoff for an auxiliary TCP pool slot whose current busy generation has proven degraded. The old generation becomes drain-only and keeps every existing relay; one authenticated successor becomes the slot's only generation eligible for new Target opens. The configured pool and eligible-slot count remain unchanged, the primary TUIC connection and UDP/health ownership never move, and at most one predecessor may drain behind an auxiliary successor. Replacement is neither stream migration nor pool expansion.
_Avoid_: reconnect active flows (the predecessor remains alive), failover retry (the triggering open waits for a successor instead of replaying payload), pool growth (only two logical slots admit new work)

**TUIC TCP successor service turn**:
A bounded pre-install readiness contract for a freshly authenticated auxiliary
replacement. Quinn snapshots that connection's current congestion window and
emits one Endpoint-bulk, ACK-eliciting transport flight; every tagged encrypted
packet byte must be acknowledged on the same path generation before the
existing identity/activity/generation CAS may install the successor. Loss,
path change, close, or the existing whole-replacement deadline fails without
retry and leaves the predecessor current. It sends no Target/business probe,
adds no timer or configured byte threshold, and is not a general bandwidth
promise.
_Avoid_: warm-up traffic (the proof is transport-native and carries no business payload), health check (it proves one exact delivered flight only), retry (loss is terminal), pool expansion (no additional eligible generation is retained)

**TUIC TCP successor service certificate**:
The immutable transport-native service proof owned by one installed auxiliary
successor generation. It records the exact stable identity, logical pool
generation, ACK-owned path generation, and positive congestion-window floor
established by the first exact pre-install service turn. Additional turns may
prove a larger predecessor handoff for that install transaction, but their
final cwnd is not persisted as lifetime readiness. The generation is Ready
for new-open admission only while identity/path still match and current cwnd
remains at or above its first-turn floor; otherwise it is Stale and the
existing bounded fresh-generation replacement seam owns recovery. Initial
pool generations are Unknown rather than degraded. The certificate is not a
configured threshold, congestion-event blacklist, bandwidth promise, or
permission to mutate existing streams.
_Avoid_: health score (it is one exact historical proof), cwnd tuning (the
floor is observed, not configured), in-place warm-up (replacement is the safe
seam), connection blacklist (a fresh generation can prove new service)

**TUIC TCP successor forward-service inheritance**:
A generation-replacement contract that preserves the exact positive
congestion window surrendered by the current auxiliary generation immediately
before an already-authorized replacement. Under the slot mutex, each
replacement attempt overwrites its transaction requirement with the maximum
of the immutable first-turn readiness floor and the exact current Quinn cwnd.
A fresh successor must complete one or more sequential **successor service
turns** on one path, within the unchanged whole-replacement deadline, until
its typed install proof reaches that requirement. Under the predecessor slot
mutex, the install CAS rechecks the latest slot-owned requirement, exact
successor identity/path/readiness, and the successor's live close/path/cwnd
state; a successor that changed or regressed after proof cannot install. After
installation, only first-turn readiness becomes the new generation's lifetime
certificate. A failed transaction cannot ratchet a later attempt.
Loss, path change, close, timeout, no cwnd progress, or an insufficient final
proof leaves the predecessor current. It adds no configured threshold, retry,
timer, Target probe, payload replay, or bandwidth promise.
_Avoid_: cwnd target (the floor is observed predecessor state), warm-up loop
(the transaction is deadline-bounded and loss-fail-closed), throughput
guarantee (it preserves surrendered service rather than predicting the WAN)

**TUIC TCP replacement-failure business fallback**:
A bounded new-open admission rule used only after one auxiliary generation
replacement fails before successor installation. The failed slot becomes
ineligible for that business open, the predecessor remains current, and only
another already-qualified current generation may receive the open through the
existing preparation/activity/lease path. The same open cannot attempt a
second replacement; no safe generation fails with both exact causes. A later
independent open has fresh replacement authority. No TUIC Connect, Target
socket, or business payload exists yet, so this is admission fallback rather
than traffic retry.
_Avoid_: retry (neither replacement nor payload is retried within the open),
failover replay (no Target request has been sent), degraded fallback
(post-failure admission is qualified-only), pool expansion (eligible ownership
does not grow)

**TUIC TCP new-stream startup service**:
A bounded per-stream send-scheduling contract. A newly opened TUIC TCP stream queues its Connect header and first non-empty business payload above incumbent normal-priority stream data, then atomically returns to its original Quinn priority after exactly one business scheduling turn. Blocked or empty writes do not consume the contract. It changes neither connection admission nor congestion/flow control and has no timer, byte threshold, Target rule, or configuration knob.
_Avoid_: stream boost (sounds tunable or permanent), fast lane (suggests separate capacity), failover (the selected QUIC connection does not change)

**TUIC TCP connection-local path-state recovery**:
The retired client policy that called Quinn `path_changed()` on an unchanged
path when an established TCP writer remained Pending and PLPMTUD black-hole
count advanced. Formal evidence showed exact ACK progress while this reset
collapsed cwnd and interrupted Target service; an established TUIC stream
cannot migrate to a replacement generation. mini_vpn therefore grants no
connection-local path reset authority. Native Quinn loss/PLPMTUD recovery,
exact ACK-stall Endpoint rebind, UDP no-RX rebind, generation replacement for
future opens, and predecessor drain remain active.
_Avoid_: active recovery (the policy is retired), stream migration (existing
TUIC streams stay on their generation), MTU tuning (native Quinn owns it)

**TUIC recovery evidence observer**:
A diagnostics-only, bounded observer beside the active Endpoint recovery policy. For exact writer-Pending episodes it records start and terminal sampled ACK-service aggregates; for exact ordered receive gaps it records one persistent-gap event with initial/current tail state. It is enabled only by `MINI_VPN_TCP_DIAG=1` and cannot rebind, reset, replay, retry, close, or replace any connection or stream. The earlier ordered-gap active migration was rejected after six false rebinds on a passing WAN transfer and is not an available recovery authority.
_Avoid_: recovery policy (the observer grants no action), ordered-gap migration (that architecture is rejected), ACK delivery proof (ACK progress proves only client-to-Exit transport service), Target delivery observer (that belongs to the paired Exit artifact)

**Direct baseline evidence**:
The pre-TUN pair of forward and reverse iperf JSON results against the Target. It proves receiver-positive physical-path continuity for the short observation window and supplies the forward receiver rate from which the longer direct discriminator derives its offered load. Low average bandwidth is valid; a complete receiver-zero interval, malformed evidence, command failure, or validator execution failure is not. It is distinct from the 300-second direct discriminator and never exercises mini_vpn.
_Avoid_: speed test (continuity and provenance, not maximum bandwidth, are the contract), VPN baseline (no TUN or VPN data plane is active)

**Target**:
The final `IP:port` an intercepted connection wants to reach (e.g. a website). On the TUN path it is extracted from smoltcp's `local_endpoint()`. The Upstream connects out to the Target.
_Avoid_: upstream, destination, remote (these collide with other concepts)

**Reconnect epoch**:
A monotonically increasing counter for the Upstream connection's generation; it increments each time a new Upstream connection is established. Used so relay tasks belonging to a previous connection cannot feed data into a socket served by the new connection (anti-crosstalk).
_Avoid_: session id, connection id (epoch is about generation, not identity)

**fake-IP**:
A placeholder IPv4 from the `198.18.0.0/15` range handed back to an application in a forged DNS response, instead of the domain's real address. The application connects to the fake-IP; the client maps it back to the domain when the TCP SYN arrives. Not a real address — never routed on the public internet.
_Avoid_: virtual IP, proxy IP (fake-IP is the precise term, matching Clash's fake-ip mode)

**fake-IP map**:
The client-held bidirectional table `domain ↔ fake-IP`. Populated when the client intercepts a plaintext DNS query — sent to **any** resolver — and forges a fake-IP response, so no plaintext query reaches a real resolver; read ("resolve") when an intercepted TCP SYN's destination falls in the fake-IP range, to recover the domain for the relay request.
_Avoid_: dns cache (it is not a cache of real DNS answers)

**UDP flow**:
One intercepted UDP conversation, identified by the app's 4-tuple `(srcIP:srcPort, dstIP:dstPort)`. Unlike a TCP **session** it has no handshake or teardown — it is born on the first datagram and reclaimed by idle timeout. Each **UDP flow** carries exactly one **Target** (the `dstIP:dstPort`, possibly a fake-IP resolved to a domain).
_Avoid_: udp session, udp connection (UDP is connectionless; "flow" signals there is no connection state)

**flow-id**:
A `u32` minted by the client, one per **UDP flow**, carried in both directions on the QUIC datagram so each side can demux a datagram back to its flow. It exists because the server's reply addresses the real Target IP, which the client cannot reverse-map to a fake-IP — the flow-id is the only reliable demux key.
_Avoid_: session id, stream id (it identifies a UDP flow, not a QUIC stream or TCP session)

**assoc-id**:
The TUIC UDP association id (`u16`) carried in a TUIC `Packet`. In mini_vpn it is allocated **one per UDP flow** (4-tuple) — the same role as **flow-id**, just 16-bit and on the TUIC wire — so the reply (tagged with assoc-id) maps back to the app endpoint and fake-IP source. We deliberately do not use TUIC's full-cone "one association per local socket" model, because that would reintroduce the reply-demux problem flow-id solves.
_Avoid_: session id (it identifies a UDP flow, not a QUIC stream or TCP session)

**UDP relay mode** (`native` / `quic`):
How a TUIC `Packet` is carried over the **Upstream**'s QUIC connection. _native_ = QUIC datagram (unreliable, low-latency, but throughput-capped on high-RTT/lossy paths and silently lossy under overload — drops the oldest buffered datagram without error). _quic_ = each `Packet` on its own QUIC **unidirectional stream** (reliable, ordered within the one packet it carries; escapes the datagram throughput ceiling, at the cost of one stream setup per packet). Per the TUIC spec the **Upstream** mirrors the mode of an association's **first** `Packet` for all of that association's downlink `Packet`s — so the mode is effectively chosen at **UDP flow** birth and cannot be flipped mid-flow. The default is _native_: real-egress measurement (2026-06-17) showed datagram sustains high-rate cleanly (~40 Mbps) once links aren't bandwidth-capped, so _quic_ mode is an opt-in (anti-censorship / oversized fallback), not the throughput path. See `docs/adr/0005-cubic-over-bbr-datagram.md`.
_Avoid_: "stream mode" alone (ambiguous with TCP relay streams); "datagram fallback" (oversized-packet stream fallback is a separate, size-driven thing within _native_ mode)

**Encrypted DNS** (DoH / DoT / DoQ / DoH3):
DNS carried inside an encrypted transport so it cannot be intercepted as plaintext: **DoH** (DNS-over-HTTPS, TCP :443), **DoT** (DNS-over-TLS, TCP :853), **DoQ** (DNS-over-QUIC, UDP :853), **DoH3** (DoH over HTTP/3, QUIC UDP :443). It **bypasses the fake-IP map** — the app gets a real address and connects directly, defeating fake-IP routing. The client therefore **blocks** known encrypted-DNS endpoints (by port for :853, by resolved domain or destination IP for :443) to force the app to fall back to plaintext DNS, which is then forged into a **fake-IP**. The plaintext fallback is intercepted **regardless of which resolver the app targets**, so fake-IP routing does not depend on the system DNS pointing at the client's resolver.
_Avoid_: "DNS leak" (that is the symptom — a real IP escaping interception; encrypted DNS is one cause)

**VLESS**:
A lightweight, stateless proxy protocol that frames one relay request (UUID auth + command + **Target** address) over an already-encrypted stream; it carries no transport security of its own — it relies on the **Transport** beneath it (here, **REALITY**).
_Avoid_: VMess (its heavier, crypto-carrying predecessor)

**REALITY**:
A TLS-camouflage **Transport** that hides its authentication inside a genuine TLS 1.3 handshake aimed at a borrowed real site, so a network observer sees ordinary HTTPS; an unauthenticated peer (including an active prober) is transparently forwarded to the real site. Carries **VLESS** as the censorship-resistant alternative to TUIC-over-QUIC, over TCP.
_Avoid_: "TLS proxy" (the whole point is to be indistinguishable from a non-proxy)

**Metrics snapshot** (`MetricsSnapshot` / `Metrics`):
The data-plane observability seam (刀11). `Metrics` is a process-level `Arc` of atomics, cloned into the two data-plane tasks (the `run_event_loop` task and the TUIC `start_udp` task) — the only handle that bridges both. It holds two kinds of value: **cumulative counters** (`dns_forged`/`dns_dropped`/`udp_drops_down`/`datagram_pressure_events`/`relays_spawned`), incremented with `fetch_add(Relaxed)` at their event sites; and **published gauges** (`active_relays`/`fake_ip_active`/`fake_ip_total`/`failover_leg`), which the loop **recomputes on a 30s tick** from its single-writer-owned state (`socket_ctxs`, `fake_pool`, the upstream's leg) and `store`s back — because that state must never be read cross-task or locked. `MetricsSnapshot` is the plain-value, `Copy` read-out (the frontend contract). It is distinct from `MetricsSink`, the knife1 per-segment **timing** seam (which stays zero-cost in production). Backpressure is counted on the **rising edge** (false→true), not per tick. See `docs/adr/0012-data-plane-observability-metrics.md`.
_Avoid_: conflating it with `MetricsSink` (timing vs counters/gauges); "live gauge" (the gauges are periodically recomputed, not +1/-1 tracked)

## Relationships

- The client reaches one **Upstream** over a **Transport** and multiplexes many intercepted TCP sessions over it (one relay stream per session).
- Each intercepted session carries exactly one **Target**; the **Upstream** dials that **Target**.
- UDP relay rides the QUIC datagram plane of the TUIC **Transport** — many **UDP flows** multiplexed by **flow-id** (no per-flow stream).
- The same **Upstream** may be reachable over more than one **Transport** (TUIC-over-QUIC, or VLESS+REALITY-over-TCP) — same destination, different camouflage. **REALITY is TCP-only**; UDP stays on the QUIC datagram plane.

## Flagged ambiguities

- "upstream" was proposed for the final destination, but the codebase already uses **Upstream** for the proxy server — resolved: final destination is **Target**, proxy server stays **Upstream**.
- smoltcp's `local_endpoint()` sounds like "this machine's address" but on the TUN path it is the **Target** (the SYN's destination address) — it is NOT the local machine's address.
