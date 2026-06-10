# HANDOFF — Stage 13b (UDP over TUIC Packet), continue from Task 3

Session handoff. Everything below is in git on branch **`claude/stage13-tuic-data-plane`**.

## Where we are

mini_vpn is migrating its data plane to the **TUIC v5 protocol** on quinn (**client-only**; the exit is a
mature **sing-box TUIC server**) — see `docs/adr/0004-tuic-protocol-data-plane.md`. Staged 13a→13d, dual-run
via `MINI_VPN_UPSTREAM=legacy|tuic` (default legacy = zero regression).

- **13a (TCP over TUIC Connect)** — DONE + cross-machine accepted (curl https://1.1.1.1 via sing-box →
  Cloudflare 301, US egress). See `docs/tech/13-tuic-tcp-connect.md`.
- **13b (UDP over TUIC Packet, native datagram)** — IN PROGRESS:
  - ✅ Task 1: TUIC Packet codec + heartbeat (`src/tuic.rs`: `encode_packet`/`decode_packet`/`encode_heartbeat`).
  - ✅ Task 2: `AssocTable` (u16 assoc-id per UDP 4-tuple, `src/tuic.rs`).
  - ⏳ Task 3–5: below.

All tests green: `cargo test --lib --bins --tests` (87 tests). Note: `cargo test --doc` is SIGKILLed by a
sandbox resource cap (no doctests exist) — that's an environment artifact, not a failure.

## First: ground yourself

- **Worktree caution**: the Bash cwd may be reset to a different worktree between calls. Run git/cargo from the
  worktree holding this branch: `cd <…>/.claude/worktrees/recursing-mclean-645d44 && …`, and `git branch
  --show-current` should print `claude/stage13-tuic-data-plane`. File edits should use absolute paths into that
  worktree.
- Read: `docs/adr/0004-…`, `docs/tech/2026-06-10-stage-13b-tuic-udp-packet-spec.md` and `…-plan.md` (the plan
  has the **byte-exact TUIC Packet wire reference**), `docs/tech/13-tuic-tcp-connect.md`.
- Skim `src/tuic.rs` (codec + config + AssocTable + `TuicUpstream`), `src/upstream.rs` (`ProxyUpstream` trait +
  `RelayStream`), `src/udp_relay.rs` (Stage-12 raw-packet UDP helpers: `parse_inbound_udp`,
  `build_udp_ip_packet`, `FourTuple`/`FlowEntry`), `src/client_tun.rs` (main loop; `Upstream` enum; tuic-mode
  TCP wiring + the rx UDP classify/`handle_udp_uplink` + downlink inject from Stage 12).

## Do: 13b Task 3–5 (per the plan)

- **Task 3 — `TuicUpstream` UDP** (`src/tuic.rs`): `send_udp(&self, datagram)` (`conn.send_datagram`; TooLarge/err
  → drop+count+log); a downlink datagram pump (`read_datagram` loop → forward bytes via a channel for the main
  loop to decode); a periodic Heartbeat task while connected; expose the downlink channel receiver from
  `connect()`. Reuse 13a's reconnect (re-spawn pump/heartbeat on reconnect).
- **Task 4 — wire tuic-mode UDP into `client_tun.rs`**: in tuic mode enable the UDP path — rx `UdpRelay` →
  `parse_inbound_udp` → `resolve_target` (fake→domain) → `AssocTable.intern` → `encode_packet` →
  `TuicUpstream.send_udp`; downlink: select on the TuicUpstream downlink channel → `decode_packet` →
  `AssocTable.resolve` → `build_udp_ip_packet(src=fake-IP, dst=app)` → `device.inject_ip_packet`. AssocTable is
  main-loop-owned. **Legacy UDP path (Stage 12) must stay byte-identical (zero regression).**
- **Task 5 — interop e2e + docs**: against the real sing-box (tuic mode) — `dig @1.1.1.1` (UDP DNS) returns an
  answer; UDP echo via a domain (ATYP=domain, fake-IP); optional QUIC/HTTP3. Then teaching note + LEARNINGS;
  mark 13b done in TODO; 13c (migration/0-RTT) next.

## Rhythm (important)

- TDD per task: write failing test → red → implement → green → commit. **`git push` after every commit** —
  there has been a concurrent session on this branch that clobbered a commit; pushing protects the work, and
  there should be only **one writer** on this branch at a time.
- End each stage with `/code-review` over the diff, then the interop acceptance against sing-box.

## Not in git (the user provides)

- **sing-box interop params** (needed only for Task 5 e2e), in env on the client:
  `MINI_VPN_UPSTREAM=tuic`, `MINI_VPN_TUIC_SERVER=<VPS_IP>:8443`, `MINI_VPN_TUIC_UUID=<uuid>`,
  `MINI_VPN_TUIC_PASSWORD=<pass>`, `MINI_VPN_TUIC_SNI=example.com`, `MINI_VPN_TUIC_CA_PATH=certs/dev/ca-cert.pem`,
  `MINI_VPN_TUIC_ALPN=h3`. (Ask the user for the actual UUID/password/IP — do NOT commit them.)
