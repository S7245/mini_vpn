# REALITY TLS 1.3: implement TLS_AES_128_GCM_SHA256 (0x1301) first, with a generic-over-hash skeleton

The hand-rolled TLS 1.3 client for REALITY (ADR-0008) implements and **known-answer-tests only
`TLS_AES_128_GCM_SHA256` (0x1301)** in 刀7. **The 刀7 code is hard-coded to SHA-256 + AES-128-GCM**
(fixed `[u8;32]` secrets / `[u8;16]` keys / `[u8;12]` IVs; `Hkdf::<Sha256>` / `Aes128Gcm` throughout) —
there is **no `CipherSuite` enum / no hash-or-AEAD genericity yet**; that refactor is **deferred** until a
second suite is actually needed. `TLS_AES_256_GCM_SHA384` (0x1302) and `TLS_CHACHA20_POLY1305_SHA256`
(0x1303) are **not implemented** — a known gap to fill in a 刀7-tail task or 刀8. To prevent silent
mis-keying, **`parse_server_hello` (刀7) already errors on any `cipher_suite != 0x1301`** at parse time.

Decided in 刀7 grill, 2026-06-23, on the back of an understand-phase research workflow; see
`docs/tech/2026-06-23-knife7-reality-handshake-*`.

## Why 0x1301 only

- **It is the only suite RFC 8448 §3 traces byte-for-byte**, so it is the only one we can prove offline
  with a known-answer test (the whole point of 刀7 being sans-IO). 0x1302 has no RFC 8448 vector (it
  would only get a weaker self-consistency/round-trip test); 0x1303 also needs a new `chacha20poly1305`
  crate dependency (absent — Stage 13d retired chacha once).
- It is the first real TLS 1.3 suite our hand-rolled ClientHello offers, and the common sing-box/REALITY
  default.

## The gap (operators and 刀8 MUST know)

**The record-layer cipher is chosen by the borrowed decoy/handshake site, NOT by sing-box, and the site is
not obliged to honour our ClientHello's cipher order.** Large AES-NI sites commonly select
**0x1302 (AES-256-GCM-SHA384)**. Until the 0x1302 path is wired, **a live REALITY server fronting such a
site will fail the handshake** — and could fail *silently* if not guarded. Mitigations:

- `parse_server_hello` **explicitly errors** on an unsupported `cipher_suite` (≠0x1301), so a 0x1302/0x1303
  decoy fails **loudly and diagnosably** (pointing here) instead of being silently mis-keyed.
- Adding 0x1302/0x1303 later means: introduce the deferred `CipherSuite`-generic schedule/record (the SHA-384
  second-hash path + a `chacha20poly1305` dep for 0x1303), then relax the parse-time cipher check. It is more
  than pure wiring (the genericity does not exist yet), but it is well-scoped and the RFC 8448-pinned 0x1301
  path is a working reference.

## Considered Options

- **0x1301 only + generic skeleton + documented gap (chosen).** Smallest correct, fully offline-KAT'd
  slice; cheap to extend. 4 of 5 research reports favoured this.
- **0x1301 + 0x1302 together now.** Covers big AES-NI decoys immediately, but 0x1302 has no RFC 8448
  vector (weaker test), adds the SHA-384 second-hash work, and enlarges 刀7. Rejected for 刀7; revisit if
  the chosen decoy site forces 0x1302.
- **All three now.** Adds a chacha dependency speculatively; rejected (YAGNI until a target server needs it).

## Consequences

- **`echo-match ≠ REALITY auth` invariant** (recorded here because 刀7 establishes the ServerHello check):
  the ServerHello `legacy_session_id_echo` matching our sealed session_id is an RFC consistency check
  **only**. On REALITY auth *failure* the decoy site still echoes our session_id, so treating echo-match
  as auth-success would wrongly proceed to VLESS against the decoy. The real auth decision is the
  temp-cert HMAC-SHA512 (刀6 `verify_server_cert`), made in 刀8. 刀7 comments/spec state this; 刀8 must
  not regress it.
- Unaffected: REALITY auth (ADR-0008) uses **AES-256-GCM** for the session_id seal — a *separate* AEAD
  from this record-layer AES-128-GCM. Do not conflate the two.

## Amendment (刀8, 2026-06-23): tighten the ClientHello cipher offer to 0x1301 only

刀8 (live handshake) resolves the gap above differently than originally anticipated. The original assumption was
"keep offering Chrome's three TLS 1.3 suites (0x1301/0x1302/0x1303) and **loud-fail** if a server-pref AES-256
decoy selects 0x1302." 刀8 grill instead **removes 0x1302/0x1303 from the ClientHello offer**
(`client_hello.rs` `CIPHERS`), so the only TLS 1.3 suite offered is **0x1301**. Per RFC 8446 §9.1 a compliant
decoy must then select 0x1301 (the only intersection) — this **roots out the loud-fail at the source**, making any
compliant borrowed site usable regardless of its AES-256 preference. Belt-and-suspenders: the acceptance helper
also runs an **openssl egress preflight** against the chosen decoy and refuses to start if it does not negotiate
`TLS_AES_128_GCM_SHA256`, and the operator still picks a known-0x1301 decoy (e.g. `gateway.icloud.com`,
`dl.google.com`).

- **Trade-off (the real cost):** offering a single TLS 1.3 suite deviates from a real Chrome's three-suite offer —
  a JA3/JA4 cipher-list hash no longer matches Chrome, and "one TLS 1.3 suite + a pile of TLS 1.2 suites" is a
  combination no real browser produces. This is a deliberate **robustness-over-fingerprint** choice (系统稳定优先),
  consistent with ADR-0008's stance that REALITY stealth is **best-effort, not byte-exact**. Offering suites we
  cannot actually complete (the schedule/record are hard-coded SHA-256/AES-128) was itself a fingerprint
  inconsistency — a real Chrome *completes* a 0x1302 handshake.
- **Reversal path:** when 0x1302/0x1303 are eventually implemented (the deferred `CipherSuite`-generic
  schedule/record + a `chacha20poly1305` dep), restore the full three-suite offer — the loud-fail disappears
  on its own and Chrome's exact cipher list is recovered. Decided in 刀8 grill; see
  `docs/tech/2026-06-23-knife8-reality-live-handshake-spec.md` §2(f).
