# REALITY TLS 1.3: implement TLS_AES_128_GCM_SHA256 (0x1301) first, with a generic-over-hash skeleton

The hand-rolled TLS 1.3 client for REALITY (ADR-0008) implements and **known-answer-tests only
`TLS_AES_128_GCM_SHA256` (0x1301)** in 刀7. The key schedule is written **generic over the hash**
and the record layer **generic over the AEAD** from day one (a `CipherSuite` enum carrying
`hash_len`/`key_len`/`iv_len`), but only the 0x1301 path is wired and verified. `TLS_AES_256_GCM_SHA384`
(0x1302) and `TLS_CHACHA20_POLY1305_SHA256` (0x1303) are **deferred** — a known gap to be filled in a
刀7-tail task or 刀8.

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

- The generic-over-hash schedule + generic-over-AEAD record make adding 0x1302/0x1303 cheap (no
  restructuring) — it is wiring + a self-consistency test, not a redesign.
- 刀8's ServerHello handling must **explicitly error** on an unsupported `cipher_suite` (not silently
  proceed), so the failure is diagnosable and points here.

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
