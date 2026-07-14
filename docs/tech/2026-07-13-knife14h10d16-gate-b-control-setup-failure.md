# Knife14 H10d16 Gate B Control Setup Failure

> Historical incident note: after this stop, the user authorized direct
> correction of evidence-backed configuration errors. The corrected `h3`
> service produced a valid same-window control, and Task 12 steps 1-3 passed.
> Current result:
> `2026-07-13-knife14h10d16-gate-b-results.md`.

Date: 2026-07-13 (VPS local/UTC date)
Source: `a54fb171ad57dc48902a79f9d31bd08d2ac41802`

## Verdict

Task 12 / Gate B did not run. The one attempted same-window Gate-aligned
sing-box control was invalid before throughput measurement because the
reconstructed Shoes server configuration omitted an explicit QUIC ALPN list.
The sing-box client offered `h3`, while the Shoes server had no configured
common protocol and rejected the cryptographic handshake.

No mini_vpn reverse-first P1 ran, no median was computed, and no fixed `64 MiB`
A-clean repeat ran. Gate B remains unlocked by the accepted composite Gate A,
but unspent.

## Frozen Source And Binaries

- clean source commit: `a54fb17`;
- mini_vpn release SHA-256:
  `34191751ff80f1408476f874fb66e74757384c061616380a26b54f0659ff5dff`;
- tunnel-suite SHA-256:
  `2091073867a67ba268e2be545d44d963f59ea01766ac693efd1eeb040903c806`;
- low-RTT runner SHA-256:
  `c4038c38ab8b1ee52d710d02ce2e6c9125b020247cee5faf10d7018489e9dfb0`;
- Gate-aligned control runner SHA-256:
  `ad6a78d334cda754dc9709350718f028365db03e99b87154577ff0a54705fd1d`;
- sing-box `1.13.14` musl binary SHA-256:
  `4ea794fddcb2ad84532adeab979a9b0d7b2052822bb3439dfb321c33c941da19`;
- Shoes `v0.2.7` binary SHA-256:
  `160202f6744b14ba84b40c014f3754f7811642300df58df81f1399063a202147`.

All three runner self-tests passed and the clean release build reproduced the
same mini_vpn binary hash as the accepted Gate A build. No product source or
runtime knob changed.

## Pre-Control Evidence

- `.27 -> .77` direct reverse: `219.493/214.494 Mbit/s` sender/receiver;
- control TUN MTU: exactly `1200`;
- target route: `.77/32` through the control TUN;
- Exit route: `.111` remained on `eth0`;
- client UDP socket: `rb=16777216`, `tb=16777216`, drop `0`;
- Shoes UDP sockets: `rb=17250000`, `tb=17250000`, drop `0`;
- `.111` UDP error/buffer-drop counters: `0`;
- Shoes was the exact one-endpoint, two-worker `v0.2.7` binary and logged
  `Loaded 2 certs/keys`, `Starting 1 server`, and a listener on UDP `8443`.

These checks rule out target capacity, route recursion, MTU, socket-buffer
capacity, and service liveness as the reason the control lacked a result.

## Exact Failure

The sing-box client opened the target flow and then reported:

```text
open connection: CRYPTO_ERROR 0x178 (remote):
peer doesn't support any known protocol
```

Shoes reported the matching server-side failure:

```text
the cryptographic handshake failed: error 120:
peer doesn't support any known protocol
```

The iperf JSON contained zero intervals and no sender/receiver aggregate. The
control runner therefore exited `FAILED`; it did not emit `INCAPABLE`, `PASS`,
or a receiver result.

The reconstructed ephemeral Shoes config contained the correct TUIC UUID,
password, certificate, key, port, and direct rule but omitted
`quic_settings.alpn_protocols: ["h3"]`. Shoes passes this configured list
directly into rustls. The accepted Gate A client uses `h3`, and the versioned
Gate-aligned control also fixes `h3`; the missing server ALPN fully predicts
the paired client/server errors before any TUIC or iperf data flow.

## Stop Decision

The strict Task 12 sequence stops here:

1. do not reinterpret this as a low control;
2. do not run any of the three mini_vpn repeats;
3. do not compute a median or use the relative-control fallback;
4. do not run the fixed-byte clean proof;
5. do not change D16, MTU, pool, QUIC windows, chunk size, or self-wake.

Before another attempt, require user confirmation because the requested single
control was consumed by an invalid setup. The correction is operational only:
render the same FIFO-only Shoes config with explicit `h3`, assert that field in
memory before writing the FIFO, verify the one-server/listener fingerprint,
then run exactly one Gate-aligned control. Continue to the three mini_vpn
repeats only if that control produces a valid receiver and zero socket drops.

## Artifacts And Cleanup

Local retained evidence:

```text
/tmp/mini_vpn_h10d16_gateb_control_failure_a54fb17/
mini_vpn_gateb_control_a54fb17.tar.gz
sha256=2a8466ddece28f3190bc14c27d4fa476798bb383c8cff976f6ecad23e2c06989
mini_vpn_gateb_shoes.log
sha256=b1c20f8e4b5ee4da12b61ab8baedb4b89212e5dceb58992b8cc1d6926f2e44fd
mini_vpn_gateb_target.log
sha256=b409eba500021a9d657463e0e90695cf08ef13141e636730b11174aa7f4b70aa
```

The bundle scan found zero environment files, private-key markers, UUID
patterns, or unredacted password assignments. `.27` control TUN/routes and
temporary client sysctls were restored. `.111` Shoes, FIFOs, binaries,
watchdog, and temporary files were removed; UDP `8443` is free and all four
socket sysctls are back at `212992`. `.77` iperf3 remains active. No macOS TUN
test ran.
