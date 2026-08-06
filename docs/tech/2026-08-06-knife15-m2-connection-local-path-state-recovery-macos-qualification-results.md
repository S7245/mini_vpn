# Knife15 M2 Connection-Local Path-State Recovery macOS Qualification Results

Date: 2026-08-06

Status: **PASS_NON_ACCEPTANCE; FORMAL M2 MAY RUN; FORMAL M2 NOT RUN**

Source: `0c521fa790ba3f1e01e3f376a0feebcbb04cd138`

Implementation: `0e94e56`

## Artifact And Provenance

The immutable real-Mac artifact is:

```text
2a45314d6f4d6a5cb1c4378d66a93a36e07b6f572b8ed2ba91bb3bfe9854a5d6  /tmp/mini_vpn_knife15_macos_20260806_101142.tar.gz
```

Its source, release binary, and runner are bound as:

```text
source_commit=0c521fa790ba3f1e01e3f376a0feebcbb04cd138
binary_sha256=23cb2d9a0a93ca93a999f13d5fa2c80a2b531d978c0d592393b2fc299b948c93
runner_sha256=516ee92772e4051b2de9c7b1a353344cef8808ddff060238f5582ba174439daa
```

The baseline and direct directories are:

```text
/tmp/mini_vpn_knife15_macos_baseline_20260806_095612
/tmp/mini_vpn_knife15_macos_direct_20260806_095708
```

The baseline passed at `34.964/8.262 Mbit/s` forward/reverse with zero
receiver gaps. The exact 300-second direct discriminator passed at
`13.539 Mbit/s`, `508,035,072B`, and zero sender/receiver intervals.

## Qualification Result

All exact phase and system gates passed:

- TCP forward: `655,622,144B`, `17.473 Mbit/s` receiver, zero retransmits,
  zero sender/receiver intervals;
- TCP reverse: `154,914,816B`, `4.131 Mbit/s` receiver, zero retransmits,
  zero sender/receiver intervals;
- UDP reverse: `92,743,160B`, `4.122 Mbit/s`, `0.223184%` loss, below the
  frozen `3%` SLI;
- short TCP forward: `23,986,176B`, `18.868 Mbit/s` receiver, four
  retransmits, zero sender/receiver intervals;
- maximum TCP sender/receiver gap: `7,208,960B`;
- DNS and both real-client checks passed;
- result integrity found zero invalid files.

The final verdict is exactly:

```text
qualification_slo_evidence=PASS
formal_m2_acceptance=NOT_RUN
PASS_NON_ACCEPTANCE
```

## Recovery Discriminator

No `tuic-connection-path-reset` action occurred. That is the correct outcome
for this healthy comparator, not missing observability:

- conn0 accumulated `12` PLPMTUD black-hole detections but its exact
  qualification writer wait was at most `274,915us`, below the unchanged
  minimum `2s` recovery bound;
- conn1 accumulated zero black-hole detections;
- the earlier selected failure had same-connection writer Pending of
  `7,001,335us` plus `144` black-hole detections.

Thus the real WAN run exercised the false-positive boundary: black-hole
events without bounded exact writer pressure did not reset a healthy
connection. The local same-identity/same-stream Quinn test remains the
mechanism proof. Formal M2 is the next opportunity to validate recovery
liveness if the original degradation recurs naturally.

Busy-epoch qualification independently marked conn0 degraded for later new
opens after its black-hole counter advanced, while the incumbent forward
stream completed without a receiver gap. Existing isolation and the new
established-stream recovery therefore remained complementary.

## Close Tail And Internal Scan

The summary recorded two `Stopped(0)` local-to-remote write failures and left
`internal_failure_scan=REVIEW`. Both occurred only after the corresponding
iperf peer had ended its timed transfer. Each relay closed with D16
`queued/leased/reserved=0/0/0B`, no permit drop, and no terminal pending debt.
The associated smoke/short results passed their unchanged data-quality gates.
These are expected close-tail races, not active-transfer or ownership
failures; the summary correctly requires review instead of silently claiming
an unconditional internal PASS.

## Conservation, Controls, And Cleanup

- Endpoint conservation maximum: `61,440B`;
- final available/live/outstanding: `61,403/0/0B`;
- maximum live/outstanding: `1,409/1,280B`;
- TUN interface errors: zero;
- gateway loss: zero and physical interface errors: zero;
- final process state: dead;
- IPv6 preflight: `safe_absent`;
- full-tunnel split routes, Exit host route, DNS, utun, and process ownership
  were restored; cleanup evidence is `PASS`;
- secret scan passed.

## Decision

The connection-local recovery architecture is retained and the short
qualification gate is complete. This artifact is not formal M2 acceptance,
but it authorizes exactly one fresh formal M2 run from the pushed descendant
with the same frozen workload and values.

If formal M2 completes plus cleanup, Knife15 M2 is accepted and M3 may open.
If an applied path reset is followed by a complete healthy-control Target
receiver-zero interval, reject this architecture and stop without tuning or
unchanged repetition. If M2 fails for another reason, preserve
`status/snapshot/stop` evidence and classify that exact invariant first.
