# Knife14bm TUN Egress Recent Pressure Plan

Date: 2026-07-05

## Design Tree

1. TUN drop feedback missed the drop because no local pressure existed.
   - Rejected: Knife14bl had repeated high-watermark pressure immediately
     before the sampled TUN drop.
2. TUN drop feedback missed the drop because sampling is 1s and pressure drained
   before the sample.
   - Accepted as the next scoped branch.
3. Increase sampling frequency.
   - Rejected for this stage: hotter sysfs polling adds operational cost and
     does not fix attribution semantics.
4. Add recent high-pressure attribution.
   - Accepted: small local state, deterministic testable behavior, no transport
     or product default tuning.

## Tasks

1. Add a unit test proving a drop sampled after pressure drains still pauses
   when high pressure was observed since the previous sample.
2. Add recent high-pressure latch fields to `TunEgressFeedbackState`.
3. Observe pressure once per event-loop iteration using existing
   `downlink_pressure_stats`.
4. Clear the latch at each feedback sample so attribution is bounded.
5. Run focused TUN egress/downlink tests, script self-tests, lib tests, clippy,
   and diff checks.
6. Update learning memory, commit, push, then run the scoped VPS reverse-first
   P1 acceptance.
