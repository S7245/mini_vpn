# Knife15 M2 Successor Service Turn Qualification Failure Results

Date: 2026-08-07

Status: **FAILURE CLASSIFIED; BUSINESS-OPEN FALLBACK SELECTED; FORMAL M2 AND M3 REMAIN BLOCKED**

Mac artifact:
`/tmp/mini_vpn_knife15_macos_20260808_030357.tar.gz`.

Mac SHA-256:
`e5c866f6c0bc14929938a86de9da5c0157acabfbee5ef10958bd3c83ac05f649`.

Exact source: `b04cb6515e48357f1fd5be6fa9a2ad48a7ff0555`.

Paired Exit artifact:
`/tmp/mini_vpn_knife15_exit_target_observer_20260808_021631.tar.gz`.

Exit SHA-256:
`21b08483c2823b9ea9f63e064d1415a8870006e9f9ef2c1911e871656761deea`.

## Accepted Controls

- direct baseline: `27.973/63.814 Mbit/s`;
- 300-second direct forward: `13.970 Mbit/s`, `524,156,928B`, no complete
  receiver-zero interval;
- smoke: `19.810/46.608 Mbit/s`;
- qualification forward: exact `524,550,144B`, about `13.980 Mbit/s`, 300
  positive receiver intervals and zero retransmits;
- IPv6, full-tunnel, real-client, routes, TUN, process, interface, Endpoint,
  and cleanup evidence passed.

Endpoint maximum/final ownership was `61,440B / 61,407/0/0B`. There were no
Endpoint rebinds and no interface errors. The test Mac completed
`status/snapshot/stop`; route, DNS, TUN, and process cleanup passed.

## Exact Failure

Cycle 1 reverse began at `03:10:01Z`. Its iperf JSON never recorded a connected
socket, interval, or byte. The runner killed the child at its unchanged
330-second hard timeout and classified the phase as failed.

At the business open, conn1 generation 1 had advanced its qualification
black-hole count from `0` to `5`. Auxiliary replacement correctly retained the
predecessor and ran the new successor service turn. The turn failed exactly:

```text
target_bytes=12,000
sent_bytes=13,058
acked_bytes=10,326
lost_bytes=1,366
reason=PacketLost
```

This is the intended fail-closed service-turn result. The successor was not
installed and no draining predecessor was created. The defect occurred one
layer higher: `acquire_tcp_pool_reservation` returned the maintenance error to
the triggering business open. The local smoltcp handle recorded
`remote_open reason=handshake_failed -> rearm`, while iperf received neither a
usable connection nor prompt terminal completion.

Later, an independent ambient open received fresh replacement authority. Its
turn passed at `12,800/12,800/0B`, and conn1 generation 2 installed in `490ms`.
This rejects a sustained client-to-Exit outage and proves that the first
business failure was caused by maintenance/admission coupling, not by the
service-turn loss rule.

## Paired Exit Evidence

The bounded observer captured `547,358` packets with zero kernel drops. Target
TCP service remained around the established low-millisecond comparator. No
fresh Target socket appeared after the passing forward completed and the
reverse phase began. This is consistent with the exact client evidence: the
failed reverse open never crossed the TUIC replacement boundary to become an
Exit-to-Target connection.

The observer was explicitly stopped and bundled. It is no longer active.

## Selected Architecture

Keep successor readiness exactly fail-closed, but isolate a failed maintenance
attempt from the triggering business admission:

1. reject successor installation and preserve the predecessor;
2. exclude the failed slot for this business open only;
3. reserve another already-qualified current generation;
4. do not attempt any second replacement for the same open;
5. fail with the exact maintenance and admission errors if no qualified
   current generation exists;
6. let a later independent open begin with fresh replacement authority.

This is not a retry of Target traffic or payload. No Target socket existed and
no business bytes were sent. It is a bounded admission fallback before the
TUIC Connect stream is created.

Do not tune the service flight, timeout, pool, MTU, D16, Endpoint, QUIC
windows, chunk, Cubic, GSO, recovery bounds, workload, or SLO. Formal M2 and M3
remain blocked pending one fresh qualification of the repaired source plus
complete cleanup.
