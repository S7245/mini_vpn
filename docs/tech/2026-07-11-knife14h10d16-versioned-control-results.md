# Knife14 H10d16 Versioned Control Results

Date: 2026-07-11
Source: `044eccbdf7de7609eef43a5682d02ceb842f086c`

## Verdict

The post-R1-R4 mature-client capability precondition failed. Gate A and Gate B
were not run. The result continues to identify a shared external TUIC service
window, not a mini_vpn D16 architecture regression.

## Fixed control shape

The clean versioned control used:

- sing-box `1.13.14`;
- MTU `1500`;
- one reverse iperf flow for `20s`;
- target-only TUN routing to `.77`;
- the Exit `.33` excluded from the TUN route;
- a mode-0600 FIFO for the transient configuration;
- client and Exit UDP socket buffer/drop capture.

No credential or persistent client configuration was written.

## Results

Direct reverse baselines were healthy:

- `.27 -> .77`: `216.172 Mbit/s` receiver;
- `.33 -> .77`: `213.865 Mbit/s` receiver.

The mature TUIC control delivered:

- sender: `13.472 Mbit/s`;
- receiver: `11.219 Mbit/s`.

Both client and Exit UDP sockets reported receive/send buffers of `16777216`
bytes and drop `0`. The target route used the control TUN while the Exit route
remained on `eth0`.

The receiver interval shape was burst/idle: isolated bursts reached about
`83`, `47`, `42`, and `30 Mbit/s`, but most one-second intervals were zero.
Exit logs showed normal TUIC acceptance and direct opens to the iperf target.
The final remote stream cancellation occurred at test completion.

Cleanup removed the control process, FIFO, and TUN, restored the original `.27`
socket sysctls, and returned both target and Exit routes to `eth0`.

Remote artifact:

`/tmp/mini_vpn_h10d16_control_044eccb/`

## Code review

No P0/P1 correctness, security, performance, ownership, lifecycle, or
cross-platform-core defect was found in the R1-R4 changes. In particular:

- the exact safe1200 real-Quinn capacity gate remains valid;
- source and runner identity were clean and hashed;
- the control exercised the intended historical shape;
- routing and socket gates excluded the known local false-attribution paths;
- D16 production code was not involved in the failed mature-client flow.

Therefore no mini_vpn code or architecture change is justified by this run.

## Next action

Do not repeat the same control in this window. After an independent external
TUIC-window change, run one versioned control again. Only receiver `>150 Mbit/s`
with both socket drops at zero authorizes one 20-second safe1200 Gate A. Gate B
remains conditional on that Gate A passing throughput and all drop/tail gates.
