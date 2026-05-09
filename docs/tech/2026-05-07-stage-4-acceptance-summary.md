# Stage 4 Acceptance Summary

## Milestone

Stage 4 completed the activation of the real TUN-side listener pool.

This means the project moved from:

- a pool-friendly skeleton

to:

- a real runtime listener pool with `pool_size = 4`

## What Was Accepted

### Runtime Capability

- the TUN path now creates a real 4-slot listener pool
- each `SocketHandle` owns an independent `SocketCtx`
- per-handle rearm behavior is isolated
- the TUN path continues to reuse the shared relay protocol

### Code Quality

- key runtime structures and helpers now include richer comments
- comments follow an English-led style with Chinese key-point reinforcement
- per-handle state and transition logs were added for observability

### Validation

The following static checks passed:

```bash
cargo test
cargo check
cargo clippy --all-targets --all-features -- -D warnings
cargo doc --no-deps
```

### Local Runtime Validation

Local debugging reached two concrete conclusions:

- runtime startup and binary execution paths were valid
- full local end-to-end validation succeeded on a machine with TUN permissions

That confirms Stage 4 is not only structurally complete, but also operationally verified.

## Key Deliverables

- real listener pool allocation in `src/client_tun.rs`
- per-handle lifecycle isolation in `src/client_tun.rs`
- Stage 4 spec
- Stage 4 implementation plan
- Stage 4 teaching note

## Commit Trail

- `41655da` `docs(tun): add stage 4 listener pool activation spec`
- `d885ebf` `docs(tun): add stage 4 implementation plan`
- `e940705` `refactor(tun): add real listener pool construction`
- `e725d31` `refactor(tun): isolate per-handle lifecycle state`
- `be08ced` `docs(tun): add stage 4 listener pool teaching note`

## Known Remaining Gaps After Stage 4

Stage 4 intentionally did not solve:

- configuration for TUN runtime inputs
- UDP-over-TUN pool work
- complete removal of hot-path `unwrap()`
- unified config across `client-direct` and `client-tun`

These remaining items are now good candidates for Stage 5 and later milestones.

## Acceptance Conclusion

Stage 4 is accepted.

It established a stable, testable listener-pool runtime for the TCP-over-TUN path and created a solid base for Stage 5 minimal configuration work.
