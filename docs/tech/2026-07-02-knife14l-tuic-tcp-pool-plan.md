# Knife14l plan - opt-in TUIC TCP connection pool

Date: 2026-07-02

## Task 1 - Parse and expose the pool size

Red:

- Add tests for pool default, invalid/zero handling, and maximum clamp.

Green:

- Add `tcp_pool` to `TuicClientConfig`.
- Parse `MINI_VPN_TUIC_TCP_POOL` in `from_env`.
- Keep `from_sources` unchanged externally and default the field to `1`.

Commit target: include with Task 2 if the implementation stays small.

## Task 2 - Round-robin TCP opens

Red:

- Add a pure selection test: pool length `3` maps cursors `0,1,2,3` to `0,1,2,0`.

Green:

- Replace the single `conn: Mutex<Connection>` with `conns: Vec<Mutex<Connection>>`.
- Add an atomic TCP cursor.
- Make `open_tcp` pick a pooled connection and then run the existing `open_bi + Connect` path.
- Keep `live_conn()` as the primary-connection helper for UDP and health.

## Task 3 - Verification

- `cargo fmt`
- `cargo test --lib tuic`
- `cargo test --lib`
- `git diff --check`

## Stage Code Review Checklist

- Correctness: default behavior is unchanged when `MINI_VPN_TUIC_TCP_POOL` is unset.
- Correctness: pool size cannot be zero, avoiding modulo-by-zero and empty connection vectors.
- Concurrency: each connection has its own reconnect mutex; round-robin cursor is atomic and relaxed.
- Performance: TCP open hot path adds only one atomic increment and one vector index.
- Maintainability: UDP and health remain clearly tied to the primary connection.

## Next Test Checklist

After commit, the live test should set `MINI_VPN_TUIC_TCP_POOL=4` for the experiment and keep all other suite variables
the same as `knife14k`.
