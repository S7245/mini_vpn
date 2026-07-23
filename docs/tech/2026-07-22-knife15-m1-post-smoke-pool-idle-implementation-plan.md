# Knife15 M1 post-smoke TCP-pool idle barrier implementation plan

Date: 2026-07-22

1. Add a focused Rust RED for transition-only TCP-pool activity publication.
2. Add shell RED fixtures proving missing, malformed, and nonzero activity
   evidence fail while a latest exact zero passes.
3. Publish `active_leases` transitions from the existing Endpoint monitor.
4. Add a bounded runner wait after smoke and independent formal M0/M1 idle
   prerequisites.
5. Run focused Rust and runner tests, then root, vendored Quinn/proto, release,
   Clippy, shell, format/diff, capacity, and secret gates.
6. Review lifecycle races, log parsing, timeout/error preservation, old paths,
   and frozen-value drift. Repair every P0/P1 before another HK run.
7. Record the result in project memory and learning files, commit one coherent
   change, push, and request one fresh HK M1 only after every local gate passes.
