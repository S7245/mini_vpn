# Learnings

## 2026-06-01 — Full jitter is observable and correct in the reconnect logs

Stage 10 cross-machine test (kill/restart US server) showed reconnect delays of
409/440/1554/354/5966ms — non-monotonic. That non-monotonicity is the proof that
full jitter is working: plain exponential backoff would be strictly increasing
(500/1000/2000...). The randomness is what spreads 5000 clients' reconnect
moments and prevents a thundering herd. When validating backoff, check that the
delays are NOT monotonic — a monotonic sequence means jitter is broken.

Also confirmed: infinite retry (4 failures then success) + reset-on-success
(second disconnect cycle restarts the attempt counter) + epoch increments per
reconnect (1→2→3). No panic when reconnecting while idle (0 in-flight to reset).

## 2026-06-01 — Byte-level transparent TCP relay preserves end-to-end TLS

Stage 9 cross-machine test against `https://1.1.1.1/` produced a full TLS 1.3
handshake where curl saw Cloudflare's real leaf certificate (`CN=cloudflare-dns.com`,
SSL.com intermediate) — not our dev cert. Confirms that the smoltcp + yamux + TLS
pipeline only carries opaque bytes; the inner TLS session is established between
the original client (curl) and the real target (Cloudflare), with our tunnel
acting as a pure pipe. HTTP/2 multiplexing also works end-to-end. This is the
right property for a VPN: the tunnel must NOT terminate the user's TLS or it
would break SNI / cert pinning / E2E privacy.

## 2026-05-27 — Verify worktree baseline before any design/implementation

The session worktree `claude/frosty-gates-10390f` was 39 commits behind `main`
(client_tun.rs was 133 lines vs main's 867). All early code reading was against
`main`'s files via absolute paths, so the design discussion nearly proceeded on a
stale base. Fixed with `git merge --ff-only main` (worktree HEAD was an ancestor of
main, 0 unique commits, no loss).

- Before designing/implementing in a worktree, check `git -C <worktree> log` and
  `git rev-list --count HEAD..main`. Don't assume the worktree == latest.

## 2026-05-27 — Run git in the worktree, not the main checkout

`cd /…/mini_vpn && git status` runs in the MAIN repo; the session's working tree is
the worktree under `.claude/worktrees/…`. New files written to the worktree won't show
in a `git status` run from main. Use `git -C <worktree-path>` or stay in the default
cwd (the worktree) — avoid `cd`-ing to the main checkout for git ops.

## 2026-05-29 — Always check the binary's startup banner before debugging behavior

A cross-machine test failed mysteriously: many `📡 收到` SYN logs, zero
`🎯 extracted target`, server kept reporting yamux disconnects. Root cause was
NOT code or topology — it was a stale binary. Stage 8 changes lived only on the
worktree branch, but the user was running `./target/debug/mini_vpn` built from
the main checkout (still at pre-Stage 8). The diagnostic tell was the startup
banner: it contained `target=httpbin.org:80` (a field Task 3 had removed),
proving the running binary predated Task 3.

- After ANY code change, first check the startup banner / version line matches
  the expected build. Don't debug behavior against a binary you didn't just compile.
- When work lives on a feature branch / worktree, prefer running the binary from
  that worktree's `target/debug/` until the branch is merged, or merge first.

## 2026-05-27 — Same-machine upstream + a global TUN host route = egress loop

Stage 8 round-trip test failed with the upstream `server` on localhost: the
`route add -host <target> -interface <our-utun>` is machine-global, so the
server's own `connect(<target>)` egress is ALSO diverted into our TUN instead of
reaching the real internet -> `Connection refused` / loop. Target extraction
itself worked (client logged `🎯 extracted target 1.1.1.1:80`, server logged the
correct target); only the outbound hop broke.

- The full byte round-trip can only be validated with the upstream on a
  DIFFERENT host (the real VPN topology — e.g. the US server). On a single
  machine the route inevitably captures the server's egress to the target.
- Always use an IP literal as the test target on dev machines: a co-resident
  fake-ip TUN proxy (Clash/Mihomo) hijacks DNS to 198.18.0.0/15.

Note: `67c466b add` committed the worktree directory itself as a gitlink
(mode 160000 `.claude/worktrees/frosty-gates-10390f`). Recursive/self-referential;
worth cleaning up separately.
