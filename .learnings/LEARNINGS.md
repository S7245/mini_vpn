# Learnings

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
