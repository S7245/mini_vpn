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

Note: `67c466b add` committed the worktree directory itself as a gitlink
(mode 160000 `.claude/worktrees/frosty-gates-10390f`). Recursive/self-referential;
worth cleaning up separately.
