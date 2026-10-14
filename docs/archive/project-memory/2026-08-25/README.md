# Project-memory snapshot: 2026-08-25

These files are the exact pre-split bytes from the project root. The migration
did not rewrite their content.

| Original | Snapshot | Bytes | SHA-256 |
|---|---|---:|---|
| `AGENTS.md` | `AGENTS.full.md` | 191559 | `42324707158538d7d90a67ff94c91936c19793f938a8f5b2ab41ca95c3e1abd7` |
| `HANDOFF.md` | `HANDOFF.full.md` | 288618 | `6e5049103d44d6b422a107f015552c369dbde35a5642ed145b6812505d05ebc9` |
| `TODO.md` | `TODO.full.md` | 234850 | `66622b1a2f5c8a9f66f6ee3393ae65801804313f9d7df5223a84782cab1c5ee2` |

## Migration acceptance

- The old five cold-start files totaled `1,718,508B`; the compact five total
  `24,534B`, a `98.5724%` byte reduction.
- Including the new canonical `docs/current/STATUS.md`, the current route totals
  `31,604B`, a `98.1610%` reduction from the old five-file route.
- All 20 compact-document references exist; five archive hashes match; no nested
  `AGENTS.md`, private-key marker, trailing whitespace, or extra EOF blank exists;
  and `git diff --check` passes.
- Independent review closed one narrowed R7 acceptance description and finished
  with unresolved P0/P1/P2 `0/0/0`.
- No exact token count is claimed because tokenizer choice is unspecified. No
  production Rust, macOS TUN, VPS, WAN, throughput, or M2 test changed or ran.

This snapshot includes the dirty-worktree state that existed immediately before
the split. Do not edit it in place; create a later dated snapshot if provenance
must advance.
