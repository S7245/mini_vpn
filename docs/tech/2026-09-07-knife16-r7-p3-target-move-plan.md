# P3 — preparatory MemoryTarget Implementation Move

Date: 2026-09-07.

Status: **PURE MOVE AND DOCUMENT CLOSURE ACCEPTED; NO READINESS/BINDING BEHAVIOR**.

## Scope and sequencing

This is the preparatory Move required by the full-P3 plan's oversized-Target
gate, not implementation of the accepted
[readiness contract](2026-09-07-knife16-r7-p3-target-readiness-contract.md).
It may precede the coupled binding/selector/guard/K/Layout behavior freeze:
the contract's before-Rust gate continues to govern all new behavior and
Interface changes. This explicit green-to-green exception changes no readiness
or capacity mechanism and cannot be used to begin a partial binding.

User authority is full P3 with causal in-scope repairs, governance and same
independent review. Main alone writes the two overlapping Rust files. Mencius
alone promotes the exact shrink inventory. No commit/push, public routing,
external run, P4+, new trait method, test Adapter repair or unrelated cleanup.
Project repair/custody rules override the skill's generic auto-revert/commit
recipe; do not erase existing dirty changes.

## Exact movement and retained Authority

Pre-Move baseline [target.rs](../../src/owned_upstream/target.rs):2017 lines, hash
`93364ebe68a4833f35f10fbd7d1d3c50e78e2617822ef19e617c98a556f7d6c4`.
Move exactly the full `impl TargetIo for MemoryTarget`, lines939–1511,
573 lines/22495 bytes including final newline; SHA-256:
`1b8ae959c7e1cf96883a9b3f0448e9f9ab6c0c98da9d4123921f673d1d6d4780`.
No byte inside that block changes. There are no relative-path or position
macros, self/super path adjustments, cfg edits or new signatures.

Allowed Rust paths/symbols:

- `src/owned_upstream/target.rs`: remove that block and its one following
  separator; add only private unconditional default-path `mod memory_io;`
  plus separator after the imports. Predicted exact count1445.
- `src/owned_upstream/target/memory_io.rs`: the unchanged block after six
  compact module-doc lines, separator, `use super::*;`, separator. Exact582
  lines; the new production file limit is1500, not the test-leaf limit800.

TargetIo, all value types, MemoryTarget/storage/config/constructors/snapshot,
feed/drain, terminalize, private numeric helpers, Debug and the entire inline
test Module remain byte-identical in the parent. Child reads ancestor-private
state; existing trait callers see exactly the same MemoryTarget implementation.
No re-export, pub(super), pass-through, extracted object, callback, parameter,
allocation or second lifecycle owner is added. Reset/cancel still synchronously
invoke the same private terminalize on the same Self; shared storage is not
split between independently owned authorities.

Deleting the child removes the real I/O/terminal Implementation rather than a
wrapper. Its compact docs name what remains outside. The later new readiness
lifecycle must cover parent feed/drain/helper mutations too; this Move does
not prove registration, global release rearm, Owner binding or bounded scans.

## Falsifiable checks

1. Correct route/privacy: default child resolves, ancestor-private names remain
   available and all existing trait callers compile with unchanged signatures.
   Any need for visibility/caller edits disproves this Move and stops it.
2. Exact movement: child body hash equals the573-line entry block; reversing
   just the removal/declaration yields the exact original parent. Test bodies,
   helpers, trait Interface and other336-entry Rust fingerprints remain intact
   except the parent and separately promoted governance file.
3. Behavior: all nine original Target tests keep their full identities and
   results, including directive WouldBlock, EOF/half-close ordering, exact
   reset/cancel, invariant rejection, live-slot reuse and terminal pressure.
4. Owning callers: Owner/Controller/continuation characterizations stay green;
   no snapshot assertion is weakened or changed to force the Move through.
5. Growth: parent1445 is below1500; remove its production grandfather entry,
   do not relabel it1445. Preserve its existing one too_many_arguments entry
   and inline-test allowlist. New child gets no debt entry.

## Gates and stop rule

Before writes: standalone governance6/6; inventory current library test names;
run Target9, Owner74, Controller44 and continuation16, each single-thread.
Any inventory discrepancy is audited before trusting counts; zero tests is not
green. Existing tests supply the characterization; no artificial behavior RED.

After the one Move: repeat those four sets, then existing root56/mailbox18/
TCP68/client-progress3; fmt and tracked/untracked whitespace/inverse/scope audit.
At the coherent shrink boundary Writer reports exact counts and stops Rust
changes. Integrator removes only `("src/owned_upstream/target.rs", 2_017)`
from the production inventory and runs standalone/Cargo all-features
governance6/6. Continue with Knife16/repository provenance162/13 and all-feature
cargo check. All commands are local/offline; known separate actor ordered-wait
fixture debt is not included or silently repaired.

The same independent Reviewer checks the whole pure-Move inverse, Authority,
scope, gates, exact inventory and regressions, then compact document closure.
No behavior starts on plan approval. An unexpected red is first diagnosed;
only a proven mechanical in-scope correction may proceed under the user's
repair exception. Interface/Authority/growth/caller expansion is a hard stop.
No default Trait poll, false zero, new helper seam or raw cleanup workaround.

Other required TargetIo implementations remain untouched: DropTrackedTarget
has room in its548-line test leaf, but FailOnceTarget and OverAcceptingTarget
are in the10965-line Owner parent with no growth room. Their own complete
preparatory Move and the actual coupled binding freeze remain later work.

## Evidence owner

`/tmp/mini-vpn-p3-target-move-tA6GgI/entry.json` freezes1146 current repository
files and the exact parent/block/governance contents; it revalidates the prior
accepted readiness documents and all336 Rust files. The same directory owns
fresh baseline/post-Move/gate logs, inventories and immutable named audits.
No script overwrites entry or a final evidence snapshot.

Scoped structural estimate before execution is8/10. Exact inverse, executable
characterization, ratchet and independent review are needed for acceptance;
full P3 remains a separate incomplete ledger in the
[P3 result](2026-09-07-knife16-r7-p3-owner-progress-local-results.md).

Same Heisenberg's independent plan review returned0/0/0 Approve, limited to
this pure two-file Move. It independently verified block/hash,1445/582 shape,
privacy, unchanged test identities, Authority and ratchet ordering. Before
source writes, standalone6 and Target9/Owner74/Controller44/continuation16
passed. This is not implementation acceptance or readiness/binding authority.

Execution reached the exact1445/582 boundary with unchanged573-line body and
full parent inverse. Target9/Owner74/Controller44/continuation16 passed before
and after; post-Move root56/mailbox18/TCP68/client3, fmt/diff, provenance162/13
and all-feature check passed. Integrator removed only the target2017 production
entry; governance is698 lines, both standalone/Cargo6/6. Existing allowance1
and inline-test records remain. Logs preserve existing warning output; no
warning-free build is claimed. Same-Reviewer implementation review returned
0/0/0 Approve, scoped structure9/10, no necessary code optimization. Its only
document P2 was historical2017 wording; the Pre-Move baseline label repaired
that, and final full-packet review returned0/0/0 Approve. Only factual status/
evidence transcription was allowed without another review. This does not
upgrade full P3 or authorize the next behavior freeze's missing Interface.
