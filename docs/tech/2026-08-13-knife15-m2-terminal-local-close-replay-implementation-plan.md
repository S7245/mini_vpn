# Knife15 M2 terminal-local-close replay implementation plan

1. Reproduce the exact `0/24/0B -> local EOF 24B -> Closed 24B` evidence in
   the runner self-test and retain the expected RED.
2. Extend only `d16_terminal_ownership_is_clean` with the exact terminal-local
   abandonment proof defined in the architecture spec.
3. Add fail-closed mutations for ownership-drop, byte mismatch, and a
   send-capable/nonterminal state.
4. Run the focused/full runner self-test and replay the immutable
   `20260813_030113` artifact without changing it.
5. Run shell, formatting/diff, secret, and code-review gates; record the
   qualification result and learning/error memory.
6. Commit and push the reviewed descendant. A repaired replay can classify
   this qualification, but it does not replace the fresh formal M2 gate.
