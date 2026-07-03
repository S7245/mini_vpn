# Knife14w plan - coalesce relay writer uplink data

1. Record the knife14v result: reverse fixed, forward still limited by small
   local-to-remote writes.
2. Add a bounded writer-side coalescing helper for queued `RelayCommand::Data`.
3. Preserve Finish ordering by carrying a `finish_after` flag out of coalescing.
4. Add local write wait diagnostics to `RelayTaskDiag` and relay close logs.
5. Add focused tests for coalescing, Finish ordering, and write-wait accounting.
6. Run local gates and review the stage before committing.
7. Update `.learnings/` with the stage result and next VPS checklist.
