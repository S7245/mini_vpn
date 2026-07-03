# Knife14z plan - congestion-control A/B acceptance

1. Record the knife14y result as a congestion/path signal rather than a
   flow-control signal.
2. Refactor the US-client tunnel suite just enough to run one isolated
   congestion-control variant at a time.
3. Add `CC_SWEEP` as an opt-in wrapper over the existing single-variant
   behavior.
4. Preserve the old single-run filenames unless `CC_SWEEP` is set.
5. Make the report and tarball include per-variant logs/probe artifacts.
6. Verify shell syntax and env-validation smoke behavior locally.
7. Perform stage code-review, update learnings, commit, and provide the next
   one-shot VPS checklist.
