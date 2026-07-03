# Knife14aa plan - direct reverse preflight

1. Summarize the knife14z result as: BBR improves forward, reverse still lacks
   direct baseline attribution.
2. Extend `scripts/knife14b-usclient-tunnel-suite.sh` preflight with a short
   direct `iperf3 -R` check.
3. Fail early when `OUT_DIR` cannot be created or written by the current user.
4. Keep the reverse check configurable with explicit env switches.
5. Preserve old single-run and CC sweep behavior.
6. Verify shell syntax, help output, and early env-guard smoke behavior.
7. Review the stage diff for quoting, failure behavior, and report clarity.
8. Update learnings, commit, push, then provide the next one-shot VPS checklist.
