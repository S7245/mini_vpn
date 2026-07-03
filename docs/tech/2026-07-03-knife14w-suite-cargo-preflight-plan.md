# Knife14w suite plan - cargo discovery before VPS preflight

1. Record that the `1144fc2` bundle failed before data-plane testing.
2. Add a bounded `find_cargo` helper to the US-client suite.
3. Prefer `CARGO`, then `PATH`, then common rustup locations for root/ubuntu.
4. Print the chosen cargo path and version into the report before building.
5. Keep missing-cargo failure early and actionable.
6. Run shell syntax and diff checks.
7. Update `.learnings/` and commit the test-harness fix.
