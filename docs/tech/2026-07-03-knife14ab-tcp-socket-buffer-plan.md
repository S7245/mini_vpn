# Knife14ab plan - TCP socket buffer BDP

1. Summarize knife14aa as: direct `.77 -> client` reverse is healthy, tunnel
   reverse remains limited, and downlink pending/backpressure is now the
   strongest signal.
2. Keep runtime defaults unchanged for ordinary/local tests.
3. Add env-configurable smoltcp TCP listener rx/tx buffer sizes.
4. Wire the configured buffers into dynamic listener creation and elastic spare
   listener creation.
5. Make the US-client suite run high-throughput acceptance with 1MiB rx/tx
   socket buffers by default.
6. Verify unit tests, shell syntax, full test suite, clippy, stage code-review,
   learning updates, commit, push, and provide the next VPS checklist.
