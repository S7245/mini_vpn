# Knife14ah plan - late remote diagnostics after local finish

> Spec:
> `docs/tech/2026-07-03-knife14ah-late-remote-diagnostics-spec.md`.

1. Add a shell parser tracer test.
   - Use a small iperf sample with 0 receiver bytes.
   - Use a log sample with `tcp-relay-write-half-closed reason=local_finish`
     followed by late remote bytes.
   - Assert `relay_late_remote` and `late_remote_after_local_finish`.

2. Implement low-RTT parser/report support.
   - Parse post-finish remote counters.
   - Add lifecycle lines to `METRIC_RE`.
   - Keep existing attribution output stable for previous pressure/loss cases.

3. Add focused relay diagnostic tests.
   - Verify counters split total remote bytes from post-local-finish bytes.
   - Verify `tcp-relay-live` / `tcp-relay-close` formatting includes the new
     counters.

4. Implement relay diagnostic counters without changing relay behavior.
   - Mark local `Finish` observation in the relay task.
   - Count remote reads/bytes after that mark.

5. Verify locally.
   - `bash -n scripts/knife14b-lowrtt-probe.sh`
   - `bash scripts/knife14b-lowrtt-probe.sh --self-test`
   - focused Rust tests for `client_tun` diagnostics
   - `git diff --check`

6. Stage code-review.
   - Check no hot-path default logging changes.
   - Check parser remains BSD awk compatible.
   - Check no secrets or env values are written.

7. Record learning and commit the coherent observability task.
