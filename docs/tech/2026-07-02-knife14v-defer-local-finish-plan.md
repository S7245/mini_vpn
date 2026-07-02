# Knife14v Plan: Defer Local Finish While Reverse Downlink Is Active

## Tasks

1. Add `SocketCtx` metadata for a pending local finish and its latest remote
   progress timestamp.
2. Add tests that first fail under the current immediate-Finish behavior.
3. Update `pump_established_uplink` to defer `RelayCommand::Finish` until remote
   progress has been quiet for the bounded window.
4. Refresh the pending-finish progress timestamp in `handle_remote_payload`.
5. Reset the pending-finish metadata on rearm.
6. Run local acceptance, stage review, and learning update.

## Review Checklist

- `RelayCommand::Data` ordering must not change.
- The deferred finish must still send at most one `Finish` per flow.
- Remote progress must extend the defer window but never remove the eventual
  bound once remote traffic stops.
- Dirty-set retention must still wake the flow until the delayed Finish is sent.
- The change must not alter `dead_slot_reap` semantics from knife14u.
