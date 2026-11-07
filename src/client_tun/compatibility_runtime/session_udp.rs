//! Session's actual UDP source retirement, not UDP publication or health policy.
//! DATA borrows this source. After DATA stops, keep its original sealing ability
//! until the original health JoinHandle is consumed; no packet is delivered here.
//! Device/open settlement proceeds in parallel at the same Session authority.
//! Unknown poll/destruction remains charged Q; never retry a poisoned future.
//! Entries: `settle`/`dispose`; real-peer tests: tuic_service/tests/health.rs.
use super::*;
use crate::failover::HealthJoinObservation;

pub(super) fn dispose(source: UdpDownlinkSource, owner: &SessionControl) -> Option<PanicCustody> {
    if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(source))) {
        // Rust cannot recover a partially destroyed value. Retain the original
        // failure and occupied Session/Q forever; never mint a clean receipt.
        return owner.quarantine(FailurePhase::SessionLoopDrop, payload);
    }
    None
}

pub(super) async fn settle(
    mut source: UdpDownlinkSource,
    receipt: HealthJoinObservation,
    owner: &SessionControl,
) -> Option<PanicCustody> {
    // Used only by decoding discarded control-tail packets, never DATA delivery,
    // fake-IP clocks or successor publication. All original buffers stay bounded.
    let discard_clock = std::time::Instant::now();
    let mut eof = false;
    loop {
        let next = source.recv(&discard_clock);
        tokio::pin!(next);
        let event = tokio::select! {
            biased;
            _ = receipt.joined() => None,
            event = std::future::poll_fn(|cx| {
                match catch_unwind(AssertUnwindSafe(|| next.as_mut().poll(cx))) {
                    Ok(Poll::Pending) => Poll::Pending,
                    Ok(Poll::Ready(event)) => Poll::Ready(Ok(event)),
                    Err(payload) => Poll::Ready(Err(payload)),
                }
            }), if !eof => Some(event),
        };
        match event {
            None => break,
            Some(Ok(None)) => eof = true, // actual EOF once; wait without busy spin
            Some(Ok(Some(event))) => {
                if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(event))) {
                    let custody = owner.quarantine(FailurePhase::SessionLoopDrop, payload);
                    std::future::pending::<()>().await;
                    drop(custody);
                }
            }
            Some(Err(payload)) => {
                let custody = owner.quarantine(FailurePhase::SessionLoop, payload);
                // Keep this exact source and poisoned future; do not repoll it.
                std::future::pending::<()>().await;
                drop(custody);
            }
        }
    }
    dispose(source, owner)
}

#[cfg(test)]
mod tests;
