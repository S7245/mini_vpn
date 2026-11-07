//! Original full-signal fatal JOIN characterization, moved unchanged.
use super::*;

#[tokio::test]
async fn poisoned_provider_still_joins_writer_blocked_by_full_monitor_notifications() {
    let mut f = fixture(false);
    f.probe
        .fail_at_write
        .store(RELAY_CHANNEL_CAPACITY / 3 + 1, Ordering::SeqCst);
    f.roles.start();
    f.roles.stops[2].take().unwrap().send(()).unwrap();
    let setup = tokio::time::timeout(std::time::Duration::from_secs(1), async {
        std::future::poll_fn(|cx| {
            f.roles.poll_handles(cx);
            if matches!(f.roles.monitor, Role::Joined(_)) {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await;
        // Wait each real notification triple so the production writer cannot
        // coalesce multiple queued DATA commands into one write.
        for n in 1..=RELAY_CHANNEL_CAPACITY / 3 {
            f.tx.send(RelayCommand::Data(vec![1])).await.unwrap();
            loop {
                if matches!(&f.roles.monitor, Role::Joined(m)
                    if m.monitor.signals.writer_signal_rx.len() == n * 3)
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        }
        f.tx.send(RelayCommand::Data(vec![1])).await.unwrap();
        loop {
            if f.probe.writes.load(Ordering::SeqCst) == RELAY_CHANNEL_CAPACITY / 3 + 1
                && matches!(&f.roles.monitor, Role::Joined(m)
                    if m.monitor.signals.writer_signal_rx.len() == RELAY_CHANNEL_CAPACITY)
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    let joined = if setup.is_ok() {
        tokio::time::timeout(
            std::time::Duration::from_millis(250),
            std::future::poll_fn(|cx| {
                f.roles.poll_fatal_joins(cx);
                if f.roles.join_count() == 3 {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            }),
        )
        .await
    } else {
        setup
    };
    // Cleanup on RED must not normalize the successfully retained inventory.
    if joined.is_err() {
        if let Role::Joined(m) = &mut f.roles.monitor {
            m.monitor.signals.writer_signal_rx.close();
            m.monitor.signals.reader_signal_rx.close();
        }
        f.roles.stop();
        drop(f.tx);
        until(&mut f.roles, false, |r| r.join_count() == 3).await;
    }
    assert!(
        joined.is_ok(),
        "full retained monitor queue must not block actual join"
    );
    let Role::Joined(m) = &f.roles.monitor else {
        panic!("actual monitor join")
    };
    assert_eq!(
        m.monitor.signals.writer_signal_rx.len(),
        RELAY_CHANNEL_CAPACITY
    );
    assert!(
        m.closed
            && m.monitor.signals.writer_signal_rx.is_closed()
            && m.monitor.signals.reader_signal_rx.is_closed()
    );
    assert_eq!(f.probe.drops.load(Ordering::SeqCst), 1);
    let Role::Joined(writer) = &f.roles.writer else {
        panic!("actual writer join")
    };
    assert_eq!(
        writer
            .result
            .failure
            .as_ref()
            .unwrap()
            .source
            .raw_os_error(),
        Some(32)
    );
    assert_eq!(m.discarded, [0, 0]);
    assert_eq!(m.drained, [false, false]);
    assert!(f.roles.writer_receipt.is_none() && f.roles.reader_receipt.is_none());
}
