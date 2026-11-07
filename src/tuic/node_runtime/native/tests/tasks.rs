use super::*;
#[test]
fn unclaimed_parent_tasks_keep_both_original_panic_outputs() {
    const NAME: &str = "tuic::node_runtime::native::tests::tasks::unclaimed_parent_tasks_keep_both_original_panic_outputs";
    if std::env::var_os("N1_UDP_TASKS_CHILD").is_none() {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", NAME, "--nocapture"])
            .env("N1_UDP_TASKS_CHILD", "1")
            .env("RUST_BACKTRACE", "0")
            .output()
            .unwrap();
        eprintln!(
            "actual Native parent task child: {:?}\n{}\n{}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            output.status.success(),
            "original unclaimed task outputs must be retained without abort"
        );
        return;
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        struct Bomb(Arc<std::sync::atomic::AtomicUsize>);
        impl Drop for Bomb {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
                panic!("original task payload Drop");
            }
        }
        let budget = bytes::BackingBudget::new(1 << 20).unwrap();
        let meter = BackingMeter::with_budget(&budget);
        let (clock, facts) = clock(&meter);
        clock.io(&io::Error::from_raw_os_error(5));
        let callbacks = Callbacks::prepare(&meter, clock.clone()).unwrap();
        let lease = callbacks.acquire(Role::Parent).unwrap();
        let guard = lease.guard();
        let mut tasks = NativeTasks::bind(TaskRoot::prepare(&meter).unwrap(), &guard);
        let drops = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut handles = Vec::new();
        for _ in 0..2 {
            let drops = drops.clone();
            handles.push(tasks.spawn(async move {
                std::panic::panic_any(Bomb(drops));
            }));
        }
        while handles.iter().any(|h| !h.is_finished()) {
            tokio::task::yield_now().await;
        }
        assert_eq!(
            tasks.len(),
            2,
            "both actual task outputs are ready and unclaimed"
        );
        drop(handles);
        drop(tasks); // exact production parent aggregate owner
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        assert!(budget.quarantined());
        assert!(budget.used() > 0);
        assert_eq!(facts.0.lock().unwrap()[0].facts().raw_os_code, Some(5));
        drop((guard, lease, callbacks, clock));
    });
}
