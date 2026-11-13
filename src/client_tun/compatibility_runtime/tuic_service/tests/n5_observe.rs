//! N5-OBS (D241/D243) tracers: `📊 Native` lateness window, budget and line
//! shape, plus the ignored CLI `serve_with` loopback smoke.
use super::super::observe::{Line, Window, format_line};
use super::*;
use std::time::Duration;

#[test]
fn n5_obs_window_reports_p99_max_and_sampled_peak() {
    let mut window = Window::default();
    assert_eq!(window.p99_ms(), None);
    for _ in 0..99 {
        window.record(Duration::from_micros(300), 10);
    }
    window.record(Duration::from_millis(400), 70);
    assert_eq!(
        window.p99_ms(),
        Some(1),
        "99 of 100 samples sit in the 0 ms bucket"
    );
    assert_eq!(window.max(), Duration::from_millis(400));
    assert_eq!(window.peak(), 70);
    window.record(Duration::from_millis(400), 0);
    assert_eq!(
        window.p99_ms(),
        Some(251),
        "overflow bucket is the last edge"
    );
}

#[test]
fn n5_obs_native_line_carries_every_section() {
    let budget = bytes::BackingBudget::with_protocol_reserve(1 << 20, 1 << 16).unwrap();
    let held = budget.try_reserve(4096).unwrap();
    let mut window = Window::default();
    window.record(Duration::from_millis(3), 8192);
    let line = format_line(&Line {
        node: None,
        budget: Some(&budget),
        flows: Some((Default::default(), false, 2)),
        tasks: 7,
        window: &window,
    });
    drop(held);
    for part in [
        "📊 Native: node=- |",
        "budget used=4096 peak=8192 limit=1048576 reserve=65536 q=false |",
        "flows live=0 closing=0 rearming=0 quarantined=0 flow_q_rows=0 flow_q_raised=2 session_q=false tail_abort=0 tail_extend=0",
        "tasks=7 late p99=≤4ms max=3ms",
    ] {
        assert!(line.contains(part), "missing {part:?} in {line}");
    }
}

/// Test-only `TunService` that lends the rig's Service and a second Handle to
/// the real CLI `serve_with`, which otherwise takes the Service's unique one.
struct Cli<'a, F: Future<Output = Completion>>(&'a mut Service<F>, Option<Handle>);

impl<F: Future<Output = Completion>> TunService for Cli<'_, F> {
    fn take_handle(&mut self) -> Option<Handle> {
        self.1.take()
    }
    fn run(&mut self) -> impl Future<Output = Report> + '_ {
        self.0.run()
    }
}

/// N5-OBS P1 local smoke (D243): the CLI `serve_with` composition (Service run
/// + verdicts + `📊 Native` observer arm, 1 s period) on the real hard Native
/// Service over loopback under load L, with one explicit peer close so the
/// Node prints `🧭` recovery transitions. No TUN/sudo: the CLI binary's only
/// extra step is the OS utun device factory. The captured stdout/stderr is
/// the evidence; line content itself is asserted by the tracers above.
#[tokio::test(flavor = "current_thread")]
#[ignore = "N5-OBS smoke: run with --ignored --nocapture; the log is the evidence"]
async fn n5_obs_cli_serve_with_smoke_prints_every_section() {
    use super::native_n4_harness::{Load, Peer};
    let (mut service, mut rig) = native_waiting::rig(Duration::from_millis(5)).await;
    let peer = Peer::start(rig.server.clone(), rig.cfg.clone());
    let stopped = std::cell::Cell::new(None);
    let cli = Cli(&mut service, Some(Handle(rig.saved.clone())));
    let script = async {
        let mut load = Load::new(&mut rig.apps, super::native_n4_recovery::standard());
        load.run_for(&mut rig.apps, Duration::from_secs(4)).await;
        let old = peer.latest().expect("live peer connection");
        println!("N5OBS explicit peer close");
        old.close(0u32.into(), b"n5-obs-smoke");
        load.run_for(&mut rig.apps, Duration::from_secs(5)).await;
        println!("N5OBS load {:?}", load.report());
        // Every published flow passed one transferred open (D243: counted at transfer).
        let line = super::super::observe::sample(&rig.saved, &Window::default());
        let field = |key: &str| {
            line.split(key)
                .nth(1)
                .and_then(|r| r.split(' ').next())
                .map(str::to_owned)
        };
        assert_eq!(field(" ok="), field(" published="), "{line}");
        assert!(
            line.contains("node=Running") && !line.contains("opens ok=0 "),
            "{line}"
        );
        native_waiting::stop_polling(&mut rig, &stopped).await
    };
    tokio::time::timeout(Duration::from_secs(60), async {
        tokio::select! {
            () = serve_with(cli, false, Some(Duration::from_secs(1))) => {},
            never = script => match never {},
        }
    })
    .await
    .expect("smoke window");
    let connections = peer.log.lock().unwrap().connections;
    println!("N5OBS connections={connections}");
    assert_eq!(
        connections, 2,
        "one recovery connection after the explicit close"
    );
    peer.stop();
}
