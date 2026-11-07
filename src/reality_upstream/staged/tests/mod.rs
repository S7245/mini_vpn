use crate::reality_upstream::{RealityClientConfig, RealityUpstream};
use crate::upstream::{
    OpenedTcpRelay, ProxyUpstream, TcpOpenAttempt, TcpOpenPhase, TcpOpenRequest,
};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Default)]
pub(super) struct DnsGate {
    state: std::sync::Mutex<(bool, bool)>,
    changed: std::sync::Condvar,
    panic: std::sync::atomic::AtomicBool,
}

impl DnsGate {
    pub(super) fn wait(&self) {
        let mut state = self.state.lock().unwrap();
        state.0 = true;
        self.changed.notify_all();
        while !state.1 {
            state = self.changed.wait(state).unwrap();
        }
        drop(state);
        assert!(
            !self.panic.load(std::sync::atomic::Ordering::Acquire),
            "original DNS worker fault"
        );
    }
    fn release(&self) {
        self.state.lock().unwrap().1 = true;
        self.changed.notify_all();
    }
}

struct GateRelease(Arc<DnsGate>);

pub(super) struct ReturnObservation {
    start: std::sync::mpsc::SyncSender<()>,
    finished: std::sync::mpsc::Receiver<()>,
}
impl ReturnObservation {
    pub(super) fn observe(self) {
        self.start.send(()).unwrap();
        self.finished.recv_timeout(Duration::from_secs(2)).unwrap();
    }
}
impl Drop for GateRelease {
    fn drop(&mut self) {
        self.0.release();
    }
}

fn turn(open: &mut impl TcpOpenAttempt<OpenedTcpRelay>) {
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    open.step(&mut cx, Instant::now());
}

fn settle_at(open: &mut impl TcpOpenAttempt<OpenedTcpRelay>, now: Instant, deadline: Instant) {
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    open.settle(&mut cx, now, deadline);
}

fn local_attempt(server: String) -> super::Attempt {
    let (_, pk) = crate::reality::auth::generate_ephemeral_keypair();
    super::create(
        Arc::new(RealityUpstream::new(RealityClientConfig {
            server,
            uuid: [0x42; 16],
            pbk: pk,
            short_id: [0; 8],
            sni: "example.com".into(),
        })),
        TcpOpenRequest {
            target: crate::shared::TargetAddr::parse("1.2.3.4:443").unwrap(),
            deadline: Instant::now() + Duration::from_secs(20),
        },
    )
    .unwrap()
}

async fn dns_finished(open: &super::Attempt) {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let super::State::Dns(job) = &open.state else {
                panic!("original DNS owner");
            };
            if job.task.is_finished() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
}

async fn until<A: TcpOpenAttempt<OpenedTcpRelay> + ?Sized>(open: &mut A, phase: TcpOpenPhase) {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            std::future::poll_fn(|cx| {
                open.step(cx, Instant::now());
                std::task::Poll::Ready(())
            })
            .await;
            if open.view().phase == phase {
                return;
            }
            assert_ne!(
                open.view().phase,
                TcpOpenPhase::Quarantined,
                "unexpected Q: {:?}",
                open.failure()
            );
            assert_ne!(
                open.view().phase,
                TcpOpenPhase::Settled,
                "unexpected failure: {:?}",
                open.failure()
            );
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("staged progress");
}

async fn authenticated_peer() -> (super::Attempt, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (sk, pk) = crate::reality::auth::generate_ephemeral_keypair();
    let upstream = Arc::new(RealityUpstream::new(RealityClientConfig {
        server: listener.local_addr().unwrap().to_string(),
        uuid: [0x42; 16],
        pbk: pk,
        short_id: [0; 8],
        sni: "example.com".into(),
    }));
    let peer = tokio::spawn(async move {
        let (mut io, _) = listener.accept().await.unwrap();
        crate::reality_upstream::tests::reality_sim_handshake(&mut io, sk).await;
        let mut rest = Vec::new();
        io.read_to_end(&mut rest).await.unwrap();
    });
    let open = super::create(
        upstream,
        TcpOpenRequest {
            target: crate::shared::TargetAddr::parse("1.2.3.4:443").unwrap(),
            deadline: Instant::now() + Duration::from_secs(20),
        },
    )
    .unwrap();
    (open, peer)
}

async fn joined(peer: tokio::task::JoinHandle<()>) {
    tokio::time::timeout(Duration::from_secs(2), peer)
        .await
        .unwrap()
        .unwrap();
}

mod lifecycle;
mod dns;
mod faults;
