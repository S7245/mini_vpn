//! Actual Session registry/Native carrier final destruction, in an isolated
//! process because two original opaque field destructors can abort unwinding.
use super::*;
use crate::client_tun::compatibility_runtime::flow_custody::transaction::{
    OpenOutcome, OpenPhase, OpenSlot,
};
use crate::client_tun::compatibility_runtime::{OpenCustody, SessionControl};
use crate::shared::errors::{FailureOrder, QuietFailureSink};

struct DropBomb {
    drops: Arc<AtomicUsize>,
    _payload: bytes::Bytes,
}
impl Drop for DropBomb {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
        panic!("original unsettled Native carrier destructor");
    }
}
struct Reader(DropBomb);
impl NativeTcpReader for Reader {
    fn poll_read_chunk(
        &mut self,
        _: &mut Context<'_>,
        _: usize,
    ) -> Poll<std::io::Result<Option<NativeTcpChunk>>> {
        panic!("this is a final custody probe, never a DATA success stub");
    }
}
struct Writer(DropBomb);
impl AsyncWrite for Writer {
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        _: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        panic!("no DATA in Drop probe")
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        panic!("no flush in Drop probe")
    }
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        panic!("no fabricated shutdown")
    }
}
#[tokio::test]
async fn last_session_with_two_unsettled_carrier_destructors_retains_custody() {
    const CHILD: &str = "N1_SOURCE_ROOT_DOUBLE_DROP_CHILD";
    if std::env::var_os(CHILD).is_none() {
        for mode in ["carrier", "joined_roles"] {
            let output = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("client_tun::compatibility_runtime::flow_custody::supervision::roles::tests::native_start::forced_root::last_session_with_two_unsettled_carrier_destructors_retains_custody")
            .arg("--nocapture").env(CHILD, mode).output().unwrap();
            eprintln!(
                "child: {}\n{}\n{}",
                output.status,
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            assert!(
                output.status.success(),
                "original Session final Drop must not double-unwind"
            );
        }
        return;
    }
    if std::env::var(CHILD).unwrap() == "joined_roles" {
        joined_role_custody().await;
        return;
    }
    let budget = bytes::BackingBudget::new(65536).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let owner = Arc::new({
        let mut control = SessionControl::default();
        control.app_meter = Some(meter.clone());
        control
    });
    let sink: Arc<dyn QuietFailureSink> = owner.clone();
    let clock = FailureOrder::default();
    clock
        .bind(
            Arc::downgrade(&sink),
            SessionControl::origin(crate::shared::errors::FailurePhase::TcpOpen),
            Some(meter.clone()),
        )
        .unwrap();
    clock.io(&std::io::Error::from_raw_os_error(5));
    let drops = Arc::new(AtomicUsize::new(0));
    let bomb = || DropBomb {
        drops: drops.clone(),
        _payload: bytes::Bytes::copy_from_slice_metered(b"held", &meter).unwrap(),
    };
    let stream = NativeTcpRelayStream::new(Box::new(Reader(bomb())), Box::new(Writer(bomb())));
    let mut sockets = SocketSet::new(Vec::new());
    let handle = sockets.add(Socket::new(
        SocketBuffer::new(vec![0; 8]),
        SocketBuffer::new(vec![0; 8]),
    ));
    {
        let mut opens = owner.opens();
        *opens = OpenCustody::default();
        assert!(matches!(opens.slots.put(0, OpenSlot {
            handle,
            epoch: 7,
            deadline: tokio::time::Instant::now(),
            phase: OpenPhase::Retained(OpenOutcome {
                opened: Some(crate::owned_upstream::OpenedOwnedTcp::Legacy(
                    crate::upstream::OpenedTcpRelay::Native(stream),
                )),
                ..Default::default()
            }),
        }), Ok(None)));
        opens.live = 1;
    }
    let publication = clock.io_deferred(&std::io::Error::from_raw_os_error(54));
    assert_eq!(
        owner
            .ledger()
            .first_failure
            .as_ref()
            .unwrap()
            .facts()
            .raw_os_code,
        Some(5)
    );
    drop(sink);
    drop(owner);
    drop(publication);
    assert_eq!(
        drops.load(Ordering::SeqCst),
        0,
        "no unjoined owner destructor may run"
    );
    assert!(budget.quarantined() && budget.used() > 0);
    assert!(
        meter.snapshot().physical_bytes > 0,
        "actual original DATA is still held"
    );
}

pub(super) struct UnusedUpstream;
#[async_trait::async_trait]
impl crate::owned_upstream::OwnedUpstream for UnusedUpstream {
    async fn open_tcp_flow(
        &self,
        _: &crate::shared::TargetAddr,
    ) -> Result<crate::owned_upstream::OpenedOwnedTcp, crate::shared::ClientError> {
        panic!("no transport open in forced Drop test")
    }
    async fn send_udp(&self, _: crate::owned_upstream::UdpUplink<'_>) {
        panic!("no UDP in forced Drop test")
    }
}
#[derive(Debug)]
struct PayloadBomb(Arc<AtomicUsize>);
impl Drop for PayloadBomb {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
        panic!("original joined role payload Drop");
    }
}
async fn joined_role_custody() {
    use crate::client_tun::compatibility_runtime::flow_custody::transaction::OpenDispatch;
    let budget = bytes::BackingBudget::new(65536).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let owner = Arc::new({
        let mut control = SessionControl::default();
        control.app_meter = Some(meter.clone());
        control
    });
    *owner.opens() = OpenCustody::with_flows();
    let (dispatch, _notices) = OpenDispatch::channel(4, Some(owner.clone()));
    let mut sockets = SocketSet::new(Vec::new());
    let handle = sockets.add(Socket::new(
        SocketBuffer::new(vec![0; 8]),
        SocketBuffer::new(vec![0; 8]),
    ));
    dispatch.begin_turn().await;
    let id = dispatch
        .reserve_flow(
            Arc::new(UnusedUpstream),
            crate::shared::TargetAddr::IpPort("127.0.0.1:9".parse().unwrap()),
            handle,
            7,
        )
        .unwrap();
    let drops = Arc::new(AtomicUsize::new(0));
    let mut roles = super::failure_order::joined_roles(vec![
        crate::upstream::NativePumpFailureValue::Panic(Box::new(PayloadBomb(drops.clone()))),
        crate::upstream::NativePumpFailureValue::Panic(Box::new(PayloadBomb(drops.clone()))),
    ])
    .await;
    for _ in 0..2 {
        assert!(matches!(
            roles.take_original_failure(),
            Some(super::super::super::roles::OriginalFailure::OpaquePanic)
        ));
    }
    assert_eq!(roles.join_count(), 5);
    assert!(!roles.settled());
    {
        let mut opens = owner.opens();
        opens.flow_mut(id).unwrap().roles = Some(roles);
        opens.live = 0; // Actual Some slot wins over this contradictory marker.
    }
    owner.ledger().first_egress_io =
        Some(Arc::new(std::io::Error::other(PayloadBomb(drops.clone()))));
    let sink: Arc<dyn QuietFailureSink> = owner.clone();
    let clock = FailureOrder::default();
    clock
        .bind(
            Arc::downgrade(&sink),
            SessionControl::origin(crate::shared::errors::FailurePhase::TcpOpen),
            Some(meter),
        )
        .unwrap();
    clock.io(&std::io::Error::from_raw_os_error(5));
    let publication = clock.io_deferred(&std::io::Error::from_raw_os_error(54));
    drop(dispatch);
    drop(sink);
    drop(owner);
    drop(publication);
    assert_eq!(
        drops.load(Ordering::SeqCst),
        0,
        "two actual role payloads and first IO remain original custody"
    );
    assert!(budget.quarantined() && budget.used() > 0);
}
impl std::fmt::Display for PayloadBomb {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("original first IO")
    }
}
impl std::error::Error for PayloadBomb {}
