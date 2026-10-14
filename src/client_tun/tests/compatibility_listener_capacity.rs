//! L0 listener-capacity characterization through the private runtime test tree.

use super::super::super::listener_registry::{
    ListenerCapacityProfile, ListenerCapacityRejection, TcpSocketBufferConfig,
};
use super::super::super::{ListenerRegistry, RegistryError, SocketCtx};
use super::super::{
    ListenerOwnedCounts, StartupProfileDisposition, StartupProfileObservation,
    prepare_client_tun_session,
};
use super::{DeviceProbe, FaultDevice, JoinGate, NoFlows};
use crate::client_tun::{NoopSink, TunRuntimeConfig};
use crate::metrics::Metrics;
use smoltcp::iface::{SocketHandle, SocketSet};
use smoltcp::socket::tcp::Socket as TcpSocket;
use std::collections::HashMap;
use std::sync::Arc;
use std::task::Poll;
use tokio::sync::mpsc;

/// #2 全局总槽上限：弹性扩容受 `max_total` 兜底，达上限返回 Capped、不再增长（防 SYN flood）。
#[test]
fn ensure_spare_listeners_respects_global_cap() {
    let mut sockets = SocketSet::new(vec![]);
    let mut ctxs: HashMap<SocketHandle, SocketCtx> = HashMap::new();
    // cap=3：pool_size=2 首建占 2；两个已关闭槽不计空闲，补足到 4 个需完整增加 4。
    let mut reg = ListenerRegistry::with_max_total(2, 3);
    reg.ensure_port(443, &mut sockets, &mut ctxs).unwrap();
    let hs: Vec<SocketHandle> = reg.handles_for_port(443).to_vec();
    for h in &hs {
        sockets.get_mut::<TcpSocket>(*h).abort();
    }
    // 要 4 空闲，但 cap=3 不足以容纳完整增量；返回 Capped 且不部分扩容。
    let err = reg
        .ensure_spare_listeners(443, 4, &mut sockets, &mut ctxs)
        .unwrap_err();
    assert!(matches!(err, RegistryError::Capped));
    assert_eq!(reg.handles_for_port(443).len(), 2);
}

#[derive(Debug, PartialEq, Eq)]
struct ListenerOwnedSnapshot {
    socket_set: Vec<SocketHandle>,
    socket_ctxs: Vec<SocketHandle>,
    ports: usize,
    port_443: Vec<SocketHandle>,
    port_8443: Vec<SocketHandle>,
    total: usize,
}

fn listener_owned_snapshot(
    sockets: &SocketSet<'_>,
    socket_ctxs: &HashMap<SocketHandle, SocketCtx>,
    registry: &ListenerRegistry,
) -> ListenerOwnedSnapshot {
    let mut socket_set = sockets.iter().map(|(handle, _)| handle).collect::<Vec<_>>();
    socket_set.sort_unstable();
    let mut socket_ctxs = socket_ctxs.keys().copied().collect::<Vec<_>>();
    socket_ctxs.sort_unstable();
    ListenerOwnedSnapshot {
        socket_set,
        socket_ctxs,
        ports: registry.port_count(),
        port_443: registry.handles_for_port(443).to_vec(),
        port_8443: registry.handles_for_port(8443).to_vec(),
        total: registry.total_handles(),
    }
}

#[test]
fn ensure_port_rejects_over_max_total_without_mutation() {
    let mut sockets = SocketSet::new(vec![]);
    let mut socket_ctxs: HashMap<SocketHandle, SocketCtx> = HashMap::new();
    let mut registry = ListenerRegistry::with_max_total(2, 3);
    registry
        .ensure_port(443, &mut sockets, &mut socket_ctxs)
        .expect("initial pool fits the test cap");
    let before = listener_owned_snapshot(&sockets, &socket_ctxs, &registry);

    let result = registry.ensure_port(8443, &mut sockets, &mut socket_ctxs);

    assert_eq!(
        listener_owned_snapshot(&sockets, &socket_ctxs, &registry),
        before,
        "a capped initial pool must not mutate listener-owned state"
    );
    assert!(matches!(result, Err(RegistryError::Capped)));
}

#[test]
fn ensure_spare_listeners_rejects_over_max_total_without_mutation() {
    let mut sockets = SocketSet::new(vec![]);
    let mut socket_ctxs: HashMap<SocketHandle, SocketCtx> = HashMap::new();
    let mut registry = ListenerRegistry::with_max_total(2, 3);
    registry
        .ensure_port(443, &mut sockets, &mut socket_ctxs)
        .expect("initial pool fits the test cap");
    for handle in registry.handles_for_port(443).to_vec() {
        sockets.get_mut::<TcpSocket>(handle).abort();
    }
    let before = listener_owned_snapshot(&sockets, &socket_ctxs, &registry);

    let result = registry.ensure_spare_listeners(443, 4, &mut sockets, &mut socket_ctxs);

    assert_eq!(
        listener_owned_snapshot(&sockets, &socket_ctxs, &registry),
        before,
        "a capped spare expansion must not mutate listener-owned state"
    );
    assert!(matches!(result, Err(RegistryError::Capped)));
}

#[tokio::test(flavor = "current_thread")]
async fn f4096_profile_rejects_rx_65536_before_listener_allocation() {
    let probe = Arc::new(DeviceProbe::default());
    let join_gate = Arc::new(JoinGate::default());
    let mut device = FaultDevice::new(Arc::clone(&probe));
    device.join_gate = Some(Arc::clone(&join_gate));
    let (downlink_tx, downlink_rx) = mpsc::channel(1);
    let mut runtime_config = TunRuntimeConfig::from_sources(Some("2")).unwrap();
    runtime_config.tcp_socket_buffers = TcpSocketBufferConfig {
        rx_bytes: 65_536,
        tx_bytes: 65_535,
        receive_window_limit: None,
    };
    runtime_config.select_f4096_for_test();

    let mut session = prepare_client_tun_session(
        device,
        Arc::new(NoFlows),
        downlink_rx,
        runtime_config,
        Arc::new(Metrics::new()),
        NoopSink,
    );
    let control = Arc::clone(&session.control);
    let pending = {
        let mut run = Box::pin(session.run());
        std::future::poll_fn(|cx| Poll::Ready(run.as_mut().poll(cx).is_pending())).await
    };
    assert!(pending, "saved session must remain pending after one poll");
    assert_eq!(
        control.startup_profile_observation(),
        Some(StartupProfileObservation {
            profile: ListenerCapacityProfile::F4096,
            effective_rx_bytes: 65_536,
            effective_tx_bytes: 65_535,
            disposition: StartupProfileDisposition::Rejected(
                ListenerCapacityRejection::RxOverBudget,
            ),
            listener_owned: ListenerOwnedCounts {
                sockets: 0,
                socket_ctxs: 0,
                ports: 0,
                total: 0,
            },
        })
    );
    drop(downlink_tx);
    drop(join_gate);
}
