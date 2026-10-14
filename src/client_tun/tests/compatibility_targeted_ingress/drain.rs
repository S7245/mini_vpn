//! Actual maintenance ingress with real application SYNs and the original root.
//! Owns only in-memory devices; no upstream open, flow actor or endpoint is faked.
//! The saved-session ACK-round test covers the composed pressure entry.
use super::*;
use crate::client_tun::{
    BufferedDownlinkConfig, DownlinkBackpressureConfig, DownlinkEgressDropDebt, ListenerRegistry,
    LocalEgressServiceConfig, TUN_RX_DRAIN_SOURCE_LOCAL_EGRESS_SERVICE, TunRxDrainDiag,
    drain_ready_tun_rx, format_tun_rx_drain_diag,
};
use crate::fake_ip::FakeIpPool;
use crate::tuic::AssocTable;
use std::collections::{HashMap, HashSet};

#[tokio::test]
async fn maintenance_drain_preserves_all_64_real_syns_with_original_48_budget() {
    maintenance_ingress(0).await;
}

#[tokio::test]
async fn maintenance_read_error_before_first_packet_preserves_original_io_error() {
    maintenance_ingress(1).await;
}

#[tokio::test]
async fn maintenance_read_error_after_complete_packet_preserves_original_io_error() {
    maintenance_ingress(2).await;
}

#[tokio::test]
async fn maintenance_lookahead_error_preserves_original_io_error() {
    maintenance_ingress(3).await;
}

#[tokio::test]
async fn maintenance_pending_synack_flush_retains_transaction_across_observer_cancel() {
    maintenance_ingress(4).await;
}

async fn maintenance_ingress(mode: usize) {
    let up = PacketLink::bounded(256);
    let down = PacketLink::bounded(256);
    let mut apps = RealApps::unconnected(down.clone(), up.clone(), 64);
    apps.targets.fill(0);
    apps.connect_range(0..64);
    apps.turn(true).await;
    let probe = Arc::new(Probe::default());
    probe
        .try_fail_read
        .store((1..=3).contains(&mode), Ordering::SeqCst);
    probe
        .try_fail_after
        .store(usize::from(mode != 1), Ordering::SeqCst);
    probe.held.store(mode == 4, Ordering::SeqCst);
    let mut device = WireDevice(
        LoopbackTunDevice::new(up.clone(), down.clone()),
        probe.clone(),
    );
    let mut iface = Interface::new(
        Config::new(HardwareAddress::Ip),
        &mut device,
        smoltcp::time::Instant::now(),
    );
    let address = Ipv4Address::new(192, 0, 2, 10);
    iface.update_ip_addrs(|ips| ips.push(IpCidr::new(address.into(), 24)).unwrap());
    iface.routes_mut().add_default_ipv4_route(address).unwrap();
    let owner = Arc::new(SessionControl::default());
    *owner.opens() = OpenCustody::with_flows();
    let (dispatch, _notices) = OpenDispatch::channel(128, Some(owner.clone()));
    let upstream_probe = Arc::new(PendingOpens::new());
    let upstream = Arc::new(LegacyOwnedUpstream::new(upstream_probe.clone()));
    let mut registry = ListenerRegistry::new(2);
    let mut sockets = SocketSet::new(vec![]);
    let mut contexts = HashMap::new();
    let mut dirty = HashSet::new();
    let mut assoc = AssocTable::new();
    let mut fake = FakeIpPool::new();
    let metrics = Metrics::default();
    let mut sink = NoopSink;
    let (events, _events_rx) = mpsc::channel(64);
    let (actions, _actions_rx) = mpsc::channel(1);
    let pressure = DownlinkBackpressureConfig {
        high_bytes: 4096,
        low_bytes: 2048,
    };
    let mut debt = DownlinkEgressDropDebt::default();
    let (mut flushes, mut failures) = (0, 0);
    let mut diag = TunRxDrainDiag::default();
    assert_eq!(
        LocalEgressServiceConfig::default().tun_rx_packets_per_cycle,
        48
    );
    let mut first = true;
    let mut completed = 0;
    loop {
        dispatch.begin_turn().await;
        let mut transaction = Box::pin(drain_ready_tun_rx(
            &mut device,
            &mut assoc,
            &mut fake,
            &upstream,
            0,
            &metrics,
            &mut registry,
            &mut sockets,
            &mut contexts,
            &mut iface,
            &mut dirty,
            &dispatch,
            &events,
            &actions,
            &mut sink,
            65536,
            pressure,
            1500,
            &mut debt,
            &mut flushes,
            &mut failures,
            &mut diag,
            if mode == 3 && first { 1 } else { 48 },
            TUN_RX_DRAIN_SOURCE_LOCAL_EGRESS_SERVICE,
            false,
            BufferedDownlinkConfig::disabled(pressure, 2),
        ));
        if mode == 4 && first {
            assert!(
                std::future::poll_fn(|cx| Poll::Ready(transaction.as_mut().poll(cx)))
                    .await
                    .is_pending()
            );
            assert!(probe.waiting.load(Ordering::SeqCst));
            assert_eq!(probe.staged.load(Ordering::SeqCst), 1);
            assert_eq!(probe.received.load(Ordering::SeqCst), 1);
            assert_eq!(probe.syn_ack.load(Ordering::SeqCst), 1);
            assert_eq!(probe.shutdown.load(Ordering::SeqCst), 0);
            assert_eq!(probe.dropped.load(Ordering::SeqCst), 0);
            assert!(
                std::future::poll_fn(|cx| Poll::Ready(transaction.as_mut().poll(cx)))
                    .await
                    .is_pending()
            );
            assert_eq!(probe.staged.load(Ordering::SeqCst), 1);
            assert_eq!(probe.received.load(Ordering::SeqCst), 1);
            assert_eq!(probe.syn_ack.load(Ordering::SeqCst), 1);
            probe.release();
        }
        let result = transaction.await;
        if (1..=3).contains(&mode) && first {
            let error = result.unwrap_err();
            assert_eq!(error.raw_os_error(), Some(5));
            let expected = usize::from(mode != 1);
            assert_eq!(diag.tcp_packets as usize, expected);
            assert_eq!(flushes as usize, expected);
            assert_eq!(probe.received.load(Ordering::SeqCst), expected);
            assert_eq!(probe.staged.load(Ordering::SeqCst), expected);
            assert_eq!(probe.syn_ack.load(Ordering::SeqCst), expected);
            completed += expected;
        } else {
            let drained = result.unwrap();
            assert_eq!(drained, (64 - completed).min(48));
            completed += drained;
            if drained == 0 {
                break;
            }
        }
        first = false;
    }
    let syn_received = sockets
        .iter()
        .filter(|(_, s)| {
            matches!(s,
        smoltcp::socket::Socket::Tcp(s) if s.state() == smoltcp::socket::tcp::State::SynReceived)
        })
        .count();
    // Classify actual replies before allowing the application stack to consume them.
    let mut replies = HashSet::new();
    let mut resets = 0;
    while apps.device.try_recv_rx().unwrap() {
        let ip = Ipv4Packet::new_checked(apps.device.rx_peek().unwrap()).unwrap();
        let tcp = TcpPacket::new_checked(ip.payload()).unwrap();
        resets += usize::from(tcp.rst());
        if tcp.syn() && tcp.ack() {
            assert!(replies.insert(tcp.dst_port()));
        }
        assert!(apps.device.stage_rx_for_iface());
    }
    apps.turn(true).await;
    let observed = (syn_received, apps.established(), replies.len(), resets);
    eprintln!(
        "MAINTENANCE_SYN actual={observed:?} diag={}",
        format_tun_rx_drain_diag(&diag)
    );
    assert_eq!(observed, (64, 64, 64, 0));
    assert_eq!(
        (
            diag.tcp_packets,
            diag.batch_iface_polls,
            diag.batch_flushes,
            diag.batch_dirty_relay_passes
        ),
        (64, 64, 64, 64)
    );
    assert_eq!(diag.tcp_batch_packets_high_water, 1);
    assert_eq!(
        (
            diag.avoided_iface_polls(),
            diag.avoided_dirty_relay_passes()
        ),
        (0, 0)
    );
    assert_eq!((flushes, failures), (64, 0));
    assert_eq!(upstream_probe.entered.load(Ordering::Acquire), 0);
    let seen = owner.flow_observation().unwrap();
    assert_eq!(
        (seen.live, seen.w, seen.f, seen.factory_started),
        (0, 0, 0, 0)
    );
    assert!(seen.audit);
    assert_eq!((up.dropped_packets(), down.dropped_packets()), (0, 0));
}
