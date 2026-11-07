//! Two original published QUIC flows; inject only the second established copy
//! refusal before socket consumption, preserving real Source and writer actors.
use super::*;
#[tokio::test(flavor = "current_thread")]
async fn native_established_refusal_keeps_original_deadline_and_other_published_flow() {
    complete_service_observed(false, ServiceUdp::None, terminal::Ending::Normal, 20).await;
}
pub(super) async fn reject_and_continue(app: &mut Apps, peer: &quinn::Connection,
    original: &mut (quinn::SendStream, quinn::RecvStream), owner: &Arc<SessionControl>) {
    let original_id = owner.flow_contracts()[0].id;
    app.enable_second_tcp();
    let (mut send, mut recv) = tokio::select! {
        result = peer.accept_bi() => result.unwrap(),
        _ = async { loop { app.turn().await; tokio::task::yield_now().await; } } => unreachable!(),
    };
    let mut expected = encode_connect(&TargetAddr::IpPort("192.0.2.10:10000".parse().unwrap()));
    expected.extend_from_slice(b"service-two");
    let mut actual = vec![0; expected.len()];
    tokio::select! {
        result = recv.read_exact(&mut actual) => result.unwrap(),
        _ = async { loop { app.turn().await; tokio::task::yield_now().await; } } => unreachable!(),
    };
    assert_eq!(actual, expected);
    assert_eq!((owner.flow_observation().unwrap().published, owner.flow_observation().unwrap().running), (2, 10));
    let marker = b"rejected-second-uplink";
    assert_eq!(app.sockets.get_mut::<smoltcp::socket::tcp::Socket>(app.second_tcp.unwrap()).send_slice(marker).unwrap(), marker.len());
    while owner.initial_uplink_refusal().is_none() { app.turn().await; tokio::task::yield_now().await; }
    owner.assert_established_uplink_refusal();
    let _ = send.finish();
    let _ = recv.stop(79u32.into());
    drop((send, recv));
    let diagnostic = tokio::time::Instant::now() + Duration::from_millis(200);
    let mut observed = false;
    while owner.flow_observation().unwrap().released != 1 {
        app.turn().await; tokio::task::yield_now().await;
        if !observed && tokio::time::Instant::now() >= diagnostic {
            eprintln!("[N1-established-refusal-wait] flow={:?} debug={:?}", owner.flow_observation(), owner.flow_debug()); observed = true;
        }
    }
    assert!(!owner.stop.stopping.load(std::sync::atomic::Ordering::Acquire));
    assert_eq!(owner.flow_contracts().len(), 1);
    assert_eq!(owner.flow_contracts()[0].id, original_id);
    let marker = b"original-survives-established-refusal";
    assert_eq!(app.sockets.get_mut::<smoltcp::socket::tcp::Socket>(app.tcp).send_slice(marker).unwrap(), marker.len());
    let mut actual = vec![0; marker.len()];
    tokio::select! {
        result = original.1.read_exact(&mut actual) => result.unwrap(),
        _ = async { loop { app.turn().await; tokio::task::yield_now().await; } } => unreachable!(),
    };
    assert_eq!(actual, marker);
    original.0.write_all(marker).await.unwrap();
    while app.tcp_reply.len() < 8192 + marker.len() { app.turn().await; tokio::task::yield_now().await; }
    assert_eq!(&app.tcp_reply[8192..], marker);
    assert!(!owner.stop.stopping.load(std::sync::atomic::Ordering::Acquire));
    owner.assert_established_uplink_refusal();
}

#[tokio::test(flavor = "current_thread")]
async fn native_established_quarantine_keeps_original_deadline_without_replacement_io() {
    complete_service_observed(false, ServiceUdp::None, terminal::Ending::Normal, 21).await;
}
pub(super) async fn quarantine(app: &mut Apps, owner: &Arc<SessionControl>, budget: &bytes::BackingBudget) {
    let marker = b"unchanged-actual-socket-on-Q";
    assert_eq!(app.sockets.get_mut::<smoltcp::socket::tcp::Socket>(app.tcp).send_slice(marker).unwrap(), marker.len());
    while !budget.quarantined() { app.turn().await; tokio::task::yield_now().await; }
    owner.assert_established_uplink_quarantine();
    assert!(budget.used() > 0);
    assert_eq!(owner.flow_observation().unwrap().released, 0);
}
