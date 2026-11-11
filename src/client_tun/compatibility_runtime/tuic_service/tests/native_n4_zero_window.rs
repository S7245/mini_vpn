//! N4 review F1 probe P2: deterministic zero-window liveness tracer on the
//! real hard Native Service. One 256 KiB downlink bulk; the App does not read
//! until it advertises a zero window; a test-side App→Service relay then drops
//! exactly the first window-update pure ACK of that flow; then the App reads.
//! A TCP sender must recover by zero-window probing (RFC 9293 §3.8.6.1); the
//! Service-side smoltcp 0.10 socket had no persist timer (RED, D176); the
//! vendored persist probe (D180) recovers it. The control (no drop) completes.
//! Owns no production authority; the App and peer come from the N4 harness.
use super::*;
use fixture::Apps;
use native_n4_harness::{BULK_PORT, Load, LoadSpec, Peer};
use smoltcp::socket::tcp;
use smoltcp::wire::{IpProtocol, Ipv4Packet, TcpPacket};
use std::cell::Cell;
use std::time::Instant;

const KIB: usize = 1024;
const BULK: usize = 256 * KIB;
/// Delivery deadline after the App resumes reading (the clean run takes ms).
const RESUME_LIMIT: Duration = Duration::from_secs(10);

/// App→Service relay census for the bulk flow (`port` = App local port).
#[derive(Debug, Default)]
struct Relay {
    port: Option<u16>,
    drop_update: bool,
    forwarded: u64,
    zero_windows: u64,
    /// Window-update pure ACKs after a zero window: (seen, dropped).
    updates: (u64, u64),
    full: u64,
}

impl Relay {
    /// `true` drops the packet: only the first window update after a zero
    /// window, and only when the variant asks for it.
    fn drop_packet(&mut self, packet: &[u8]) -> bool {
        let Some(port) = self.port else { return false };
        let Ok(ip) = Ipv4Packet::new_checked(packet) else { return false };
        if ip.next_header() != IpProtocol::Tcp {
            return false;
        }
        let Ok(tcp) = TcpPacket::new_checked(ip.payload()) else { return false };
        if tcp.src_port() != port || tcp.dst_port() != BULK_PORT {
            return false;
        }
        let pure_ack = tcp.ack() && !tcp.syn() && !tcp.fin() && !tcp.rst();
        if !pure_ack || !tcp.payload().is_empty() {
            return false;
        }
        if tcp.window_len() == 0 {
            self.zero_windows += 1;
            return false;
        }
        if self.zero_windows == 0 {
            return false;
        }
        self.updates.0 += 1;
        let drop = self.drop_update && self.updates.1 == 0;
        self.updates.1 += u64::from(drop);
        drop
    }

    fn pump(&mut self, from_app: &mut LoopbackTunDevice, to_service: &PacketLink) {
        while from_app.try_recv_rx().unwrap() {
            let Some(packet) = from_app.rx_take() else { break };
            if self.drop_packet(&packet) {
                continue;
            }
            self.forwarded += 1;
            self.full += u64::from(!to_service.push(packet));
        }
    }
}

struct Outcome {
    relay: Relay,
    /// (got, done, corrupt) of the bulk.
    bulk: (usize, bool, bool),
    /// App socket at the end: (recv_queue, state).
    app: (usize, tcp::State),
    resumed: bool,
}

async fn zero_window_run(drop_update: bool) -> Outcome {
    let (mut service, mut rig) = native_waiting::rig(Duration::from_millis(5)).await;
    let peer = Peer::start(rig.server.clone(), rig.cfg.clone());
    // Re-home the App on a private outbound link that only the relay drains.
    let [to_service, from_service] = rig.links.clone();
    let app_out = PacketLink::bounded(64);
    rig.apps = Apps::from_device_with_target(
        LoopbackTunDevice::new(from_service, app_out.clone()),
        rig.apps.target,
    );
    let mut from_app = LoopbackTunDevice::new(app_out, PacketLink::bounded(1));
    let (stopped, mut out) = (Cell::new(None), None);
    let report = native_waiting::drive(&mut service, Duration::from_secs(60), async {
        let mut relay = Relay {
            drop_update,
            ..Relay::default()
        };
        let mut load = Load::new(&mut rig.apps, LoadSpec::default());
        let bulk = load.bulk(&mut rig.apps, BULK, 64 * KIB);
        load.bulks[bulk].reading = false;
        let handle = load.bulks[bulk].handle;
        relay.port = rig
            .apps
            .sockets
            .get::<tcp::Socket>(handle)
            .local_endpoint()
            .map(|e| e.port);
        let zero_limit = Instant::now() + Duration::from_secs(15);
        let mut resume_limit = None;
        loop {
            load.run_for(&mut rig.apps, Duration::from_millis(1)).await;
            relay.pump(&mut from_app, &to_service);
            let now = Instant::now();
            if resume_limit.is_none() && relay.zero_windows > 0 {
                // The App reads only after the Service saw the zero window.
                load.bulks[bulk].reading = true;
                resume_limit = Some(now + RESUME_LIMIT);
            }
            if load.bulks[bulk].done.is_some()
                || resume_limit.map_or(now >= zero_limit, |limit| now >= limit)
            {
                break;
            }
        }
        let b = &load.bulks[bulk];
        let socket = rig.apps.sockets.get::<tcp::Socket>(handle);
        out = Some(Outcome {
            bulk: (b.got, b.done.is_some(), b.corrupt),
            app: (socket.recv_queue(), socket.state()),
            resumed: resume_limit.is_some(),
            relay,
        });
        native_waiting::stop_polling(&mut rig, &stopped).await
    })
    .await;
    let outcome = out.unwrap();
    println!(
        "N4P2 drop_update={drop_update} resumed={} bulk={:?} app(recv_queue,state)={:?} relay={:?} links_dropped={:?}",
        outcome.resumed,
        outcome.bulk,
        outcome.app,
        outcome.relay,
        rig.links.each_ref().map(|l| l.dropped_packets()),
    );
    assert!(outcome.resumed, "App never advertised a zero window");
    assert_eq!(outcome.relay.full, 0, "relay link full");
    native_waiting::assert_clean_stop(
        report,
        stopped.get().unwrap().elapsed(),
        &rig.budget,
        &rig.handle,
        false,
    );
    peer.stop();
    outcome
}

/// Control: the same flow without the drop completes after the zero window.
#[tokio::test(flavor = "current_thread")]
async fn n4_p2_zero_window_without_loss_completes() {
    let outcome = zero_window_run(false).await;
    assert_eq!(outcome.relay.updates.1, 0);
    assert_eq!(outcome.bulk, (BULK, true, false), "control delivery");
}

/// F1 tracer: losing the single window update must not stall the flow.
#[tokio::test(flavor = "current_thread")]
async fn n4_p2_lost_window_update_still_completes() {
    let outcome = zero_window_run(true).await;
    assert_eq!(outcome.relay.updates.1, 1, "exactly one window update dropped");
    assert_eq!(
        outcome.bulk,
        (BULK, true, false),
        "flow stalled after its only window update was lost: app={:?}",
        outcome.app
    );
}
