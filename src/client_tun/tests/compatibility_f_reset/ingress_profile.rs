//! Passive SynBurst packet/preparation diagnostic at the existing reset adapter.
//! Counts successful staging and produced TX bytes, not TCP acceptance or FIFO depth.
//! Fixed port bitmap bounds unique ACK flags; the parent full test owns the driver.
use super::*;
use std::sync::Mutex;
use std::time::{Duration, Instant};

#[derive(Clone, Copy)]
pub(super) struct Packet {
    syn: bool,
    ack: bool,
    fin: bool,
    rst: bool,
    payload: bool,
    source: u16,
    destination: u16,
}
pub(super) fn packet(bytes: &[u8]) -> Option<Packet> {
    let ip = Ipv4Packet::new_checked(bytes).ok()?;
    if ip.next_header() != smoltcp::wire::IpProtocol::Tcp {
        return None;
    }
    let tcp = TcpPacket::new_checked(ip.payload()).ok()?;
    Some(Packet {
        syn: tcp.syn(),
        ack: tcp.ack(),
        fin: tcp.fin(),
        rst: tcp.rst(),
        payload: !tcp.payload().is_empty(),
        source: tcp.src_port(),
        destination: tcp.dst_port(),
    })
}
#[derive(Default, Debug)]
struct Counts {
    packets: usize,
    syn: usize,
    syn_ack: usize,
    ack_only: usize,
    payload: usize,
    rst: usize,
    fin: usize,
}
impl Counts {
    fn add(&mut self, p: Packet) {
        self.packets += 1;
        self.syn += usize::from(p.syn && !p.ack);
        self.syn_ack += usize::from(p.syn && p.ack);
        self.ack_only += usize::from(p.ack && !p.syn && !p.fin && !p.rst && !p.payload);
        self.payload += usize::from(p.payload);
        self.rst += usize::from(p.rst);
        self.fin += usize::from(p.fin);
    }
}
struct State {
    rx: Counts,
    tx: Counts,
    ack_ports: [bool; 4225],
    unique_ack: usize,
    prepare: Option<Instant>,
    prepare_count: usize,
    prepare_total: Duration,
    prepare_max: Duration,
}
impl Default for State {
    fn default() -> Self {
        Self {
            rx: Counts::default(),
            tx: Counts::default(),
            ack_ports: [false; 4225],
            unique_ack: 0,
            prepare: None,
            prepare_count: 0,
            prepare_total: Duration::ZERO,
            prepare_max: Duration::ZERO,
        }
    }
}
#[derive(Default)]
pub(super) struct IngressProfile(Mutex<State>);
impl IngressProfile {
    pub(super) fn begin_prepare(&self) {
        self.0.lock().unwrap().prepare = Some(Instant::now());
    }
    pub(super) fn end_prepare(&self) {
        let mut s = self.0.lock().unwrap();
        if let Some(mark) = s.prepare.take() {
            let elapsed = mark.elapsed();
            s.prepare_count += 1;
            s.prepare_total += elapsed;
            s.prepare_max = s.prepare_max.max(elapsed);
        }
    }
    pub(super) fn staged(&self, p: Option<Packet>) {
        let Some(p) = p else { return };
        let mut s = self.0.lock().unwrap();
        s.rx.add(p);
        if p.destination == 10_000 && p.ack && !p.syn && !p.rst && !p.fin {
            if let Some(i) = p
                .source
                .checked_sub(40_000)
                .map(usize::from)
                .filter(|i| *i < 4225)
            {
                if !s.ack_ports[i] {
                    s.ack_ports[i] = true;
                    s.unique_ack += 1;
                }
            }
        }
    }
    pub(super) fn produced(&self, bytes: &[u8]) {
        if let Some(p) = packet(bytes) {
            self.0.lock().unwrap().tx.add(p);
        }
    }
    pub(super) fn boundary(&self, name: &str) {
        let s = self.0.lock().unwrap();
        eprintln!(
            "F wire {name}: staged={:?} produced={:?} unique_ack_ports={} prepare_count={} prepare_total={:?} prepare_max={:?} prepare_open={}",
            s.rx,
            s.tx,
            s.unique_ack,
            s.prepare_count,
            s.prepare_total,
            s.prepare_max,
            s.prepare.is_some()
        );
    }
}
