//! Owns the external in-memory peer queue, bounds and wakeups. Device backing
//! and settlement remain with Device. Entries: push/pop/read_packet; lifecycle
//! characterization lives in loopback/tests and loopback/native/tests.
use super::*;

/// 单向 IP 包管道（一端 push，另一端 pop + await）。内部 `Arc` 共享，可廉价 clone。
#[derive(Clone, Default)]
pub struct PacketLink {
    pub(super) queue: Arc<Mutex<VecDeque<BytesMut>>>,
    pub(super) notify: Arc<Notify>,
    space: Arc<Notify>,
    pub(super) capacity_packets: Option<usize>,
    pub(super) dropped_packets: Arc<AtomicU64>,
    pub(super) high_water_packets: Arc<AtomicU64>,
}

impl PacketLink {
    pub(super) fn new() -> Self {
        Self::default()
    }
    pub(crate) fn bounded(capacity_packets: usize) -> Self {
        Self {
            capacity_packets: Some(capacity_packets.max(1)),
            ..Self::default()
        }
    }
    pub(crate) fn push(&self, pkt: BytesMut) -> bool {
        let mut queue = self.queue.lock().unwrap();
        if self
            .capacity_packets
            .is_some_and(|capacity| queue.len() >= capacity)
        {
            self.dropped_packets.fetch_add(1, Ordering::Relaxed);
            return false;
        }
        queue.push_back(pkt);
        self.high_water_packets
            .fetch_max(queue.len() as u64, Ordering::Relaxed);
        drop(queue);
        self.notify.notify_one();
        true
    }
    pub(super) fn pop(&self) -> Option<BytesMut> {
        let packet = self.queue.lock().unwrap().pop_front();
        if packet.is_some() {
            self.space.notify_waiters();
        }
        packet
    }
    pub(crate) fn dropped_packets(&self) -> u64 {
        self.dropped_packets.load(Ordering::Relaxed)
    }
    pub(crate) fn high_water_packets(&self) -> u64 {
        self.high_water_packets.load(Ordering::Relaxed)
    }
}

impl IngressPacketSource for PacketLink {
    const CUT_SAFE: bool = true;
    async fn read_packet(&mut self, slot: &mut PumpPacket) -> std::io::Result<SourceRead> {
        loop {
            if let Some(packet) = self.pop() {
                slot.adopt(packet);
                return Ok(SourceRead::Packet);
            }
            self.notify.notified().await;
        }
    }
}

mod native;
pub(super) use native::flush;
pub(super) fn native_report() -> native::Report {
    native::Report::default()
}
