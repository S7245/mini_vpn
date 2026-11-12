//! N4 combined-acceptance harness (D145, `docs/current/n4-design-20261109.md`).
//! Owns only test-side load: a multi-socket App driver with standard
//! load L (long echo TCP, periodic UDP, fake-IP DNS churn + short TCP) plus
//! event-loop lateness, task and inventory sampling. Owns no production
//! authority; the echo peer is `native_n4_peer`, cells live in `native_n4_*`
//! sibling leaves.
use super::*;
use fixture::Apps;
use smoltcp::socket::{tcp, udp};
use smoltcp::wire::Ipv4Address;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

pub(super) use native_n4_peer::{
    BULK_PORT, ECHO_PORT, HALF_CLOSE_PORT, Peer, RESET_PORT, SHORT_PORT, SLOW_TAIL_BYTES,
    SLOW_TAIL_PORT, UDP_PORT,
};
const ROUND: usize = 1024;
const UDP_LEN: usize = 200;

/// One long echo TCP flow: back-to-back 1 KiB rounds.
struct Long {
    handle: smoltcp::iface::SocketHandle,
    sent: bool,
    got: usize,
    started: Instant,
    /// (round start, round latency).
    rounds: Vec<(Instant, Duration)>,
    failures: usize,
}

/// One periodic UDP flow (same 4-tuple for the whole run).
struct Udp {
    handle: smoltcp::iface::SocketHandle,
    seq: u32,
    next: Instant,
    sent_at: std::collections::HashMap<u32, Instant>,
    /// (send time, round trip).
    rtts: Vec<(Instant, Duration)>,
    duplicates: usize,
    /// Payloads that are not this flow's own echo (late/foreign generation).
    foreign: usize,
    /// Their first bytes (F-N4a: distinguishes artifact from leak).
    foreign_heads: Vec<String>,
}

/// One DNS-resolved short TCP flow.
struct Short {
    handle: smoltcp::iface::SocketHandle,
    host: String,
    started: Instant,
    sent: bool,
    done: Option<Duration>,
    failed: bool,
}

/// One downlink bulk transfer requested from `BULK_PORT`.
pub(super) struct Bulk {
    pub(super) handle: smoltcp::iface::SocketHandle,
    want: usize,
    pub(super) got: usize,
    pub(super) reading: bool,
    requested: bool,
    started: Instant,
    pub(super) done: Option<Duration>,
    pub(super) corrupt: bool,
}

#[derive(Clone, Copy, Default)]
pub(super) struct LoadSpec {
    pub(super) long: usize,
    pub(super) udp: usize,
    pub(super) dns_every: Option<Duration>,
    /// DNS flood: `(every, per burst, total)`; answers open no short TCP.
    pub(super) dns_burst: Option<(Duration, usize, usize)>,
}

pub(super) struct Load {
    spec: LoadSpec,
    long: Vec<Long>,
    udp: Vec<Udp>,
    dns: Option<smoltcp::iface::SocketHandle>,
    dns_next: Instant,
    dns_sent: std::collections::HashMap<u16, (String, Instant, bool)>,
    dns_rtts: Vec<Duration>,
    dns_ids: u16,
    dns_servfail: usize,
    burst_sent: usize,
    shorts: Vec<Short>,
    short_ok: usize,
    short_failed: usize,
    pub(super) bulks: Vec<Bulk>,
    next_port: u16,
    pub(super) lateness: Vec<Duration>,
    last_turn: Option<Instant>,
}

fn tcp_socket(len: usize) -> tcp::Socket<'static> {
    tcp::Socket::new(
        tcp::SocketBuffer::new(vec![0; len]),
        tcp::SocketBuffer::new(vec![0; len]),
    )
}

fn udp_socket(packets: usize) -> udp::Socket<'static> {
    let buffer = || {
        udp::PacketBuffer::new(
            vec![udp::PacketMetadata::EMPTY; packets],
            vec![0; packets * 512],
        )
    };
    udp::Socket::new(buffer(), buffer())
}

fn dns_query(id: u16, name: &str) -> Vec<u8> {
    let mut q = vec![];
    q.extend_from_slice(&id.to_be_bytes());
    q.extend_from_slice(&[0x01, 0x00, 0, 1, 0, 0, 0, 0, 0, 0]);
    for label in name.split('.') {
        q.push(u8::try_from(label.len()).unwrap());
        q.extend_from_slice(label.as_bytes());
    }
    q.extend_from_slice(&[0, 0, 1, 0, 1]);
    q
}

impl Load {
    pub(super) fn new(apps: &mut Apps, spec: LoadSpec) -> Self {
        let mut load = Self {
            spec,
            long: vec![],
            udp: vec![],
            dns: None,
            dns_next: Instant::now(),
            dns_sent: Default::default(),
            dns_rtts: vec![],
            dns_ids: 0,
            dns_servfail: 0,
            burst_sent: 0,
            shorts: vec![],
            short_ok: 0,
            short_failed: 0,
            bulks: vec![],
            next_port: 42000,
            lateness: vec![],
            last_turn: None,
        };
        for _ in 0..spec.long {
            let handle = load.connect(apps, Ipv4Address::from(apps.target), ECHO_PORT, 8192);
            load.long.push(Long {
                handle,
                sent: false,
                got: 0,
                started: Instant::now(),
                rounds: vec![],
                failures: 0,
            });
        }
        for index in 0..spec.udp {
            let mut socket = udp_socket(16);
            socket.bind(43000 + u16::try_from(index).unwrap()).unwrap();
            load.udp.push(Udp {
                handle: apps.sockets.add(socket),
                seq: 0,
                next: Instant::now(),
                sent_at: Default::default(),
                rtts: vec![],
                duplicates: 0,
                foreign: 0,
                foreign_heads: Vec::new(),
            });
        }
        if spec.dns_every.is_some() || spec.dns_burst.is_some() {
            let mut socket = udp_socket(if spec.dns_burst.is_some() { 512 } else { 16 });
            socket.bind(45000).unwrap();
            load.dns = Some(apps.sockets.add(socket));
        }
        load
    }

    pub(super) fn connect(
        &mut self,
        apps: &mut Apps,
        ip: Ipv4Address,
        port: u16,
        buffer: usize,
    ) -> smoltcp::iface::SocketHandle {
        let mut socket = tcp_socket(buffer);
        self.next_port = self.next_port.checked_add(1).unwrap();
        socket
            .connect(apps.iface.context(), (ip, port), self.next_port)
            .unwrap();
        apps.sockets.add(socket)
    }

    /// One App turn: drive every flow, then poll the stack.
    pub(super) async fn turn(&mut self, apps: &mut Apps) {
        let now = Instant::now();
        if let Some(last) = self.last_turn {
            // Expected cadence is the caller's 5ms sleep; the excess is loop stall.
            self.lateness
                .push(now.saturating_duration_since(last + Duration::from_millis(5)));
        }
        for long in &mut self.long {
            let socket = apps.sockets.get_mut::<tcp::Socket>(long.handle);
            if !socket.is_active() || matches!(socket.state(), tcp::State::CloseWait) {
                long.failures += 1;
                socket.abort();
                self.next_port = self.next_port.checked_add(1).unwrap();
                let target = (Ipv4Address::from(apps.target), ECHO_PORT);
                socket
                    .connect(apps.iface.context(), target, self.next_port)
                    .unwrap();
                (long.sent, long.got) = (false, 0);
                continue;
            }
            if !long.sent
                && socket.can_send()
                && socket.send_capacity() - socket.send_queue() >= ROUND
            {
                socket.send_slice(&[0x5a; ROUND]).unwrap();
                (long.sent, long.got, long.started) = (true, 0, now);
            }
            while socket.can_recv() {
                let mut b = [0u8; 2048];
                long.got += socket.recv_slice(&mut b).unwrap();
            }
            if long.sent && long.got >= ROUND {
                long.rounds.push((long.started, now - long.started));
                long.sent = false;
            }
        }
        for (index, flow) in self.udp.iter_mut().enumerate() {
            let socket = apps.sockets.get_mut::<udp::Socket>(flow.handle);
            while socket.can_recv() {
                let (b, _) = socket.recv().unwrap();
                if b.len() != UDP_LEN || usize::from(b[4]) != index {
                    flow.foreign += 1;
                    let head = &b[..b.len().min(24)];
                    flow.foreign_heads
                        .push(String::from_utf8_lossy(head).into_owned());
                    println!(
                        "N4 foreign flow={index} len={} head={:?}",
                        b.len(),
                        String::from_utf8_lossy(head)
                    );
                    continue;
                }
                let seq = u32::from_be_bytes(b[..4].try_into().unwrap());
                match flow.sent_at.remove(&seq) {
                    Some(at) => flow.rtts.push((at, now - at)),
                    None => flow.duplicates += 1,
                }
            }
            if now >= flow.next {
                let mut payload = [0u8; UDP_LEN];
                payload[..4].copy_from_slice(&flow.seq.to_be_bytes());
                payload[4] = u8::try_from(index).unwrap();
                let target = (Ipv4Address::from(apps.target), UDP_PORT);
                if socket.send_slice(&payload, target).is_ok() {
                    flow.sent_at.insert(flow.seq, now);
                }
                flow.seq += 1;
                flow.next = now + Duration::from_millis(50);
            }
        }
        self.dns_turn(apps, now);
        self.bulk_turn(apps, now);
        apps.turn().await;
        self.last_turn = Some(Instant::now());
    }

    fn dns_turn(&mut self, apps: &mut Apps, now: Instant) {
        let Some(dns) = self.dns else { return };
        let mut answered = vec![];
        {
            let socket = apps.sockets.get_mut::<udp::Socket>(dns);
            while socket.can_recv() {
                let (b, _) = socket.recv().unwrap();
                let id = u16::from_be_bytes([b[0], b[1]]);
                let rcode = b[3] & 0x0f;
                if let Some((host, at, short)) = self.dns_sent.remove(&id) {
                    self.dns_rtts.push(now - at);
                    self.dns_servfail += usize::from(rcode != 0);
                    if short && rcode == 0 && b.len() >= 4 {
                        let ip = &b[b.len() - 4..];
                        answered.push((host, Ipv4Address::new(ip[0], ip[1], ip[2], ip[3])));
                    }
                }
            }
            if self.spec.dns_every.is_some_and(|_| now >= self.dns_next) {
                self.dns_ids = self.dns_ids.wrapping_add(1);
                let host = format!("n4-{}.example", self.dns_ids);
                let resolver = (Ipv4Address::new(8, 8, 8, 8), 53);
                if socket
                    .send_slice(&dns_query(self.dns_ids, &host), resolver)
                    .is_ok()
                {
                    self.dns_sent.insert(self.dns_ids, (host, now, true));
                }
                self.dns_next = now + self.spec.dns_every.unwrap();
            }
            if let Some((every, per, total)) = self.spec.dns_burst {
                if now >= self.dns_next && self.burst_sent < total {
                    for _ in 0..per.min(total - self.burst_sent) {
                        self.dns_ids = self.dns_ids.wrapping_add(1);
                        let host = format!("n4b-{}.example", self.dns_ids);
                        let resolver = (Ipv4Address::new(8, 8, 8, 8), 53);
                        // A full App socket is an App-side refusal, counted unsent.
                        if socket
                            .send_slice(&dns_query(self.dns_ids, &host), resolver)
                            .is_err()
                        {
                            break;
                        }
                        self.dns_sent.insert(self.dns_ids, (host, now, false));
                        self.burst_sent += 1;
                    }
                    self.dns_next = now + every;
                }
            }
        }
        for (host, ip) in answered {
            let handle = self.connect(apps, ip, SHORT_PORT, 1024);
            self.shorts.push(Short {
                handle,
                host,
                started: now,
                sent: false,
                done: None,
                failed: false,
            });
        }
        for short in self
            .shorts
            .iter_mut()
            .filter(|s| s.done.is_none() && !s.failed)
        {
            let socket = apps.sockets.get_mut::<tcp::Socket>(short.handle);
            if short.sent && socket.state() == tcp::State::Closed {
                // Typed refusal/reset of the short flow (counted, not retried).
                short.failed = true;
                self.short_failed += 1;
                continue;
            }
            if !short.sent && socket.can_send() {
                short.sent = socket.send_slice(b"short").is_ok();
            }
            if socket.can_recv() {
                let mut b = [0u8; 64];
                let _ = socket.recv_slice(&mut b);
                short.done = Some(now - short.started);
                self.short_ok += 1;
                socket.close();
            }
        }
        // Closed short sockets leave the set so the run stays bounded.
        let sockets = &mut apps.sockets;
        self.shorts.retain(|s| {
            let closed = (s.done.is_some() || s.failed)
                && sockets.get::<tcp::Socket>(s.handle).state() == tcp::State::Closed;
            if closed {
                sockets.remove(s.handle);
            }
            !closed
        });
    }

    /// Request `bytes` of downlink from `BULK_PORT` on a socket of `buffer`.
    pub(super) fn bulk(&mut self, apps: &mut Apps, bytes: usize, buffer: usize) -> usize {
        let handle = self.connect(apps, Ipv4Address::from(apps.target), BULK_PORT, buffer);
        self.bulks.push(Bulk {
            handle,
            want: bytes,
            got: 0,
            reading: true,
            requested: false,
            started: Instant::now(),
            done: None,
            corrupt: false,
        });
        self.bulks.len() - 1
    }

    fn bulk_turn(&mut self, apps: &mut Apps, now: Instant) {
        for bulk in &mut self.bulks {
            let socket = apps.sockets.get_mut::<tcp::Socket>(bulk.handle);
            if !bulk.requested && socket.can_send() {
                let want = u64::try_from(bulk.want).unwrap().to_be_bytes();
                bulk.requested = socket.send_slice(&want).is_ok();
                bulk.started = now;
            }
            while bulk.reading && socket.can_recv() {
                let mut b = [0u8; 16 * 1024];
                let n = socket.recv_slice(&mut b).unwrap();
                let base = bulk.got;
                // Peer pattern: byte k of the stream is (k mod 64Ki) mod 251.
                bulk.corrupt |= b[..n]
                    .iter()
                    .enumerate()
                    .any(|(i, v)| usize::from(*v) != (base + i) % (64 * 1024) % 251);
                bulk.got += n;
            }
            if bulk.done.is_none() && bulk.got >= bulk.want {
                bulk.done = Some(now - bulk.started);
                socket.close();
            }
        }
    }

    /// Hold the DNS schedule (periodic and burst) until `until`.
    pub(super) fn hold_dns(&mut self, until: Instant) {
        self.dns_next = until;
    }

    /// App-side RST of the first `n` long flows; they reconnect next turn
    /// without counting as failures (App cancellation, M4(b)).
    pub(super) fn reset_longs(&mut self, apps: &mut Apps, n: usize) {
        for long in self.long.iter_mut().take(n) {
            let socket = apps.sockets.get_mut::<tcp::Socket>(long.handle);
            socket.abort();
            self.next_port = self.next_port.checked_add(1).unwrap();
            let target = (Ipv4Address::from(apps.target), ECHO_PORT);
            socket
                .connect(apps.iface.context(), target, self.next_port)
                .unwrap();
            (long.sent, long.got) = (false, 0);
        }
    }

    /// First long round started at/after `at`: completion − `at`.
    pub(super) fn first_round_after(&self, at: Instant) -> Option<Duration> {
        self.long
            .iter()
            .flat_map(|l| l.rounds.iter())
            .filter(|(start, _)| *start >= at)
            .map(|(start, d)| *start + *d - at)
            .min()
    }

    /// First long round completing at/after `at` (surviving in-flight rounds
    /// included): completion − `at`.
    pub(super) fn first_completion_after(&self, at: Instant) -> Option<Duration> {
        self.long
            .iter()
            .flat_map(|l| l.rounds.iter())
            .map(|(start, d)| *start + *d)
            .filter(|done| *done >= at)
            .map(|done| done - at)
            .min()
    }

    /// First UDP datagram sent at/after `at` whose echo returned: reply − `at`.
    /// Heads of every non-echo payload delivered to any UDP flow, in order.
    pub(super) fn foreign_heads(&self) -> Vec<String> {
        self.udp
            .iter()
            .flat_map(|u| u.foreign_heads.iter().cloned())
            .collect()
    }
    pub(super) fn first_udp_after(&self, at: Instant) -> Option<Duration> {
        self.udp
            .iter()
            .flat_map(|u| u.rtts.iter())
            .filter(|(sent, _)| *sent >= at)
            .map(|(sent, d)| *sent + *d - at)
            .min()
    }

    /// Long-round latency percentiles for rounds started in `[from, to)`.
    pub(super) fn rounds_between(&self, from: Instant, to: Instant) -> (usize, [Duration; 4]) {
        let mut v: Vec<_> = self
            .long
            .iter()
            .flat_map(|l| l.rounds.iter())
            .filter(|(start, _)| *start >= from && *start < to)
            .map(|r| r.1)
            .collect();
        (v.len(), percentiles(&mut v))
    }

    /// Run the load for `span` (callers sample between calls).
    pub(super) async fn run_for(&mut self, apps: &mut Apps, span: Duration) {
        let start = Instant::now();
        while start.elapsed() < span {
            self.turn(apps).await;
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    pub(super) fn report(&self) -> LoadReport {
        let mut rounds: Vec<_> = self
            .long
            .iter()
            .flat_map(|l| l.rounds.iter().map(|r| r.1))
            .collect();
        let mut udp: Vec<_> = self
            .udp
            .iter()
            .flat_map(|u| u.rtts.iter().map(|r| r.1))
            .collect();
        let udp_sent: u32 = self.udp.iter().map(|u| u.seq).sum();
        // In-flight datagrams sent in the last 500ms are not counted as lost.
        let cutoff = Instant::now() - Duration::from_millis(500);
        let udp_lost = self
            .udp
            .iter()
            .flat_map(|u| u.sent_at.values())
            .filter(|at| **at < cutoff)
            .count();
        let mut dns = self.dns_rtts.clone();
        LoadReport {
            rounds: rounds.len(),
            round_failures: self.long.iter().map(|l| l.failures).sum(),
            round_p: percentiles(&mut rounds),
            udp_sent: udp_sent as usize,
            udp_lost,
            udp_duplicates: self.udp.iter().map(|u| u.duplicates).sum(),
            udp_foreign: self.udp.iter().map(|u| u.foreign).sum(),
            udp_p: percentiles(&mut udp),
            dns_sent: usize::from(self.dns_ids),
            dns_answered: dns.len(),
            dns_unanswered: self.dns_sent.len(),
            dns_p: percentiles(&mut dns),
            dns_servfail: self.dns_servfail,
            shorts_open: self
                .shorts
                .iter()
                .filter(|s| s.done.is_none() && !s.failed)
                .count(),
            short_ok: self.short_ok,
            short_failed: self.short_failed,
            late_p: percentiles(&mut self.lateness.clone()),
        }
    }

    pub(super) fn short_hosts_pending(&self) -> Vec<String> {
        self.shorts
            .iter()
            .filter(|s| s.done.is_none())
            .map(|s| s.host.clone())
            .collect()
    }
}

/// (P50, P95, P99, max).
pub(super) fn percentiles(values: &mut [Duration]) -> [Duration; 4] {
    if values.is_empty() {
        return [Duration::ZERO; 4];
    }
    values.sort();
    let at = |p: usize| values[(values.len() - 1) * p / 100];
    [at(50), at(95), at(99), values[values.len() - 1]]
}

#[derive(Debug)]
pub(super) struct LoadReport {
    pub(super) rounds: usize,
    pub(super) round_failures: usize,
    pub(super) round_p: [Duration; 4],
    pub(super) udp_sent: usize,
    pub(super) udp_lost: usize,
    pub(super) udp_duplicates: usize,
    pub(super) udp_foreign: usize,
    pub(super) udp_p: [Duration; 4],
    pub(super) dns_sent: usize,
    pub(super) dns_answered: usize,
    pub(super) dns_unanswered: usize,
    pub(super) dns_p: [Duration; 4],
    pub(super) dns_servfail: usize,
    pub(super) shorts_open: usize,
    pub(super) short_ok: usize,
    pub(super) short_failed: usize,
    pub(super) late_p: [Duration; 4],
}

/// Session/flow/budget/task census at one instant.
#[derive(Debug, Clone, Copy, Default)]
pub(super) struct Census {
    pub(super) q: bool,
    pub(super) first_failure: bool,
    pub(super) live: usize,
    pub(super) running: usize,
    pub(super) stopped_producers: usize,
    pub(super) settled_providers: usize,
    pub(super) opening: usize,
    pub(super) waiting: usize,
    pub(super) budget_used: usize,
    pub(super) tasks: usize,
}

pub(super) fn census(saved: &Control, budget: &bytes::BackingBudget, peer: &Peer) -> Census {
    let tasks = tokio::runtime::Handle::current()
        .metrics()
        .num_alive_tasks();
    let mut out = Census {
        budget_used: budget.used(),
        tasks: tasks.saturating_sub(peer.live.load(Ordering::SeqCst) + 1),
        ..Census::default()
    };
    if let Some(control) = native_waiting::session(saved) {
        let ledger = control.ledger();
        (out.q, out.first_failure) = (ledger.quarantined(), ledger.first_failure.is_some());
        drop(ledger);
        if let Some(f) = control.flow_observation() {
            out.live = f.live;
            out.running = f.running;
            out.stopped_producers = f.stopped_producers;
            out.settled_providers = f.settled_providers;
            out.opening = f.opening_providers;
            out.waiting = f.waiting;
        }
    }
    out
}
