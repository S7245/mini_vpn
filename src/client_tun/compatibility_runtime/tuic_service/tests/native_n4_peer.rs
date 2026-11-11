//! N4 generic TUIC echo peer (D145): accepts every client connection, echoes
//! Connect streams and Packet datagrams for any target, with per-port
//! behaviours (half-close, slow tail, bulk, reset) and a shared `PeerLog`.
//! Owns only test-side peer state; split from `native_n4_harness` (size gate).
use super::*;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Peer behaviours selected by the Connect target port.
pub(super) const ECHO_PORT: u16 = 10000;
pub(super) const SHORT_PORT: u16 = 8080;
/// Reply once, then FIN while keeping the receive side open (F-N2a).
pub(super) const HALF_CLOSE_PORT: u16 = 10066;
/// Reply, then a `SLOW_TAIL_BYTES` pattern tail, then FIN (D147 slow reader).
pub(super) const SLOW_TAIL_PORT: u16 = 10067;
pub(super) const SLOW_TAIL_BYTES: usize = 96 * 1024;
/// The App sends an 8-byte big-endian length; the peer writes that many
/// pattern bytes, then FIN (M1 bulk, M5(ii) slow consumer, M6 budget).
pub(super) const BULK_PORT: u16 = 10068;
/// The peer resets the stream after the first request bytes (M5(i)).
pub(super) const RESET_PORT: u16 = 10070;
/// N4 review F2: RESET_STREAM `DELAYED_RESET_AFTER` after the CONNECT (a
/// remote close that still arrives while Node → peer is lost).
pub(super) const DELAYED_RESET_PORT: u16 = 10071;
pub(super) const DELAYED_RESET_AFTER: Duration = Duration::from_secs(6);
pub(super) const UDP_PORT: u16 = 10001;

/// Peer-side record shared with the scenario.
#[derive(Default)]
pub(super) struct PeerLog {
    pub(super) connections: usize,
    pub(super) targets: Vec<String>,
    pub(super) half_closed: usize,
    pub(super) resets: usize,
    /// Bulk bytes accepted by the peer's QUIC send side so far.
    pub(super) bulk_written: usize,
    /// Last client association id seen per peer connection (by accept order).
    pub(super) assoc: Vec<Option<u16>>,
}

pub(super) struct Peer {
    pub(super) log: Arc<Mutex<PeerLog>>,
    /// Live peer stream/datagram tasks (excluded from the Service task census).
    pub(super) live: Arc<AtomicUsize>,
    pub(super) conns: Arc<Mutex<Vec<quinn::Connection>>>,
    acceptor: tokio::task::JoinHandle<()>,
}

impl Peer {
    pub(super) fn start(server: quinn::Endpoint, cfg: TuicClientConfig) -> Self {
        let log = Arc::new(Mutex::new(PeerLog::default()));
        let live = Arc::new(AtomicUsize::new(0));
        let conns = Arc::new(Mutex::new(Vec::new()));
        let (l, n, c) = (log.clone(), live.clone(), conns.clone());
        let acceptor = tokio::spawn(async move {
            loop {
                let conn = native_waiting::accept(&server, &cfg).await;
                let index = {
                    let mut log = l.lock().unwrap();
                    log.connections += 1;
                    log.assoc.push(None);
                    log.connections - 1
                };
                c.lock().unwrap().push(conn.clone());
                spawn_counted(&n, serve_datagrams(conn.clone(), l.clone(), index));
                let (l2, n2) = (l.clone(), n.clone());
                spawn_counted(&n, async move {
                    while let Ok((tx, rx)) = conn.accept_bi().await {
                        spawn_counted(&n2, serve_stream(tx, rx, l2.clone()));
                    }
                });
            }
        });
        Self {
            log,
            live,
            conns,
            acceptor,
        }
    }
    pub(super) fn latest(&self) -> Option<quinn::Connection> {
        self.conns.lock().unwrap().last().cloned()
    }
    pub(super) fn stop(self) {
        self.acceptor.abort();
    }
}

fn spawn_counted(live: &Arc<AtomicUsize>, work: impl Future<Output = ()> + Send + 'static) {
    live.fetch_add(1, Ordering::SeqCst);
    let live = live.clone();
    tokio::spawn(async move {
        work.await;
        live.fetch_sub(1, Ordering::SeqCst);
    });
}

/// Parse a TUIC address at `pos`: (target, encoded length).
pub(super) fn parse_addr(buf: &[u8], pos: usize) -> Option<(TargetAddr, usize)> {
    let port = |at: usize| Some(u16::from_be_bytes([*buf.get(at)?, *buf.get(at + 1)?]));
    match *buf.get(pos)? {
        0x01 => {
            let ip: [u8; 4] = buf.get(pos + 1..pos + 5)?.try_into().ok()?;
            let addr = std::net::SocketAddr::from((ip, port(pos + 5)?));
            Some((TargetAddr::IpPort(addr), 7))
        }
        0x00 => {
            let len = usize::from(*buf.get(pos + 1)?);
            let host = String::from_utf8(buf.get(pos + 2..pos + 2 + len)?.to_vec()).ok()?;
            let port = port(pos + 2 + len)?;
            Some((TargetAddr::DomainPort { host, port }, 4 + len))
        }
        _ => None,
    }
}

fn target_port(target: &TargetAddr) -> u16 {
    match target {
        TargetAddr::IpPort(addr) => addr.port(),
        TargetAddr::DomainPort { port, .. } => *port,
    }
}

async fn serve_stream(
    mut tx: quinn::SendStream,
    mut rx: quinn::RecvStream,
    log: Arc<Mutex<PeerLog>>,
) {
    let mut head = Vec::new();
    let target = loop {
        let mut b = [0u8; 64];
        match rx.read(&mut b).await {
            Ok(Some(n)) => head.extend_from_slice(&b[..n]),
            _ => return,
        }
        if let Some((target, len)) = parse_addr(&head, 2) {
            head.drain(..2 + len);
            break target;
        }
    };
    log.lock().unwrap().targets.push(format!("{target:?}"));
    let port = target_port(&target);
    if port == DELAYED_RESET_PORT {
        tokio::time::sleep(DELAYED_RESET_AFTER).await;
    }
    if port == RESET_PORT || port == DELAYED_RESET_PORT {
        let _ = tx.reset(1u32.into());
        let _ = rx.stop(1u32.into());
        log.lock().unwrap().resets += 1;
        return;
    }
    if port == BULK_PORT {
        return serve_bulk(tx, rx, head, log).await;
    }
    let half = matches!(port, HALF_CLOSE_PORT | SLOW_TAIL_PORT);
    let mut pending = head;
    loop {
        if !pending.is_empty() {
            if port == SLOW_TAIL_PORT {
                pending.extend((0..SLOW_TAIL_BYTES).map(|i| (i % 251) as u8));
            }
            if tx.write_all(&pending).await.is_err() {
                return;
            }
            pending.clear();
            if half {
                let _ = tx.finish();
                log.lock().unwrap().half_closed += 1;
                // Keep the receive side open until the App side ends it.
                let mut sink = [0u8; 1024];
                while let Ok(Some(_)) = rx.read(&mut sink).await {}
                return;
            }
        }
        let mut b = [0u8; 4096];
        match rx.read(&mut b).await {
            Ok(Some(n)) => pending.extend_from_slice(&b[..n]),
            _ => {
                let _ = tx.finish();
                return;
            }
        }
    }
}

async fn serve_bulk(
    mut tx: quinn::SendStream,
    mut rx: quinn::RecvStream,
    mut head: Vec<u8>,
    log: Arc<Mutex<PeerLog>>,
) {
    while head.len() < 8 {
        let mut b = [0u8; 64];
        match rx.read(&mut b).await {
            Ok(Some(n)) => head.extend_from_slice(&b[..n]),
            _ => return,
        }
    }
    let total = usize::try_from(u64::from_be_bytes(head[..8].try_into().unwrap())).unwrap();
    let chunk: Vec<u8> = (0..64 * 1024).map(|i| (i % 251) as u8).collect();
    let mut sent = 0;
    while sent < total {
        let n = chunk.len().min(total - sent);
        if tx.write_all(&chunk[..n]).await.is_err() {
            return;
        }
        sent += n;
        log.lock().unwrap().bulk_written += n;
    }
    let _ = tx.finish();
    let mut sink = [0u8; 1024];
    while let Ok(Some(_)) = rx.read(&mut sink).await {}
}

async fn serve_datagrams(conn: quinn::Connection, log: Arc<Mutex<PeerLog>>, index: usize) {
    while let Ok(datagram) = conn.read_datagram().await {
        let Some(meta) = crate::tuic::decode_packet_meta(&datagram) else {
            continue;
        };
        log.lock().unwrap().assoc[index] = Some(meta.assoc_id);
        let Some((target, _)) = parse_addr(&datagram, 10) else {
            continue;
        };
        let reply = encode_packet(meta.assoc_id, &target, meta.data);
        if conn.send_datagram(reply.into()).is_err() {
            return;
        }
    }
}
