//! One requested cold-control observation, never a per-packet production audit.
//! Holds only Copy geometry. Original SocketSet/ctx/registry retain every value.
use super::RelayEventTestBridge;
use crate::client_tun::{SocketCtx, SocketHandle, ListenerRegistry};
use smoltcp::iface::{SocketSet, SocketStorage};
use smoltcp::socket::Socket;
use std::collections::{HashMap, HashSet};

#[derive(Clone, Copy, Debug)]
pub(in crate::client_tun) struct Snapshot {
    pub sockets: usize,
    pub rx_bytes: usize,
    pub tx_bytes: usize,
    pub storage_len: usize,
    pub storage_capacity: usize,
    pub storage_bytes: usize,
    pub contexts: usize,
    pub context_capacity: usize,
    pub context_pair: (usize, usize),
    pub registry: crate::client_tun::listener_registry::tests::capacity::RegistryLayout,
    pub registry_handles: usize,
    pub ports: usize,
}
impl RelayEventTestBridge {
    pub(in crate::client_tun) fn request_capacity(&self) {
        let mut probe = self.capacity_probe.lock().unwrap();
        assert!(probe.is_none());
        *probe = Some(None);
    }
    pub(in crate::client_tun) fn take_capacity(&self) -> Option<Snapshot> {
        let mut probe = self.capacity_probe.lock().unwrap();
        let value = (*probe).flatten()?;
        *probe = None;
        Some(value)
    }
    pub(in crate::client_tun) fn observe_capacity(
        &self, sockets: &SocketSet<'_>, contexts: &HashMap<SocketHandle, SocketCtx>, registry: &ListenerRegistry,
    ) {
        let mut probe = self.capacity_probe.lock().unwrap();
        if !matches!(*probe, Some(None)) { return; }
        // This harness-only bounded set validates both directions without
        // assigning ownership or charging observer storage as product inventory.
        let mut registered = HashSet::new();
        for handle in registry.all_handles() { assert!(registered.insert(handle)); }
        let mut snapshot = Snapshot {
            sockets: 0, rx_bytes: 0, tx_bytes: 0,
            storage_len: sockets.storage_geometry().0,
            storage_capacity: sockets.storage_geometry().1,
            storage_bytes: std::alloc::Layout::array::<SocketStorage<'_>>(sockets.storage_geometry().1).unwrap().size(),
            contexts: contexts.len(), context_capacity: contexts.capacity(),
            context_pair: (std::alloc::Layout::new::<(SocketHandle, SocketCtx)>().size(),
                std::alloc::Layout::new::<(SocketHandle, SocketCtx)>().align()),
            registry: registry.allocation_layout_for_test(),
            registry_handles: registry.total_handles(), ports: registry.port_count(),
        };
        assert_eq!(registered.len(), snapshot.registry_handles);
        for (handle, socket) in sockets.iter() {
            let Socket::Tcp(tcp) = socket else { panic!("original TCP-only listener owner"); };
            assert!(contexts.contains_key(&handle) && registered.remove(&handle));
            assert!(sockets.try_get::<smoltcp::socket::tcp::Socket>(handle).is_some());
            assert_eq!((tcp.recv_capacity(), tcp.send_capacity()), (65535, 65535));
            snapshot.sockets += 1;
            snapshot.rx_bytes += tcp.recv_capacity();
            snapshot.tx_bytes += tcp.send_capacity();
        }
        assert!(registered.is_empty());
        assert_eq!(snapshot.sockets, snapshot.contexts);
        *probe = Some(Some(snapshot));
    }
}
