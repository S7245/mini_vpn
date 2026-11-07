//! Original listener owner geometry; these are real table/Vec capacities.
use super::super::*;
#[derive(Clone, Copy, Debug)]
pub(in crate::client_tun) struct RegistryLayout {
    pub ports: usize,
    pub port_capacity: usize,
    pub pair_size: usize,
    pub pair_align: usize,
    pub handles: usize,
    pub handle_capacity: usize,
    pub largest_handle_capacity: usize,
    pub handle_bytes: usize,
    pub pool_size: usize,
    pub max_total: usize,
}
impl ListenerRegistry {
    pub(in crate::client_tun) fn allocation_layout_for_test(&self) -> RegistryLayout {
        let pair = std::alloc::Layout::new::<(u16, Vec<SocketHandle>)>();
        let mut result = RegistryLayout {
            ports: self.ports.len(), port_capacity: self.ports.capacity(),
            pair_size: pair.size(), pair_align: pair.align(), handles: 0,
            handle_capacity: 0, largest_handle_capacity: 0, handle_bytes: 0,
            pool_size: self.pool_size, max_total: self.max_total,
        };
        for handles in self.ports.values() {
            result.handles = result.handles.checked_add(handles.len()).unwrap();
            result.handle_capacity = result.handle_capacity.checked_add(handles.capacity()).unwrap();
            result.handle_bytes = result.handle_bytes.checked_add(
                std::alloc::Layout::array::<SocketHandle>(handles.capacity()).unwrap().size()).unwrap();
            result.largest_handle_capacity = result.largest_handle_capacity.max(handles.capacity());
        }
        assert_eq!(result.handles, self.total);
        result
    }
}

#[test]
fn actual_listener_max_ports_and_slots_keep_complete_geometry_on_refusal() {
    // Same 4352-slot/64-port bound and legal full-size payload buffers; selecting
    // 68 initial listeners per port reaches both independent caps together.
    let mut registry = ListenerRegistry::with_socket_buffers_and_max_total(
        68, TcpSocketBufferConfig { rx_bytes: 65535, tx_bytes: 65535, receive_window_limit: None }, 4352);
    let mut sockets = SocketSet::new(Vec::new());
    let mut contexts = HashMap::new();
    for port in 1..=64 { registry.ensure_port(port, &mut sockets, &mut contexts).unwrap(); }
    let actual = registry.allocation_layout_for_test();
    assert_eq!((actual.ports, actual.handles, registry.total_handles()), (64, 4352, 4352));
    assert_eq!(sockets.iter().count(), contexts.len());
    assert_eq!(contexts.len(), 4352);
    let mut handles = std::collections::HashSet::new();
    for handle in registry.all_handles() {
        assert!(handles.insert(handle));
        assert!(contexts.contains_key(&handle));
        let socket = sockets.try_get::<TcpSocket>(handle).unwrap();
        assert_eq!((socket.recv_capacity(), socket.send_capacity()), (65535, 65535));
    }
    let before = (sockets.storage_geometry(), contexts.capacity(), actual.port_capacity, actual.handle_capacity);
    assert!(matches!(registry.ensure_port(65, &mut sockets, &mut contexts), Err(RegistryError::Capped)));
    assert!(matches!(registry.ensure_spare_listeners(1, 69, &mut sockets, &mut contexts), Err(RegistryError::Capped)));
    let after = registry.allocation_layout_for_test();
    assert_eq!(before, (sockets.storage_geometry(), contexts.capacity(), after.port_capacity, after.handle_capacity));
    assert_eq!((registry.total_handles(), contexts.len()), (4352, 4352));
    assert_eq!(registry.all_handles().collect::<std::collections::HashSet<_>>(), handles);
    let pair = std::alloc::Layout::new::<(SocketHandle, SocketCtx)>();
    eprintln!("[N1-listener-full-geometry] actual={actual:?} contexts={} context_capacity={} context_pair_size={} context_pair_align={} sockets={:?} payload_bytes={}",
        contexts.len(), contexts.capacity(), pair.size(), pair.align(), sockets.storage_geometry(), 4352 * 2 * 65535);
}
