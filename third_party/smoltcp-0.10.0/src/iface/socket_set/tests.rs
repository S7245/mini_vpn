//! Checked access validates the actual private slot identity, not its type alone.
use super::*;

#[test]
fn checked_lookup_refuses_mismatched_physical_handle() {
    use crate::socket::tcp::{Socket as TcpSocket, SocketBuffer};
    let mut sockets = SocketSet::new(Vec::new());
    let handle = sockets.add(TcpSocket::new(
        SocketBuffer::new(vec![0; 32]), SocketBuffer::new(vec![0; 32]),
    ));
    assert!(sockets.try_get::<TcpSocket>(handle).is_some());
    sockets.item_mut(handle).unwrap().meta.handle = SocketHandle(handle.0 + 1);
    // The real storage and TCP value still exist; neither proves this handle.
    assert!(sockets.item(handle).is_some());
    assert!(TcpSocket::downcast(&sockets.item(handle).unwrap().socket).is_some());
    assert!(sockets.try_get::<TcpSocket>(handle).is_none());
    sockets.item_mut(handle).unwrap().meta.handle = handle;
    assert!(sockets.try_get::<TcpSocket>(handle).is_some());
}

#[test]
fn storage_geometry_retains_owned_spare_and_borrowed_slots_after_removal() {
    use crate::socket::tcp::{Socket as TcpSocket, SocketBuffer};
    let slots = Vec::with_capacity(17);
    let capacity = slots.capacity();
    let mut owned = SocketSet::new(slots);
    assert_eq!(owned.storage_geometry(), (0, capacity));
    let handle = owned.add(TcpSocket::new(SocketBuffer::new(vec![0; 7]), SocketBuffer::new(vec![0; 11])));
    assert_eq!(owned.storage_geometry(), (1, capacity));
    drop(owned.remove(handle));
    assert_eq!(owned.iter().count(), 0);
    assert_eq!(owned.storage_geometry(), (1, capacity));
    let mut slots = [SocketStorage::EMPTY, SocketStorage::EMPTY];
    let borrowed = SocketSet::new(&mut slots[..]);
    assert_eq!(borrowed.storage_geometry(), (2, 2));
}
