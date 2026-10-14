//! Transfers actual Interface input effects into the original dirty set.
//! Interface retains notifications across cancellable flushes; this synchronous
//! handoff adds no flow authority or queue. Unknown/multiple input scans only at
//! that fallback edge, never per ordinary TCP packet. Entry: collect.
//! Tests: compatibility_targeted_ingress/finite_tx and ingress/tests.
use crate::client_tun::*;
use smoltcp::iface::IngressEffects;

pub(super) fn collect(
    iface: &mut Interface,
    sockets: &SocketSet<'_>,
    dirty: &mut HashSet<SocketHandle>,
    flow_mode: bool,
) {
    let effect = iface.take_ingress_effects(sockets);
    if !flow_mode {
        return;
    }
    match effect {
        IngressEffects::None => {}
        IngressEffects::Tcp(handle) => {
            dirty.insert(handle);
        }
        IngressEffects::FullFallback => {
            dirty.extend(sockets.iter().filter_map(|(handle, socket)| {
                matches!(socket, smoltcp::socket::Socket::Tcp(_)).then_some(handle)
            }));
        }
    }
}
