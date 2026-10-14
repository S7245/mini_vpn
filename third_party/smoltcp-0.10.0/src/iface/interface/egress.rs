//! Single socket dispatch shared by full and explicitly covered egress polls.
//! Interface owns protocol state; this child borrows sockets/device tokens and
//! owns no queue or scheduling ledger. Exhaustion ends the current traversal.
//! Callers still own poll/flush/lifecycle ordering. Tests: tests/egress.rs.
use super::*;
use crate::iface::socket_set::Item;

impl Interface {
    pub(super) fn socket_egress<D>(&mut self, device: &mut D, sockets: &mut SocketSet<'_>) -> bool
    where
        D: Device + ?Sized,
    {
        let _caps = device.capabilities();
        let mut emitted_any = false;
        for item in sockets.items_mut() {
            if !self.socket_egress_item(device, item, &mut emitted_any) {
                break;
            }
        }
        emitted_any
    }

    pub(super) fn socket_egress_targets<D, I>(
        &mut self,
        device: &mut D,
        sockets: &mut SocketSet<'_>,
        targets: I,
    ) -> bool
    where
        D: Device + ?Sized,
        I: Iterator<Item = crate::iface::SocketHandle>,
    {
        let _caps = device.capabilities();
        let mut emitted_any = false;
        for handle in targets {
            // poll prevalidates before ingress; no socket is removed during poll.
            let Some(item) = sockets.item_mut(handle) else {
                continue;
            };
            if !self.socket_egress_item(device, item, &mut emitted_any) {
                break;
            }
        }
        emitted_any
    }

    fn socket_egress_item<D>(
        &mut self,
        device: &mut D,
        item: &mut Item<'_>,
        emitted_any: &mut bool,
    ) -> bool
    where
        D: Device + ?Sized,
    {
        enum EgressError {
            Exhausted,
            Dispatch(DispatchError),
        }
        if !item
            .meta
            .egress_permitted(self.inner.now, |ip_addr| self.inner.has_neighbor(&ip_addr))
        {
            return true;
        }

        let mut neighbor_addr = None;
        let mut respond = |inner: &mut InterfaceInner, meta: PacketMeta, response: IpPacket| {
            neighbor_addr = Some(response.ip_repr().dst_addr());
            let t = device.transmit(inner.now).ok_or_else(|| {
                net_debug!("failed to transmit IP: device exhausted");
                EgressError::Exhausted
            })?;

            inner
                .dispatch_ip(t, meta, response, &mut self.fragmenter)
                .map_err(EgressError::Dispatch)?;

            *emitted_any = true;

            Ok(())
        };

        let result = match &mut item.socket {
            #[cfg(feature = "socket-raw")]
            Socket::Raw(socket) => socket.dispatch(&mut self.inner, |inner, response| {
                respond(inner, PacketMeta::default(), IpPacket::Raw(response))
            }),
            #[cfg(feature = "socket-icmp")]
            Socket::Icmp(socket) => {
                socket.dispatch(&mut self.inner, |inner, response| match response {
                    #[cfg(feature = "proto-ipv4")]
                    (IpRepr::Ipv4(ipv4_repr), IcmpRepr::Ipv4(icmpv4_repr)) => respond(
                        inner,
                        PacketMeta::default(),
                        IpPacket::Icmpv4((ipv4_repr, icmpv4_repr)),
                    ),
                    #[cfg(feature = "proto-ipv6")]
                    (IpRepr::Ipv6(ipv6_repr), IcmpRepr::Ipv6(icmpv6_repr)) => respond(
                        inner,
                        PacketMeta::default(),
                        IpPacket::Icmpv6((ipv6_repr, icmpv6_repr)),
                    ),
                    #[allow(unreachable_patterns)]
                    _ => unreachable!(),
                })
            }
            #[cfg(feature = "socket-udp")]
            Socket::Udp(socket) => socket.dispatch(&mut self.inner, |inner, meta, response| {
                respond(inner, meta, IpPacket::Udp(response))
            }),
            #[cfg(feature = "socket-tcp")]
            Socket::Tcp(socket) => socket.dispatch(&mut self.inner, |inner, response| {
                respond(inner, PacketMeta::default(), IpPacket::Tcp(response))
            }),
            #[cfg(feature = "socket-dhcpv4")]
            Socket::Dhcpv4(socket) => socket.dispatch(&mut self.inner, |inner, response| {
                respond(inner, PacketMeta::default(), IpPacket::Dhcpv4(response))
            }),
            #[cfg(feature = "socket-dns")]
            Socket::Dns(socket) => socket.dispatch(&mut self.inner, |inner, (ip, udp, payload)| {
                respond(
                    inner,
                    PacketMeta::default(),
                    IpPacket::Udp((ip, udp, payload)),
                )
            }),
        };

        match result {
            Err(EgressError::Exhausted) => return false, // Device buffer full.
            Err(EgressError::Dispatch(_)) => {
                // `NeighborCache` already takes care of rate limiting the neighbor discovery
                // requests from the socket. However, without an additional rate limiting
                // mechanism, we would spin on every socket that has yet to discover its
                // neighbor.
                item.meta.neighbor_missing(
                    self.inner.now,
                    neighbor_addr.expect("non-IP response packet"),
                );
            }
            Ok(()) => {}
        }
        true
    }
}
