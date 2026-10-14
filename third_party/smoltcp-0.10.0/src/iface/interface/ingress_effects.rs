//! Bounded notification custody for input actually consumed by this Interface.
//! One TCP handle is exact; any additional frame or unknown input collapses to
//! a conservative full scan. No tuple index, allocation, task or packet queue.
//! Poll keeps notifications until the owner takes them, including across flush
//! suspension and later full/timer polls. Tests: tests/ingress_effects.rs.
use super::*;
use crate::iface::SocketHandle;

/// Unconsumed readiness notifications, not packet acceptance or flow ownership.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IngressEffects {
    None,
    Tcp(SocketHandle),
    FullFallback,
}

impl IngressEffects {
    pub(super) fn merge(&mut self, next: Self) {
        if next != Self::None {
            *self = if *self == Self::None {
                next
            } else {
                Self::FullFallback
            };
        }
    }
}

impl Interface {
    /// Transfer saved input notifications to the caller's readiness owner.
    /// No poll clears them; cancellation of an async flush cannot lose custody.
    pub fn take_ingress_effects(&mut self, sockets: &SocketSet<'_>) -> IngressEffects {
        let effect = self.checked_ingress_effects(sockets);
        self.inner.ingress_effects = IngressEffects::None;
        self.inner.frame_ingress_effect = IngressEffects::None;
        effect
    }

    fn checked_ingress_effects(&self, sockets: &SocketSet<'_>) -> IngressEffects {
        if self.inner.frame_ingress_effect != IngressEffects::None {
            return IngressEffects::FullFallback;
        }
        match self.inner.ingress_effects {
            IngressEffects::Tcp(handle) => {
                let valid = sockets.item(handle).map_or(false, |item| {
                    #[cfg(feature = "socket-tcp")]
                    if matches!(item.socket, Socket::Tcp(_)) {
                        return true;
                    }
                    let _ = item;
                    false
                });
                if valid {
                    IngressEffects::Tcp(handle)
                } else {
                    IngressEffects::FullFallback
                }
            }
            effect => effect,
        }
    }

    pub(super) fn socket_egress_with_ingress<D, I>(
        &mut self,
        device: &mut D,
        sockets: &mut SocketSet<'_>,
        targets: &Option<I>,
    ) -> bool
    where
        D: Device + ?Sized,
        I: Iterator<Item = SocketHandle> + Clone,
    {
        match (targets, self.checked_ingress_effects(sockets)) {
            (Some(targets), IngressEffects::None) if targets.clone().next().is_some() => {
                self.socket_egress_targets(device, sockets, targets.clone())
            }
            (Some(targets), IngressEffects::Tcp(handle)) => {
                // Existing targets precede the newly observed handle; avoid a
                // duplicate dispatch without allocating or sorting a new set.
                let extra = (!targets.clone().any(|h| h == handle)).then_some(handle);
                self.socket_egress_targets(device, sockets, targets.clone().chain(extra))
            }
            _ => self.socket_egress(device, sockets),
        }
    }
}
