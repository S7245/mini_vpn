#[cfg(feature = "harness")]
mod inbound;

// Preserve the old inspector characterization through the actual single parser.
pub(in crate::client_tun) fn inspect_inbound_tcp(packet: &[u8]) -> Option<(u16, bool)> {
    super::parse_inbound_tcp(packet).map(|packet| (packet.port, packet.is_clean_syn))
}
