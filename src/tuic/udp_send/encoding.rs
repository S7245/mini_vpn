//! Complete TUIC Packet/address encoding; owns the output allocation only.
//! No transport, association, queue, clock or refund authority is added here.
//! Public compatibility entry stays on tuic; packet golden tests stay there.
use super::super::{
    ATYP_DOMAIN, ATYP_IPV4, ATYP_IPV6, CMD_PACKET, SocketAddr, TUIC_VER, TargetAddr,
};

use bytes::{BackingMeter, BufMut, Bytes, BytesMut, MeteredCopyError};

/// 编码 TUIC 地址：`[ATYP][ADDR][PORT:u16 BE]`。
/// 中文要点：域名 `[len:u8][bytes]`，IPv4 4B，IPv6 16B；域名超 255 字节按 255 截断(不 panic)。
pub fn encode_address(target: &TargetAddr) -> Vec<u8> {
    let mut v = Vec::new();
    write_address(&mut v, target);
    v
}

fn write_address(v: &mut impl BufMut, target: &TargetAddr) {
    match target {
        TargetAddr::IpPort(SocketAddr::V4(a)) => {
            v.put_u8(ATYP_IPV4);
            v.put_slice(&a.ip().octets());
            v.put_slice(&a.port().to_be_bytes());
        }
        TargetAddr::IpPort(SocketAddr::V6(a)) => {
            v.put_u8(ATYP_IPV6);
            v.put_slice(&a.ip().octets());
            v.put_slice(&a.port().to_be_bytes());
        }
        TargetAddr::DomainPort { host, port } => {
            let bytes = host.as_bytes();
            let len = bytes.len().min(u8::MAX as usize);
            v.put_u8(ATYP_DOMAIN);
            v.put_u8(len as u8);
            v.put_slice(&bytes[..len]);
            v.put_slice(&port.to_be_bytes());
        }
    }
}

/// 编码 TUIC `Packet`(native datagram)：
/// `[0x05][0x02][ASSOC:u16][PKT_ID:u16=0][FRAG_TOTAL=1][FRAG_ID=0][SIZE:u16][ADDR][data]`。
pub fn encode_packet(assoc_id: u16, target: &TargetAddr, data: &[u8]) -> Vec<u8> {
    let addr = encode_address(target);
    let mut v = Vec::with_capacity(10 + addr.len() + data.len());
    v.push(TUIC_VER);
    v.push(CMD_PACKET);
    v.extend_from_slice(&assoc_id.to_be_bytes());
    v.extend_from_slice(&0u16.to_be_bytes()); // PKT_ID(native 不重组,固定 0)
    v.push(1); // FRAG_TOTAL
    v.push(0); // FRAG_ID
    v.extend_from_slice(&(data.len() as u16).to_be_bytes()); // SIZE
    v.extend_from_slice(&addr);
    v.extend_from_slice(data);
    observe_encoding(v.capacity());
    v
}

pub(super) fn native_packet(
    association: u16,
    target: &TargetAddr,
    data: &[u8],
    meter: &BackingMeter,
) -> Result<Bytes, MeteredCopyError> {
    let address_len = match target {
        TargetAddr::IpPort(SocketAddr::V4(_)) => 7,
        TargetAddr::IpPort(SocketAddr::V6(_)) => 19,
        TargetAddr::DomainPort { host, .. } => 4 + host.len().min(255),
    };
    let len = data
        .len()
        .checked_add(10 + address_len)
        .ok_or(MeteredCopyError::TooLarge)?;
    if len > super::MAX_TUIC_PACKET_BYTES || data.len() > u16::MAX as usize {
        return Err(MeteredCopyError::TooLarge);
    }
    let mut output = BytesMut::try_zeroed_metered(len, meter)?;
    let mut cursor = &mut output[..];
    cursor.put_slice(&[TUIC_VER, CMD_PACKET]);
    cursor.put_u16(association);
    cursor.put_slice(&[0, 0, 1, 0]);
    cursor.put_u16(data.len() as u16);
    write_address(&mut cursor, target);
    cursor.put_slice(data);
    observe_encoding(output.capacity());
    Ok(output.freeze())
}
fn observe_encoding(_capacity: usize) {
    #[cfg(test)]
    crate::owned_upstream::LegacyOwnedUpstream::<super::super::TuicUpstream>::observe_udp_encoding(
        _capacity,
    );
}

pub(super) fn heartbeat(meter: Option<&BackingMeter>) -> Result<Bytes, MeteredCopyError> {
    let wire = [TUIC_VER, super::super::CMD_HEARTBEAT];
    match meter {
        Some(meter) if meter.budget_limit().is_some() => {
            BytesMut::try_copy_from_slice_metered(&wire, meter).map(BytesMut::freeze)
        }
        _ => Ok(super::super::encode_heartbeat().into()),
    }
}
