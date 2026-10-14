//! Owns plaintext DNS interception, fake-IP answers and reply injection.
//! It does not own mapping lifetime or the TUN event loop. Queries never
//! escape upstream; response identity and endpoints follow the incoming query.
//! Entry points are `forge_dns_reply` and `handle_dns_hijack`. Characterization
//! tests live in the parent test Module; capacity tests live in this child.

use super::{FakeIpPool, Metrics, TunIo, build_udp_ip_packet, parse_inbound_udp};
use crate::dns::{self, Answer};
use crate::udp_relay::UdpInbound;

/// 刀5：把发往**任意** resolver 的明文 DNS 查询，本地伪造成 fake-IP 回包（裸包构造）。
/// 中文要点：A 查询 → 分配 fake-IP 并回伪造 A 记录；AAAA/其它 → NODATA；不可解析 → `None`
/// （调用方丢弃，**绝不转发真 DNS**——转发即泄漏真实 IP，绕过 fake-IP）。回包源 = app 当初查询的
/// resolver（`udp.dst_ip:53`），目的 = app 原端点；否则 app 的 socket 认不出回包而丢弃。裸包能任意
/// 设 src（smoltcp 受限于本接口 IP、对无界 resolver 集合做不到，见 ADR-0007）。纯逻辑（只依赖
/// `UdpInbound` + `&mut FakeIpPool`），无 device/async，便于单测。
pub(super) fn forge_dns_reply(
    udp: &UdpInbound<'_>,
    fake_pool: &mut FakeIpPool,
    now_secs: u64,
) -> Option<Vec<u8>> {
    let q = dns::parse_query(udp.payload)?;
    let resp = if q.qtype == dns::QTYPE_A {
        let answer = fake_pool
            .alloc(&q.qname, now_secs)
            .map_or(Answer::ServFail, |ip| Answer::A(ip, 5));
        dns::build_response(&q, answer)
    } else {
        let kind = if q.qtype == dns::QTYPE_AAAA {
            "AAAA"
        } else {
            "other"
        };
        println!("🪪 DNS {} ({}) → NODATA", q.qname, kind);
        dns::build_response(&q, Answer::NoData)
    };
    // 回包：src = 被查询的 resolver:53，dst = app 原端点（src/dst 对调）。
    build_udp_ip_packet((udp.dst_ip, 53), (udp.src_ip, udp.src_port), &resp)
}

/// 刀5：rx 热路径的裸包 DNS 劫持薄壳——解析入站 :53 包 → `forge_dns_reply` → 注入回包到 TUN。
/// 中文要点：`forge_dns_reply` 返回 `None`（不可解析）→ 静默丢弃（app 重查自愈，绝不转发真 DNS）。
/// 与 UDP relay 下行注入同款（`inject_ip_packet` + `flush_tx`）；泛型 `D: TunIo` 使生产/harness 共用。
pub(super) async fn handle_dns_hijack<D: TunIo>(
    pkt: &[u8],
    fake_pool: &mut FakeIpPool,
    device: &mut D,
    now_secs: u64,
    metrics: &Metrics,
) {
    let Some(udp) = parse_inbound_udp(pkt) else {
        // 生产里 classify_inbound 已保证 parse 成功才路由到 Dns → 此早返在生产中实为 dead。
        // **不计数**（避免与下方 forge None 的 dns_dropped 双计；防御性早返不该污染指标）。
        return;
    };
    if let Some(reply) = forge_dns_reply(&udp, fake_pool, now_secs) {
        metrics.inc_dns_forged();
        device.inject_ip_packet(&reply);
        if let Err(e) = device.flush_tx().await {
            println!("DNS 劫持回包 flush 失败: {e}");
        }
    } else {
        // 不可解析查询：静默丢（forge_dns_reply 已记 app 重查自愈），计入 dns_dropped。
        metrics.inc_dns_dropped();
    }
}

#[cfg(test)]
mod tests;
