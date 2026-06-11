mod client_tun;
mod device;
mod dns;
mod fake_ip;

#[tokio::main]
async fn main() {
    // Stage 13d 起退役 legacy（自研 server / 直连 client / yamux），仅保留 TUN + TUIC 客户端。
    match std::env::args().nth(1).as_deref() {
        Some("client") | Some("client-tun") | None => client_tun::start_tun_proxy().await,
        Some(other) => panic!("未知运行模式: {other}（13d 起仅支持 client-tun）"),
    }
}
