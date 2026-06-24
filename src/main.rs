use mini_vpn::client_tun;
use mini_vpn::reality_upstream;

#[tokio::main]
async fn main() {
    // Stage 13d 起退役 legacy（自研 server / 直连 client / yamux），仅保留 TUN + TUIC 客户端。
    match std::env::args().nth(1).as_deref() {
        Some("client") | Some("client-tun") | None => client_tun::start_tun_proxy().await,
        // 刀8 诊断：直连探针（无 TUN/无 sudo），用 MINI_VPN_REALITY_* env 跑一次 REALITY 握手 + HTTP GET。
        // 用法：MINI_VPN_REALITY_*=... [MINI_VPN_REALITY_DEBUG=1] ./mini_vpn reality-probe [host:80]
        Some("reality-probe") => {
            let target = std::env::args().nth(2).unwrap_or_else(|| "example.com:80".into());
            reality_upstream::reality_probe(&target).await;
        }
        Some(other) => panic!("未知运行模式: {other}（支持 client-tun | reality-probe）"),
    }
}
