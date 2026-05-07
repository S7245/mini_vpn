mod client;
mod client_tun;
mod device;
mod server;

#[tokio::main]
async fn main() {
    let mode = std::env::args()
        .nth(1)
        .expect("请指定运行模式: server、client-direct 或 client-tun");

    if mode == "server" {
        server::run().await;
    } else if mode == "client-direct" {
        client::run().await;
    } else if mode == "client" || mode == "client-tun" {
        client_tun::start_tun_proxy().await;
    } else {
        panic!("未知运行模式: {mode}");
    }
}
