mod client;
mod server;
mod client_tun;
mod device;

#[tokio::main]
async fn main() {
    let mode = std::env::args()
        .nth(1)
        .expect("请指定运行模式: client 或 server");

    if mode == "server" {
        server::run().await;
    } else if mode == "client" {
        //client::run().await;
        client_tun::start_tun_proxy().await;
    }
}
