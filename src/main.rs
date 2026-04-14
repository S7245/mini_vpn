mod client;
mod server;

#[tokio::main]
async fn main() {
    let mode = std::env::args()
        .nth(1)
        .expect("请指定运行模式: client 或 server");

    if mode == "server" {
        server::run().await;
    } else if mode == "client" {
        client::run().await;
    }
}
