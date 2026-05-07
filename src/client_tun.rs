use crate::device::VirtualTunDevice;
use smoltcp::iface::{Config as SmolConfig, Interface, SocketHandle, SocketSet};
use smoltcp::socket::tcp::{Socket as TcpSocket, SocketBuffer as TcpSocketBuffer};
use smoltcp::wire::{IpAddress, IpCidr};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_util::compat::{FuturesAsyncReadCompatExt, TokioAsyncReadCompatExt};
use yamux::{Config as YamuxConfig, Connection, Mode};
use tokio_rustls::TlsConnector;
use tokio_rustls::rustls::ServerName;
use tokio_rustls::rustls::{ClientConfig, RootCertStore};
use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;
use tokio_rustls::rustls::Certificate;

use std::collections::HashMap;
use tokio::sync::mpsc;

pub async fn start_tun_proxy() {
    let mut root_cert_store = RootCertStore::empty();
    let cert_file = &mut BufReader::new(File::open("cert.pem").unwrap());
    let certs = rustls_pemfile::certs(cert_file).unwrap();
    for cert in certs {
        root_cert_store.add(&Certificate(cert)).unwrap();
    }
    let config = ClientConfig::builder()
        .with_safe_defaults()
        .with_root_certificates(root_cert_store)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));

    // 记录：房间号 -> 专属通道的发送端 (注意 Sender 需要指定发送的数据类型 Vec<u8>)
    let mut active_connections: HashMap<SocketHandle, mpsc::Sender<Vec<u8>>> = HashMap::new();
    // 全局回信通道：接收端 global_rx 留在主循环，发送端 global_tx 会被克隆给每个后台车厢
    let (global_tx, mut global_rx) = tokio::sync::mpsc::channel::<(SocketHandle, Vec<u8>)>(1024);
    // =========== 1. 初始化底层网卡和通道 ===========

    // 1. 初始化 TUN 设备(化底层网卡和通道) / 创建操作系统的原生异步虚拟网卡
    let raw_tun = create_tun_device().await;
    // 使用 raw_tun 实例化我们的包装器
    let mut device = VirtualTunDevice::new(raw_tun);

    // =========== 2. 初始化 smoltcp 酒店和路由器 ===========
    // 2. 初始化 smoltcp 的“酒店”
    let mut sockets = SocketSet::new(vec![]);

    let tcp_rx_buffer = TcpSocketBuffer::new(vec![0; 65535]);
    let tcp_tx_buffer = TcpSocketBuffer::new(vec![0; 65535]);
    let mut tcp_socket = TcpSocket::new(tcp_rx_buffer, tcp_tx_buffer);
    // 为了让它能接客（比如截获我们在终端里发起的 curl 请求），我们需要让它开始监听 (Listen) 特定的端口，比如 HTTP 常用的 80 端口。
    tcp_socket.listen(80).unwrap();
    // socket_handle：房间号/把手
    let socket_handle = sockets.add(tcp_socket);

    // 3. 初始化 smoltcp 的“虚拟路由器”
    let config = SmolConfig::new(smoltcp::wire::HardwareAddress::Ip);
    // 这里传入了包装好的 &mut device
    let mut iface = Interface::new(config, &mut device, smoltcp::time::Instant::now());

    // 给虚拟路由器配置 IP 地址 (10.0.0.1/24)
    iface.update_ip_addrs(|ip_addrs| {
        ip_addrs
            .push(IpCidr::new(IpAddress::v4(10, 0, 0, 2), 24))
            .unwrap();
    });

    // 3. 初始化定时器 (例如每 5 毫秒触发一次)
    let mut timer = tokio::time::interval(std::time::Duration::from_millis(5));
    println!("🚀 TUN 虚拟网卡主循环启动！试试 ping 10.0.0.2");

    let domain = match ServerName::try_from("localhost") {
        Ok(domain) => domain,
        Err(e) => {
            println!("解析 SNI 域名失败: {e:?}");
            return;
        }
    };

    let server_stream = TcpStream::connect("127.0.0.1:8081")
        .await
        .expect("TcpStream 连接错误");

     let tls_stream = match connector.clone().connect(domain, server_stream).await {
        Ok(s) => s,
        Err(e) => {
            println!("与代理服务端 TLS 握手失败: {:?}", e);
            return;
        }
    };
    println!("✅ 成功连接到洛杉矶代理服务器！");
    let mut yamux_conn =
        Connection::new(tls_stream.compat(), YamuxConfig::default(), Mode::Client);
    // 获取遥控器 ctr
    let ctr = yamux_conn.control();

    //使用 tokio::spawn 把 Yamux 引擎放到后台 poll 运行。🚂
    tokio::spawn(async move {
        while let Ok(Some(_)) = yamux_conn.next_stream().await {}
        println!("与服务端的 Yamux 长连接已断开，请重启 Client");
    });

    loop {
        let mut ctrl = ctr.clone();
        tokio::select! {
            // 在 tokio::select! 里面，模仿网卡或定时器的写法，增加一个新的监听分支来接收 global_rx 里的数据吗？
            // 🌟 新增分支：监听洛杉矶回传的信件
            Some((handle, payload)) = global_rx.recv() =>{
                println!("📬 从大邮筒收到 {} 字节数据，准备送往房间 {:?}", payload.len(), handle);
                // ❓ 任务：
                // 1. 从 sockets 集合中，获取这个 handle 对应的 TcpSocket 可变引用。
                let tcp_socket = sockets.get_mut::<TcpSocket>(handle);
                // 2. 检查 payload 是否为空。
                // 如果为空，说明服务端关闭了连接，我们需要关闭这个 Socket 并从 active_connections 中移除。
                if payload.is_empty() {
                    tcp_socket.abort();
                    active_connections.remove(&handle);
                    tcp_socket.listen(80).unwrap();
                } else {
                    // 2. 将 payload 塞进这个 Socket 的发送缓冲区里。
                    tcp_socket.send_slice(&payload).unwrap();
                    println!("✅ 成功将洛杉矶的回信发给本地浏览器！");
                    // 立刻主动“推”路由器一把。
                    let timestamp = smoltcp::time::Instant::now();
                    iface.poll(timestamp, &mut device, &mut sockets);
                    device.flush_tx().await.unwrap();
                }
            }
            // 分支 1: 全局回信通道接收到了新数据包
            // 分支 1: 物理网卡接收到了新数据包
            res = device.wait_for_rx() =>{
                if res.is_ok(){
                    // 🌟 加上这行雷达代码
                    if let Some(buf) = &device.rx_buffer {
                        println!("📡 收到来自操作系统的包，大小: {} 字节", buf.len());
                        // 🌟 新增：打印前 4 个字节，看看是不是 macOS 偷偷加的料
                        println!("🔍 包的前 4 字节: {:?}", &buf[..4.min(buf.len())]);
                    }
                    let timestamp = smoltcp::time::Instant::now();

                    // ❓ 任务 1: 调用 iface 的推进方法，依次传入 timestamp, &mut device 和 &mut sockets
                    iface.poll(timestamp, &mut device, &mut sockets);

                    // ❓ 任务 2: 异步调用 device 的发货方法，把 smoltcp 产生的回包真正发给网卡
                    // 提示: 这是一个 async 方法，别忘了 .await 和错误处理 (比如 .unwrap())
                    device.flush_tx().await.unwrap();

                    // 🌟 查房时机 1：网卡收到新包并处理完后
                    // ❓ 任务：在这里写代码查房
                    let tcp_socket = sockets.get_mut::<TcpSocket>(socket_handle);
                    // 询问是否有数据,检查接收缓冲区里有没有新鲜出炉的数据。
                    if tcp_socket.can_recv() {
                        // 1. 在闭包外准备一个空的篮子
                        let mut payload: Option<Vec<u8>> = None;

                        // recv() 它会把缓冲区里的数据作为一个切片 data: &[u8] 传给你的闭包（闭包就是 { ... } 里的代码块）。
                        tcp_socket.recv(|data|{
                            // 把 data 复制到 payload 篮子
                            payload = Some(data.to_vec());
                            // 告诉 smoltcp 我们处理完了所有数据，把它从缓冲区里清空
                            (data.len(),())
                        }).unwrap();

                        // 3. 离开闭包结界后，安全地进行异步操作
                        if let Some(payload) = payload {
                            if let Some(tx) = active_connections.get_mut(&socket_handle) {
                                tx.send(payload).await.unwrap();
                            } else {
                                // 1. 创建去程通道 (主循环 -> 车厢)
                                let (tx, mut rx) = tokio::sync::mpsc::channel(1024);
                                active_connections.insert(socket_handle, tx.clone());
                                // 2. 申请车厢
                                let yamux_stream = ctrl.open_stream().await.expect("打开 Yamux 流失败");
                                let mut tokio_yamux_stream = yamux_stream.compat();

                                // ================= 🌟 把暗号代码搬到这里 =================
                                let fake_header = b"GET / HTTP/1.1\r\nHost: www.bing.com\r\n\r\n";
                                tokio_yamux_stream.write_all(fake_header).await.unwrap();
                                tokio_yamux_stream.write_all(b"httpbin.org:80\n").await.unwrap();

                                // 发送第一笔请求
                                tx.send(payload).await.unwrap();
                                // 3. 🌟 克隆一份全局回信通道的发送端，带入后台
                                let back_tx = global_tx.clone();

                                tokio::spawn(async move {
                                    let mut buf = [0u8; 65536]; // 准备一个接收洛杉矶数据的缓冲区
                                    loop {
                                        tokio::select! {
                                            // ================= 分支 1：去程 (Local -> Remote) =================
                                            local_msg = rx.recv() => {
                                                match local_msg {
                                                    Some(payload) => {
                                                        // ❓ 任务 1：使用 tokio_yamux_stream 把 data 发给洛杉矶
                                                        // 提示：遇到 err 时可以直接 break 退出循环，结束这个后台任务
                                                        match tokio_yamux_stream.write_all(&payload).await {
                                                            Ok(_) => {
                                                                // 成功发送
                                                                println!("✅ 成功发送 {} 字节数据到洛杉矶", payload.len());
                                                            }
                                                            Err(e) => {
                                                                println!("写入 Yamux 流失败: {:?}", e);
                                                                break;
                                                            }
                                                        }
                                                    }
                                                    None => {
                                                        // 主循环把发送端丢弃了（本地连接断开）
                                                        println!("本地房间 {:?} 已关闭通道", socket_handle);
                                                        break;
                                                    }
                                                }
                                            }
                                            // ================= 分支 2：回程 (Remote -> Local) =================
                                            remote_msg = tokio_yamux_stream.read(&mut buf) => {
                                                match remote_msg {
                                                    Ok(0) => {
                                                        // 读到 0 字节，说明洛杉矶服务器主动关门了 (EOF)
                                                        println!("洛杉矶服务器关闭了车厢 {:?}", socket_handle);

                                                        /*
                                                        💡 发送一个空的 Vec 作为“断开连接”的暗号给主循环！
                                                        在网络字节流的世界里，如果你想传递真正的数据，这个数据的长度至少是 1。所以，一个长度为 0 的空数组（空切片），就是全宇宙通用的“连接已关闭 (EOF)”的终极暗号。
                                                        既然我们的大邮筒只能接收 (SocketHandle, Vec<u8>)，那我们完全可以在 Ok(0) 的时候，人工制造一个空的 Vec 发过去：
                                                        */
                                                        back_tx.send((socket_handle, vec![])).await.unwrap();
                                                        break;
                                                    }
                                                    Ok(n) => {
                                                        // 成功收到了 n 字节的数据！
                                                        // ❓ 任务 2：提取 buf 的前 n 个字节，打包成 (socket_handle, 真实数据)
                                                        // 然后用 back_tx 发进全局大邮筒。
                                                        let data = buf[..n].to_vec();
                                                        back_tx.send((socket_handle, data)).await.unwrap();
                                                    }
                                                    Err(e) => {
                                                        println!("读取 Yamux 流失败（从洛杉矶读取失败）: {:?}", e);
                                                        break;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                });
                            }
                        }

                    }
                }
            }
            // 分支 2: 时钟滴答，处理超时重传等后台任务
            _ = timer.tick() =>{
                let timestamp = smoltcp::time::Instant::now();
                // ❓ 任务 3: 这里需要做和上面完全一样的推进和发货操作
                iface.poll(timestamp, &mut device, &mut sockets);
                device.flush_tx().await.unwrap();

                // 🌟 查房时机 2：时钟滴答，处理完后台任务后
                // ❓ 任务：在这里写同样的查房代码，看看是否有新的包需要处理
                let tcp_socket = sockets.get_mut::<TcpSocket>(socket_handle);
                // 询问是否有数据,检查接收缓冲区里有没有新鲜出炉的数据。
                if tcp_socket.can_recv() {
                        // 1. 在闭包外准备一个空的篮子
                        let mut payload: Option<Vec<u8>> = None;

                        // recv() 它会把缓冲区里的数据作为一个切片 data: &[u8] 传给你的闭包（闭包就是 { ... } 里的代码块）。
                        tcp_socket.recv(|data|{
                            // 把 data 复制到 payload 篮子
                            payload = Some(data.to_vec());
                            // 告诉 smoltcp 我们处理完了所有数据，把它从缓冲区里清空
                            (data.len(),())
                        }).unwrap();

                        // 3. 离开闭包结界后，安全地进行异步操作
                        if let Some(payload) = payload {
                            if let Some(tx) = active_connections.get_mut(&socket_handle) {
                                tx.send(payload).await.unwrap();
                            } else {
                                // 1. 创建去程通道 (主循环 -> 车厢)
                                let (tx, mut rx) = tokio::sync::mpsc::channel(1024);
                                active_connections.insert(socket_handle, tx.clone());
                                // 2. 申请车厢
                                let yamux_stream = ctrl.open_stream().await.expect("打开 Yamux 流失败");
                                let mut tokio_yamux_stream = yamux_stream.compat();
                                // 发送第一笔请求
                                tx.send(payload).await.unwrap();
                                // 3. 🌟 克隆一份全局回信通道的发送端，带入后台
                                let back_tx = global_tx.clone();

                                tokio::spawn(async move {
                                    let mut buf = [0u8; 65536]; // 准备一个接收洛杉矶数据的缓冲区
                                    loop {
                                        tokio::select! {
                                            // ================= 分支 1：去程 (Local -> Remote) =================
                                            local_msg = rx.recv() => {
                                                match local_msg {
                                                    Some(payload) => {
                                                        // ❓ 任务 1：使用 tokio_yamux_stream 把 data 发给洛杉矶
                                                        // 提示：遇到 err 时可以直接 break 退出循环，结束这个后台任务
                                                        match tokio_yamux_stream.write_all(&payload).await {
                                                            Ok(_) => {
                                                                // 成功发送
                                                                println!("✅ 成功发送 {} 字节数据到洛杉矶", payload.len());
                                                            }
                                                            Err(e) => {
                                                                println!("写入 Yamux 流失败: {:?}", e);
                                                                break;
                                                            }
                                                        }
                                                    }
                                                    None => {
                                                        // 主循环把发送端丢弃了（本地连接断开）
                                                        println!("本地房间 {:?} 已关闭通道", socket_handle);
                                                        break;
                                                    }
                                                }
                                            }
                                            // ================= 分支 2：回程 (Remote -> Local) =================
                                            remote_msg = tokio_yamux_stream.read(&mut buf) => {
                                                match remote_msg {
                                                    Ok(0) => {
                                                        // 读到 0 字节，说明洛杉矶服务器主动关门了 (EOF)
                                                        println!("洛杉矶服务器关闭了车厢 {:?}", socket_handle);

                                                        /*
                                                        💡 发送一个空的 Vec 作为“断开连接”的暗号给主循环！
                                                        在网络字节流的世界里，如果你想传递真正的数据，这个数据的长度至少是 1。所以，一个长度为 0 的空数组（空切片），就是全宇宙通用的“连接已关闭 (EOF)”的终极暗号。
                                                        既然我们的大邮筒只能接收 (SocketHandle, Vec<u8>)，那我们完全可以在 Ok(0) 的时候，人工制造一个空的 Vec 发过去：
                                                        */
                                                        back_tx.send((socket_handle, vec![])).await.unwrap();
                                                        break;
                                                    }
                                                    Ok(n) => {
                                                        // 成功收到了 n 字节的数据！
                                                        // ❓ 任务 2：提取 buf 的前 n 个字节，打包成 (socket_handle, 真实数据)
                                                        // 然后用 back_tx 发进全局大邮筒。
                                                        let data = buf[..n].to_vec();
                                                        back_tx.send((socket_handle, data)).await.unwrap();
                                                    }
                                                    Err(e) => {
                                                        println!("读取 Yamux 流失败（从洛杉矶读取失败）: {:?}", e);
                                                        break;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                });
                            }
                        }

                    }
            }
        }
    }
}

pub async fn create_tun_device() -> tun::AsyncDevice {
    let mut config = tun::Configuration::default();

    config
        .address((10, 0, 0, 1)) // 网卡的 IP 地址
        .destination((10, 0, 0, 2)) // 🌟 新增：告诉 OS 水管另一头是谁！
        .netmask((255, 255, 255, 0)) // 子网掩码
        .up(); // 启动网卡

    #[cfg(target_os = "macos")]
    config.layer(tun::Layer::L3); // macOS 通常需要显式指定三层（IP层）

    // 创建异步读取的 TUN 设备
    tun::create_as_async(&config).expect("无法创建 TUN 设备")
}
