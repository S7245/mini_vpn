use crate::device::VirtualTunDevice;
use smoltcp::iface::{Config, Interface, SocketSet};
use smoltcp::socket::tcp::{Socket as TcpSocket, SocketBuffer as TcpSocketBuffer};
use smoltcp::wire::{IpAddress, IpCidr};

pub async fn start_tun_proxy() {
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
    let config = Config::new(smoltcp::wire::HardwareAddress::Ip);
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

    loop {
        tokio::select! {
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
                    let mut tcp_socket = sockets.get_mut::<TcpSocket>(socket_handle);
                    // 询问是否有数据,检查接收缓冲区里有没有新鲜出炉的数据。
                    if tcp_socket.can_recv() {
                        // recv() 它会把缓冲区里的数据作为一个切片 data: &[u8] 传给你的闭包（闭包就是 { ... } 里的代码块）。
                        tcp_socket.recv(|data|{
                            println!("Received: {:?}", data);
                            // ❓ 任务：将 data (它是 &[u8] 字节切片) 转换为 UTF-8 字符串并打印出来。
                            // 提示：你可以使用 std::str::from_utf8(data) 来尝试转换。
                            let utf8_str = std::str::from_utf8(data).unwrap_or("Invalid UTF-8 data");
                            println!("Received UTF-8: {}", utf8_str);

                            // 告诉 smoltcp 我们处理完了所有数据，把它从缓冲区里清空
                            (data.len(),())
                        }).unwrap();
                        
                        let response = b"HTTP/1.1 200 OK\r\nContent-Length: 13\r\n\r\nHello, smoltcp!";
                        // 加上发送这段 response 的代码，并调用 close() 方法关闭连接吗？
                        tcp_socket.send_slice(response).unwrap();
                        tcp_socket.close();
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
                let mut tcp_socket = sockets.get_mut::<TcpSocket>(socket_handle);
                // 询问是否有数据,检查接收缓冲区里有没有新鲜出炉的数据。
                if tcp_socket.can_recv() {
                    // recv() 它会把缓冲区里的数据作为一个切片 data: &[u8] 传给你的闭包（闭包就是 { ... } 里的代码块）。
                    tcp_socket.recv(|data|{
                        println!("Received: {:?}", data);
                        // ❓ 任务：将 data (它是 &[u8] 字节切片) 转换为 UTF-8 字符串并打印出来。
                        // 提示：你可以使用 std::str::from_utf8(data) 来尝试转换。
                        let utf8_str = std::str::from_utf8(data).unwrap_or("Invalid UTF-8 data");
                        println!("Received UTF-8: {}", utf8_str);
                            // 告诉 smoltcp 我们处理完了所有数据，把它从缓冲区里清空
                        (data.len(),())
                    }).unwrap();

                    let response = b"HTTP/1.1 200 OK\r\nContent-Length: 13\r\n\r\nHello, smoltcp!";
                        // 加上发送这段 response 的代码，并调用 close() 方法关闭连接吗？
                    tcp_socket.send_slice(response).unwrap();
                    tcp_socket.close();
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
