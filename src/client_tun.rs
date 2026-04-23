// use tokio::io::{AsyncReadExt, AsyncWriteExt};
// use tokio::net::{TcpListener, TcpStream};

// use std::fs::File;
// use std::io::BufReader;
// use std::sync::Arc;
// use tokio_rustls::rustls::Certificate;

// use std::convert::TryFrom;
// use tokio_rustls::TlsConnector;
// use tokio_rustls::rustls::ServerName;
// use tokio_rustls::rustls::{ClientConfig, RootCertStore};

// use tokio_util::compat::{FuturesAsyncReadCompatExt, TokioAsyncReadCompatExt};
// use yamux::{Config, Connection, Mode};


/*
    A.初始化虚拟网卡 🏗️
    我们需要创建一个函数来初始化 `tun` 设备。这就像是给电脑插上一张“隐形的网卡”。

    首先，要让 smoltcp 能够管理成百上千个并发的网页请求，我们需要给它创建一个用来存放所有虚拟 TCP/UDP 连接的“容器”。
    你可以把 SocketSet 想象成一家“网络酒店”。无论是 TCP 还是 UDP，所有的虚拟连接（Socket）都必须“登记入住”到这个集合里，统一由 smoltcp 路由器进行管理。
*/

use smoltcp::Smoltcp;
use tun::AbstractDevice;

use smoltcp::iface::{Config, Interface, SocketSet};
use smoltcp::wire::{IpAddress, IpCidr};
// 假设我们已经写好了一个适配器 VirtualTunDevice
// use crate::device::VirtualTunDevice;

pub async fn start_tun_proxy() {
    // =========== 1. 初始化底层网卡和通道 ===========
    // 1. 初始化 TUN 设备(化底层网卡和通道)
    let mut tun_device = create_tun_device().await;
    // let mut device = VirtualTunDevice::new(raw_tun); // 包装成 smoltcp 认识的 Device
    
    // 1. 建立核心邮局，容量为 1024 个数据包 📬
    // rx (接收端)：留在主循环世界。主循环一边盯着网卡，一边盯着 rx。一旦远端代理服务器有网页数据传回来，主循环就从 rx 里把数据拿出来，交给 smoltcp 打包成标准的 IP 数据包，最后写回 TUN 网卡。
    // tx (发送端)：要交给协程世界。每当有一个新的 TCP 连接建立，我们就把 tx 交给新孵化出来的 Yamux 协程。当协程从隧道里读到了互联网的响应，它就用 tx 把数据塞进通道。
    let (tx, mut rx) = tokio::sync::mpsc::channel::<Vec<u8>>(1024);
    // 3. 初始化定时器 (例如每 5 毫秒触发一次)
    let mut timer = tokio::time::interval(std::time::Duration::from_millis(5));
    
    // =========== 2. 初始化 smoltcp 酒店和路由器 ===========
    // 2. 初始化 smoltcp 的“酒店”
    let mut sockets = SocketSet::new(vec![]);
    // 3. 初始化 smoltcp 的“虚拟路由器”
    let mut config = Config::new(smoltcp::wire::HardwareAddress::Ip);
    // 这里传入了包装好的 &mut device
    let mut iface = Interface::new(config, &mut device, smoltcp::time::Instant::now());
    
    // 给虚拟路由器配置 IP 地址 (10.0.0.1/24)
    iface.update_ip_addrs(|ip_addrs| {
        ip_addrs.push(IpCidr::new(IpAddress::v4(10, 0, 0, 1), 24)).unwrap();
    });
    
    println!("🚀 TUN 虚拟网卡主循环启动！");
    
    // =========== 3. 终极收发室主循环 ===========
    loop {
        tokio::select! {
            // 分支 1：监听网卡，网卡有数据进来 (在 VirtualTunDevice 内部进行 await)
            Ok(read) = tun_device.read(&mut tun_buf) => {
                // 1. 将 tun_buf 的数据喂给 smoltcp
                // 2. 调用 smoltcp 的 iface.poll() 推进状态机
                // 获取当前时刻
                let timestamp = smoltcp::time::Instant::now();
                iface.poll(timestamp,&mut device,&mut sockets);
                // 3. 检查 smoltcp 内部是否有新的 TCP 连接就绪？
                // 4. 如果有，立刻克隆 tx，并 tokio::spawn 开启 Yamux 隧道处理！
            }

            // 分支 2：远端代理有数据传回来，监听协程寄回来的远端网页数据
            Some(data) = rx.recv() => {
                // 1. 将 data 写入 smoltcp 对应连接的发送缓冲区
                // 2. 调用 iface.poll() 产生真实的 IP 回包
                // 3. 将 IP 回包写入 tun_device
            }

            // 分支 3：定时器时间到了（滴答），定时器滴答 (处理 TCP 超时重传等后台任务)
            _ = timer.tick() => {
                // 什么网络数据都没收到，但时间到了，也去摇一下状态机处理重传
                // iface.poll(...)
            }
        }
    }
}

pub async fn create_tun_device() -> tun::AsyncDevice {
    let mut config = tun::Configuration::default();

    config
        .address((10, 0, 0, 1)) // 网卡的 IP 地址
        .netmask((255, 255, 255, 0)) // 子网掩码
        .up(); // 启动网卡

    #[cfg(target_os = "macos")]
    config.layer(tun::Layer::L3); // macOS 通常需要显式指定三层（IP层）

    // 创建异步读取的 TUN 设备
    tun::create_as_async(&config).expect("无法创建 TUN 设备")
}

pub async fn run() {
    let tun_device = create_tun_device().await;

    let mut tun_buf = [0u8; 1500];
    let mut yamux_buf = [0u8; 65536];

    loop {
        // 这个循环的唯一工作：喂养 smoltcp 处理 TUN 网卡拦截到的 IP 数据包
        let read = tun_device.read(&mut tun_buf).await.unwrap();
        let packet = &tun_buf[..read];
        println!("收到 TUN 数据包: {:?}", packet);
        // 把生肉交给主厨
        smoltcp.consume(packet);
        smoltcp.poll(); // 摇动状态机
    }
}

pub async fn tun_test() {
    let mut tun_buf = [0u8; 1500];
    let mut yamux_buf = [0u8; 65536];
    // 接下来：把 &buf[..read] 这块“生肉”交给 smoltcp 处理
    loop {
        // 这个循环的唯一工作：喂养 smoltcp 处理 TUN 网卡拦截到的 IP 数据包
        let read = tun_device.read(&mut tun_buf).await.unwrap();
        let packet = &tun_buf[..read];
        // 把生肉交给主厨
        // smoltcp.consume(packet);
        // smoltcp.poll(); // 摇动状态机
        while let Some(mut virtual_tcp_stream) = smoltcp_listener.accept().await {
            // 获取目标地址 (比如 github.com:443)
            let virtual_tcp_addr = virtual_tcp_stream.target_addr();
            println!("收到目标地址: {:?}", virtual_tcp_addr);
            // 🌟 你的魔法：开启一个完全独立的异步任务
            tokio::spawn(async move {
                // 处理数据
                // 1. 申请车厢：let yamux_stream = ctrl.open_stream().await;
                // 2. 对接隧道：let mut tokio_yamux_stream = yamux_stream.compat();
                // 3. 伪装与寻址：发送 fake_header 和 target_addr

                // 4. 开始双向透传！
                // tokio::io::copy_bidirectional( ... ).await;
            });
            // 4. 产出：把 smoltcp 内部产生的回包（如 SYN-ACK）写回 TUN 网卡
        }
    }
}

fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
    // 1. 借用 SocketSet，找到我们的 Socket 房间
    let mut sockets = self.sockets.borrow_mut();
    let socket = sockets.get_mut::<TcpSocket>(self.handle);

    // 2. 检查是否有数据可读
    if socket.can_recv() {
        // 【核心操作 A】: 既然有数据，就用 recv_slice 把它读出来！
        let mut temp_buf = [0u8; 1500];
        let n = socket.recv_slice(&mut temp_buf).expect("读取失败");
        
        buf.put_slice(&temp_buf[..n]); // 塞进 Tokio 的 ReadBuf
        Poll::Ready(Ok(())) // 告诉 Tokio：数据拿到了，你可以发走了！
    } else {
        // 【核心操作 B】: 没数据？那就先睡一会
        // ⚠️ 极其重要：我们需要把当前任务的“闹钟” (cx.waker()) 存起来
        // 这样当主循环里的 iface.poll() 收到新数据时，才知道该叫醒谁。
        self.register_waker(cx.waker()); 
        
        Poll::Pending // 告诉 Tokio：我现在没货，请把我挂起
    }
}