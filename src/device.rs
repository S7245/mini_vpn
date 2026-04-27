use smoltcp::phy::{Device, DeviceCapabilities, Medium, RxToken, TxToken};
use std::collections::VecDeque;
use tokio::io::{AsyncReadExt, AsyncWriteExt}; // ⚠️ 极其重要：引入异步读写魔法
use bytes::BytesMut;

// 条件编译宏。这意味着如果在 Linux 系统上编译这段代码，编译器会自动忽略 PI 头逻辑，直接按标准处理。
#[cfg(target_os = "macos")]
const UTUN_IPV4_HEADER: [u8; 4] = [0, 0, 0, 2];

/// 虚拟 TUN 设备包装器：连接异步物理网卡与同步 smoltcp 协议栈的桥梁
pub struct VirtualTunDevice {
    pub device: tun::AsyncDevice,
    /// 收货仓库：存放刚从网卡读出来、还没被 smoltcp 吃掉的一个完整 IP 包
    pub rx_buffer: Option<BytesMut>,
    /// 发货仓库：存放 smoltcp 已经打包好、排队等待发给物理网卡的 IP 包队列
    pub tx_queue: VecDeque<BytesMut>,
}

// 让我们稍微打磨一下这个基础结构体，顺便给它加上一个创建实例的关联函数：
impl VirtualTunDevice {
    /// 构造函数
    pub fn new(device: tun::AsyncDevice) -> Self {
        Self {
            device,
            rx_buffer: None,
            tx_queue: VecDeque::new(),
        }
    }

    // 主循环(tokio::select!) -> 异步方法(wait_for_rx()): 它是我们在“异步网卡”和“同步仓库”之间进货的桥梁
    // 当这个方法(wait_for_rx())被 .await 唤醒时，说明底层的 tun0 网卡来数据包了，我们需要把它读出来，存进 rx_buffer 这个“收货仓库”里，准备等下喂给 smoltcp。
    /// 异步进货：等待物理网卡吐出数据，存入 rx_buffer
    pub async fn wait_for_rx(&mut self) -> std::io::Result<()> {
        
        // macOS utun raw read/write 总是带 4 字节 packet information 头。
        #[cfg(target_os = "macos")]
        let mut buf = BytesMut::zeroed(1504);
        #[cfg(not(target_os = "macos"))]
        let mut buf = BytesMut::zeroed(1500);

        // 2. 异步等待网卡吐出数据，并拿到读取的字节数 (n)
        let n = self.device.read(&mut buf).await?;

        #[cfg(target_os = "macos")]
        {
            if n < UTUN_IPV4_HEADER.len() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "short utun packet",
                ));
            }
            buf.truncate(n);
            self.rx_buffer = Some(buf.split_off(UTUN_IPV4_HEADER.len()));
        }

        #[cfg(not(target_os = "macos"))]
        {
            buf.truncate(n);
            self.rx_buffer = Some(buf.advance(n));
        }

        Ok(())
    }

    pub async fn flush_tx(&mut self) -> std::io::Result<()> {
        // // 1. 从发货仓库里取一个包
        // let packet = self.tx_queue.pop_front().ok_or("发货仓库为空")?;
        // // 2. 异步写入网卡
        // self.device.write_all(&packet).await?;
        // // 3. 返回成功
        // Ok(())
        while let Some(packet) = self.tx_queue.pop_front() {
            // 无论是 macOS 还是 Linux，发货仓库里的包已经是完美形态了，直接发！
            self.device.write_all(&packet).await?;
        }
        Ok(())
    }
}

// ==========================================
// 下面是面向 smoltcp 的“同步面”实现
// ==========================================

/*
临时仓库” (Buffers/Queues)
- 收货仓库 (rx_buffer)：在主循环的 device.wait_for_rx().await 里，我们先异步地把网卡数据读出来，存进这个仓库。然后调用 iface.poll() 时，smoltcp 同步调用 receive()，我们就直接把仓库里的包递给它。
- 发货仓库 (tx_queue)：当 smoltcp 同步调用 transmit() 吐出回包（比如 SYN-ACK）时，我们不能立刻 .await 写入网卡，而是先把它塞进这个发货仓库。等 poll() 结束回到主循环，我们再慢慢用 write_all().await 把仓库里的包清空。
*/

/// 收货单：保管从 rx_buffer 拿出来的包裹
pub struct TunRxToken {
    buffer: BytesMut,
}

impl RxToken for TunRxToken {
    fn consume<R, F>(mut self, f: F) -> R
    where
        F: FnOnce(&mut [u8]) -> R,
    {
        // 我们的任务：执行闭包 f，并把 self.buffer 的可变引用/切片传给它
        // 最后返回 f 的执行结果
        // 将包裹拆开（转为可变切片）喂给 smoltcp 处理
        println!("收货单-RxToken:{:?}", &self.buffer[0..]);
        f(&mut self.buffer)
    }
}

/// 发货单：持有发货仓库的可变钥匙
pub struct TunTxToken<'a> {
    queue: &'a mut VecDeque<BytesMut>,
}

// “造箱子”：在发货仓库里造一个指定大小的箱子，准备把真实数据写进去
impl<'a> TxToken for TunTxToken<'a> {
    fn consume<R, F>(self, len: usize, f: F) -> R
    where
        F: FnOnce(&mut [u8]) -> R,
    {
        #[cfg(target_os = "macos")]
        {
            // 1. 造一个多出 4 字节的箱子
            let mut buffer = BytesMut::zeroed(4 + len);
            // 2. 提前把 PI 头印在箱子的最前面
            buffer[0..4].copy_from_slice(&[0, 0, 0, 2]);
            // 3. 接下来该让 smoltcp 填数据了
            // ❓ 任务：既然前 4 个字节已经被占用了，你应该如何写，才能把 buffer 中从索引 4 开始到最后的“剩余部分”，作为可变切片传给闭包 f 呢？
            let result = f(&mut buffer[4..]);
            self.queue.push_back(buffer);
            result
        }
        #[cfg(not(target_os = "macos"))]
        {
            println!("发货单-TxToken:{:?}", len);
            // 1. 按 smoltcp 要求的长度，造一个填满 0 的新箱子
            // 1. 造一个指定大小的空箱子
            let mut buffer = BytesMut::zeroed(len);
            // 2. 把箱子交给 f，smoltcp 会把真实的 IP 数据写进去
            // 2. 让 smoltcp 把真实数据填进去
            let result = f(&mut buffer);
            // 3. 箱子装满了！现在把它塞进发货队列的尾部
            // 3. 扔进发货队列排队
            self.queue.push_back(buffer);
            // 4. 返回执行结果
            result
        }
    }
}

/// 正式登记为 smoltcp 认可的物理设备
impl Device for VirtualTunDevice {
    type RxToken<'a> = TunRxToken;
    type TxToken<'a> = TunTxToken<'a>;

    fn capabilities(&self) -> DeviceCapabilities {
        let mut caps = DeviceCapabilities::default();
        caps.max_transmission_unit = 1500; // 标准网卡 MTU
        caps.medium = Medium::Ip; // 我们处理的是纯 IP 包 (三层)，不是以太网帧 (二层)

        // 🌟 新增：解决操作系统校验和卸载问题
        let mut cs = smoltcp::phy::ChecksumCapabilities::default();
        // 设置为 Tx 表示：接收 (Rx) 时不检查校验和，但发送 (Tx) 时强制计算校验和
        cs.tcp = smoltcp::phy::Checksum::Tx;
        cs.ipv4 = smoltcp::phy::Checksum::Tx;
        cs.icmpv4 = smoltcp::phy::Checksum::Tx;
        caps.checksum = cs;

        caps
    }

    fn receive(
        &mut self,
        _timestamp: smoltcp::time::Instant,
    ) -> Option<(Self::RxToken<'_>, Self::TxToken<'_>)> {
        // ⚠️ 这里的逻辑是将 rx_buffer 里的数据拿出来，打包成 RxToken
        // 如果仓库里有货，就 take() 拿走，打包成收货单和发货单交给 smoltcp
        // 如果仓库里没有货，就返回 None
        // println!("接收包：{:?}", self.rx_buffer);
        self.rx_buffer.take().map(|buffer| {
            (
                TunRxToken { buffer },
                TunTxToken {
                    queue: &mut self.tx_queue,
                },
            )
        })
    }

    fn transmit(&mut self, _timestamp: smoltcp::time::Instant) -> Option<Self::TxToken<'_>> {
        // 只需要返回一个持有 tx_queue 可变引用的 TxToken 即可
        // 直接给一张发货单，里面带着发货仓库的钥匙
        println!("发货单：{:?}", self.tx_queue);
        Some(TunTxToken {
            queue: &mut self.tx_queue,
        })
    }
}


/*
设备能力：DeviceCapabilities { medium: Ip, max_transmission_unit: 1500, max_burst_size: None, checksum: ChecksumCapabilities { ipv4: Both, udp: Both, tcp: Both, icmpv4: Both } }
📡 收到来自操作系统的包，大小: 84 字节
🔍 包的前 4 字节: [69, 0, 0, 84]
接收包：Some([69, 0, 0, 84, 192, 228, 0, 0, 64, 1, 165, 194, 10, 0, 0, 1, 10, 0, 0, 2, 8, 0, 95, 174, 248, 212, 0, 3, 105, 235, 27, 20, 0, 1, 47, 118, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55])
收货单-RxToken:[69, 0, 0, 84, 192, 228, 0, 0, 64, 1, 165, 194, 10, 0, 0, 1, 10, 0, 0, 2, 8, 0, 95, 174, 248, 212, 0, 3, 105, 235, 27, 20, 0, 1, 47, 118, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55]
发货单-TxToken:84
接收包：None
*/