// 这里的 self.handle 是我们之前提到的 SocketHandle (房间号)
fn poll_read(
    self: Pin<&mut Self>,
    cx: &mut Context<'_>,
    buf: &mut ReadBuf<'_>,
) -> Poll<io::Result<()>> {
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

// 这里的 self.handle 是我们之前提到的 SocketHandle (房间号)
// 注意签名：buf 是 &[u8]，返回值包含写了多少个字节
fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
    // 1. 借用 SocketSet，找到我们的 Socket 房间
    let mut sockets = self.sockets.borrow_mut();
    let socket = sockets.get_mut::<TcpSocket>(self.handle);

    if socket.can_send() {
        // 【核心修改】：直接把 Tokio 给我们的 buf 塞进 smoltcp！
        let n = socket.send_slice(buf).expect("发送失败");
        Poll::Ready(Ok(n)) // 告诉 Tokio：我成功发走了 n 个字节！
    } else {
        self.register_waker(cx.waker());
        Poll::Pending // 告诉 Tokio：我现在没货，请把我挂起
    }
}
