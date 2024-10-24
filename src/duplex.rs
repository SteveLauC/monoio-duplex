//! Implementation of [`DuplexStream`].

use super::simplex::SimplexStream;

use futures::FutureExt;
use monoio::io::AsyncReadRent;
use monoio::io::AsyncWriteRent;
use std::rc::Rc;
use std::sync::Mutex;

/// Create a new pair of `DuplexStream`s that act like a pair of connected sockets.
///
/// The `max_buf_size` argument is the maximum amount of bytes that can be
/// written to a side before the write returns `Poll::Pending`.
pub fn duplex(max_buf_size: usize) -> (DuplexStream, DuplexStream) {
    let one = Rc::new(Mutex::new(SimplexStream::new_unsplit(max_buf_size)));
    let two = Rc::new(Mutex::new(SimplexStream::new_unsplit(max_buf_size)));

    (
        DuplexStream {
            read: one.clone(),
            write: two.clone(),
        },
        DuplexStream {
            read: two,
            write: one,
        },
    )
}

/// A bidirectional pipe to read and write bytes in memory.
///
/// A pair of `DuplexStream`s are created together, and they act as a "channel"
/// that can be used as in-memory IO types. Writing to one of the pairs will
/// allow that data to be read from the other, and vice versa.
///
/// # Closing a `DuplexStream`
///
/// If one end of the `DuplexStream` channel is dropped, any pending reads on
/// the other side will continue to read data until the buffer is drained, then
/// they will signal EOF by returning 0 bytes. Any writes to the other side,
/// including pending ones (that are waiting for free space in the buffer) will
/// return `Err(BrokenPipe)` immediately.
///
/// # Example
///
/// ```
/// # use monoio::io::{AsyncReadRentExt, AsyncWriteRentExt};
/// # use monoio_duplex::simplex::simplex;
/// # use monoio::RuntimeBuilder;
/// # use monoio::FusionDriver;
/// # use monoio_duplex::duplex::duplex;
/// #
/// # let mut rt = RuntimeBuilder::<FusionDriver>::new().enable_all().build().unwrap();
/// # rt.block_on(async {
/// let (mut client, mut server) = duplex(64);
///
/// let (write_result, _buf) = client.write_all(b"ping").await;
/// assert_eq!(write_result.unwrap(), 4);
///
/// let mut buf = [0u8; 4];
/// let (read_result, buf) = server.read_exact(vec![0_u8; 4]).await;
/// assert_eq!(read_result.unwrap(), 4);
/// assert_eq!(&buf, b"ping");
///
/// let (write_result, _buf) = server.write_all(b"poong").await;
/// assert_eq!(write_result.unwrap(), 5);
///
/// let (read_result, buf) = client.read_exact(vec![0_u8; 5]).await;
/// assert_eq!(read_result.unwrap(), 5);
/// assert_eq!(&buf, b"poong");
/// # });
/// ```
#[derive(Debug)]
pub struct DuplexStream {
    read: Rc<Mutex<SimplexStream>>,
    write: Rc<Mutex<SimplexStream>>,
}

impl Drop for DuplexStream {
    fn drop(&mut self) {
        // notify the other side of the closure
        self.write.lock().unwrap().close_write();
        self.read.lock().unwrap().close_read();
    }
}

/// A helper macro to `.await` the I/O function, used in our I/O traits
/// implementations.
///
/// Different from a plain `function().await`, it drops the lock guard if future
/// `function()` is pending.
macro_rules! await_io_future {
    // For functions that do not have arugments: flush/shutdown
    ($trait:ident, $function:ident, $guard:expr) => {{
        let opt_read_ready = <SimplexStream as $trait>::$function(&mut *$guard).now_or_never();

        match opt_read_ready {
            Some(result) => result,
            None => {
                // drop the Mutex guard or it could deadlock
                // https://github.com/SteveLauC/monoio-duplex/issues/7
                drop($guard);
                std::future::pending().await
            }
        }
    }};

    // For functions with a `buf` arugment: read/readv/write/writev
    ($trait:ident, $function:ident, $guard:expr, $buf:expr) => {{
        let opt_read_ready =
            <SimplexStream as $trait>::$function(&mut *$guard, $buf).now_or_never();

        match opt_read_ready {
            Some(result) => result,
            None => {
                // drop the Mutex guard or it could deadlock
                // https://github.com/SteveLauC/monoio-duplex/issues/7
                drop($guard);
                std::future::pending().await
            }
        }
    }};
}

impl AsyncReadRent for DuplexStream {
    #[allow(clippy::await_holding_lock)] // false-positive, we explicitly dropped the guard before the await point.
    async fn read<T: monoio::buf::IoBufMut>(&mut self, buf: T) -> monoio::BufResult<usize, T> {
        let mut read_simplex = self.read.lock().unwrap();
        await_io_future!(AsyncReadRent, read, read_simplex, buf)
    }

    #[allow(clippy::await_holding_lock)] // false-positive, we explicitly dropped the guard before the await point.
    async fn readv<T: monoio::buf::IoVecBufMut>(&mut self, buf: T) -> monoio::BufResult<usize, T> {
        let mut read_simplex = self.read.lock().unwrap();
        await_io_future!(AsyncReadRent, readv, read_simplex, buf)
    }
}

impl AsyncWriteRent for DuplexStream {
    #[allow(clippy::await_holding_lock)] // false-positive, we explicitly dropped the guard before the await point.
    async fn write<T: monoio::buf::IoBuf>(&mut self, buf: T) -> monoio::BufResult<usize, T> {
        let mut write_simplex = self.write.lock().unwrap();
        await_io_future!(AsyncWriteRent, write, write_simplex, buf)
    }

    #[allow(clippy::await_holding_lock)] // false-positive, we explicitly dropped the guard before the await point.
    async fn writev<T: monoio::buf::IoVecBuf>(
        &mut self,
        buf_vec: T,
    ) -> monoio::BufResult<usize, T> {
        let mut write_simplex = self.write.lock().unwrap();
        await_io_future!(AsyncWriteRent, writev, write_simplex, buf_vec)
    }

    #[allow(clippy::await_holding_lock)] // false-positive, we explicitly dropped the guard before the await point.
    async fn flush(&mut self) -> std::io::Result<()> {
        let mut write_simplex = self.write.lock().unwrap();
        await_io_future!(AsyncWriteRent, flush, write_simplex)
    }

    #[allow(clippy::await_holding_lock)] // false-positive, we explicitly dropped the guard before the await point.
    async fn shutdown(&mut self) -> std::io::Result<()> {
        let mut write_simplex = self.write.lock().unwrap();
        await_io_future!(AsyncWriteRent, shutdown, write_simplex)
    }
}

/// SAFETY:
///
/// > Users should ensure the read operations are indenpendence from the write
/// > ones, the methods from AsyncReadRent and AsyncWriteRent can execute
/// > concurrently.
///
/// For `DuplexStream`, read and write operate on different `SimplexStream`, so
/// it is definitely safe to split it out.
unsafe impl monoio::io::Split for DuplexStream {}

#[cfg(test)]
mod tests {
    use monoio::io::AsyncReadRentExt;

    use super::*;

    /// To guard this behavior:
    ///
    /// > If one end of the `DuplexStream` channel is dropped,
    /// >
    /// > * Any pending reads on the other side will continue to read data until
    /// >   the buffer is drained, then they will signal EOF by returning 0 bytes
    /// >
    /// > * Any writes to the other side including pending ones (that are waiting
    /// >   for free space in the buffer) will return `Err(BrokenPipe)` immediately.
    #[monoio::test(enable_timer = true)]
    async fn close_one_end() {
        let (mut drop_side, mut use_side) = duplex(64);

        // before dropping the `drop_side`, let's populate some data
        let (write_result, _buf) = drop_side
            .write("dropdropdropdrop".as_bytes().to_vec())
            .await;
        assert_eq!(write_result.unwrap(), 16);
        drop(drop_side);

        // test read
        // First, let's read the messages out using a buffer of size 4
        for _ in 0..4 {
            let (read_result, msg) = use_side.read_exact(vec![0_u8; 4]).await;
            assert_eq!(read_result.unwrap(), 4);
            assert_eq!(msg, b"drop");
        }
        // Now the buffer should be empty, the next read should return `Ready(Ok(4))`
        let read_buf = vec![0_u8; 10];
        let all_0s = read_buf.clone();
        let (read_result, should_be_all_0s) = use_side.read(read_buf).await;
        assert_eq!(read_result.unwrap(), 0);
        assert_eq!(should_be_all_0s, all_0s);

        // test write
        let (write_result, _buf) = use_side.write(b"are you still there?").await;
        assert_eq!(
            write_result.unwrap_err().kind(),
            std::io::ErrorKind::BrokenPipe
        );
    }

    #[monoio::test(enable_timer = true)]
    async fn pending_read_will_not_hold_mutex_gaurd() {
        let (mut client, server) = duplex(100);
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();

        monoio::spawn(async move {
            // send task start signal
            tx.send(()).unwrap();
            // this line should block
            let (_result, _buf) = client.read(vec![0_u8; 10]).await;
        });

        rx.await.unwrap();

        drop(server);
    }

    #[monoio::test(enable_timer = true)]
    async fn pending_write_will_not_hold_mutex_gaurd() {
        let (mut client, server) = duplex(10);
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();

        // write 10 bytes to make future writes pending
        let (write_result, _buf) = client.write(vec![0_u8; 10]).await;
        assert_eq!(write_result.unwrap(), 10);

        monoio::spawn(async move {
            // send task start signal
            tx.send(()).unwrap();
            // this line should block
            let (_result, _buf) = client.write(vec![0_u8; 10]).await;
        });

        rx.await.unwrap();

        drop(server);
    }
}
