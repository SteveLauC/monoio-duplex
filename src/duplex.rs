//! Implementation of [`DuplexStream`].

use super::simplex::SimplexStream;

use async_lock::Mutex;
use monoio::io::AsyncReadRent;
use monoio::io::AsyncWriteRent;
use std::rc::Rc;

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
        futures::executor::block_on(async {
            // notify the other side of the closure
            self.write.lock().await.close_write();
            self.read.lock().await.close_read();
        })
    }
}

impl AsyncReadRent for DuplexStream {
    async fn read<T: monoio::buf::IoBufMut>(&mut self, buf: T) -> monoio::BufResult<usize, T> {
        let mut read_simplex = self.read.lock().await;
        <SimplexStream as AsyncReadRent>::read(&mut *read_simplex, buf).await
    }

    async fn readv<T: monoio::buf::IoVecBufMut>(&mut self, buf: T) -> monoio::BufResult<usize, T> {
        let mut read_simplex = self.read.lock().await;
        <SimplexStream as AsyncReadRent>::readv(&mut *read_simplex, buf).await
    }
}

impl AsyncWriteRent for DuplexStream {
    async fn write<T: monoio::buf::IoBuf>(&mut self, buf: T) -> monoio::BufResult<usize, T> {
        let mut write_simplex = self.write.lock().await;
        <SimplexStream as AsyncWriteRent>::write(&mut *write_simplex, buf).await
    }

    async fn writev<T: monoio::buf::IoVecBuf>(
        &mut self,
        buf_vec: T,
    ) -> monoio::BufResult<usize, T> {
        let mut write_simplex = self.write.lock().await;
        <SimplexStream as AsyncWriteRent>::writev(&mut *write_simplex, buf_vec).await
    }

    async fn flush(&mut self) -> std::io::Result<()> {
        let mut write_simplex = self.write.lock().await;
        <SimplexStream as AsyncWriteRent>::flush(&mut *write_simplex).await
    }

    async fn shutdown(&mut self) -> std::io::Result<()> {
        let mut write_simplex = self.write.lock().await;
        <SimplexStream as AsyncWriteRent>::shutdown(&mut *write_simplex).await
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
}
