//! Implementation of [`SimplexStream`].

use bytes::Buf;
use bytes::BytesMut;
use monoio::io::AsyncReadRent;
use monoio::io::AsyncWriteRent;
use monoio::io::OwnedReadHalf;
use monoio::io::OwnedWriteHalf;
use std::cell::UnsafeCell;
use std::future::Future;
use std::pin::Pin;
use std::task;
use std::task::Poll;
use std::task::Waker;

/// A unidirectional pipe to read and write bytes in memory.
///
/// It can be constructed by [`simplex`] function which will create a pair of
/// reader and writer or by calling [`SimplexStream::new_unsplit`] that will
/// create a handle for both reading and writing.
///
/// # Example
///
/// ```
/// # use monoio::io::{AsyncReadRentExt, AsyncWriteRentExt};
/// # use monoio_duplex::simplex::simplex;
/// # use monoio::RuntimeBuilder;
/// # use monoio::FusionDriver;
/// #
/// # let mut rt = RuntimeBuilder::<FusionDriver>::new().enable_all().build().unwrap();
/// # rt.block_on(async {
/// let (mut receiver, mut sender) = simplex(64);
///
/// let (write_result, _msg) = sender.write_all("ping").await;
/// write_result.unwrap();
///
/// let (read_result, buf) = receiver.read_exact(vec![0_u8; 4]).await;
/// read_result.unwrap();
///
/// assert_eq!(&buf, b"ping");
/// # });
/// ```
#[derive(Debug)]
pub struct SimplexStream {
    /// The buffer storing the bytes written, also read from.
    ///
    /// Using a `BytesMut` because it has efficient `Buf` and `BufMut`
    /// functionality already. Additionally, it can try to copy data in the
    /// same buffer if there read index has advanced far enough.
    buffer: BytesMut,
    /// Determines if the write side has been closed.
    is_closed: bool,
    /// The maximum amount of bytes that can be written before returning
    /// `Poll::Pending`.
    max_buf_size: usize,
    /// If the `read` side has been polled and is pending, this is the waker
    /// for that parked task.
    read_waker: Option<Waker>,
    /// If the `write` side has filled the `max_buf_size` and returned
    /// `Poll::Pending`, this is the waker for that parked task.
    write_waker: Option<Waker>,
}

/// Creates unidirectional buffer that acts like in memory pipe.
///
/// The `max_buf_size` argument is the maximum amount of bytes that can be
/// written to a buffer before the it returns `Poll::Pending`.
///
/// # Unify reader and writer
///
/// Not supported because Monoio does not have an `unsplit()` method.
pub fn simplex(
    max_buf_size: usize,
) -> (OwnedReadHalf<SimplexStream>, OwnedWriteHalf<SimplexStream>) {
    monoio::io::Splitable::into_split(SimplexStream::new_unsplit(max_buf_size))
}

impl SimplexStream {
    /// Creates unidirectional buffer that acts like in memory pipe. To create split
    /// version with separate reader and writer you can use [`simplex`] function.
    ///
    /// The `max_buf_size` argument is the maximum amount of bytes that can be
    /// written to a buffer before the it returns `Poll::Pending`.
    pub fn new_unsplit(max_buf_size: usize) -> SimplexStream {
        SimplexStream {
            buffer: BytesMut::new(),
            is_closed: false,
            max_buf_size,
            read_waker: None,
            write_waker: None,
        }
    }

    /// Closes and notifies the reader tasks.
    fn close_write(&mut self) {
        self.is_closed = true;
        // needs to notify any readers that no more data will come
        if let Some(waker) = self.read_waker.take() {
            waker.wake();
        }
    }

    /// Closes and notifies the writer tasks.
    #[allow(unused)] // TODO: remove this attribute once DuplexStream is implemented
    fn close_read(&mut self) {
        self.is_closed = true;
        // needs to notify any writers that they have to abort
        if let Some(waker) = self.write_waker.take() {
            waker.wake();
        }
    }

    /// Performs the read operation.
    ///
    /// It has a `cx` argument so that we can access task's waker.
    fn read_with_context<Buf: monoio::buf::IoBufMut>(
        &mut self,
        buf: &mut Buf,
        cx: &mut task::Context<'_>,
    ) -> Poll<std::io::Result<usize>> {
        if self.buffer.has_remaining() {
            let max = self.buffer.remaining().min(buf.bytes_total());
            let buf_write_ptr = buf.write_ptr();
            unsafe {
                std::ptr::copy_nonoverlapping(self.buffer.as_ptr(), buf_write_ptr, max);
            }

            self.buffer.advance(max);
            if max > 0 {
                // The passed `buf` might have been empty, don't wake up if
                // no bytes have been moved.
                if let Some(waker) = self.write_waker.take() {
                    waker.wake();
                }
            }
            Poll::Ready(Ok(max))
        } else if self.is_closed {
            Poll::Ready(Ok(0))
        } else {
            self.read_waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }

    /// Performs the readv operation.
    ///
    /// It has a `cx` argument so that we can access task's waker.
    fn readv_with_context<Buf: monoio::buf::IoVecBufMut>(
        &mut self,
        buf: &mut Buf,
        cx: &mut task::Context<'_>,
    ) -> Poll<std::io::Result<usize>> {
        // Calculate how many bytes can `buf` accommodate by adding its `iov_len`s up.
        let buf_bytes_total = {
            let mut n_bytes = 0;

            for idx in 0..buf.write_iovec_len() {
                let iovec_ptr = unsafe { buf.write_iovec_ptr().add(idx) };
                let iovec_buf_len = unsafe { (*iovec_ptr).iov_len };

                n_bytes += iovec_buf_len;
            }

            n_bytes
        };

        if self.buffer.has_remaining() {
            // How many bytes we can copy
            let mut n_to_copy = self.buffer.remaining().min(buf_bytes_total);
            // A clone to return
            let n_to_copy_clone = n_to_copy;

            // Copy these buffers
            for idx in 0..buf.write_iovec_len() {
                let iovec_ptr = unsafe { buf.write_iovec_ptr().add(idx) };
                let iovec_buf_ptr = unsafe { (*iovec_ptr).iov_base }.cast::<u8>();
                let iovec_buf_len = unsafe { (*iovec_ptr).iov_len };
                let n_to_copy_to_this_buf = std::cmp::min(n_to_copy, iovec_buf_len);

                unsafe {
                    std::ptr::copy_nonoverlapping(
                        self.buffer.as_ptr(),
                        iovec_buf_ptr,
                        n_to_copy_to_this_buf,
                    );
                }

                self.buffer.advance(n_to_copy_to_this_buf);
                // Won't underflow as `n_to_write_to_this_buf should be le n_to_write`
                n_to_copy -= n_to_copy_to_this_buf;
            }

            if n_to_copy > 0 {
                // The passed `buf` might have been empty, don't wake up if
                // no bytes have been moved.
                if let Some(waker) = self.write_waker.take() {
                    waker.wake();
                }
            }
            Poll::Ready(Ok(n_to_copy_clone))
        } else if self.is_closed {
            Poll::Ready(Ok(0))
        } else {
            self.read_waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }

    /// Performs the write operation.
    ///
    /// It has a `cx` argument so that we can access task's waker.
    fn write_with_context<Buf: monoio::buf::IoBuf>(
        &mut self,
        buf: &Buf,
        cx: &mut task::Context<'_>,
    ) -> Poll<std::io::Result<usize>> {
        if self.is_closed {
            return Poll::Ready(Err(std::io::ErrorKind::BrokenPipe.into()));
        }
        let avail = self.max_buf_size - self.buffer.len();
        if avail == 0 {
            self.write_waker = Some(cx.waker().clone());
            return Poll::Pending;
        }

        let n_bytes_to_append = buf.bytes_init().min(avail);
        let buf_read_ptr = buf.read_ptr();
        // SAFETY:
        //
        // 1. buf_read_ptr should be valid for n_bytes_to_append bytes
        // 2. buf_read_ptr is aligned to the size of T, i.e., sizeof(u8), 1
        // 3. buf_read_ptr should is non-null
        // 4. "data must point to len consecutive properly initialized values of type T", any byte is a valid byte
        // 5. returned buf_slice has lifetime 'a, it is guaranteed that during
        //    this lifetime, this piece of memory won't be modified as it is behind an immutable reference
        // 6. > The total size len * mem::size_of::<T>() of the slice must be no
        //    > larger than isize::MAX, and adding that size to data must not “wrap around” the address space.
        //    This should be guaranteed by Monoio
        let buf_slice = unsafe { std::slice::from_raw_parts(buf_read_ptr, n_bytes_to_append) };

        self.buffer
            .extend_from_slice(&buf_slice[..n_bytes_to_append]);

        if let Some(waker) = self.read_waker.take() {
            waker.wake();
        }

        Poll::Ready(Ok(n_bytes_to_append))
    }

    /// Performs the writev operation.
    ///
    /// It has a `cx` argument so that we can access task's waker.
    fn writev_with_context<Buf: monoio::buf::IoVecBuf>(
        &mut self,
        buf: &Buf,
        cx: &mut task::Context<'_>,
    ) -> Poll<std::io::Result<usize>> {
        if self.is_closed {
            return Poll::Ready(Err(std::io::ErrorKind::BrokenPipe.into()));
        }
        let avail = self.max_buf_size - self.buffer.len();
        if avail == 0 {
            self.write_waker = Some(cx.waker().clone());
            return Poll::Pending;
        }

        // Calculate how many bytes we have in `buf` by adding its `iov_len`s up.
        let buf_bytes_total = {
            let mut n_bytes = 0;

            for idx in 0..buf.read_iovec_len() {
                let iovec_ptr = unsafe { buf.read_iovec_ptr().add(idx) };
                let iovec_buf_len = unsafe { (*iovec_ptr).iov_len };
                n_bytes += iovec_buf_len;
            }

            n_bytes
        };

        // `n_to_write` bytes can be copied from `buf` to `self.buffer`
        let mut n_to_copy = std::cmp::min(avail, buf_bytes_total);
        // A clone to return
        let n_to_copy_clone = n_to_copy;

        for idx in 0..buf.read_iovec_len() {
            let iovec_ptr = unsafe { buf.read_iovec_ptr().add(idx) };
            let iovec_buf_ptr = unsafe { (*iovec_ptr).iov_base }.cast::<u8>().cast_const();
            let iovec_buf_len = unsafe { (*iovec_ptr).iov_len };
            let n_to_copy_from_this_buf = std::cmp::min(n_to_copy, iovec_buf_len);
            // SAFETY:
            //
            // This is actually **unsafe** since we don't have a guarantee that
            // the memory referred by `slice_to_copy` won't be modified during
            // its lifetime as Monoio stores uses `libc::iovec` in the interface,
            // which is simply a starting pointer and length, anything could
            // happen.
            let slice_to_copy =
                unsafe { std::slice::from_raw_parts(iovec_buf_ptr, n_to_copy_from_this_buf) };

            self.buffer.extend_from_slice(slice_to_copy);

            n_to_copy -= n_to_copy_from_this_buf;
        }

        if let Some(waker) = self.read_waker.take() {
            waker.wake();
        }

        Poll::Ready(Ok(n_to_copy_clone))
    }
}

/// Future wrapper for read operations.
///
/// # Why is it needed
///
/// `monoio::io::AsyncReadRent`'s I/O interfaces, unlike the `poll_xxx` interfaces
/// in Tokio, don't have access to `std::task::Context`, so that we cannot access
/// the `waker` either. However, in order to wake up the pending write task after
/// future read operation, we have to access the waker and store it.  So we define
/// this wrapper and implement `Future` for it, where we can access the waker.
struct SimplexStreamReadFuture<'stream, Buf: monoio::buf::IoBufMut> {
    // `UnsafeCell` is used in the following 2 fields to access them mutably
    // through immutable references. If we don't use it, we will have 2 mutable
    // references to `SimplexStreamWriteFuture` in its `Future` impl.
    stream: UnsafeCell<&'stream mut SimplexStream>,
    buf: UnsafeCell<Buf>,

    _marker: std::marker::PhantomData<Buf>,
}

impl<'stream, Buf: monoio::buf::IoBufMut> SimplexStreamReadFuture<'stream, Buf> {
    /// Construct a `SimplexStreamReadFuture` with given arguments
    fn new(stream: &'stream mut SimplexStream, buf: Buf) -> Self {
        let stream = UnsafeCell::new(stream);
        let buf = UnsafeCell::new(buf);

        Self {
            stream,
            buf,
            _marker: std::marker::PhantomData,
        }
    }

    /// Access the `stream` field mutably through an immutable reference to `self`.
    ///
    /// # SAFETY
    ///
    /// Know what you are doing before using this function.
    unsafe fn stream_mut(&self) -> &mut SimplexStream {
        let raw_ptr = self.stream.get();
        *raw_ptr
    }

    /// Access the `buf` field mutably through an immutable reference to `self`.
    ///
    /// # SAFETY
    ///
    /// Know what you are doing before using this function.
    unsafe fn buf_mut(&self) -> &mut Buf {
        let raw_ptr = self.buf.get();
        &mut *raw_ptr
    }
}

// We should implement `Future` for `SimplexStreamReadFuture`, but `.await`ing a
// future would take it ownership, which is not something we want because we still
// need the `buf` stored in this wrapper, implementing `Future` for its mutable
// reference solves the problem.
impl<'a, Buf: monoio::buf::IoBufMut> Future for &mut SimplexStreamReadFuture<'a, Buf> {
    type Output = std::io::Result<usize>;

    fn poll(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<Self::Output> {
        // SAFETY:
        //
        // We are acquiring 2 mutable references to `self`, but they target different
        // fields, so it should be safe.
        let (stream, buf) = unsafe { (self.stream_mut(), self.buf_mut()) };
        SimplexStream::read_with_context(stream, buf, cx)
    }
}

/// Future wrapper for readv operations.
///
/// Basically same as [`SimplexStreamReadFuture`], see its documentation for
/// more details.
struct SimplexStreamReadvFuture<'stream, Buf: monoio::buf::IoVecBufMut> {
    stream: UnsafeCell<&'stream mut SimplexStream>,
    buf: UnsafeCell<Buf>,
    _marker: std::marker::PhantomData<Buf>,
}

impl<'stream, Buf: monoio::buf::IoVecBufMut> SimplexStreamReadvFuture<'stream, Buf> {
    /// Construct a `SimplexStreamReadvFuture` with given arguments
    fn new(stream: &'stream mut SimplexStream, buf: Buf) -> Self {
        let stream = UnsafeCell::new(stream);
        let buf = UnsafeCell::new(buf);

        Self {
            stream,
            buf,
            _marker: std::marker::PhantomData,
        }
    }

    /// Access the `stream` field mutably through an immutable reference to `self`.
    ///
    /// # SAFETY
    ///
    /// Know what you are doing before using this function.
    unsafe fn stream_mut(&self) -> &mut SimplexStream {
        let raw_ptr = self.stream.get();
        *raw_ptr
    }

    /// Access the `buf` field mutably through an immutable reference to `self`.
    ///
    /// # SAFETY
    ///
    /// Know what you are doing before using this function.
    unsafe fn buf_mut(&self) -> &mut Buf {
        let raw_ptr = self.buf.get();
        &mut *raw_ptr
    }
}

// See `SimplexStreamReadFuture`'s `Future` impl for why we are implementing
// `Future` for its mutable reference.
impl<'a, Buf: monoio::buf::IoVecBufMut> Future for &mut SimplexStreamReadvFuture<'a, Buf> {
    type Output = std::io::Result<usize>;

    fn poll(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<Self::Output> {
        // SAFETY:
        //
        // See `SimpleStreamReadFuture`'s impl.
        let (stream, buf) = unsafe { (self.stream_mut(), self.buf_mut()) };
        SimplexStream::readv_with_context(stream, buf, cx)
    }
}

impl AsyncReadRent for SimplexStream {
    async fn read<T: monoio::buf::IoBufMut>(&mut self, buf: T) -> monoio::BufResult<usize, T> {
        let mut future_wrapper = SimplexStreamReadFuture::new(self, buf);
        let result = (&mut future_wrapper).await;

        (result, future_wrapper.buf.into_inner())
    }

    async fn readv<T: monoio::buf::IoVecBufMut>(&mut self, buf: T) -> monoio::BufResult<usize, T> {
        let mut future_wrapper = SimplexStreamReadvFuture::new(self, buf);
        let result = (&mut future_wrapper).await;

        (result, future_wrapper.buf.into_inner())
    }
}

/// Future wrapper for write operations.
///
/// # Why is it needed
///
/// See [`SimplexStreamReadFuture`]'s doc.
struct SimplexStreamWriteFuture<'stream, Buf: monoio::buf::IoBuf> {
    stream: UnsafeCell<&'stream mut SimplexStream>,
    buf: Buf,
    _marker: std::marker::PhantomData<Buf>,
}

impl<'stream, Buf: monoio::buf::IoBuf> SimplexStreamWriteFuture<'stream, Buf> {
    /// Creates a new `SimplexStreamWriteFuture`.
    fn new(stream: &'stream mut SimplexStream, buf: Buf) -> Self {
        let stream = UnsafeCell::new(stream);

        Self {
            stream,
            buf,
            _marker: std::marker::PhantomData,
        }
    }

    /// Access the `stream` field mutably through an immutable reference to `self`.
    ///
    /// # SAFETY
    ///
    /// Know what you are doing before using this function.
    unsafe fn stream_mut(&self) -> &mut SimplexStream {
        let raw_ptr = self.stream.get();
        *raw_ptr
    }
}

impl<'stream, Buf: monoio::buf::IoBuf> Future for &mut SimplexStreamWriteFuture<'stream, Buf> {
    type Output = std::io::Result<usize>;

    fn poll(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<Self::Output> {
        // SAFETY:
        //
        // 1. This function creates a mutable reference to self
        // 2. Later we will have an immutable reference to self `&self.buf`
        // 3. Even though they both refer to `self`, they target differenct fields
        //    so it should be safe.
        let stream = unsafe { self.stream_mut() };
        SimplexStream::write_with_context(stream, &self.buf, cx)
    }
}

/// Future wrapper for writev operations.
///
/// Basically same as `SimplexStreamWritevFuture`.
struct SimplexStreamWritevFuture<'stream, Buf: monoio::buf::IoVecBuf> {
    stream: UnsafeCell<&'stream mut SimplexStream>,
    buf: Buf,
    _marker: std::marker::PhantomData<Buf>,
}

impl<'stream, Buf: monoio::buf::IoVecBuf> SimplexStreamWritevFuture<'stream, Buf> {
    /// Creates a new `SimplexStreamWriteFuture`.
    fn new(stream: &'stream mut SimplexStream, buf: Buf) -> Self {
        let stream = UnsafeCell::new(stream);

        Self {
            stream,
            buf,
            _marker: std::marker::PhantomData,
        }
    }

    /// Access the `stream` field mutably through an immutable reference to `self`.
    ///
    /// # SAFETY
    ///
    /// Know what you are doing before using this function.
    unsafe fn stream_mut(&self) -> &mut SimplexStream {
        let raw_ptr = self.stream.get();
        *raw_ptr
    }
}

impl<'stream, Buf: monoio::buf::IoVecBuf> Future for &mut SimplexStreamWritevFuture<'stream, Buf> {
    type Output = std::io::Result<usize>;

    fn poll(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<Self::Output> {
        // SAFETY:
        //
        // See `SimplexStreamWriteFuture`'s impl.
        let stream = unsafe { self.stream_mut() };
        SimplexStream::writev_with_context(stream, &self.buf, cx)
    }
}

impl AsyncWriteRent for SimplexStream {
    async fn write<T: monoio::buf::IoBuf>(&mut self, buf: T) -> monoio::BufResult<usize, T> {
        let mut future_wrapper = SimplexStreamWriteFuture::new(self, buf);
        let result = (&mut future_wrapper).await;

        (result, future_wrapper.buf)
    }

    async fn writev<T: monoio::buf::IoVecBuf>(
        &mut self,
        buf_vec: T,
    ) -> monoio::BufResult<usize, T> {
        let mut future_wrapper = SimplexStreamWritevFuture::new(self, buf_vec);
        let result = (&mut future_wrapper).await;

        (result, future_wrapper.buf)
    }

    async fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }

    async fn shutdown(&mut self) -> std::io::Result<()> {
        self.close_write();
        Ok(())
    }
}

/// SAFETY:
///
/// > Users should ensure the read operations are indenpendence from the write
/// > ones, the methods from AsyncReadRent and AsyncWriteRent can execute
/// > concurrently.
///
/// For `SimplexSteam`, read and write modify different cursors, so they are
/// independent, it is safe to split.
unsafe impl monoio::io::Split for SimplexStream {}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper function to poll the future, and returns its current state.
    fn poll_in_place<F: std::future::Future>(fut: Pin<&mut F>) -> Poll<F::Output> {
        let waker = futures::task::noop_waker();
        let mut cx = futures::task::Context::from_waker(&waker);
        fut.poll(&mut cx)
    }

    #[monoio::test(enable_timer = true)]
    async fn basic_read_write() {
        let mut simplex = SimplexStream::new_unsplit(100);

        let msg = "helloworld";
        let (result, _buf) = simplex.write(msg.as_bytes().to_vec()).await;
        assert_eq!(result.unwrap(), msg.len());

        let (result, buf) = simplex.read(vec![0_u8; 20]).await;
        assert_eq!(result.unwrap(), msg.len());
        assert_eq!(&buf[0..msg.len()], msg.as_bytes());
        assert_eq!(&buf[msg.len()..], vec![0_u8; 10]);

        // write more
        let msg = "helloworldhelloworld";
        let (result, _buf) = simplex.write(msg.as_bytes().to_vec()).await;
        assert_eq!(result.unwrap(), msg.len());

        let (result, buf) = simplex.read(vec![0_u8; 20]).await;
        assert_eq!(result.unwrap(), msg.len());
        assert_eq!(&buf[0..msg.len()], msg.as_bytes());
    }

    #[monoio::test(enable_timer = true)]
    async fn read_empty_simplex() {
        let mut simplex = SimplexStream::new_unsplit(10);
        let read_future = std::pin::pin!(simplex.read(Vec::new()));
        let read_future_state = poll_in_place(read_future);
        assert!(read_future_state.is_pending());
    }

    #[monoio::test(enable_timer = true)]
    async fn write_full_simplex() {
        const MAX_BUF_SIZE: usize = 5;
        let mut simplex = SimplexStream::new_unsplit(MAX_BUF_SIZE);
        let (result, _) = simplex.write(vec![0_u8; MAX_BUF_SIZE]).await;
        assert!(matches!(result, Ok(5)));

        let write_future = std::pin::pin!(simplex.write(b"more"));
        let write_future_state = poll_in_place(write_future);
        assert!(write_future_state.is_pending());
    }
}
