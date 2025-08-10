use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use boardswarm_client::client::VolumeIoRW;
use bytes::BytesMut;
use futures::ready;
use tokio::io::{AsyncSeek, AsyncWrite};

enum BatchState {
    Empty,
    Batched(BytesMut),
}

/// Helper to write to buffer close to the maximum write side of a [VolumeIoRW].
///
/// Optionally this helper can discard flush requests. A flush for boardswarm volumes is relatively
/// heavy as it forces a network roundtrip and might cause non-optimal writes to the underlying
/// volumes. It is recommended to enable this by calling [BatchWriter::discard_flush] for
/// write-only use-cases. Especially when using helpers like [tokio::io::copy]) which treat this as
/// a low cost operation.
///
/// When the [BatchWriter] is dropped any outstanding data will be lost. To avoid that
/// call either [BatchWriter::force_flush] or [tokio::io::AsyncWriteExt::shutdown]. Not that for
/// some boardswarm targets calling `Shutdown` is required to ensure all buffers are written to the
/// device.
pub struct BatchWriter {
    buffer_size: usize,
    writer: VolumeIoRW,
    state: BatchState,
    discard_flush: bool,
    pending_seek: Option<std::io::SeekFrom>,
}

impl BatchWriter {
    pub fn new(writer: VolumeIoRW) -> Self {
        let blocksize = writer.blocksize().unwrap_or(4096) as usize;
        let buffer_size = VolumeIoRW::MAX_WRITE_SIZE - (VolumeIoRW::MAX_WRITE_SIZE % blocksize);

        let state = BatchState::Empty;
        Self {
            buffer_size,
            writer,
            state,
            discard_flush: false,
            pending_seek: None,
        }
    }

    pub fn discard_flush(mut self) -> Self {
        self.discard_flush = true;
        self
    }

    fn poll_flush_data(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        let me = self.get_mut();
        loop {
            match me.state {
                BatchState::Empty => return Poll::Ready(Ok(())),
                BatchState::Batched(ref mut b) => {
                    let written = ready!(Pin::new(&mut me.writer).poll_write(cx, b))?;
                    if written >= b.len() {
                        me.state = BatchState::Empty;
                    } else {
                        let _ = b.split_to(written);
                    }
                }
            }
        }
    }

    #[allow(dead_code)]
    pub fn force_flush(&mut self) -> impl Future<Output = Result<(), std::io::Error>> + '_ {
        std::future::poll_fn(|cx| {
            ready!(Self::poll_flush_data(Pin::new(self), cx))?;
            Pin::new(&mut self.writer).poll_flush(cx)
        })
    }
}

impl AsyncWrite for BatchWriter {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        let me = self.get_mut();
        match me.state {
            BatchState::Empty => {
                if buf.len() >= me.buffer_size {
                    Pin::new(&mut me.writer).poll_write(cx, buf)
                } else {
                    let mut batch = BytesMut::with_capacity(me.buffer_size);
                    batch.extend_from_slice(buf);
                    me.state = BatchState::Batched(batch);
                    Poll::Ready(Ok(buf.len()))
                }
            }
            BatchState::Batched(ref mut b) => {
                let cached = b.len();
                if buf.len() + cached < me.buffer_size {
                    b.extend_from_slice(buf);
                    Poll::Ready(Ok(buf.len()))
                } else {
                    let remaining = me.buffer_size - cached;
                    b.extend_from_slice(&buf[0..remaining]);
                    match Pin::new(&mut me.writer).poll_write(cx, b) {
                        Poll::Ready(Ok(written)) if written >= b.len() => {
                            me.state = BatchState::Empty;
                            Poll::Ready(Ok(remaining))
                        }
                        Poll::Ready(Ok(written)) => {
                            let _ = b.split_to(written);
                            Poll::Ready(Ok(remaining))
                        }
                        Poll::Ready(Err(e)) => {
                            b.truncate(cached);
                            Poll::Ready(Err(e))
                        }
                        Poll::Pending => {
                            b.truncate(cached);
                            Poll::Pending
                        }
                    }
                }
            }
        }
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        if self.discard_flush {
            Poll::Ready(Ok(()))
        } else {
            ready!(Self::poll_flush_data(self.as_mut(), cx))?;
            Pin::new(&mut self.writer).poll_flush(cx)
        }
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        ready!(Self::poll_flush_data(self.as_mut(), cx))?;
        Pin::new(&mut self.writer).poll_shutdown(cx)
    }
}

impl AsyncSeek for BatchWriter {
    fn start_seek(
        self: std::pin::Pin<&mut Self>,
        position: std::io::SeekFrom,
    ) -> std::io::Result<()> {
        let me = self.get_mut();
        match me.state {
            BatchState::Empty => Pin::new(&mut me.writer).start_seek(position),
            BatchState::Batched(_) => {
                if me.pending_seek.is_some() {
                    Err(std::io::Error::other("Seek in progress"))
                } else {
                    me.pending_seek = Some(position);
                    Ok(())
                }
            }
        }
    }

    fn poll_complete(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<std::io::Result<u64>> {
        ready!(Self::poll_flush_data(self.as_mut(), cx))?;
        let me = self.get_mut();
        if let Some(pending_seek) = me.pending_seek.take() {
            Pin::new(&mut me.writer).start_seek(pending_seek)?;
        }
        Pin::new(&mut me.writer).poll_complete(cx)
    }
}
