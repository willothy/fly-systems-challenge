//! Willothy note: This is a copy of the code from the tokio-serde-json crate. I have vendored it
//! in order to update the dependencies without forking, since the original crate has not been
//! updated in 9 months and this is a small side project that will never see actual use.

//! This crate provides the utilities needed to easily implement a Tokio
//! transport using [serde] for serialization and deserialization of frame
//! values.
//!
//! # Introduction
//!
//! This crate provides [transport] combinators that transform a stream of
//! frames encoded as bytes into a stream of frame values. It is expected that
//! the framing happens at another layer. One option is to use a [length
//! delimited] framing transport.
//!
//! The crate provides two traits that must be implemented: [`Serializer`] and
//! [`Deserializer`]. Implementations of these traits are then passed to
//! [`Framed`] along with the upstream [`Stream`] or
//! [`Sink`] that handles the byte encoded frames.
//!
//! By doing this, a transformation pipeline is built. For reading, it looks
//! something like this:
//!
//! * `tokio_serde::Framed`
//! * `tokio_util::codec::FramedRead`
//! * `tokio::net::TcpStream`
//!
//! The write half looks like:
//!
//! * `tokio_serde::Framed`
//! * `tokio_util::codec::FramedWrite`
//! * `tokio::net::TcpStream`
//!
//! # Examples
//!
//! For an example, see how JSON support is implemented:
//!
//! * [server](https://github.com/carllerche/tokio-serde/blob/master/examples/server.rs)
//! * [client](https://github.com/carllerche/tokio-serde/blob/master/examples/client.rs)
//!
//! [serde]: https://serde.rs
//! [serde-json]: https://github.com/serde-rs/json
//! [transport]: https://tokio.rs/docs/going-deeper/transports/
//! [length delimited]: https://docs.rs/tokio-util/0.2/tokio_util/codec/length_delimited/index.html
//! [`Serializer`]: trait.Serializer.html
//! [`Deserializer`]: trait.Deserializer.html
//! [`Framed`]: struct.Framed.html
//! [`Stream`]: https://docs.rs/futures/0.3/futures/stream/trait.Stream.html
//! [`Sink`]: https://docs.rs/futures/0.3/futures/sink/trait.Sink.html

use bytes::{Bytes, BytesMut};
use futures::{Sink, TryStream};
// use futures_core::{ready, Stream, TryStream};
// use futures_sink::Sink;
use pin_project::pin_project;
use std::{
    marker::PhantomData,
    pin::Pin,
    task::{ready, Context, Poll},
};
use tokio_stream::Stream;

/// Serializes a value into a destination buffer
///
/// Implementations of `Serializer` are able to take values of type `T` and
/// convert them to a byte representation. The specific byte format, i.e. JSON,
/// protobuf, binpack, ... is an implementation detail.
///
/// The `serialize` function takes `&mut self`, allowing for `Serializer`
/// instances to be created with runtime configuration settings.
///
/// # Examples
///
/// An integer serializer that allows the width to be configured.
///
/// ```
/// use tokio_serde::Serializer;
/// use bytes::{Buf, Bytes, BytesMut, BufMut};
/// use std::pin::Pin;
///
/// struct IntSerializer {
///     width: usize,
/// }
///
/// #[derive(Debug)]
/// enum Error {
///     Overflow,
/// }
///
/// impl Serializer<u64> for IntSerializer {
///     type Error = Error;
///
///     fn serialize(self: Pin<&mut Self>, item: &u64) -> Result<Bytes, Self::Error> {
///         assert!(self.width <= 8);
///
///         let max = (1 << (self.width * 8)) - 1;
///
///         if *item > max {
///             return Err(Error::Overflow);
///         }
///
///         let mut ret = BytesMut::with_capacity(self.width);
///         ret.put_uint(*item, self.width);
///         Ok(ret.into())
///     }
/// }
///
/// let mut serializer = IntSerializer { width: 3 };
///
/// let buf = Pin::new(&mut serializer).serialize(&5).unwrap();
/// assert_eq!(buf, &b"\x00\x00\x05"[..]);
/// ```
pub trait Serializer<T> {
    type Error;

    /// Serializes `item` into a new buffer
    ///
    /// The serialization format is specific to the various implementations of
    /// `Serializer`. If the serialization is successful, a buffer containing
    /// the serialized item is returned. If the serialization is unsuccessful,
    /// an error is returned.
    ///
    /// Implementations of this function should not mutate `item` via any sort
    /// of internal mutability strategy.
    ///
    /// See the trait level docs for more detail.
    fn serialize(self: Pin<&mut Self>, item: &T) -> Result<Bytes, Self::Error>;
}

/// Deserializes a value from a source buffer
///
/// Implementatinos of `Deserializer` take a byte buffer and return a value by
/// parsing the contents of the buffer according to the implementation's format.
/// The specific byte format, i.e. JSON, protobuf, binpack, is an implementation
/// detail
///
/// The `deserialize` function takes `&mut self`, allowing for `Deserializer`
/// instances to be created with runtime configuration settings.
///
/// It is expected that the supplied buffer represents a full value and only
/// that value. If after deserializing a value there are remaining bytes the
/// buffer, the deserializer will return an error.
///
/// # Examples
///
/// An integer deserializer that allows the width to be configured.
///
/// ```
/// use tokio_serde::Deserializer;
/// use bytes::{BytesMut, Buf};
/// use std::pin::Pin;
///
/// struct IntDeserializer {
///     width: usize,
/// }
///
/// #[derive(Debug)]
/// enum Error {
///     Underflow,
///     Overflow
/// }
///
/// impl Deserializer<u64> for IntDeserializer {
///     type Error = Error;
///
///     fn deserialize(self: Pin<&mut Self>, buf: &BytesMut) -> Result<u64, Self::Error> {
///         assert!(self.width <= 8);
///
///         if buf.len() > self.width {
///             return Err(Error::Overflow);
///         }
///
///         if buf.len() < self.width {
///             return Err(Error::Underflow);
///         }
///
///         let ret = std::io::Cursor::new(buf).get_uint(self.width);
///         Ok(ret)
///     }
/// }
///
/// let mut deserializer = IntDeserializer { width: 3 };
///
/// let i = Pin::new(&mut deserializer).deserialize(&b"\x00\x00\x05"[..].into()).unwrap();
/// assert_eq!(i, 5);
/// ```
pub trait Deserializer<T> {
    type Error;

    /// Deserializes a value from `buf`
    ///
    /// The serialization format is specific to the various implementations of
    /// `Deserializer`. If the deserialization is successful, the value is
    /// returned. If the deserialization is unsuccessful, an error is returned.
    ///
    /// See the trait level docs for more detail.
    fn deserialize(self: Pin<&mut Self>, src: &BytesMut) -> Result<T, Self::Error>;
}

/// Adapts a transport to a value sink by serializing the values and to a stream of values by deserializing them.
///
/// It is expected that the buffers yielded by the supplied transport be framed. In
/// other words, each yielded buffer must represent exactly one serialized
/// value.
///
/// The provided transport will receive buffer values containing the
/// serialized value. Each buffer contains exactly one value. This sink will be
/// responsible for writing these buffers to an `AsyncWrite` using some sort of
/// framing strategy.
///
/// The specific framing strategy is left up to the
/// implementor. One option would be to use [length_delimited] provided by
/// [tokio-util].
///
/// [length_delimited]: http://docs.rs/tokio-util/0.2/tokio_util/codec/length_delimited/index.html
/// [tokio-util]: http://crates.io/crates/tokio-util
#[pin_project]
#[derive(Debug)]
pub struct Framed<Transport, Item, SinkItem, Codec> {
    #[pin]
    inner: Transport,
    #[pin]
    codec: Codec,
    item: PhantomData<(Item, SinkItem)>,
}

impl<Transport, Item, SinkItem, Codec> Framed<Transport, Item, SinkItem, Codec> {
    /// Creates a new `Framed` with the given transport and codec.
    pub fn new(inner: Transport, codec: Codec) -> Self {
        Self {
            inner,
            codec,
            item: PhantomData,
        }
    }

    /// Returns a reference to the underlying transport wrapped by `Framed`.
    ///
    /// Note that care should be taken to not tamper with the underlying transport as
    /// it may corrupt the sequence of frames otherwise being worked with.
    pub fn get_ref(&self) -> &Transport {
        &self.inner
    }

    /// Returns a mutable reference to the underlying transport wrapped by
    /// `Framed`.
    ///
    /// Note that care should be taken to not tamper with the underlying transport as
    /// it may corrupt the sequence of frames otherwise being worked with.
    pub fn get_mut(&mut self) -> &mut Transport {
        &mut self.inner
    }

    /// Consumes the `Framed`, returning its underlying transport.
    ///
    /// Note that care should be taken to not tamper with the underlying transport as
    /// it may corrupt the sequence of frames otherwise being worked with.
    pub fn into_inner(self) -> Transport {
        self.inner
    }
}

impl<Transport, Item, SinkItem, Codec> Stream for Framed<Transport, Item, SinkItem, Codec>
where
    Transport: TryStream<Ok = BytesMut>,
    Transport::Error: From<Codec::Error>,
    BytesMut: From<Transport::Ok>,
    Codec: Deserializer<Item>,
{
    type Item = Result<Item, Transport::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match ready!(self.as_mut().project().inner.try_poll_next(cx)) {
            Some(bytes) => Poll::Ready(Some(Ok(self
                .as_mut()
                .project()
                .codec
                .deserialize(&bytes?)?))),
            None => Poll::Ready(None),
        }
    }
}

impl<Transport, Item, SinkItem, Codec> Sink<SinkItem> for Framed<Transport, Item, SinkItem, Codec>
where
    Transport: Sink<Bytes>,
    Codec: Serializer<SinkItem>,
    Codec::Error: Into<Transport::Error>,
{
    type Error = Transport::Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project().inner.poll_ready(cx)
    }

    fn start_send(mut self: Pin<&mut Self>, item: SinkItem) -> Result<(), Self::Error> {
        let res = self.as_mut().project().codec.serialize(&item);
        let bytes = res.map_err(Into::into)?;

        self.as_mut().project().inner.start_send(bytes)?;

        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project().inner.poll_flush(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        ready!(self.as_mut().poll_flush(cx))?;
        self.project().inner.poll_close(cx)
    }
}

pub type SymmetricallyFramed<Transport, Value, Codec> = Framed<Transport, Value, Value, Codec>;

pub mod formats {
    pub use self::json::*;

    mod json {
        use std::{io::Write, marker::PhantomData, pin::Pin};

        use crate::tokio_serde::{Deserializer, Serializer};

        use bytes::{Buf, BufMut, Bytes, BytesMut};
        use educe::Educe;
        use serde::{de::DeserializeOwned, Deserialize, Serialize};
        use tokio_util::codec::{Decoder, Encoder};

        /// JSON codec using [serde_json](https://docs.rs/serde_json) crate.
        #[derive(Educe)]
        #[educe(Debug, Default, Clone, Copy)]
        pub struct Json<Item, SinkItem> {
            #[educe(Debug(ignore))]
            ghost: PhantomData<(Item, SinkItem)>,
        }

        pub type SymmetricalJson<T> = Json<T, T>;

        // impl<Item, SinkItem> Deserializer<Item> for Json<Item, SinkItem>
        // where
        //     for<'a> Item: Deserialize<'a>,
        // {
        //     type Error = serde_json::Error;
        //
        //     fn deserialize(self: Pin<&mut Self>, src: &BytesMut) -> Result<Item, Self::Error> {
        //         serde_json::from_reader(std::io::Cursor::new(src).reader())
        //     }
        // }
        //
        // impl<Item, SinkItem> Serializer<SinkItem> for Json<Item, SinkItem>
        // where
        //     SinkItem: Serialize,
        // {
        //     type Error = serde_json::Error;
        //
        //     fn serialize(self: Pin<&mut Self>, item: &SinkItem) -> Result<Bytes, Self::Error> {
        //         serde_json::to_vec(item).map(Into::into)
        //     }
        // }

        impl<Item, SinkItem> Decoder for Json<Item, SinkItem>
        where
            for<'a> Item: DeserializeOwned + Deserialize<'a>,
        {
            type Item = Item;
            type Error = std::io::Error;

            fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
                if src.trim_ascii().len() == 0 {
                    src.clear();
                    return Ok(None);
                }

                let item = match serde_json::from_reader(std::io::Cursor::new(&mut *src).reader()) {
                    Ok(item) => {
                        src.clear();
                        item
                    }
                    Err(err) if err.is_eof() || err.is_syntax() => {
                        return Ok(None);
                    }
                    Err(e) => return Err(e.into()),
                };

                Ok(Some(item))
            }
        }

        impl<Item, SinkItem> Encoder<Item> for Json<Item, SinkItem>
        where
            Item: Serialize,
        {
            type Error = std::io::Error;

            fn encode(&mut self, item: Item, dst: &mut BytesMut) -> Result<(), Self::Error> {
                let mut w = dst.writer();

                serde_json::to_writer(&mut w, &item)?;

                w.write(b"\n")?;

                Ok(())
            }
        }
    }
}
