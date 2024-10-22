//! Port of Tokio's [`SimplexStream`][simplex] and [`DuplexStream`][duplex] for
//! Monoio.
//!
//! [simplex]: https://docs.rs/tokio/latest/tokio/io/struct.SimplexStream.html
//! [duplex]: https://docs.rs/tokio/latest/tokio/io/struct.DuplexStream.html

pub mod duplex;
pub mod simplex;
