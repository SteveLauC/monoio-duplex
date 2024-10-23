# monoio-duplex

[![crates.io](https://img.shields.io/crates/v/signalfut?style=flat-square&logo=rust)](https://crates.io/crates/monoio-duplex)
[![docs.rs](https://img.shields.io/badge/docs.rs-signalfut-blue?style=flat-square&logo=docs.rs)](https://docs.rs/monoio-duplex)
[![BUILD](https://github.com/stevelauc/monoio-duplex/workflows/ci/badge.svg)](https://github.com/stevelauc/monoio-duplex/actions/workflows/ci.yml)
![Crates.io](https://img.shields.io/crates/d/monoio-duplex?color=orange)

Port of Tokio's [`SimplexStream`][simplex] and [`DuplexStream`][duplex] for 
[Monoio].

[Monoio]: https://github.com/bytedance/monoio
[simplex]: https://docs.rs/tokio/latest/tokio/io/struct.SimplexStream.html
[duplex]: https://docs.rs/tokio/latest/tokio/io/struct.DuplexStream.html