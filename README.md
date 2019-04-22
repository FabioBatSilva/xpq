# xpq
xpq is a command line program for sampling, analyzing parquet files.

**NOTICE: THIS CLIENT IS UNDER ACTIVE DEVELOPMENT, USE AT YOUR OWN RISK**

## Requirements
- Rust nightly

See [Working with nightly Rust](https://github.com/rust-lang-nursery/rustup.rs/blob/master/README.md#working-with-nightly-rust)
to install nightly toolchain and set it as default.

### Installation
You can compile from source by

```bash
git clone git://github.com/FabioBatSilva/xpq
cd xpq
cargo install --path .
```

### Available commands

* **schema** - Show parquet schema.
* **sample** - Read rows from parquet.

### Quick tour
