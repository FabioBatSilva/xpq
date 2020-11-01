# xpq
xpq is a simple command line program for analyzing parquet files.

![Build Status](https://github.com/FabioBatSilva/xpq/workflows/ci/badge.svg?branch=master)


## Requirements
- Rust nightly

See [Working with nightly Rust](https://github.com/rust-lang-nursery/rustup.rs/blob/master/README.md#working-with-nightly-rust)
to install nightly toolchain and set it as default.

### Installation
Binaries for Linux and macOS are available [from Github](https://github.com/FabioBatSilva/xpq/releases/latest).

To install the binary download the [latest](https://github.com/FabioBatSilva/xpq/releases/latest) release.

```bash
curl -s https://api.github.com/repos/FabioBatSilva/xpq/releases/latest \
  | grep "browser_download_url" \
  | grep apple-darwin \
  | cut -d : -f 2,3 \
  | tr -d \" \
  | wget -qi -
```

Make it executable
```bash
chmod +x ./xpq-*-apple-darwin

mv ./xpq-*-apple-darwin /usr/local/bin/xpq
```

Alternatively, you can compile and install using [Cargo](https://crates.io/install) :

```bash
cargo install xpq
```

You can also compile from source using cargo

```bash
cargo install --git https://github.com/FabioBatSilva/xpq.git --force
```

### Available commands

* **read** - Read rows.
* **count** - Show num of rows.
* **schema** - Show parquet schema.
* **sample** - Randomly sample rows from parquet.
* **frequency** - Show frequency counts for each value.

### Quick tour

Grab some parquet data :

```
wget -O users.parquet https://github.com/apache/spark/blob/master/examples/src/main/resources/users.parquet?raw=true

```

Check the schema :
```
xpq schema users.parquet

message example.avro.User {
  REQUIRED BYTE_ARRAY name (UTF8);
  OPTIONAL BYTE_ARRAY favorite_color (UTF8);
  REQUIRED group favorite_numbers (LIST) {
    REPEATED INT32 array;
  }
}
```

Check the number of rows :
```
xpq count users.parquet

 count
 2
```

Read some data :
```
xpq read users.parquet

 name      favorite_color  favorite_numbers
 "Alyssa"  null            [3, 9, 15, 20]
 "Ben"     "red"           []
```
