**NOTICE: THIS CLIENT IS UNDER ACTIVE DEVELOPMENT, USE AT YOUR OWN RISK**

# xpq
xpq is a simple command line program for analyzing parquet files.

[![Build Status](https://travis-ci.org/FabioBatSilva/xpq.svg?branch=master)](https://travis-ci.org/FabioBatSilva/xpq)


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

* **count** - Show num of rows.
* **schema** - Show parquet schema.
* **sample** - Randomly sample rows from parquet.

### Quick tour

Grab the some parquet data :

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
xpq sample users.parquet

 name      favorite_color  favorite_numbers
 "Alyssa"  null            [3, 9, 15, 20]
 "Ben"     "red"           []
```
