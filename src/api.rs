use parquet::errors::ParquetError;
use quick_error::quick_error;
use std::io;
use std::path::PathBuf;
use std::result;

quick_error! {
    /// Set of errors that can be produced during different operations.
    #[derive(Debug, PartialEq, Eq)]
    pub enum Error {
        /// General Parquet error.
        Parquet(path: PathBuf, err: ParquetError) {
            display("{} >>> {}", path.display(), err)
            description("Parquet error")
        }
        InvalidParquet(path: PathBuf) {
            display("Invalid parquet: {}", path.display())
            description("Invalid parquet")
            from()
        }
        IO(err: String) {
            display("IO error: {}", err)
            description("IO error")
            from(e: io::Error) -> (format!("{}", e))
        }
        CSV(err: String) {
            display("CSV error: {}", err)
            description("CSV error")
            from(e: csv::Error) -> (format!("{}", e))
        }
        Filter(err: String) {
            display("Filter error: {}", err)
            description("Filter error")
            from(e: regex::Error) -> (format!("{}", e))
        }
        /// Invalid argument error.
        InvalidArgument(name: String) {
            display("Invalid argument: {}", name)
            description("Invalid argument")
        }
    }
}

/// A specialized `Result` for all errors.
pub type Result<T> = result::Result<T, Error>;

#[cfg(test)]
pub(crate) mod tests {
    extern crate chrono;
    extern crate tempfile;

    use self::tempfile::{Builder, NamedTempFile, TempDir};
    use std::iter;
    use std::sync::Arc;
    use std::{fs, path::Path};

    use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
    use parquet_derive::ParquetRecordWriter;

    use parquet::file::properties::WriterProperties;
    use parquet::file::writer::SerializedFileWriter;
    use parquet::record::RecordWriter;

    #[derive(Debug, ParquetRecordWriter)]
    pub struct SimpleMessage {
        pub field_int32: i32,
        pub field_int64: i64,
        pub field_float: f32,
        pub field_double: f64,
        pub field_string: String,
        pub field_boolean: bool,
        pub field_timestamp: NaiveDateTime,
    }

    pub fn temp_file(name: &str, suffix: &str) -> NamedTempFile {
        Builder::new()
            .suffix(suffix)
            .prefix(name)
            .rand_bytes(5)
            .tempfile()
            .expect("Fail to create tmp file")
    }

    pub fn temp_dir() -> TempDir {
        Builder::new()
            .rand_bytes(5)
            .tempdir()
            .expect("Fail to create tmp file")
    }

    pub fn create_simple_messages(num: usize) -> Vec<SimpleMessage> {
        let iter = 1..=num;
        let odd_even = |i| if i % 2 != 0 { "odd" } else { "even" };
        let timestamp = |i| {
            NaiveDateTime::new(
                NaiveDate::from_ymd((2010 + i) as i32, 1, 1),
                NaiveTime::from_hms(0, 0, 0),
            )
        };
        let repeat = |i: usize, n: usize| {
            iter::repeat(i)
                .map(|e| e.to_string())
                .take(n)
                .collect::<Vec<_>>()
                .concat()
        };

        iter.map(|i| SimpleMessage {
            field_int32: i as i32,
            field_int64: repeat(i, 2).parse().unwrap(),
            field_float: format!("{}.3", repeat(i, 3)).parse().unwrap(),
            field_double: format!("{}.4", repeat(i, 4)).parse().unwrap(),
            field_string: format!("{} {}", odd_even(i), repeat(i, 5)),
            field_boolean: i % 2 == 0,
            field_timestamp: timestamp(i),
        })
        .collect()
    }

    pub fn write_simple_messages_parquet(path: &Path, vec: &[SimpleMessage]) {
        let schema = vec.schema().unwrap();
        let props = Arc::new(WriterProperties::builder().build());
        let file = fs::File::create(path).unwrap();
        let mut writer = SerializedFileWriter::new(file, schema, props).unwrap();
        let mut row_group = writer.next_row_group().unwrap();

        vec.write_to_row_group(&mut row_group).unwrap();

        row_group.close().unwrap();
        writer.close().unwrap();
    }
}
