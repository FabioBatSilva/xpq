use parquet::errors::ParquetError;
use quick_error::quick_error;
use std::io;
use std::path::PathBuf;
use std::result;

quick_error! {
    /// Set of errors that can be produced during different operations.
    #[derive(Debug, PartialEq)]
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
        /// Invalid argument error.
        InvalidArgument(name: String) {
            display("Invalid argument: {}", name)
            description("Invalid argument")
        }
    }
}

/// A specialized `Result` for all errors.
pub type Result<T> = result::Result<T, Error>;
