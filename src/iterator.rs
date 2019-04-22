use parquet::file::reader::SerializedFileReader;
use std::fs::File;
use std::path::{Path, PathBuf};
use walkdir::{DirEntry, WalkDir};

type ParquetReaderResult = Result<SerializedFileReader<File>, String>;

fn is_parquet_file(entry: &DirEntry) -> bool {
    if !entry.path().is_file() {
        return false;
    }

    return entry
        .file_name()
        .to_str()
        .map(|s| s.ends_with(".parquet"))
        .unwrap_or(false);
}

fn open_file(path: &Path) -> Result<File, String> {
    return File::open(path).map_err(|e| format!("Failed open parquet file : {}", e));
}

fn read_parquet_file(path: &Path) -> ParquetReaderResult {
    return SerializedFileReader::new(open_file(path)?)
        .map_err(|e| format!("Failed read parquet file : {}", e));
}

fn parquet_dir_iter(path: &Path) -> impl Iterator<Item = PathBuf> {
    return WalkDir::new(path)
        .contents_first(true)
        .into_iter()
        .filter_entry(is_parquet_file)
        .filter_map(|e| e.ok())
        .map(|e| e.into_path());
}

fn parquet_file_iter(path: &Path) -> impl Iterator<Item = PathBuf> {
    return WalkDir::new(path)
        .into_iter()
        .filter_map(|e| e.ok())
        .map(|e| e.into_path());
}

fn parquet_path_iter(path: &Path) -> Box<Iterator<Item = PathBuf>> {
    return match path.is_file() {
        true => Box::new(parquet_file_iter(path)),
        false => Box::new(parquet_dir_iter(path)),
    };
}

pub struct ParquetPathIterator {
    iter: Box<Iterator<Item = PathBuf>>,
}

impl ParquetPathIterator {
    pub fn new(path: &Path) -> Self {
        Self {
            iter: parquet_path_iter(path),
        }
    }
}

impl Iterator for ParquetPathIterator {
    type Item = PathBuf;

    fn next(&mut self) -> Option<PathBuf> {
        return self.iter.next();
    }
}

pub struct ParquetFileReader {
    iter: ParquetPathIterator,
}

impl ParquetFileReader {
    pub fn new(iter: ParquetPathIterator) -> Self {
        Self { iter: iter }
    }
}

impl Iterator for ParquetFileReader {
    type Item = ParquetReaderResult;

    fn next(&mut self) -> Option<ParquetReaderResult> {
        return self.iter.next().map(|p| read_parquet_file(&p));
    }
}
