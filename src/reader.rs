use parquet::file::reader::FileReader;
use parquet::file::reader::SerializedFileReader;
use parquet::record::reader::RowIter;
use parquet::record::Row;
use std::fs::File;
use std::path::{Path, PathBuf};
use walkdir::{DirEntry, WalkDir};

pub type ParquetFileReader = SerializedFileReader<File>;
pub type ParquetReaderResult = Result<ParquetFileReader, String>;
pub type ParquetReadersResult = Result<Vec<ParquetFileReader>, String>;
pub type ParquetRowIteratorResult<'a> = Result<ParquetRowIterator<'a>, String>;

fn is_parquet_file(entry: &DirEntry) -> bool {
    if !entry.path().is_file() {
        return false;
    }

    entry
        .file_name()
        .to_str()
        .map(|s| s.ends_with(".parquet"))
        .unwrap_or(false)
}

#[inline]
fn open_file(path: &Path) -> Result<File, String> {
    File::open(path).map_err(|e| format!("{} {}", path.display(), e))
}

#[inline]
fn create_parquet_reader(path: &Path) -> ParquetReaderResult {
    SerializedFileReader::new(open_file(path)?)
        .map_err(|e| format!("{} - {}", path.display(), e))
}

#[inline]
fn create_row_iter<'a>(reader: &'a ParquetFileReader) -> Result<RowIter<'a>, String> {
    reader
        .get_row_iter(None)
        .map_err(|e| format!("Failed iterate parquet file : {}", e))
}

#[inline]
fn walk_parquet_dir(path: &Path) -> Vec<PathBuf> {
    WalkDir::new(path)
        .contents_first(true)
        .into_iter()
        .filter_entry(is_parquet_file)
        .filter_map(|e| e.ok())
        .map(|e| e.into_path())
        .collect::<Vec<_>>()
}

fn walk_parquet(path: &Path) -> Vec<PathBuf> {
    match path.is_file() {
        true => vec![path.to_path_buf()],
        false => walk_parquet_dir(path),
    }
}

pub fn get_parquet_readers(path: &Path) -> ParquetReadersResult {
    let mut vec = Vec::new();

    for p in walk_parquet(path) {
        vec.push(create_parquet_reader(&p)?);
    }

    Ok(vec)
}

pub struct ParquetRowIterator<'a> {
    vec: Vec<RowIter<'a>>,
    index: usize,
}

impl<'a> ParquetRowIterator<'a> {
    pub fn new(vec: Vec<RowIter<'a>>) -> Self {
        Self { vec: vec, index: 0 }
    }

    pub fn of(readers: &'a Vec<ParquetFileReader>) -> ParquetRowIteratorResult<'a> {
        let mut vec = Vec::new();

        for r in readers {
            vec.push(create_row_iter(r)?);
        }

        Ok(Self::new(vec))
    }
}

impl<'a> Iterator for ParquetRowIterator<'a> {
    type Item = Row;

    fn next(&mut self) -> Option<Self::Item> {
        while self.index < self.vec.len() {
            let next = self.vec[self.index].next();

            if next.is_some() {
                return next;
            }

            self.index = self.index + 1;
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use utils::test_utils;

    #[test]
    fn test_path_to_reader() {
        let dir = test_utils::temp_dir();
        let path1 = dir.path().join("1.snappy.parquet");
        let path2 = dir.path().join("2.snappy.parquet");
        let path3 = dir.path().join("3.snappy.parquet");

        File::create(path1.clone()).unwrap();
        File::create(path2.clone()).unwrap();

        test_utils::write_simple_message_parquet(
            &path1,
            &test_utils::SimpleMessage {
                field_int32: 1,
                field_int64: 2,
                field_float: 3.3,
                field_double: 4.4,
                field_string: "5".to_string(),
                field_boolean: true,
                field_timestamp: vec![0, 0, 2454923],
            },
        );

        let result1 = create_parquet_reader(&path1);
        let result2 = create_parquet_reader(&path2);
        let result3 = create_parquet_reader(&path3);

        assert_eq!(result1.is_ok(), true);
        assert_eq!(result2.is_ok(), false);
        assert_eq!(result3.is_ok(), false);
    }

    #[test]
    fn test_reader_to_row_iter() {
        let dir = test_utils::temp_dir();
        let path = dir.path().join("file.parquet");

        File::create(path.clone()).unwrap();

        test_utils::write_simple_message_parquet(
            &path,
            &test_utils::SimpleMessage {
                field_int32: 1,
                field_int64: 2,
                field_float: 3.3,
                field_double: 4.4,
                field_string: "5".to_string(),
                field_boolean: true,
                field_timestamp: vec![0, 0, 2454923],
            },
        );

        let reader = create_parquet_reader(&path).unwrap();
        let result = create_row_iter(&reader);

        assert_eq!(result.is_ok(), true);
    }

    #[test]
    fn test_walk_parquet() {
        let dir = test_utils::temp_dir();
        let path1 = dir.path().join("part-1.snappy.parquet");
        let path2 = dir.path().join("part-2.snappy.parquet");
        let path3 = dir.path().join("_SUCCESS");

        File::create(path1.clone()).unwrap();
        File::create(path2.clone()).unwrap();
        File::create(path3.clone()).unwrap();

        let mut dir_vec = walk_parquet(dir.path());
        let file1_vec = walk_parquet(&path1);
        let file2_vec = walk_parquet(&path2);
        let file3_vec = walk_parquet(&path3);

        dir_vec.sort();

        assert_eq!(dir_vec.len(), 2);
        assert_eq!(dir_vec, vec![path1.clone(), path2.clone()]);

        assert_eq!(file1_vec.len(), 1);
        assert_eq!(file1_vec, vec![path1.clone()]);

        assert_eq!(file2_vec.len(), 1);
        assert_eq!(file2_vec, vec![path2.clone()]);

        assert_eq!(file3_vec.len(), 1);
        assert_eq!(file3_vec, vec![path3.clone()]);
    }

    #[test]
    fn test_get_parquet_readers() {
        let dir = test_utils::temp_dir();
        let path1 = dir.path().join("1.snappy.parquet");
        let path2 = dir.path().join("2.snappy.parquet");
        let path3 = dir.path().join("_SUCCESS");
        let root = dir.path();

        File::create(path1.clone()).unwrap();
        File::create(path2.clone()).unwrap();
        File::create(path3.clone()).unwrap();

        {
            test_utils::write_simple_message_parquet(
                &path1,
                &test_utils::SimpleMessage {
                    field_int32: 1,
                    field_int64: 2,
                    field_float: 3.3,
                    field_double: 4.4,
                    field_string: "5".to_string(),
                    field_boolean: true,
                    field_timestamp: vec![0, 0, 2454923],
                },
            );

            test_utils::write_simple_message_parquet(
                &path2,
                &test_utils::SimpleMessage {
                    field_int32: 11,
                    field_int64: 22,
                    field_float: 33.3,
                    field_double: 44.4,
                    field_string: "55".to_string(),
                    field_boolean: false,
                    field_timestamp: vec![4165425152, 13, 2454923],
                },
            );
        }

        let result_ok = get_parquet_readers(&root);
        let result_err = get_parquet_readers(&path3);

        assert_eq!(result_ok.is_ok(), true);
        assert_eq!(result_ok.unwrap().len(), 2);

        assert_eq!(result_err.is_err(), true);
        assert_eq!(
            result_err.err().unwrap(),
            format!(
                "{} - Parquet error: Invalid Parquet file. Size is smaller than footer",
                path3.to_string_lossy()
            )
        );
    }
}
