use parquet::file::metadata::ParquetMetaDataPtr;
use parquet::file::reader::FileReader;
use parquet::file::reader::SerializedFileReader;
use parquet::record::reader::RowIter;
use parquet::record::Row;
use parquet::record::RowFormatter;
use std::collections::HashMap;
use std::fs::File;
use std::path::{Path, PathBuf};
use walkdir::{DirEntry, WalkDir};

pub type ParquetFileReader = SerializedFileReader<File>;
pub type ParquetReaderResult = Result<ParquetFileReader, String>;
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
    File::open(path).map_err(|e| format!("{} >>> {}", path.display(), e))
}

#[inline]
fn create_parquet_reader(path: &Path) -> ParquetReaderResult {
    SerializedFileReader::new(open_file(path)?)
        .map_err(|e| format!("{} >>> {}", path.display(), e))
}

#[inline]
fn create_row_iter(reader: &ParquetFileReader) -> Result<RowIter, String> {
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
        .filter_map(Result::ok)
        .map(DirEntry::into_path)
        .collect::<Vec<_>>()
}

fn walk_parquet(path: &Path) -> Vec<PathBuf> {
    Some(Path::new(&path))
        .filter(|p| p.is_file())
        .map(|p| vec![p.to_path_buf()])
        .unwrap_or_else(|| walk_parquet_dir(path))
}

fn get_parquet_readers(path: &Path) -> Result<Vec<ParquetFileReader>, String> {
    let mut vec = Vec::new();

    for p in walk_parquet(path) {
        vec.push(create_parquet_reader(&p)?);
    }

    Ok(vec)
}

fn get_row_fields(
    reader: &ParquetFileReader,
    columns: Option<Vec<String>>,
) -> Vec<(usize, String)> {
    let metadata = reader.metadata().file_metadata();
    let schema = metadata.schema();
    let mut result = Vec::new();
    let fields = schema.get_fields();
    let enumerate = fields.iter().enumerate();

    match columns {
        Some(names) => {
            let map = enumerate
                .map(|t| (t.1.name().to_lowercase(), t.0))
                .collect::<HashMap<_, _>>();

            for name in names {
                if let Some(index) = map.get(&name.to_lowercase()) {
                    result.push((*index, String::from(fields[*index].name())));
                }
            }
        }
        None => {
            for (index, field) in enumerate {
                result.push((index, String::from(field.name())));
            }
        }
    }

    result
}

pub struct ParquetFile {
    files: Vec<ParquetFileReader>,
}

impl ParquetFile {
    pub fn new(files: Vec<ParquetFileReader>) -> Self {
        Self { files }
    }

    pub fn num_files(&self) -> usize {
        self.files.len()
    }

    pub fn metadata(&self, i: usize) -> Option<ParquetMetaDataPtr> {
        self.files.get(i).map(FileReader::metadata)
    }

    pub fn to_row_iter(&self) -> ParquetRowIteratorResult {
        ParquetRowIterator::of(&self.files)
    }

    pub fn to_row_fmt_iter(
        &self,
        field_names: Option<Vec<String>>,
    ) -> Result<ParquetRowFormatterIterator, String> {
        let row_iter = self.to_row_iter()?;
        let fields = get_row_fields(&self.files[0], field_names);

        Ok(ParquetRowFormatterIterator::new(row_iter, fields))
    }

    pub fn file_metadata_num_rows(&self, i: usize) -> usize {
        self.metadata(i)
            .map(|m| m.file_metadata())
            .map(|m| m.num_rows())
            .unwrap_or(0) as usize
    }

    pub fn file_iterator_num_rows(&self, i: usize) -> usize {
        match self.files.get(i) {
            Some(f) => f.get_row_iter(None).ok().map(Iterator::count).unwrap_or(0),
            None => 0,
        }
    }

    #[allow(clippy::op_ref)]
    pub fn file_num_rows(&self, i: usize) -> usize {
        Some(self.file_metadata_num_rows(i))
            .filter(|c| c > &0)
            .unwrap_or_else(|| self.file_iterator_num_rows(i))
    }

    pub fn num_rows(&self) -> usize {
        let mut count = 0;

        for i in 0..self.num_files() {
            count += self.file_num_rows(i);
        }

        count
    }

    pub fn of(path: &Path) -> Result<ParquetFile, String> {
        let files = get_parquet_readers(path)?;
        let parquet = ParquetFile::new(files);

        Ok(parquet)
    }
}

pub struct ParquetRowIterator<'a> {
    vec: Vec<RowIter<'a>>,
    index: usize,
}

impl<'a> ParquetRowIterator<'a> {
    pub fn new(vec: Vec<RowIter<'a>>) -> Self {
        Self { vec, index: 0 }
    }

    pub fn of(readers: &'a [ParquetFileReader]) -> ParquetRowIteratorResult<'a> {
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

            self.index += 1;
        }

        None
    }
}

pub struct ParquetRowFormatterIterator<'a> {
    iter: ParquetRowIterator<'a>,
    fields: Vec<(usize, String)>,
}

impl<'a> ParquetRowFormatterIterator<'a> {
    pub fn new(iter: ParquetRowIterator<'a>, fields: Vec<(usize, String)>) -> Self {
        Self { iter, fields }
    }

    pub fn field_names(&self) -> Vec<String> {
        self.fields.iter().map(|e| e.1.clone()).collect()
    }

    fn format(&self, row: &Row) -> Vec<String> {
        self.fields
            .iter()
            .map(|e| format!("{}", row.fmt(e.0)))
            .collect()
    }
}

impl<'a> Iterator for ParquetRowFormatterIterator<'a> {
    type Item = Vec<String>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|r| self.format(&r))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use parquet::record::RowAccessor;
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
                field_timestamp: vec![0, 0, 2_454_923],
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
                field_timestamp: vec![0, 0, 2_454_923],
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
                    field_timestamp: vec![0, 0, 2_454_923],
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
                    field_timestamp: vec![4_165_425_152, 13, 2_454_923],
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
                "{} >>> Parquet error: Invalid Parquet file. Size is smaller than footer",
                path3.to_string_lossy()
            )
        );
    }

    #[test]
    fn test_get_row_fields() {
        let dir = test_utils::temp_dir();
        let path = dir.path().join("1.snappy.parquet");

        File::create(path.clone()).unwrap();

        let msg = test_utils::SimpleMessage {
            field_int32: 1,
            field_int64: 2,
            field_float: 3.3,
            field_double: 4.4,
            field_string: "5".to_string(),
            field_boolean: true,
            field_timestamp: vec![0, 0, 2_454_923],
        };

        test_utils::write_simple_messages_parquet(&path, &[&msg]);

        let reader = create_parquet_reader(&path).unwrap();
        let result1 = get_row_fields(&reader, None);
        let result2 = get_row_fields(
            &reader,
            Some(vec![
                String::from("field_timestamp"),
                String::from("FIELD_INT64"),
                String::from("field_int32"),
            ]),
        );

        assert_eq!(result1.len(), 7);
        assert_eq!(
            result1,
            vec![
                (0, String::from("field_int32")),
                (1, String::from("field_int64")),
                (2, String::from("field_float")),
                (3, String::from("field_double")),
                (4, String::from("field_string")),
                (5, String::from("field_boolean")),
                (6, String::from("field_timestamp"))
            ]
        );

        assert_eq!(result2.len(), 3);
        assert_eq!(
            result2,
            vec![
                (6, String::from("field_timestamp")),
                (1, String::from("field_int64")),
                (0, String::from("field_int32"))
            ]
        );
    }

    #[test]
    fn test_parquet_file_of_invalid_file() {
        let dir = test_utils::temp_dir();
        let path = dir.path().join("NOT_A_PARQUET");

        File::create(path.clone()).unwrap();

        let result = ParquetFile::of(&path);

        assert_eq!(result.is_err(), true);
        assert_eq!(
            result.err(),
            Some(format!(
                "{} >>> Parquet error: Invalid Parquet file. Size is smaller than footer",
                path.to_string_lossy()
            ))
        );
    }

    #[test]
    fn test_parquet_file_to_row_iter() {
        let dir = test_utils::temp_dir();
        let path = dir.path().join("1.snappy.parquet");

        File::create(path.clone()).unwrap();

        let msg1 = test_utils::SimpleMessage {
            field_int32: 1,
            field_int64: 2,
            field_float: 3.3,
            field_double: 4.4,
            field_string: "5".to_string(),
            field_boolean: true,
            field_timestamp: vec![0, 0, 2_454_923],
        };

        let msg2 = test_utils::SimpleMessage {
            field_int32: 11,
            field_int64: 22,
            field_float: 33.3,
            field_double: 44.4,
            field_string: "55".to_string(),
            field_boolean: false,
            field_timestamp: vec![4_165_425_152, 13, 2_454_923],
        };

        test_utils::write_simple_messages_parquet(&path, &[&msg1, &msg2]);

        let parquet = ParquetFile::of(&path).unwrap();
        let row_iter = parquet.to_row_iter().unwrap();
        let values = row_iter.collect::<Vec<_>>();

        assert_eq!(values.len(), 2);
        assert_eq!(values[0].get_int(0), Ok(msg1.field_int32));
        assert_eq!(values[1].get_int(0), Ok(msg2.field_int32));
    }

    #[test]
    fn test_parquet_file_to_row_fmt_iter() {
        let dir = test_utils::temp_dir();
        let path = dir.path().join("1.snappy.parquet");

        File::create(path.clone()).unwrap();

        let msg1 = test_utils::SimpleMessage {
            field_int32: 1,
            field_int64: 2,
            field_float: 3.3,
            field_double: 4.4,
            field_string: "5".to_string(),
            field_boolean: true,
            field_timestamp: vec![0, 0, 2_454_923],
        };

        let msg2 = test_utils::SimpleMessage {
            field_int32: 11,
            field_int64: 22,
            field_float: 33.3,
            field_double: 44.4,
            field_string: "55".to_string(),
            field_boolean: false,
            field_timestamp: vec![4_165_425_152, 13, 2_454_923],
        };

        test_utils::write_simple_messages_parquet(&path, &[&msg1, &msg2]);

        let fields = vec![String::from("field_int32"), String::from("field_int64")];
        let parquet = ParquetFile::of(&path).unwrap();
        let row_iter = parquet.to_row_fmt_iter(Some(fields.clone())).unwrap();
        let field_names = row_iter.field_names();
        let values = row_iter.collect::<Vec<_>>();

        assert_eq!(values.len(), 2);

        assert_eq!(values[0], vec!["1", "2"]);
        assert_eq!(values[1], vec!["11", "22"]);
        assert_eq!(field_names, fields.clone());
    }
}
