use api::Error;
use api::Result;
use either::Either;
use parquet::file::metadata::ParquetMetaDataPtr;
use parquet::file::reader::FileReader;
use parquet::file::reader::SerializedFileReader;
use parquet::record::reader::RowIter;
use parquet::record::Row;
use parquet::record::RowFormatter;
use regex::Regex;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::fs::File;
use std::path::{Path, PathBuf};
use walkdir::{DirEntry, WalkDir};

pub type ParquetFileReader = SerializedFileReader<File>;

#[inline]
fn create_parquet_reader(path: &Path) -> Result<ParquetFileReader> {
    SerializedFileReader::try_from(path)
        .map_err(|e| Error::Parquet(path.to_path_buf(), e))
}

#[inline]
fn file_metadata_num_rows(reader: &ParquetFileReader) -> usize {
    let metadata = reader.metadata();
    let file_meta = metadata.file_metadata();

    file_meta.num_rows() as usize
}

#[inline]
fn file_iterator_num_rows(reader: ParquetFileReader) -> usize {
    let iter = reader.into_iter();

    iter.count()
}

#[inline]
fn get_row_fields(
    reader: &ParquetFileReader,
    columns: &Option<Vec<String>>,
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
#[inline]
fn get_row_filters(
    filelds: &[(usize, String)],
    filters: &Option<HashMap<String, Regex>>,
) -> Option<HashMap<usize, Regex>> {
    match filters {
        Some(filter_map) => {
            let mut result = HashMap::new();
            let field_map = filelds
                .iter()
                .enumerate()
                .map(|t| ((t.1).1.to_lowercase(), t.0))
                .collect::<HashMap<_, _>>();

            for (field, regex) in filter_map.into_iter() {
                if let Some(index) = field_map.get(&field.to_lowercase()) {
                    result.insert(*index, regex.clone());
                }
            }

            Some(result)
        }
        None => None,
    }
}

pub struct ParquetFile {
    path: PathBuf,
    fields: Option<Vec<String>>,
    filters: Option<HashMap<String, Regex>>,
}

impl ParquetFile {
    pub fn new(path: PathBuf) -> Self {
        Self {
            path,
            fields: None,
            filters: None,
        }
    }

    pub fn with_fields(self, fields: Option<Vec<String>>) -> Self {
        Self {
            fields,
            path: self.path,
            filters: self.filters,
        }
    }

    pub fn with_filters(self, filters: Option<HashMap<String, Regex>>) -> Self {
        Self {
            filters,
            path: self.path,
            fields: self.fields,
        }
    }

    pub fn num_rows(&self) -> usize {
        self.files()
            .map(|p| create_parquet_reader(p.as_path()))
            .filter_map(Result::ok)
            .map(|r| {
                let meta_num_rouws = file_metadata_num_rows(&r);

                if meta_num_rouws > 0 {
                    return meta_num_rouws;
                }

                file_iterator_num_rows(r)
            })
            .sum()
    }

    pub fn field_names(&self) -> Result<Vec<String>> {
        self.files()
            .nth(0)
            .map(|p| create_parquet_reader(p.as_path()))
            .map(|r| {
                let fields = get_row_fields(&r?, &self.fields);
                let names = fields.iter().map(|e| e.1.clone()).collect();

                Ok(names)
            })
            .unwrap_or_else(|| Err(Error::from(self.path.to_path_buf())))
    }

    pub fn metadata(&self) -> Result<ParquetMetaDataPtr> {
        self.files()
            .nth(0)
            .map(|p| create_parquet_reader(p.as_path()))
            .map(|r| Ok(r?.metadata()))
            .unwrap_or_else(|| Err(Error::from(self.path.to_path_buf())))
    }

    pub fn iter(&self) -> impl Iterator<Item = Result<Vec<String>>> + '_ {
        let iter = self.files();
        let field_names = self.fields.clone();
        let field_filter = self.filters.clone();

        iter.map(move |p| {
            let reader = create_parquet_reader(p.as_path())?;
            let fields = get_row_fields(&reader, &field_names);
            let filters = get_row_filters(&fields, &field_filter);
            let row_iter: RowIter<'static> = reader.into_iter();
            let iterator: Iter<_> = Iter::new(row_iter, fields, filters);

            Ok(iterator)
        })
        .flat_map(|r| match r {
            Ok(iter) => iter,
            Err(e) => Iter::err(e),
        })
    }

    fn files(&self) -> impl Iterator<Item = PathBuf> {
        let is_file = self.path.is_file();
        let is_parquet = |entry: &DirEntry| {
            // accept partition directories
            if entry.path().is_dir() {
                return true;
            }

            entry
                .file_name()
                .to_str()
                .map(|s| s.ends_with(".parquet"))
                .unwrap_or(false)
        };

        WalkDir::new(&self.path)
            .contents_first(true)
            .into_iter()
            .filter_entry(move |e| is_file || is_parquet(e))
            .filter_map(std::result::Result::ok)
            .map(DirEntry::into_path)
            .filter(|p| p.is_file())
    }
}

impl From<&Path> for ParquetFile {
    fn from(path: &Path) -> Self {
        ParquetFile::new(path.to_path_buf())
    }
}

impl From<(&Path, Option<Vec<String>>)> for ParquetFile {
    fn from(tuple: (&Path, Option<Vec<String>>)) -> Self {
        ParquetFile::from(tuple.0).with_fields(tuple.1)
    }
}

struct Iter<T> {
    fields: Vec<(usize, String)>,
    values: Either<T, Vec<Error>>,
    filters: Option<HashMap<usize, Regex>>,
}

impl<T> Iter<T>
where
    T: Iterator<Item = Row>,
{
    fn new(
        values: T,
        fields: Vec<(usize, String)>,
        filters: Option<HashMap<usize, Regex>>,
    ) -> Self {
        Self {
            values: Either::Left(values),
            filters,
            fields,
        }
    }

    fn err(error: Error) -> Self {
        Self {
            values: Either::Right(vec![error]),
            filters: None,
            fields: vec![],
        }
    }

    fn filter_map_row(
        row: Row,
        fields: &[(usize, String)],
        filters: &Option<HashMap<usize, Regex>>,
    ) -> Option<Result<Vec<String>>> {
        let result = fields
            .iter()
            .map(|e| format!("{}", row.fmt(e.0)))
            .collect::<Vec<_>>();

        if let Some(ref vec) = filters {
            for (i, regex) in vec {
                if !regex.is_match(&result[*i]) {
                    return None;
                }
            }
        }

        Some(Ok(result))
    }

    fn next_row(
        iter: &mut Iterator<Item = Row>,
        fields: &[(usize, String)],
        filters: &Option<HashMap<usize, Regex>>,
    ) -> Option<Result<Vec<String>>> {
        // while next try to find a matching row
        for row in iter {
            if let Some(next) = Iter::<T>::filter_map_row(row, fields, filters) {
                return Some(next);
            }
        }

        None
    }

    fn next_err(err: &mut Vec<Error>) -> Option<Result<Vec<String>>> {
        err.pop().map(std::result::Result::Err)
    }
}

impl<T> Iterator for Iter<T>
where
    T: Iterator<Item = Row>,
{
    type Item = Result<Vec<String>>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.values {
            Either::Left(ref mut iter) => {
                Iter::<T>::next_row(iter, &self.fields, &self.filters)
            }
            Either::Right(ref mut err) => Iter::<T>::next_err(err),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use api;
    use api::tests::time_to_str;
    use std::fs::File;

    #[test]
    fn test_path_to_reader() {
        let dir = api::tests::temp_dir();
        let path1 = dir.path().join("1.snappy.parquet");
        let path2 = dir.path().join("2.snappy.parquet");
        let path3 = dir.path().join("3.snappy.parquet");
        let msg = api::tests::SimpleMessage {
            field_int32: 1,
            field_int64: 2,
            field_float: 3.3,
            field_double: 4.4,
            field_string: "5".to_string(),
            field_boolean: true,
            field_timestamp: vec![0, 0, 2_454_923],
        };

        File::create(path1.clone()).unwrap();
        File::create(path2.clone()).unwrap();

        api::tests::write_simple_messages_parquet(&path1, &[msg]);

        let result1 = create_parquet_reader(&path1);
        let result2 = create_parquet_reader(&path2);
        let result3 = create_parquet_reader(&path3);

        assert_eq!(result1.is_ok(), true);
        assert_eq!(result2.is_ok(), false);
        assert_eq!(result3.is_ok(), false);
    }

    #[test]
    fn test_parquet_files() {
        let dir = api::tests::temp_dir();
        let path1 = dir.path().join("part-1.snappy.parquet");
        let path2 = dir.path().join("part-2.snappy.parquet");
        let path3 = dir.path().join("_SUCCESS");

        File::create(path1.clone()).unwrap();
        File::create(path2.clone()).unwrap();
        File::create(path3.clone()).unwrap();

        let parquet_dir = ParquetFile::from(dir.path());
        let parquet_file1 = ParquetFile::from(path1.as_path());
        let parquet_file2 = ParquetFile::from(path2.as_path());
        let parquet_file3 = ParquetFile::from(path3.as_path());

        let mut dir_vec = parquet_dir.files().collect::<Vec<_>>();
        let file1_vec = parquet_file1.files().collect::<Vec<_>>();
        let file2_vec = parquet_file2.files().collect::<Vec<_>>();
        let file3_vec = parquet_file3.files().collect::<Vec<_>>();

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
    fn test_get_row_fields() {
        let dir = api::tests::temp_dir();
        let path = dir.path().join("1.snappy.parquet");

        File::create(path.clone()).unwrap();

        let msg = api::tests::SimpleMessage {
            field_int32: 1,
            field_int64: 2,
            field_float: 3.3,
            field_double: 4.4,
            field_string: "5".to_string(),
            field_boolean: true,
            field_timestamp: vec![0, 0, 2_454_923],
        };

        api::tests::write_simple_messages_parquet(&path, &[msg]);

        let reader = create_parquet_reader(&path).unwrap();
        let result1 = get_row_fields(&reader, &None);
        let result2 = get_row_fields(
            &reader,
            &Some(vec![
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
    fn test_parquet_file_num_files() {
        let dir = api::tests::temp_dir();
        let path1 = dir.path().join("1.snappy.parquet");
        let path2 = dir.path().join("2.snappy.parquet");

        File::create(path1.clone()).unwrap();
        File::create(path2.clone()).unwrap();

        let msg1 = api::tests::SimpleMessage {
            field_int32: 1,
            field_int64: 2,
            field_float: 3.3,
            field_double: 4.4,
            field_string: "5".to_string(),
            field_boolean: true,
            field_timestamp: vec![0, 0, 2_454_923],
        };

        let msg2 = api::tests::SimpleMessage {
            field_int32: 11,
            field_int64: 22,
            field_float: 33.3,
            field_double: 44.4,
            field_string: "55".to_string(),
            field_boolean: false,
            field_timestamp: vec![4_165_425_152, 13, 2_454_923],
        };

        api::tests::write_simple_messages_parquet(&path1, &[msg1]);
        api::tests::write_simple_messages_parquet(&path2, &[msg2]);

        let parquet_dir = ParquetFile::from(dir.path());
        let parquet_path1 = ParquetFile::from(path1.as_path());
        let parquet_path2 = ParquetFile::from(path2.as_path());

        assert_eq!(2, parquet_dir.files().count());
        assert_eq!(1, parquet_path1.files().count());
        assert_eq!(1, parquet_path2.files().count());
    }

    #[test]
    fn test_parquet_file_num_rows() {
        let dir = api::tests::temp_dir();
        let path1 = dir.path().join("1.snappy.parquet");
        let path2 = dir.path().join("2.snappy.parquet");
        let path3 = dir.path().join("NOT_A_PARQUET");

        File::create(path1.clone()).unwrap();
        File::create(path2.clone()).unwrap();
        File::create(path3.clone()).unwrap();

        let msg1 = api::tests::SimpleMessage {
            field_int32: 1,
            field_int64: 2,
            field_float: 3.3,
            field_double: 4.4,
            field_string: "5".to_string(),
            field_boolean: true,
            field_timestamp: vec![0, 0, 2_454_923],
        };

        let msg2 = api::tests::SimpleMessage {
            field_int32: 11,
            field_int64: 22,
            field_float: 33.3,
            field_double: 44.4,
            field_string: "55".to_string(),
            field_boolean: false,
            field_timestamp: vec![4_165_425_152, 13, 2_454_923],
        };

        let msg3 = api::tests::SimpleMessage {
            field_int32: 111,
            field_int64: 222,
            field_float: 332.3,
            field_double: 444.4,
            field_string: "555".to_string(),
            field_boolean: false,
            field_timestamp: vec![4_165_425_152, 13, 2_454_923],
        };

        api::tests::write_simple_messages_parquet(&path1, &[msg1, msg2]);
        api::tests::write_simple_messages_parquet(&path2, &[msg3]);

        let parquet_dir = ParquetFile::from(dir.path());
        let parquet_path1 = ParquetFile::from(path1.as_path());
        let parquet_path2 = ParquetFile::from(path2.as_path());
        let parquet_path3 = ParquetFile::from(path3.as_path());

        assert_eq!(3, parquet_dir.num_rows());
        assert_eq!(2, parquet_path1.num_rows());
        assert_eq!(1, parquet_path2.num_rows());
        assert_eq!(0, parquet_path3.num_rows());
    }

    #[test]
    fn test_parquet_file_metadata() {
        let dir = api::tests::temp_dir();
        let empty = api::tests::temp_dir();
        let path1 = dir.path().join("ok.parquet");
        let path2 = dir.path().join("bad.parquet");

        File::create(path1.clone()).unwrap();
        File::create(path2.clone()).unwrap();

        let msg1 = api::tests::SimpleMessage {
            field_int32: 1,
            field_int64: 2,
            field_float: 3.3,
            field_double: 4.4,
            field_string: "5".to_string(),
            field_boolean: true,
            field_timestamp: vec![0, 0, 2_454_923],
        };

        api::tests::write_simple_messages_parquet(&path1, &[msg1]);

        let parquet_ok = ParquetFile::from(path1.as_path());
        let parquet_err = ParquetFile::from(path2.as_path());
        let parquet_empty = ParquetFile::from(empty.path());
        let result_empty = parquet_empty.metadata();
        let result_err = parquet_err.metadata();
        let result_ok = parquet_ok.metadata();

        assert_eq!(result_ok.is_ok(), true);
        assert_eq!(result_err.is_err(), true);
        assert_eq!(result_empty.is_err(), true);

        assert_eq!(
            format!("{}", result_err.err().unwrap()),
            format!(
                "{} >>> Parquet error: Invalid Parquet file. Size is smaller than footer",
                path2.to_string_lossy()
            )
        );

        assert_eq!(
            result_empty.err().unwrap(),
            Error::InvalidParquet(empty.path().to_path_buf())
        );
    }

    #[test]
    fn test_reader_to_row_iter() -> Result<()> {
        let dir = api::tests::temp_dir();
        let path = dir.path().join("file.parquet");

        File::create(path.clone()).unwrap();

        let msg1 = api::tests::SimpleMessage {
            field_int32: 1,
            field_int64: 2,
            field_float: 3.3,
            field_double: 4.4,
            field_string: "5".to_string(),
            field_boolean: true,
            field_timestamp: vec![0, 0, 2_454_923],
        };

        let msg2 = api::tests::SimpleMessage {
            field_int32: 11,
            field_int64: 22,
            field_float: 33.3,
            field_double: 44.4,
            field_string: "55".to_string(),
            field_boolean: false,
            field_timestamp: vec![4_165_425_152, 13, 2_454_923],
        };

        api::tests::write_simple_messages_parquet(path.as_path(), &[msg1, msg2]);

        let fields = vec![String::from("field_int32"), String::from("field_int64")];
        let reader = ParquetFile::from(dir.path()).with_fields(Some(fields));
        let result = reader.iter().filter_map(Result::ok).collect::<Vec<_>>();

        assert_eq!(result.len(), 2);
        assert_eq!(result[0], vec!["1", "2"]);
        assert_eq!(result[1], vec!["11", "22"]);

        Ok(())
    }

    #[test]
    fn test_reader_to_row_iter_fmt() {
        let dir = api::tests::temp_dir();
        let path = dir.path().join("file.parquet");

        File::create(path.clone()).unwrap();

        let msg1 = api::tests::SimpleMessage {
            field_int32: 1,
            field_int64: 2,
            field_float: 3.3,
            field_double: 4.4,
            field_string: "5".to_string(),
            field_boolean: true,
            field_timestamp: vec![0, 0, 2_454_923],
        };

        let msg2 = api::tests::SimpleMessage {
            field_int32: 11,
            field_int64: 22,
            field_float: 33.3,
            field_double: 44.4,
            field_string: "55".to_string(),
            field_boolean: false,
            field_timestamp: vec![4_165_425_152, 13, 2_454_923],
        };

        api::tests::write_simple_messages_parquet(&path, &[msg1, msg2]);

        let reader = ParquetFile::from(dir.path());
        let result = reader.iter().filter_map(Result::ok).collect::<Vec<_>>();

        assert_eq!(result.len(), 2);

        assert_eq!(
            result[0],
            vec![
                "1",
                "2",
                "3.3",
                "4.4",
                "\"5\"",
                "true",
                &time_to_str(1_238_544_000_000)
            ]
        );
        assert_eq!(
            result[1],
            vec![
                "11",
                "22",
                "33.3",
                "44.4",
                "\"55\"",
                "false",
                &time_to_str(1_238_544_060_000)
            ]
        );
    }

    #[test]
    fn test_reader_to_row_iter_err() {
        let dir = api::tests::temp_dir();
        let path = dir.path().join("NOT_A_PARQUET");

        File::create(path.clone()).unwrap();

        let parquet = ParquetFile::from(path.as_path());
        let result = parquet.iter().collect::<Vec<_>>();

        assert_eq!(result.len(), 1);
        assert_eq!(
            result[0],
            Err(Error::Parquet(
                path.to_path_buf(),
                parquet::errors::ParquetError::General(String::from(
                    "Invalid Parquet file. Size is smaller than footer"
                ))
            ))
        );
    }

    #[test]
    fn test_reader_field_names() {
        let dir = api::tests::temp_dir();
        let path = dir.path().join("file.parquet");

        File::create(path.clone()).unwrap();

        let msg = api::tests::SimpleMessage {
            field_int32: 1,
            field_int64: 2,
            field_float: 3.3,
            field_double: 4.4,
            field_string: "5".to_string(),
            field_boolean: true,
            field_timestamp: vec![0, 0, 2_454_923],
        };

        api::tests::write_simple_messages_parquet(&path, &[msg]);

        let fields = vec![String::from("field_string"), String::from("FIELD_INT32")];

        let result_fields = ParquetFile::from(dir.path())
            .with_fields(Some(fields))
            .field_names();

        let result_all = ParquetFile::from(dir.path()).field_names();

        assert_eq!(
            result_fields,
            Ok(vec![
                String::from("field_string"),
                String::from("field_int32")
            ])
        );

        assert_eq!(
            result_all,
            Ok(vec![
                String::from("field_int32"),
                String::from("field_int64"),
                String::from("field_float"),
                String::from("field_double"),
                String::from("field_string"),
                String::from("field_boolean"),
                String::from("field_timestamp"),
            ])
        );
    }

    #[test]
    fn test_reader_field_names_err() {
        let dir = api::tests::temp_dir();
        let empty = api::tests::temp_dir();
        let path = dir.path().join("NOT_A_PARQUET");

        File::create(path.clone()).unwrap();

        let parquet_empty = ParquetFile::from(empty.path());
        let parquet_bad = ParquetFile::from(path.as_path());
        let result_empty = parquet_empty.field_names();
        let result_bad = parquet_bad.field_names();

        assert_eq!(
            format!("{}", result_bad.err().unwrap()),
            format!(
                "{} >>> Parquet error: Invalid Parquet file. Size is smaller than footer",
                path.to_string_lossy()
            )
        );

        assert_eq!(
            result_empty.err().unwrap(),
            Error::InvalidParquet(empty.path().to_path_buf())
        );
    }

    #[test]
    #[allow(clippy::trivial_regex)]
    fn test_reader_field_filter() {
        let dir = api::tests::temp_dir();
        let path = dir.path().join("file.parquet");

        File::create(path.clone()).unwrap();

        let msg1 = api::tests::SimpleMessage {
            field_int32: 1,
            field_int64: 2,
            field_float: 3.3,
            field_double: 4.4,
            field_string: "odd 1".to_string(),
            field_boolean: true,
            field_timestamp: vec![0, 0, 2_454_923],
        };

        let msg2 = api::tests::SimpleMessage {
            field_int32: 11,
            field_int64: 22,
            field_float: 33.3,
            field_double: 44.4,
            field_string: "even 1".to_string(),
            field_boolean: false,
            field_timestamp: vec![4_165_425_152, 13, 2_454_923],
        };

        let msg3 = api::tests::SimpleMessage {
            field_int32: 111,
            field_int64: 222,
            field_float: 333.3,
            field_double: 444.4,
            field_string: "odd 2".to_string(),
            field_boolean: false,
            field_timestamp: vec![4_165_425_152, 13, 2_454_923],
        };

        api::tests::write_simple_messages_parquet(&path, &[msg1, msg2, msg3]);

        let mut filters = HashMap::new();
        let fields = vec![String::from("field_int32"), String::from("field_string")];

        filters.insert(String::from("field_string"), Regex::new("odd").unwrap());

        let result = ParquetFile::from(dir.path())
            .with_filters(Some(filters))
            .with_fields(Some(fields))
            .iter()
            .filter_map(Result::ok)
            .collect::<Vec<_>>();

        assert_eq!(result.len(), 2);

        assert_eq!(result[0], vec!["1", "\"odd 1\""]);
        assert_eq!(result[1], vec!["111", "\"odd 2\""]);
    }
}
