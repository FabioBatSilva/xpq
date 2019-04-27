use crate::command::args;
use crate::output::TableOutputWriter;
use crate::reader::{get_parquet_readers, ParquetRowIterator};
use clap::{App, Arg, ArgMatches, SubCommand};
use parquet::file::reader::FileReader;
use parquet::record::Row as ParquetRow;
use parquet::record::RowFormatter;
use std::io::Write;

pub fn def() -> App<'static, 'static> {
    SubCommand::with_name("sample")
        .about("Sample parquet data")
        .arg(
            Arg::with_name("limit")
                .validator(args::validate_number)
                .help("Sample size limit")
                .default_value("100")
                .long("limit")
                .short("l"),
        )
        .arg(
            Arg::with_name("format")
                .help("Output format")
                .possible_values(&["table"])
                .default_value("table")
                .long("format")
                .short("f"),
        )
        .arg(
            Arg::with_name("path")
                .validator(args::validate_path)
                .help("Path to parquet")
                .required(true)
                .index(1),
        )
}

fn format(row: &ParquetRow) -> Vec<String> {
    let mut values = Vec::new();

    for i in 0..row.len() {
        values.push(format!("{}", row.fmt(i)));
    }

    values
}

fn metadata_headers(reader: &FileReader) -> Vec<String> {
    let metadata = reader.metadata().file_metadata();
    let schema = metadata.schema();
    let mut headers = Vec::new();

    for field in schema.get_fields() {
        headers.push(String::from(field.name()));
    }

    headers
}

pub fn run<W: Write>(matches: &ArgMatches, out: &mut W) -> Result<(), String> {
    let limit = args::usize_value(matches, "limit")?;
    let path = args::path_value(matches, "path")?;
    let readers = get_parquet_readers(path)?;
    let rows = ParquetRowIterator::of(&readers)?;
    let iter = rows.take(limit).map(|r| format(&r));

    if readers.is_empty() {
        return Ok(());
    }

    let headers = metadata_headers(&readers[0]);
    let mut writer = TableOutputWriter::new(headers, iter);

    writer.write(out)
}

#[cfg(test)]
mod tests {
    extern crate chrono;

    use self::chrono::{Local, TimeZone};
    use super::*;
    use std::io::Cursor;
    use std::str;
    use utils::test_utils;

    #[inline]
    fn time_to_str(value: u64) -> String {
        let dt = Local.timestamp((value / 1000) as i64, 0);
        let s = format!("{}", dt.format("%Y-%m-%d %H:%M:%S %:z"));

        s
    }

    #[test]
    fn test_sample_simple_messages() {
        let mut output = Cursor::new(Vec::new());
        let parquet = test_utils::temp_file("msg", ".parquet");
        let expected = vec![
            " field_int32  field_int64  field_float  field_double  field_string  field_boolean  field_timestamp ",
            &format!(" 1            2            3.3          4.4           \"5\"           true           {} ", time_to_str(1_238_544_000_000)),
            &format!(" 11           22           33.3         44.4          \"55\"          false          {} ", time_to_str(1_238_544_060_000)),
            ""
        ]
        .join("\n");

        let subcomand = def();
        let arg_vec = vec!["sample", parquet.path().to_str().unwrap()];
        let args = subcomand.get_matches_from_safe(arg_vec).unwrap();

        {
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

            test_utils::write_simple_messages_parquet(&parquet.path(), &[&msg1, &msg2]);

            assert_eq!(true, run(&args, &mut output).is_ok());
        }

        let vec = output.into_inner();
        let actual = str::from_utf8(&vec).unwrap();

        assert_eq!(actual, expected);
    }
}
