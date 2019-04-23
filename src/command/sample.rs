use clap::{App, Arg, ArgMatches, SubCommand};
use command::args;
use iterator::{ParquetFileReader, ParquetPathIterator};
use parquet::file::reader::FileReader;
use parquet::record::Row as ParquetRow;
use parquet::record::RowFormatter;
use prettytable::{format, Cell, Row, Table};
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
    let mut values: Vec<String> = Vec::new();

    for i in 0..row.len() {
        values.push(format!("{}", row.fmt(i)));
    }

    return values;
}

fn sample_file(
    reader: &FileReader,
    table: &mut Table,
    limit: usize,
) -> Result<(), String> {
    let mut iter = reader
        .get_row_iter(None)
        .map_err(|e| format!("Failed iterate parquet file : {}", e))?;

    let metadata = reader.metadata().file_metadata();
    let schema = metadata.schema();

    if table.is_empty() {
        let mut titles = Vec::new();

        for field in schema.get_fields() {
            titles.push(Cell::new(field.name()))
        }

        table.set_titles(Row::new(titles));
    }

    while let Some(row) = iter.next() {
        let mut row_values = Vec::new();

        for value in format(&row) {
            row_values.push(Cell::new(&value));
        }

        table.add_row(Row::new(row_values));

        if table.len() >= limit {
            break;
        }
    }

    return Ok(());
}

pub fn run<W: Write>(matches: &ArgMatches, out: &mut W) -> Result<(), String> {
    let limit = args::usize_value(matches, "limit")?;
    let path = args::path_value(matches, "path")?;

    let paths = ParquetPathIterator::new(path);
    let reader = ParquetFileReader::new(paths);
    let table = &mut Table::new();

    table.set_format(*format::consts::FORMAT_CLEAN);

    for p in reader {
        sample_file(&p?, table, limit)?
    }

    table.print(out).expect("Fail to print table");

    return Ok(());
}

#[cfg(test)]
mod tests {
    extern crate chrono;

    use self::chrono::{Local, TimeZone};
    use super::*;
    use std::fs;
    use std::fs::File;
    use utils::test_utils;

    #[inline]
    fn time_to_str(value: u64) -> String {
        let dt = Local.timestamp((value / 1000) as i64, 0);
        let s = format!("{}", dt.format("%Y-%m-%d %H:%M:%S %:z"));

        return s;
    }

    #[test]
    fn test_sample_simple_messages() {
        let parquet = test_utils::temp_file("msg", ".parquet");
        let output = test_utils::temp_file("schema", ".out");
        let expected = vec![
            " field_int32  field_int64  field_float  field_double  field_string  field_boolean  field_timestamp ",
            &format!(" 1            2            3.3          4.4           \"5\"           true           {} ", time_to_str(1238544000000)),
            &format!(" 11           22           33.3         44.4          \"55\"          false          {} ", time_to_str(1238544060000)),
            ""
        ]
        .join("\n");

        let subcomand = def();
        let arg_vec = vec!["sample", parquet.path().to_str().unwrap()];
        let args = subcomand.get_matches_from_safe(arg_vec).unwrap();

        {
            let mut file = File::create(&output).unwrap();
            let msg1 = test_utils::SimpleMessage {
                field_int32: 1,
                field_int64: 2,
                field_float: 3.3,
                field_double: 4.4,
                field_string: "5".to_string(),
                field_boolean: true,
                field_timestamp: vec![0, 0, 2454923],
            };
            let msg2 = test_utils::SimpleMessage {
                field_int32: 11,
                field_int64: 22,
                field_float: 33.3,
                field_double: 44.4,
                field_string: "55".to_string(),
                field_boolean: false,
                field_timestamp: vec![4165425152, 13, 2454923],
            };

            test_utils::write_simple_messages_parquet(
                &parquet.path(),
                &vec![&msg1, &msg2],
            );

            assert_eq!(true, run(&args, &mut file).is_ok());
        }

        assert_eq!(expected, fs::read_to_string(output.path()).unwrap());
    }
}
