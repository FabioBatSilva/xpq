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
            Arg::with_name("output")
                .help("Output format")
                .default_value("table")
                .long("output")
                .short("o"),
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

    // Print the table to stdout
    table.print(out).expect("Fail to print table");

    return Ok(());
}

#[cfg(test)]

mod tests {
    use super::*;
    use std::fs;
    use std::fs::File;
    use utils::test_utils;

    #[test]
    fn test_sample_simple_message() {
        let parquet = test_utils::temp_file("msg", ".parquet");
        let output = test_utils::temp_file("schema", ".out");
        let expected = vec![
            " field_int32  field_int64  field_float  field_double  field_string  field_boolean  field_timestamp ",
            " 111          222          333.3        444.4         \"555\"         false          2009-03-31 20:01:00 -04:00 ",
            ""
        ].join("\n");

        let subcomand = def();
        let arg_vec = vec!["sample", parquet.path().to_str().unwrap()];
        let args = subcomand.get_matches_from_safe(arg_vec).unwrap();

        {
            let mut file = File::create(&output).unwrap();
            let msg = test_utils::SimpleMessage {
                field_int32: 111,
                field_int64: 222,
                field_float: 333.3,
                field_double: 444.4,
                field_string: "555".to_string(),
                field_boolean: false,
                field_timestamp: vec![4165425152, 13, 2454923],
            };

            test_utils::write_simple_message_parquet(&parquet.path(), &msg);

            assert_eq!(true, run(&args, &mut file).is_ok());
        }

        assert_eq!(expected, fs::read_to_string(output.path()).unwrap());
    }
}
