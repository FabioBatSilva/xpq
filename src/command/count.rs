use clap::{App, Arg, ArgMatches, SubCommand};
use command::args;
use iterator::{ParquetFileReader, ParquetPathIterator};
use parquet::file::reader::FileReader;
use prettytable::{format, Cell, Row, Table};
use std::io::Write;

pub fn def() -> App<'static, 'static> {
    SubCommand::with_name("count")
        .about("Show num of rows")
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

fn count_file(reader: &FileReader) -> Result<i64, String> {
    let metadata = reader.metadata().file_metadata();
    let count = metadata.num_rows();

    return Ok(count);
}

pub fn run<W: Write>(matches: &ArgMatches, out: &mut W) -> Result<(), String> {
    let path = args::path_value(matches, "path")?;
    let paths = ParquetPathIterator::new(path);
    let reader = ParquetFileReader::new(paths);
    let mut table = Table::new();
    let mut count: i64 = 0;

    table.set_format(*format::consts::FORMAT_CLEAN);
    table.set_titles(Row::new(vec![Cell::new("count")]));

    for p in reader {
        count = count + count_file(&p?)?
    }

    table.add_row(Row::new(vec![Cell::new(&format!("{}", count))]));
    table.print(out).expect("Fail to print table");

    return Ok(());
}
