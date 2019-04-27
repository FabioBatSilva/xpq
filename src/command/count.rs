use crate::command::args;
use crate::output::TableOutputWriter;
use crate::reader::get_parquet_readers;
use clap::{App, Arg, ArgMatches, SubCommand};
use parquet::file::reader::FileReader;
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

    Ok(count)
}

pub fn run<W: Write>(matches: &ArgMatches, out: &mut W) -> Result<(), String> {
    let path = args::path_value(matches, "path")?;
    let readers = get_parquet_readers(path)?;
    let mut count: i64 = 0;

    for p in readers {
        count += count_file(&p)?;
    }

    let headers = vec![String::from("COUNT")];
    let values = vec![vec![format!("{}", count)]];

    let iter = values.into_iter();
    let mut writer = TableOutputWriter::new(headers, iter);

    writer.write(out)
}
