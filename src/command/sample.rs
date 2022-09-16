use crate::api::Result;
use crate::command::args;
use crate::output::{OutputFormat, OutputWriter};
use crate::reader::ParquetFile;
use clap::{App, Arg, ArgMatches, SubCommand};
use rand::seq::SliceRandom;
use rand::thread_rng;
use std::collections::HashSet;
use std::io::Write;

pub fn def() -> App<'static> {
    SubCommand::with_name("sample")
        .about("Randomly sample rows from parquet")
        .arg(
            Arg::with_name("columns")
                .help("Select columns from parquet")
                .takes_value(true)
                .long("columns")
                .multiple(true)
                .short('c'),
        )
        .arg(
            Arg::with_name("sample")
                .validator(args::validate_number)
                .help("Sample size limit")
                .default_value("100")
                .long("sample")
                .short('s'),
        )
        .arg(
            Arg::with_name("format")
                .help("Output format")
                .possible_values(OutputFormat::values())
                .default_value("table")
                .long("format")
                .short('f'),
        )
        .arg(
            Arg::with_name("path")
                .validator(args::validate_path)
                .help("Path to parquet")
                .required(true)
                .index(1),
        )
}

fn sample_indexes(sample: usize, size: usize) -> HashSet<usize> {
    let mut vec = (0..size).collect::<Vec<_>>();
    let mut rng = thread_rng();

    vec.shuffle(&mut rng);

    vec.iter().take(sample).cloned().collect()
}

pub fn run<W: Write>(matches: &ArgMatches, out: &mut W) -> Result<()> {
    let format = args::output_format_value(matches, "format")?;
    let columns = args::string_values(matches, "columns")?;
    let sample = args::usize_value(matches, "sample")?;
    let path = args::path_value(matches, "path")?;
    let parquet = ParquetFile::from((path, columns));
    let headers = parquet.field_names()?;
    let size = parquet.num_rows();

    let rows = parquet.iter();
    let indexes = sample_indexes(sample, size);
    let iter = rows
        .enumerate()
        .filter(|t| indexes.contains(&t.0))
        .map(|r| r.1);

    let mut writer = OutputWriter::new(headers, iter).format(format);

    writer.write(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api;
    use std::io::Cursor;
    use std::str;

    #[test]
    fn test_sample_simple_messages() {
        let mut output = Cursor::new(Vec::new());
        let parquet = api::tests::temp_file("msg", ".parquet");
        let expected = vec![
            "field_int32  field_int64  field_float  field_double  field_string  field_boolean  field_timestamp",
            "1            11           111.3        1111.4        \"odd 11111\"   false          2011-01-01 00:00:00 +00:00",
            "2            22           222.3        2222.4        \"even 22222\"  true           2012-01-01 00:00:00 +00:00",
            ""
        ]
        .join("\n");

        let subcomand = def();
        let msgs = api::tests::create_simple_messages(2);
        let arg_vec = vec!["sample", parquet.path().to_str().unwrap()];
        let args = subcomand.get_matches_from_safe(arg_vec).unwrap();

        api::tests::write_simple_messages_parquet(&parquet.path(), &msgs);

        assert_eq!(true, run(&args, &mut output).is_ok());

        let vec = output.into_inner();
        let actual = str::from_utf8(&vec).unwrap();

        assert_eq!(actual, expected);
    }

    #[test]
    fn test_sample_simple_messages_columns() {
        let mut output = Cursor::new(Vec::new());
        let parquet = api::tests::temp_file("msg", ".parquet");
        let path_str = parquet.path().to_str().unwrap();
        let path = parquet.path();
        let expected = vec![
            "field_boolean  field_int32",
            "false          1",
            "true           2",
            "",
        ]
        .join("\n");

        let subcomand = def();
        let msgs = api::tests::create_simple_messages(2);
        let arg_vec = vec!["sample", path_str, "-c=field_boolean", "-c=field_int32"];
        let args = subcomand.get_matches_from_safe(arg_vec).unwrap();

        api::tests::write_simple_messages_parquet(&path, &msgs);

        assert_eq!(true, run(&args, &mut output).is_ok());

        let vec = output.into_inner();
        let actual = str::from_utf8(&vec).unwrap();

        assert_eq!(actual, expected);
    }

    #[test]
    fn test_sample_simple_messages_with_format_vertical() {
        let mut output = Cursor::new(Vec::new());
        let parquet = api::tests::temp_file("msg", ".parquet");
        let path_str = parquet.path().to_str().unwrap();
        let expected = vec![
            "",
            "field_boolean:  false",
            "field_int32:    1",
            "",
            "field_boolean:  true",
            "field_int32:    2",
            "",
        ]
        .join("\n");

        let subcomand = def();
        let arg_vec = vec![
            "sample",
            path_str,
            "-f=vertical",
            "-c=field_boolean",
            "-c=field_int32",
        ];

        let msgs = api::tests::create_simple_messages(2);
        let args = subcomand.get_matches_from_safe(arg_vec).unwrap();

        api::tests::write_simple_messages_parquet(&parquet.path(), &msgs);

        assert_eq!(true, run(&args, &mut output).is_ok());

        let vec = output.into_inner();
        let actual = str::from_utf8(&vec).unwrap();

        assert_eq!(actual, expected);
    }

    #[test]
    fn test_sample_simple_messages_with_format_csv() {
        let mut output = Cursor::new(Vec::new());
        let parquet = api::tests::temp_file("msg", ".parquet");
        let path_str = parquet.path().to_str().unwrap();
        let expected = vec![
            "field_int32,field_timestamp",
            "1,2011-01-01 00:00:00 +00:00",
            "2,2012-01-01 00:00:00 +00:00",
            "",
        ]
        .join("\n");

        let subcomand = def();
        let arg_vec = vec![
            "sample",
            path_str,
            "-f=csv",
            "-c=field_int32,field_timestamp",
        ];

        let msgs = api::tests::create_simple_messages(2);
        let args = subcomand.get_matches_from_safe(arg_vec).unwrap();

        api::tests::write_simple_messages_parquet(&parquet.path(), &msgs);

        assert_eq!(true, run(&args, &mut output).is_ok());

        let vec = output.into_inner();
        let actual = str::from_utf8(&vec).unwrap();

        assert_eq!(actual, expected);
    }
}
