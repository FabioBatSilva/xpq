use crate::command::args;
use crate::output::{OutputFormat, OutputWriter};
use crate::reader::ParquetFile;
use crate::api::Result;
use clap::{App, Arg, ArgMatches, SubCommand};
use std::io::Write;

pub fn def() -> App<'static, 'static> {
    SubCommand::with_name("read")
        .about("Read rows from parquet")
        .arg(
            Arg::with_name("columns")
                .help("Select columns from parquet")
                .takes_value(true)
                .long("columns")
                .multiple(true)
                .short("c"),
        )
        .arg(
            Arg::with_name("search")
                .validator(args::validate_filter)
                .help("Search columns")
                .takes_value(true)
                .long("search")
                .multiple(true)
                .short("s"),
        )
        .arg(
            Arg::with_name("limit")
                .validator(args::validate_number)
                .help("Max number of rows")
                .default_value("500")
                .long("limit")
                .short("l"),
        )
        .arg(
            Arg::with_name("format")
                .help("Output format")
                .possible_values(&OutputFormat::values())
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

pub fn run<W: Write>(matches: &ArgMatches, out: &mut W) -> Result<()> {
    let format = args::output_format_value(matches, "format")?;
    let columns = args::string_values(matches, "columns")?;
    let search = args::filter_values(matches, "search")?;
    let limit = args::usize_value(matches, "limit")?;
    let path = args::path_value(matches, "path")?;
    let parquet = ParquetFile::from(path)
        .with_fields(columns)
        .with_filters(search);

    let headers = parquet.field_names()?;
    let iter = parquet.iter().take(limit);
    let mut writer = OutputWriter::new(headers, iter).format(format);

    writer.write(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use api;
    use std::io::Cursor;
    use std::str;

    #[test]
    fn test_read_simple_messages() {
        let mut output = Cursor::new(Vec::new());
        let parquet = api::tests::temp_file("msg", ".parquet");
        let path_str = parquet.path().to_str().unwrap();
        let path = parquet.path();
        let expected = vec![
            "field_int32  field_int64",
            "1            11",
            "2            22",
            "",
        ]
        .join("\n");

        let subcomand = def();
        let arg_vec = vec!["read", path_str, "-l=2", "-c=field_int32,field_int64"];
        let args = subcomand.get_matches_from_safe(arg_vec).unwrap();
        let msgs = api::tests::create_simple_messages(3);

        api::tests::write_simple_messages_parquet(&path, &msgs);

        assert_eq!(true, run(&args, &mut output).is_ok());

        let vec = output.into_inner();
        let actual = str::from_utf8(&vec).unwrap();

        assert_eq!(actual, expected);
    }

    #[test]
    fn test_read_simple_messages_with_filters() {
        let mut output = Cursor::new(Vec::new());
        let parquet = api::tests::temp_file("msg", ".parquet");
        let path_str = parquet.path().to_str().unwrap();
        let path = parquet.path();
        let expected = vec![
            "field_int32  field_string",
            "1            \"odd 11111\"",
            "3            \"odd 33333\"",
            "",
        ]
        .join("\n");

        let subcomand = def();
        let msgs = api::tests::create_simple_messages(3);
        let args = subcomand
            .get_matches_from_safe(vec![
                "read",
                path_str,
                "-s=field_string:odd",
                "-c=field_int32,field_string",
            ])
            .unwrap();

        api::tests::write_simple_messages_parquet(&path, &msgs);

        assert_eq!(true, run(&args, &mut output).is_ok());

        let vec = output.into_inner();
        let actual = str::from_utf8(&vec).unwrap();

        assert_eq!(actual, expected);
    }

    #[test]
    fn test_read_simple_messages_with_format_vertical() {
        let mut output = Cursor::new(Vec::new());
        let parquet = api::tests::temp_file("msg", ".parquet");
        let path_str = parquet.path().to_str().unwrap();
        let path = parquet.path();
        let expected = vec![
            "",
            "field_int32:  1",
            "field_int64:  11",
            "",
            "field_int32:  2",
            "field_int64:  22",
            "",
        ]
        .join("\n");

        let subcomand = def();
        let msgs = api::tests::create_simple_messages(2);
        let arg_vec = vec!["read", path_str, "-f=v", "-c=field_int32,field_int64"];
        let args = subcomand.get_matches_from_safe(arg_vec).unwrap();

        api::tests::write_simple_messages_parquet(&path, &msgs);

        assert_eq!(true, run(&args, &mut output).is_ok());

        let vec = output.into_inner();
        let actual = str::from_utf8(&vec).unwrap();

        assert_eq!(actual, expected);
    }

    #[test]
    fn test_read_simple_messages_with_format_csv() {
        let mut output = Cursor::new(Vec::new());
        let parquet = api::tests::temp_file("msg", ".parquet");
        let path_str = parquet.path().to_str().unwrap();
        let path = parquet.path();

        let subcomand = def();
        let msgs = api::tests::create_simple_messages(2);
        let arg_vec = vec!["read", path_str, "-f=csv", "-c=field_int32,field_string"];
        let args = subcomand.get_matches_from_safe(arg_vec).unwrap();

        api::tests::write_simple_messages_parquet(&path, &msgs);

        assert_eq!(true, run(&args, &mut output).is_ok());

        let vec = output.into_inner();
        let actual = str::from_utf8(&vec).unwrap();
        let expected = "field_int32,field_string\n1,\"odd 11111\"\n2,\"even 22222\"\n";

        assert_eq!(actual, expected);
    }
}
