use crate::api::Result;
use crate::command::args;
use crate::output::{OutputFormat, OutputWriter};
use crate::reader::ParquetFile;
use clap::{App, Arg, ArgMatches, SubCommand};
use std::io::Write;

pub fn def() -> App<'static> {
    SubCommand::with_name("count")
        .about("Show num of rows")
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

pub fn run<W: Write>(matches: &ArgMatches, out: &mut W) -> Result<()> {
    let format = args::output_format_value(matches, "format")?;
    let path = args::path_value(matches, "path")?;
    let parquet = ParquetFile::from(path);
    let count = parquet.num_rows();

    let headers = vec![String::from("COUNT")];
    let values = vec![Ok(vec![format!("{}", count)])];

    let iter = values.into_iter();
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
    fn test_count_simple_messages() {
        let mut output = Cursor::new(Vec::new());
        let parquet = api::tests::temp_file("msg", ".parquet");
        let expected = "COUNT\n2\n";

        let subcomand = def();
        let msgs = api::tests::create_simple_messages(2);
        let arg_vec = vec!["count", parquet.path().to_str().unwrap()];
        let args = subcomand.get_matches_from_safe(arg_vec).unwrap();

        api::tests::write_simple_messages_parquet(parquet.path(), &msgs);

        assert!(run(&args, &mut output).is_ok());

        let vec = output.into_inner();
        let actual = str::from_utf8(&vec).unwrap();

        assert_eq!(actual, expected);
    }

    #[test]
    fn test_count_simple_messages_vertical_format() {
        let mut output = Cursor::new(Vec::new());
        let parquet = api::tests::temp_file("msg", ".parquet");
        let expected = "\nCOUNT:  2\n";

        let subcomand = def();
        let msgs = api::tests::create_simple_messages(2);
        let arg_vec = vec!["count", parquet.path().to_str().unwrap(), "-f=v"];
        let args = subcomand.get_matches_from_safe(arg_vec).unwrap();

        api::tests::write_simple_messages_parquet(parquet.path(), &msgs);

        assert!(run(&args, &mut output).is_ok());

        let vec = output.into_inner();
        let actual = str::from_utf8(&vec).unwrap();

        assert_eq!(actual, expected);
    }

    #[test]
    fn test_count_simple_messages_vertical_csv() {
        let mut output = Cursor::new(Vec::new());
        let parquet = api::tests::temp_file("msg", ".parquet");
        let expected = "COUNT\n2\n";

        let subcomand = def();
        let msgs = api::tests::create_simple_messages(2);
        let arg_vec = vec!["count", parquet.path().to_str().unwrap(), "-f=c"];
        let args = subcomand.get_matches_from_safe(arg_vec).unwrap();

        api::tests::write_simple_messages_parquet(parquet.path(), &msgs);

        assert!(run(&args, &mut output).is_ok());

        let vec = output.into_inner();
        let actual = str::from_utf8(&vec).unwrap();

        assert_eq!(actual, expected);
    }
}
