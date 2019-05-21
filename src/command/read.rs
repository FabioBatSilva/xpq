use crate::command::args;
use crate::output::TableOutputWriter;
use crate::reader::ParquetFile;
use api::Result;
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
            Arg::with_name("limit")
                .validator(args::validate_number)
                .help("Max number of rows")
                .default_value("300")
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

pub fn run<W: Write>(matches: &ArgMatches, out: &mut W) -> Result<()> {
    let columns = args::string_values(matches, "columns")?;
    let limit = args::usize_value(matches, "limit")?;
    let path = args::path_value(matches, "path")?;
    let parquet = ParquetFile::from((path, columns));
    let headers = parquet.field_names()?;

    let iter = parquet.iter().take(limit);
    let mut writer = TableOutputWriter::new(headers, iter);

    writer.write(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use std::str;
    use utils::test_utils;

    #[test]
    fn test_read_simple_messages() {
        let mut output = Cursor::new(Vec::new());
        let parquet = test_utils::temp_file("msg", ".parquet");
        let path_str = parquet.path().to_str().unwrap();
        let path = parquet.path();
        let expected = vec![
            "field_int32  field_int64",
            "1            2",
            "11           22",
            "",
        ]
        .join("\n");

        let subcomand = def();
        let arg_vec = vec!["read", path_str, "-l=2", "-c=field_int32,field_int64"];
        let args = subcomand.get_matches_from_safe(arg_vec).unwrap();

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
        let msg3 = test_utils::SimpleMessage {
            field_int32: 111,
            field_int64: 222,
            field_float: 333.3,
            field_double: 444.4,
            field_string: "555".to_string(),
            field_boolean: false,
            field_timestamp: vec![4_165_425_152, 13, 2_454_923],
        };

        test_utils::write_simple_messages_parquet(&path, &[&msg1, &msg2, &msg3]);

        assert_eq!(true, run(&args, &mut output).is_ok());

        let vec = output.into_inner();
        let actual = str::from_utf8(&vec).unwrap();

        assert_eq!(actual, expected);
    }
}
