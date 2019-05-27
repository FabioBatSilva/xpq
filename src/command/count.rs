use crate::command::args;
use crate::output::TableOutputWriter;
use crate::reader::ParquetFile;
use api::Result;
use clap::{App, Arg, ArgMatches, SubCommand};
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

pub fn run<W: Write>(matches: &ArgMatches, out: &mut W) -> Result<()> {
    let path = args::path_value(matches, "path")?;
    let parquet = ParquetFile::from(path);
    let count = parquet.num_rows();

    let headers = vec![String::from("COUNT")];
    let values = vec![Ok(vec![format!("{}", count)])];

    let iter = values.into_iter();
    let mut writer = TableOutputWriter::new(headers, iter);

    writer.write(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use api;
    use std::io::Cursor;
    use std::str;

    #[test]
    fn test_count_simple_messages() {
        let mut output = Cursor::new(Vec::new());
        let parquet = api::tests::temp_file("msg", ".parquet");
        let expected = "COUNT\n2\n";

        let subcomand = def();
        let arg_vec = vec!["count", parquet.path().to_str().unwrap()];
        let args = subcomand.get_matches_from_safe(arg_vec).unwrap();

        {
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

            api::tests::write_simple_messages_parquet(&parquet.path(), &[&msg1, &msg2]);

            assert_eq!(true, run(&args, &mut output).is_ok());
        }

        let vec = output.into_inner();
        let actual = str::from_utf8(&vec).unwrap();

        assert_eq!(actual, expected);
    }
}
