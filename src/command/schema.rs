use crate::reader::ParquetFile;
use clap::{App, Arg, ArgMatches, SubCommand};
use command::args;
use parquet::schema::printer::print_schema;
use std::io::Write;

pub fn def() -> App<'static, 'static> {
    SubCommand::with_name("schema")
        .about("Show parquet schema")
        .arg(
            Arg::with_name("format")
                .help("Output format")
                .possible_values(&["hive"])
                .default_value("hive")
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

pub fn run<W: Write>(matches: &ArgMatches, out: &mut W) -> Result<(), String> {
    let path = args::path_value(matches, "path")?;
    let parquet = ParquetFile::from(path);
    let metadata = parquet.metadata()?;
    let file_meta = metadata.file_metadata();
    let schema = file_meta.schema();

    print_schema(out, &schema);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use std::str;
    use utils::test_utils;

    #[test]
    fn test_schema_simple_message() {
        let mut output = Cursor::new(Vec::new());
        let parquet = test_utils::temp_file("msg", "parquet");
        let expected = vec![
            "message simple_message {",
            "  OPTIONAL INT32 field_int32;",
            "  OPTIONAL INT64 field_int64;",
            "  OPTIONAL FLOAT field_float;",
            "  OPTIONAL DOUBLE field_double;",
            "  OPTIONAL BYTE_ARRAY field_string (UTF8);",
            "  OPTIONAL BOOLEAN field_boolean;",
            "  OPTIONAL INT96 field_timestamp;",
            "}",
            "",
        ]
        .join("\n");

        let subcomand = def();
        let arg_vec = vec!["schema", parquet.path().to_str().unwrap()];
        let args = subcomand.get_matches_from_safe(arg_vec).unwrap();

        {
            let msg = test_utils::SimpleMessage {
                field_int32: 111,
                field_int64: 222,
                field_float: 333.3,
                field_double: 444.4,
                field_string: "555".to_string(),
                field_boolean: false,
                field_timestamp: vec![0, 0, 2_454_923],
            };

            test_utils::write_simple_message_parquet(&parquet.path(), &msg);

            assert_eq!(true, run(&args, &mut output).is_ok());
        }

        let vec = output.into_inner();
        let actual = str::from_utf8(&vec).unwrap();

        assert_eq!(actual, expected);
    }
}
