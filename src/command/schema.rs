use crate::api::Result;
use crate::command::args;
use crate::reader::ParquetFile;
use clap::{App, Arg, ArgMatches, SubCommand};
use parquet::schema::printer::print_schema;
use std::io::Write;

pub fn def() -> App<'static> {
    SubCommand::with_name("schema")
        .about("Show parquet schema")
        .arg(
            Arg::with_name("format")
                .help("Output format")
                .possible_values(&["hive"])
                .default_value("hive")
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
    let path = args::path_value(matches, "path")?;
    let parquet = ParquetFile::from(path);
    let schema = parquet.schema()?;

    print_schema(out, &schema);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api;
    use std::io::Cursor;
    use std::str;

    #[test]
    fn test_schema_simple_message() {
        let mut output = Cursor::new(Vec::new());
        let parquet = api::tests::temp_file("msg", "parquet");
        let expected = vec![
            "message rust_schema {",
            "  REQUIRED INT32 field_int32;",
            "  REQUIRED INT64 field_int64;",
            "  REQUIRED FLOAT field_float;",
            "  REQUIRED DOUBLE field_double;",
            "  REQUIRED BYTE_ARRAY field_string (STRING);",
            "  REQUIRED BOOLEAN field_boolean;",
            "  REQUIRED INT64 field_timestamp (TIMESTAMP_MILLIS);",
            "}",
            "",
        ]
        .join("\n");

        let subcomand = def();
        let msgs = api::tests::create_simple_messages(1);
        let arg_vec = vec!["schema", parquet.path().to_str().unwrap()];
        let args = subcomand.get_matches_from_safe(arg_vec).unwrap();

        api::tests::write_simple_messages_parquet(&parquet.path(), &msgs);

        assert_eq!(true, run(&args, &mut output).is_ok());

        let vec = output.into_inner();
        let actual = str::from_utf8(&vec).unwrap();

        assert_eq!(actual, expected);
    }
}
