use clap::{App, Arg, ArgMatches, SubCommand};
use command::args;
use iterator::{ParquetFileReader, ParquetPathIterator};
use parquet::file::reader::FileReader;
use parquet::schema::printer::print_schema;
use std::io::Write;
use std::path::Path;

pub fn def() -> App<'static, 'static> {
    SubCommand::with_name("schema")
        .about("Show parquet schema")
        .arg(
            Arg::with_name("output")
                .help("Output format")
                .default_value("hive")
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

fn schema<W: Write>(path: &Path, out: &mut W) -> Result<(), String> {
    let paths = ParquetPathIterator::new(path);
    let mut reader = ParquetFileReader::new(paths);
    let parquet = reader.next().ok_or_else(|| "Unable to read file")??;

    let metadata = parquet.metadata().file_metadata();
    let schema = metadata.schema();

    print_schema(out, &schema);

    Ok(())
}

pub fn run<W: Write>(matches: &ArgMatches, out: &mut W) -> Result<(), String> {
    let path = args::path_value(matches, "path")?;

    return schema(path, out);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::fs::File;
    use utils::test_utils;

    #[test]
    fn test_schema_simple_message() {
        let parquet = test_utils::temp_file("msg", "parquet");
        let output = test_utils::temp_file("schema", "out");
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
            let mut file = File::create(&output).unwrap();
            let msg = test_utils::SimpleMessage {
                field_int32: 111,
                field_int64: 222,
                field_float: 333.3,
                field_double: 444.4,
                field_string: "555".to_string(),
                field_boolean: false,
                field_timestamp: vec![0, 0, 2454923],
            };

            test_utils::write_simple_message_parquet(&parquet.path(), &msg);

            assert_eq!(true, run(&args, &mut file).is_ok());
        }

        assert_eq!(expected, fs::read_to_string(output).unwrap());
    }
}
