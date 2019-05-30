use crate::command::args;
use crate::output::TableOutputWriter;
use crate::reader::ParquetFile;
use api::Result;
use clap::{App, Arg, ArgMatches, SubCommand};
use stats::Frequencies;
use std::io::Write;

fn compute<I>(num_fields: usize, iter: I) -> Result<Vec<Frequencies<String>>>
where
    I: Iterator<Item = Result<Vec<String>>>,
{
    let mut vec: Vec<_> = (0..num_fields).map(|_| Frequencies::new()).collect();

    for row in iter {
        for (i, val) in row?.iter().enumerate() {
            vec[i].add(val.to_string());
        }
    }

    Ok(vec)
}

fn format_row(field: &str, name: &str, count: u64) -> Vec<String> {
    vec![field.to_string(), name.to_string(), count.to_string()]
}

fn format_rows(
    fields: Vec<String>,
    vec: Vec<Frequencies<String>>,
) -> impl Iterator<Item = Result<Vec<String>>> {
    vec.into_iter()
        .enumerate()
        .map(move |t| {
            let header = (&fields[t.0]).to_string();
            let counts =
                t.1.least_frequent()
                    .into_iter()
                    .map(|c| (c.0.to_string(), c.1))
                    .collect::<Vec<_>>();

            (header, counts)
        })
        .flat_map(|t| {
            let header = t.0.to_string();
            let counts = t.1.into_iter();

            counts.map(move |c| Ok(format_row(&header, &c.0, c.1)))
        })
}

pub fn def() -> App<'static, 'static> {
    SubCommand::with_name("frequency")
        .about("Show frequency counts for each column/value")
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
                .default_value("500")
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
    let fields = parquet.field_names()?;
    let rows = parquet.iter().take(limit);
    let vec = compute(fields.len(), rows)?;
    let headers = vec![
        String::from("FIELD"),
        String::from("VALUE"),
        String::from("COUNT"),
    ];

    let iter = format_rows(fields, vec);
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
    fn test_simple_messages_frequency() {
        let mut output = Cursor::new(Vec::new());
        let parquet = api::tests::temp_file("msg", ".parquet");
        let path_str = parquet.path().to_str().unwrap();
        let path = parquet.path();

        let subcomand = def();
        let arg_vec = vec!["frequency", path_str, "-l=2", "-c=field_int32,field_string"];
        let args = subcomand.get_matches_from_safe(arg_vec).unwrap();

        let msg1 = api::tests::SimpleMessage {
            field_int32: 1,
            field_int64: 2,
            field_float: 3.3,
            field_double: 4.4,
            field_string: "two".to_string(),
            field_boolean: true,
            field_timestamp: vec![0, 0, 2_454_923],
        };
        let msg2 = api::tests::SimpleMessage {
            field_int32: 2,
            field_int64: 22,
            field_float: 33.3,
            field_double: 44.4,
            field_string: "two".to_string(),
            field_boolean: false,
            field_timestamp: vec![4_165_425_152, 13, 2_454_923],
        };

        api::tests::write_simple_messages_parquet(&path, &[&msg1, &msg2]);

        assert_eq!(true, run(&args, &mut output).is_ok());

        let vec = output.into_inner();
        let actual = str::from_utf8(&vec).unwrap();

        assert!(actual.starts_with("FIELD         VALUE  COUNT"));
        assert!(actual.contains("field_int32   1      1"));
        assert!(actual.contains("field_int32   2      1"));
        assert!(actual.contains("field_string  \"two\"  2"));
        assert!(actual.ends_with(""));
    }
}
