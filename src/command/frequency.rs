use crate::api::Result;
use crate::command::args;
use crate::output::{OutputFormat, OutputWriter};
use crate::reader::ParquetFile;
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

pub fn def() -> App<'static> {
    SubCommand::with_name("frequency")
        .about("Show frequency counts for each column/value")
        .arg(
            Arg::with_name("columns")
                .help("Select columns from parquet")
                .takes_value(true)
                .long("columns")
                .multiple(true)
                .short('c'),
        )
        .arg(
            Arg::with_name("search")
                .validator(args::validate_filter)
                .help("Search columns")
                .takes_value(true)
                .long("search")
                .multiple(true)
                .short('s'),
        )
        .arg(
            Arg::with_name("limit")
                .validator(args::validate_number)
                .help("Max number of rows")
                .default_value("500")
                .long("limit")
                .short('l'),
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

pub fn run<W: Write>(matches: &ArgMatches, out: &mut W) -> Result<()> {
    let format = args::output_format_value(matches, "format")?;
    let columns = args::string_values(matches, "columns")?;
    let search = args::filter_values(matches, "search")?;
    let limit = args::usize_value(matches, "limit")?;
    let path = args::path_value(matches, "path")?;
    let parquet = ParquetFile::from(path)
        .with_fields(columns)
        .with_filters(search);

    let fields = parquet.field_names()?;
    let rows = parquet.iter().take(limit);
    let vec = compute(fields.len(), rows)?;
    let headers = vec![
        String::from("FIELD"),
        String::from("VALUE"),
        String::from("COUNT"),
    ];

    let iter = format_rows(fields, vec);
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
    fn test_simple_messages_frequency() {
        let mut output = Cursor::new(Vec::new());
        let parquet = api::tests::temp_file("msg", ".parquet");
        let path_str = parquet.path().to_str().unwrap();
        let path = parquet.path();

        let subcomand = def();
        let msgs = api::tests::create_simple_messages(4);
        let arg_vec = vec![
            "frequency",
            path_str,
            "-l=3",
            "-c=field_int32,field_boolean",
        ];
        let args = subcomand.get_matches_from_safe(arg_vec).unwrap();

        api::tests::write_simple_messages_parquet(&path, &msgs);

        assert_eq!(true, run(&args, &mut output).is_ok());

        let vec = output.into_inner();
        let actual = str::from_utf8(&vec).unwrap();

        assert_eq!(6, actual.lines().count());
        assert!(actual.starts_with("FIELD          VALUE  COUNT"));
        assert!(actual.contains("field_int32    1      1"));
        assert!(actual.contains("field_int32    2      1"));
        assert!(actual.contains("field_int32    3      1"));
        assert!(actual.contains("field_boolean  true   1"));
        assert!(actual.contains("field_boolean  false  2"));
        assert!(actual.ends_with(""));
    }

    #[test]
    fn test_simple_messages_frequency_with_filters() {
        let mut output = Cursor::new(Vec::new());
        let parquet = api::tests::temp_file("msg", ".parquet");
        let path_str = parquet.path().to_str().unwrap();
        let path = parquet.path();

        let subcomand = def();
        let msgs = api::tests::create_simple_messages(3);
        let args = subcomand
            .get_matches_from_safe(vec![
                "read",
                path_str,
                "-s=field_boolean:false",
                "-c=field_int32,field_boolean",
            ])
            .unwrap();

        api::tests::write_simple_messages_parquet(&path, &msgs);

        assert_eq!(true, run(&args, &mut output).is_ok());

        let vec = output.into_inner();
        let actual = str::from_utf8(&vec).unwrap();

        assert_eq!(4, actual.lines().count());
        assert!(actual.starts_with("FIELD          VALUE  COUNT"));
        assert!(actual.contains("field_int32    1      1"));
        assert!(actual.contains("field_int32    3      1"));
        assert!(actual.contains("field_boolean  false  2"));
        assert!(actual.ends_with(""));
    }

    #[test]
    fn test_simple_messages_frequency_vertical_format() {
        let mut output = Cursor::new(Vec::new());
        let parquet = api::tests::temp_file("msg", ".parquet");
        let path_str = parquet.path().to_str().unwrap();
        let path = parquet.path();

        let subcomand = def();
        let msgs = api::tests::create_simple_messages(9);
        let arg_vec = vec!["frequency", path_str, "-f=v", "-c=field_boolean"];
        let args = subcomand.get_matches_from_safe(arg_vec).unwrap();

        api::tests::write_simple_messages_parquet(&path, &msgs);

        assert_eq!(true, run(&args, &mut output).is_ok());

        let vec = output.into_inner();
        let actual = str::from_utf8(&vec).unwrap();

        assert_eq!(8, actual.lines().count());
        assert!(actual.contains(
            &vec![
                "",
                "FIELD:  field_boolean",
                "VALUE:  true", // 4 true
                "COUNT:  4",
                ""
            ]
            .join("\n")
        ));
        assert!(actual.contains(
            &vec![
                "",
                "FIELD:  field_boolean",
                "VALUE:  false",
                "COUNT:  5",
                ""
            ]
            .join("\n")
        ));
        assert!(actual.ends_with(""));
    }

    #[test]
    fn test_simple_messages_frequency_vertical_csv() {
        let mut output = Cursor::new(Vec::new());
        let parquet = api::tests::temp_file("msg", ".parquet");
        let path_str = parquet.path().to_str().unwrap();
        let path = parquet.path();

        let subcomand = def();
        let msgs = api::tests::create_simple_messages(3);
        let arg_vec = vec!["frequency", path_str, "-f=csv", "-c=field_timestamp"];
        let args = subcomand.get_matches_from_safe(arg_vec).unwrap();

        api::tests::write_simple_messages_parquet(&path, &msgs);

        assert_eq!(true, run(&args, &mut output).is_ok());

        let vec = output.into_inner();
        let actual = str::from_utf8(&vec).unwrap();

        assert_eq!(3, actual.lines().count());
        assert!(actual.starts_with("FIELD,VALUE,COUNT"));
        assert!(actual.contains(&format!(
            "field_timestamp,{},2",
            api::tests::time_to_str(1_238_544_000_000)
        )));
        assert!(actual.contains(&format!(
            "field_timestamp,{},1",
            api::tests::time_to_str(1_238_544_060_000)
        )));
        assert!(actual.ends_with(""));
    }
}
