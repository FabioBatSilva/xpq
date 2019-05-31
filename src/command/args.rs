use crate::api::{Error, Result};
use crate::output::OutputFormat;
use clap::ArgMatches;
use std::convert::TryFrom;
use std::path::Path;
use std::str;

/// Gets the value of a specific argument
/// Converting the ArgMatches value to a Path reference.
///
/// If the option wasn't present or is invalid returns
/// `crate::api::Error::InvalidArgument`.
pub fn path_value<'a>(matches: &'a ArgMatches<'a>, name: &str) -> Result<&'a Path> {
    matches
        .value_of(name)
        .map(|p| Path::new(p))
        .filter(|p| p.exists())
        .ok_or_else(|| Error::InvalidArgument(name.to_string()))
}

/// Gets all values of a specific argument.
///
/// If the option wasn't present `None` or `Some(crate::api::Error::InvalidArgument)` when
/// invalid.
pub fn string_values(matches: &ArgMatches, name: &str) -> Result<Option<Vec<String>>> {
    matches
        .values_of(name)
        .map(|v| {
            v.flat_map(|s| s.split(','))
                .map(String::from)
                .collect::<Vec<_>>()
        })
        .or_else(|| Some(vec![]))
        .map(|vec| Some(vec).filter(|v| !v.is_empty()))
        .ok_or_else(|| Error::InvalidArgument(name.to_string()))
}

/// Gets the value of a specific argument
/// Converting the ArgMatches value to a usize.
///
/// If the option wasn't present or is invalid returns
/// `crate::api::Error::InvalidArgument`.
pub fn usize_value(matches: &ArgMatches, name: &str) -> Result<usize> {
    matches
        .value_of(name)
        .map(str::parse)
        .filter(std::result::Result::is_ok)
        .map(std::result::Result::unwrap)
        .ok_or_else(|| Error::InvalidArgument(name.to_string()))
}

/// Gets the value of a specific argument
/// Converting the ArgMatches value to a `crate::output::OutputFormat`.
///
/// If the option wasn't present or is invalid returns
/// `crate::api::Error::InvalidArgument`.
pub fn output_format_value(matches: &ArgMatches, name: &str) -> Result<OutputFormat> {
    matches
        .value_of(name)
        .map(String::from)
        .map(OutputFormat::try_from)
        .filter(std::result::Result::is_ok)
        .map(std::result::Result::unwrap)
        .ok_or_else(|| Error::InvalidArgument(name.to_string()))
}

pub fn validate_number(value: String) -> std::result::Result<(), String> {
    value
        .parse::<usize>()
        .map(|_| ())
        .map_err(|err| err.to_string())
}

pub fn validate_path(value: String) -> std::result::Result<(), String> {
    Some(Path::new(&value))
        .filter(|p| p.exists())
        .map(|_| ())
        .ok_or_else(|| format!("Path '{}' does not exist", value))
}

#[cfg(test)]
mod tests {
    use super::*;
    use api;
    use clap::{App, Arg};

    #[test]
    fn test_args_validate_path() {
        let tmp = api::tests::temp_file("tmp", ".file");
        let valid = String::from(tmp.path().to_string_lossy());
        let invalid = String::from("NOT VALID");

        assert_eq!(Ok(()), validate_path(valid));
        assert_eq!(
            Err(format!("Path '{}' does not exist", invalid)),
            validate_path(invalid)
        );
    }

    #[test]
    fn test_args_validate_number() {
        let valid = String::from("123");
        let invalid = String::from("NOT VALID");

        assert_eq!(Ok(()), validate_number(valid));
        assert_eq!(
            Err("invalid digit found in string".to_string()),
            validate_number(invalid)
        );
    }

    #[test]
    fn test_args_usize_value() {
        let name = "limit";
        let valid = create_matches(name, "123");
        let invalid = create_matches(name, "NOT VALID");

        assert_eq!(Ok(123), usize_value(&valid, name));
        assert_eq!(
            Err(Error::InvalidArgument("limit".to_string())),
            usize_value(&invalid, name)
        );
    }

    #[test]
    fn test_args_output_format_value() {
        let name = "format";
        let valid = create_matches(name, "table");
        let invalid = create_matches(name, "NOT VALID");

        assert_eq!(Ok(OutputFormat::Tabular), output_format_value(&valid, name));
        assert_eq!(
            Err(Error::InvalidArgument(name.to_string())),
            output_format_value(&invalid, name)
        );
    }

    #[test]
    fn test_args_string_values() {
        let name = "values";
        let present = create_mult_matches(name, &[name, "1", "2", "3"]);
        let missing = create_mult_matches(name, &[name]);

        assert_eq!(
            Ok(Some(vec![
                String::from("1"),
                String::from("2"),
                String::from("3")
            ])),
            string_values(&present, name)
        );

        assert_eq!(Ok(None), string_values(&missing, name));
    }

    #[test]
    fn test_args_string_values_comma_separated() {
        let name = "values";
        let result1 = create_mult_matches(name, &[name, "1,2,3,4,5"]);
        let result2 = create_mult_matches(name, &[name, "aa", "bb,cc", "dd"]);

        assert_eq!(
            Ok(Some(vec![
                String::from("1"),
                String::from("2"),
                String::from("3"),
                String::from("4"),
                String::from("5"),
            ])),
            string_values(&result1, name)
        );

        assert_eq!(
            Ok(Some(vec![
                String::from("aa"),
                String::from("bb"),
                String::from("cc"),
                String::from("dd"),
            ])),
            string_values(&result2, name)
        );
    }

    #[test]
    fn test_args_path_value() {
        let name = "path";
        let tmp = api::tests::temp_file("tmp", ".file");
        let path = tmp.path().to_str().unwrap();
        let valid = create_matches(name, path);
        let invalid = create_matches(name, "NOT VALID");

        assert_eq!(Ok(Path::new(path)), path_value(&valid, name));
        assert_eq!(
            Err(Error::InvalidArgument("path".to_string())),
            path_value(&invalid, name)
        );
    }

    fn create_matches<'a>(name: &'a str, value: &'a str) -> ArgMatches<'a> {
        App::new(name)
            .arg(Arg::with_name(name).index(1).required(true))
            .get_matches_from_safe(vec![name, value])
            .unwrap()
    }

    fn create_mult_matches<'a>(name: &'a str, values: &[&str]) -> ArgMatches<'a> {
        App::new(name)
            .arg(Arg::with_name(name).index(1).required(false).multiple(true))
            .get_matches_from_safe(values)
            .unwrap()
    }
}
