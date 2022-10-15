use crate::api::{Error, Result};
use crate::output::OutputFormat;
use clap::ArgMatches;
use regex::Regex;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::path::Path;
use std::str;

/// Gets the value of a specific argument
/// Converting the ArgMatches value to a Path reference.
///
/// If the option wasn't present or is invalid returns
/// `crate::api::Error::InvalidArgument`.
pub fn path_value<'a>(matches: &'a ArgMatches, name: &str) -> Result<&'a Path> {
    matches
        .value_of(name)
        .map(Path::new)
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

/// Gets all values of a specific argument.
///
/// If the option wasn't present `None` or `Some(crate::api::Error::InvalidArgument)` when
/// invalid.
pub fn filter_values(
    matches: &ArgMatches,
    name: &str,
) -> Result<Option<HashMap<String, Regex>>> {
    match matches.values_of(name) {
        Some(values) => {
            let mut result = HashMap::new();
            let filters = values.map(String::from).collect::<Vec<_>>();

            for entry in filters {
                let filter = entry.as_str();
                let parts = filter.splitn(2, ':').collect::<Vec<_>>();

                if parts.len() != 2 || parts[0].is_empty() || parts[1].is_empty() {
                    return Err(Error::InvalidArgument(name.to_string()));
                }

                let field = String::from(parts[0]);
                let regex = Regex::new(parts[1])?;

                result.insert(field, regex);
            }

            Ok(Some(result))
        }
        None => Ok(None),
    }
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

pub fn validate_number(value: &str) -> std::result::Result<(), String> {
    value
        .parse::<usize>()
        .map(|_| ())
        .map_err(|err| err.to_string())
}

pub fn validate_path(value: &str) -> std::result::Result<(), String> {
    Some(Path::new(&value))
        .filter(|p| p.exists())
        .map(|_| ())
        .ok_or_else(|| format!("Path '{}' does not exist", value))
}

pub fn validate_filter(value: &str) -> std::result::Result<(), String> {
    Some(value)
        .map(|s| {
            s.splitn(2, ':')
                .map(ToString::to_string)
                .collect::<Vec<_>>()
        })
        .filter(|s| s.len() == 2)
        .filter(|s| !s[0].is_empty() && !s[1].is_empty())
        .map(|s| Regex::new(&s[1]))
        .filter(std::result::Result::is_ok)
        .map(|_| ())
        .ok_or_else(|| {
            format!(
                "Invalid filter expression. Expected '<column>:<regex>' got '{}'",
                value
            )
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api;
    use clap::{App, Arg};

    #[test]
    fn test_args_validate_path() {
        let tmp = api::tests::temp_file("tmp", ".file");
        let valid = tmp.path().to_str().unwrap();
        let invalid = "NOT VALID";

        assert_eq!(Ok(()), validate_path(valid));
        assert_eq!(
            Err(format!("Path '{}' does not exist", invalid)),
            validate_path(invalid)
        );
    }

    #[test]
    fn test_args_validate_number() {
        let valid = "123";
        let invalid = "NOT VALID";

        assert_eq!(Ok(()), validate_number(valid));
        assert_eq!(
            Err("invalid digit found in string".to_string()),
            validate_number(invalid)
        );
    }

    #[test]
    fn test_args_validate_filter() {
        assert_eq!(Ok(()), validate_filter("foo:bar"));
        assert_eq!(Ok(()), validate_filter("foo:^ns::[a-zA-Z]*$"));

        assert_eq!(
            Err(
                "Invalid filter expression. Expected '<column>:<regex>' got 'NOT VALID'"
                    .to_string()
            ),
            validate_filter("NOT VALID")
        );

        assert_eq!(
            Err(
                "Invalid filter expression. Expected '<column>:<regex>' got 'foo'"
                    .to_string()
            ),
            validate_filter("foo")
        );

        assert_eq!(
            Err(
                "Invalid filter expression. Expected '<column>:<regex>' got 'bar:'"
                    .to_string()
            ),
            validate_filter("bar:")
        );

        assert_eq!(
            Err(
                "Invalid filter expression. Expected '<column>:<regex>' got ':bar'"
                    .to_string()
            ),
            validate_filter(":bar")
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
    fn test_args_filter_values() {
        let name = "filters";

        let missing_matches = create_mult_matches(name, &[name]);
        let missing_result = filter_values(&missing_matches, name);

        let simple_matches = create_mult_matches(name, &[name, "field:[a-z]"]);
        let simple_result = filter_values(&simple_matches, name);

        let url_matches = create_mult_matches(name, &[name, "url:^http://"]);
        let url_result = filter_values(&url_matches, name);

        let mult_matches = create_mult_matches(name, &[name, "a:A", "b:B"]);
        let mult_result = filter_values(&mult_matches, name);

        let regex_matches = create_mult_matches(name, &[name, "foo:^ns::[a-zA-Z]*$"]);
        let regex_result = filter_values(&regex_matches, name);

        assert!(missing_result.is_ok());
        assert!(missing_result.as_ref().unwrap().is_none());

        assert!(simple_result.is_ok());
        assert!(simple_result.as_ref().unwrap().is_some());

        assert!(url_result.is_ok());
        assert!(url_result.as_ref().unwrap().is_some());

        assert!(mult_result.is_ok());
        assert!(mult_result.as_ref().unwrap().is_some());

        assert!(regex_result.is_ok());
        assert!(regex_result.as_ref().unwrap().is_some());

        let simple_result_map = simple_result.unwrap().unwrap();
        let regex_result_map = regex_result.unwrap().unwrap();
        let mult_result_map = mult_result.unwrap().unwrap();
        let url_result_map = url_result.unwrap().unwrap();

        assert_eq!(1, simple_result_map.len());
        assert_eq!("[a-z]", simple_result_map.get("field").unwrap().as_str());

        assert_eq!(1, url_result_map.len());
        assert_eq!("^http://", url_result_map.get("url").unwrap().as_str());

        assert_eq!(1, regex_result_map.len());
        assert_eq!(
            "^ns::[a-zA-Z]*$",
            regex_result_map.get("foo").unwrap().as_str()
        );

        assert_eq!(2, mult_result_map.len());
        assert_eq!("A", mult_result_map.get("a").unwrap().as_str());
        assert_eq!("B", mult_result_map.get("b").unwrap().as_str());
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

    fn create_matches<'a>(name: &'a str, value: &'a str) -> ArgMatches {
        App::new(name)
            .arg(Arg::with_name(name).index(1).required(true))
            .get_matches_from_safe(vec![name, value])
            .unwrap()
    }

    fn create_mult_matches<'a>(name: &'a str, values: &[&str]) -> ArgMatches {
        App::new(name)
            .arg(Arg::with_name(name).index(1).required(false).multiple(true))
            .get_matches_from_safe(values)
            .unwrap()
    }
}
