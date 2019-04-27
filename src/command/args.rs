use clap::ArgMatches;
use std::path::Path;
use std::str;

pub fn path_value<'a>(
    matches: &'a ArgMatches<'a>,
    name: &str,
) -> Result<&'a Path, String> {
    matches
        .value_of(name)
        .map(|p| Path::new(p))
        .filter(|p| p.exists())
        .ok_or_else(|| format!("Invalid argument : {}", name))
}

pub fn usize_value(matches: &ArgMatches, name: &str) -> Result<usize, String> {
    matches
        .value_of(name)
        .map(str::parse)
        .filter(Result::is_ok)
        .map(Result::unwrap)
        .ok_or_else(|| format!("Invalid argument : {}", name))
}

pub fn validate_number(value: String) -> Result<(), String> {
    value
        .parse::<usize>()
        .map(|_| ())
        .map_err(|err| err.to_string())
}

pub fn validate_path(value: String) -> Result<(), String> {
    Some(Path::new(&value))
        .filter(|p| p.exists())
        .map(|_| ())
        .ok_or_else(|| format!("Path '{}' does not exist", value))
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::{App, Arg};
    use utils::test_utils;

    #[test]
    fn test_args_validate_path() {
        let tmp = test_utils::temp_file("tmp", ".file");
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
            Err("Invalid argument : limit".to_string()),
            usize_value(&invalid, name)
        );
    }

    #[test]
    fn test_args_path_value() {
        let name = "path";
        let tmp = test_utils::temp_file("tmp", ".file");
        let path = tmp.path().to_str().unwrap();
        let valid = create_matches(name, path);
        let invalid = create_matches(name, "NOT VALID");

        assert_eq!(Ok(Path::new(path)), path_value(&valid, name));
        assert_eq!(
            Err("Invalid argument : path".to_string()),
            path_value(&invalid, name)
        );
    }

    fn create_matches<'a>(name: &'a str, value: &'a str) -> ArgMatches<'a> {
        App::new(name)
            .arg(Arg::with_name(name).index(1).required(true))
            .get_matches_from_safe(vec![name, value])
            .unwrap()
    }
}
