use clap::ArgMatches;
use std::path::Path;

pub fn path_value<'a>(
    matches: &'a ArgMatches<'a>,
    name: &str,
) -> Result<&'a Path, String> {
    matches
        .value_of(name)
        .map(|p| Path::new(p))
        .ok_or(format!("Invalid argument : {}", name))
}

pub fn usize_value(matches: &ArgMatches, name: &str) -> Result<usize, String> {
    matches
        .value_of(name)
        .map(|s| s.parse())
        .map(|n| n.unwrap())
        .ok_or(format!("Invalid argument : {}", name))
}

pub fn validate_number(value: String) -> Result<(), String> {
    value
        .parse::<usize>()
        .map(|_| ())
        .map_err(|err| err.to_string())
}

pub fn validate_path(value: String) -> Result<(), String> {
    match Path::new(&value).exists() {
        true => Ok(()),
        _ => Err(format!("Path '{}' does not exist", value)),
    }
}
