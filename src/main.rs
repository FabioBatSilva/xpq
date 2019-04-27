extern crate clap;
extern crate parquet;
extern crate prettytable;
extern crate walkdir;

use clap::{App, AppSettings, ArgMatches};

mod command;
mod output;
mod reader;
mod utils;

fn run(matches: ArgMatches) -> Result<(), String> {
    let out = &mut std::io::stdout();

    match matches.subcommand() {
        ("schema", Some(m)) => command::schema::run(m, out),
        ("sample", Some(m)) => command::sample::run(m, out),
        ("count", Some(m)) => command::count::run(m, out),
        _ => Ok(()),
    }
}

fn main() {
    let app = App::new("pq")
        .version("0.1.0")
        .setting(AppSettings::ArgRequiredElseHelp)
        .author("Fabio B. Silva <fabio.bat.silva@gmail.com>")
        .about("Parquet command line toolkit written in Rust")
        .subcommands(vec![
            command::count::def(),
            command::schema::def(),
            command::sample::def(),
        ]);

    if let Err(e) = run(app.get_matches()) {
        panic!("Application error: {}", e);
    }
}
