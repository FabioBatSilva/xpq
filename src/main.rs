extern crate clap;
extern crate parquet;
extern crate prettytable;
extern crate rand;
extern crate walkdir;

use clap::{App, AppSettings, ArgMatches};
use std::process;

mod command;
mod output;
mod reader;
mod utils;

fn run(matches: ArgMatches) -> Result<(), String> {
    let out = &mut std::io::stdout();

    match matches.subcommand() {
        ("read", Some(m)) => command::read::run(m, out),
        ("schema", Some(m)) => command::schema::run(m, out),
        ("sample", Some(m)) => command::sample::run(m, out),
        ("count", Some(m)) => command::count::run(m, out),
        _ => Ok(()),
    }
}

fn main() {
    let app = App::new("xpq")
        .version(env!("CARGO_PKG_VERSION"))
        .setting(AppSettings::ArgRequiredElseHelp)
        .author("Fabio B. Silva <fabio.bat.silva@gmail.com>")
        .about("Simple Parquet command line toolkit.")
        .subcommands(vec![
            command::read::def(),
            command::count::def(),
            command::schema::def(),
            command::sample::def(),
        ]);

    if let Err(e) = run(app.get_matches()) {
        eprintln!("{}", e);
        process::exit(1);
    }
}
