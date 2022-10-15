use clap::{App, AppSettings, ArgMatches};
use std::process;

mod api;
mod command;
mod output;
mod reader;

fn run(matches: ArgMatches) -> api::Result<()> {
    let out = &mut std::io::stdout();

    match matches.subcommand() {
        Some(("read", args)) => command::read::run(args, out),
        Some(("schema", args)) => command::schema::run(args, out),
        Some(("sample", args)) => command::sample::run(args, out),
        Some(("count", args)) => command::count::run(args, out),
        Some(("frequency", args)) => command::frequency::run(args, out),
        _ => Ok(()),
    }
}

fn main() {
    let app = App::new(env!("CARGO_PKG_NAME"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .version(env!("CARGO_PKG_VERSION"))
        .setting(AppSettings::ArgRequiredElseHelp)
        .subcommands(vec![
            command::read::def(),
            command::count::def(),
            command::schema::def(),
            command::sample::def(),
            command::frequency::def(),
        ]);

    if let Err(e) = run(app.get_matches()) {
        eprintln!("{}", e);
        process::exit(1);
    }
}
