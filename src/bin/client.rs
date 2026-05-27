use clap::Parser;
use quill_sql::database::{Database, DatabaseOptions};
use quill_sql::session::SessionContext;
use quill_sql::transaction::IsolationLevel;
use quill_sql::utils::util::pretty_format_tuples;
use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;
use std::str::FromStr;

#[derive(Debug, Parser, PartialEq)]
#[clap(author, version, about, long_about= None)]
struct Args {
    #[clap(short = 'f', long, help = "Path to your database file")]
    file: Option<String>,
    #[clap(
        long,
        help = "Default isolation level (read-uncommitted|read-committed|snapshot-isolation|serializable)",
        value_name = "LEVEL"
    )]
    isolation_level: Option<String>,
}

fn main() {
    env_logger::init();
    let args = Args::parse();

    let default_isolation_level = args
        .isolation_level
        .as_deref()
        .map(IsolationLevel::from_str)
        .transpose()
        .unwrap_or_else(|e| panic!("invalid isolation level: {}", e));
    let db_options = DatabaseOptions {
        default_isolation_level,
        ..DatabaseOptions::default()
    };

    let mut db = if let Some(path) = args.file {
        Database::new_on_disk_with_options(path.as_str(), db_options.clone())
            .unwrap_or_else(|e| panic!("fail to open {} file, err: {}", path, e))
    } else {
        Database::new_temp_with_options(db_options).expect("fail to open temp database")
    };
    let mut session = SessionContext::new(db.default_isolation());

    println!(":) Welcome to the bustubx, please input sql.");
    let mut rl = DefaultEditor::new().expect("created editor");
    rl.load_history(".history").ok();

    loop {
        let readline = rl.readline("quillssql=# ");
        match readline {
            Ok(line) => {
                let _ = rl.add_history_entry(line.as_str());
                if line == "exit" || line == "\\q" {
                    println!("bye!");
                    break;
                }
                let result = db.run_with_session(&mut session, &line);
                match result {
                    Ok(tuples) => {
                        if !tuples.is_empty() {
                            println!("{}", pretty_format_tuples(&tuples))
                        }
                    }
                    Err(e) => println!("{}", e),
                }
            }
            Err(ReadlineError::Interrupted) => {
                println!("CTRL-C");
                break;
            }
            Err(ReadlineError::Eof) => {
                println!("CTRL-D");
                break;
            }
            Err(err) => {
                println!("Error: {:?}", err);
                break;
            }
        }
    }
    db.flush().unwrap();

    rl.save_history(".history").ok();
}
