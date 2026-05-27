use clap::Parser;
use quill_sql::database::{Database, DatabaseOptions};
use quill_sql::transaction::IsolationLevel;
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

#[tokio::main]
async fn main() {
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

    let db = if let Some(path) = args.file {
        Database::new_on_disk_with_options(path.as_str(), db_options.clone())
            .unwrap_or_else(|e| panic!("fail to open {} file, err: {}", path, e))
    } else {
        Database::new_temp_with_options(db_options).expect("fail to open temp database")
    };

    println!(":) Welcome to QuillSQL, please input SQL.");
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
                let result = db.run(&line).await;
                match result {
                    Ok(output) => {
                        if !output.is_empty() {
                            match output.pretty_table() {
                                Ok(table) => println!("{}", table),
                                Err(err) => println!("{}", err),
                            }
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
