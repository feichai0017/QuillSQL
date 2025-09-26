use clap::Parser;
use quill_sql::database::{Database, DatabaseOptions, WalOptions};
use quill_sql::utils::util::pretty_format_tuples;
use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;
use std::path::PathBuf;

#[derive(Debug, Parser, PartialEq)]
#[clap(author, version, about, long_about= None)]
struct Args {
    #[clap(short = 'f', long, help = "Path to your database file")]
    file: Option<String>,
    #[clap(long, help = "Directory to store WAL files")]
    wal_dir: Option<PathBuf>,
    #[clap(long, help = "WAL segment size in bytes")]
    wal_segment_size: Option<u64>,
    #[clap(long, help = "Whether WAL flush should fsync (true/false)")]
    wal_sync_on_flush: Option<bool>,
}

fn main() {
    env_logger::init();
    let args = Args::parse();

    let wal_options = WalOptions {
        directory: args.wal_dir,
        segment_size: args.wal_segment_size,
        sync_on_flush: args.wal_sync_on_flush,
    };
    let db_options = DatabaseOptions { wal: wal_options };

    let mut db = if let Some(path) = args.file {
        Database::new_on_disk_with_options(path.as_str(), db_options.clone())
            .unwrap_or_else(|e| panic!("fail to open {} file, err: {}", path, e))
    } else {
        Database::new_temp_with_options(db_options).expect("fail to open temp database")
    };

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
                let result = db.run(&line);
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
