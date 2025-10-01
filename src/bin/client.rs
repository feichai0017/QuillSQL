use clap::Parser;
use quill_sql::database::{Database, DatabaseOptions, WalOptions};
use quill_sql::session::SessionContext;
use quill_sql::transaction::IsolationLevel;
use quill_sql::utils::util::pretty_format_tuples;
use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;
use std::path::PathBuf;
use std::str::FromStr;

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
    #[clap(
        long,
        help = "Background WAL writer interval in milliseconds (0 to disable)"
    )]
    wal_writer_interval_ms: Option<u64>,
    #[clap(long, help = "Maximum WAL records to buffer before auto-flush")]
    wal_buffer_capacity: Option<usize>,
    #[clap(long, help = "Flush WAL when pending bytes reach this threshold")]
    wal_flush_coalesce_bytes: Option<usize>,
    #[clap(long, help = "Whether transaction commit waits for WAL durability")]
    wal_synchronous_commit: Option<bool>,
    #[clap(
        long,
        help = "Checkpoint interval in milliseconds (0 to disable)",
        value_name = "MS"
    )]
    wal_checkpoint_interval_ms: Option<u64>,
    #[clap(long, help = "Number of WAL segments to retain on disk")]
    wal_retain_segments: Option<usize>,
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

    let wal_options = WalOptions {
        directory: args.wal_dir,
        segment_size: args.wal_segment_size,
        sync_on_flush: args.wal_sync_on_flush,
        writer_interval_ms: args.wal_writer_interval_ms.map(|val| {
            if val == 0 {
                None
            } else {
                Some(val)
            }
        }),
        buffer_capacity: args.wal_buffer_capacity,
        flush_coalesce_bytes: args.wal_flush_coalesce_bytes,
        synchronous_commit: args.wal_synchronous_commit,
        checkpoint_interval_ms: args.wal_checkpoint_interval_ms.map(|val| {
            if val == 0 {
                None
            } else {
                Some(val)
            }
        }),
        retain_segments: args.wal_retain_segments,
    };
    let default_isolation_level = args
        .isolation_level
        .as_deref()
        .map(IsolationLevel::from_str)
        .transpose()
        .unwrap_or_else(|e| panic!("invalid isolation level: {}", e));
    let db_options = DatabaseOptions {
        wal: wal_options,
        default_isolation_level,
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
