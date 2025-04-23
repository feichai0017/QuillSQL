use clap::Parser;
use quill_sql::database::Database;
use quill_sql::utils::util::pretty_format_tuples;
use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;

#[derive(Debug, Parser, PartialEq)]
#[clap(author, version, about, long_about= None)]
struct Args {
    #[clap(short = 'f', long, help = "Path to your database file")]
    file: Option<String>,
}

fn main() {
    env_logger::init();
    let args = Args::parse();

    let mut db = if let Some(path) = args.file {
        Database::new_on_disk(path.as_str())
            .unwrap_or_else(|e| panic!("fail to open {} file, err: {}", path, e))
    } else {
        Database::new_temp().expect("fail to open temp database")
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
