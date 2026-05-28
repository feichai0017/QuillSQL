use clap::Parser;
use quill_sql::database::{Database, DatabaseOptions};
use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;

#[derive(Debug, Parser, PartialEq)]
#[clap(author, version, about, long_about= None)]
struct Args {
    #[clap(short = 'd', long, help = "Directory for local Arrow/Parquet data")]
    data_dir: Option<String>,
}

#[tokio::main]
async fn main() {
    env_logger::init();
    let args = Args::parse();

    let db = Database::new(DatabaseOptions {
        data_dir: args.data_dir.map(Into::into),
        ..Default::default()
    })
    .expect("fail to open database");

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
