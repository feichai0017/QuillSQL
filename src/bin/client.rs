use futures::{SinkExt, TryStreamExt};
use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;
use std::env;
use std::{error::Error, net::SocketAddr};
use tokio::net::TcpStream;
use tokio_util::codec::{FramedRead, FramedWrite, LinesCodec};

const RESPONSE_END: &str = "!!!end!!!";

pub struct Client {
    stream: TcpStream,
    txn_version: Option<u64>,
}

impl Client {
    pub async fn new(addr: SocketAddr) -> Result<Self, Box<dyn Error>> {
        let stream = TcpStream::connect(addr).await?;
        Ok(Self {
            stream,
            txn_version: None,
        })
    }

    pub async fn execute_sql(&mut self, sql_cmd: &str) -> Result<(), Box<dyn Error>> {
        let (r, w) = self.stream.split();
        let mut sink = FramedWrite::new(w, LinesCodec::new());
        let mut stream = FramedRead::new(r, LinesCodec::new());

        // 发送命令并执行
        sink.send(sql_cmd).await?;

        // 拿到结果并打印
        while let Some(res) = stream.try_next().await? {
            if res == RESPONSE_END {
                break;
            }
            // 解析事务命令
            if res.starts_with("TRANSACTION") {
                let args = res.split(" ").collect::<Vec<_>>();
                if args[2] == "COMMIT" || args[2] == "ROLLBACK" {
                    self.txn_version = None;
                }
                if args[2] == "BEGIN" {
                    let version = args[1].parse::<u64>().unwrap();
                    self.txn_version = Some(version);
                }
            }
            println!("{}", res);
        }
        Ok(())
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        if self.txn_version.is_some() {
            futures::executor::block_on(self.execute_sql("ROLLBACK;")).expect("rollback failed");
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let addr = env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:8080".to_string());

    let addr = addr.parse::<SocketAddr>()?;
    let mut client = Client::new(addr).await?;

    let mut editor = DefaultEditor::new()?;
    loop {
        let prompt = match client.txn_version {
            Some(version) => format!("sqldb#{}> ", version),
            None => "sqldb> ".into(),
        };
        let readline = editor.readline(&prompt);
        match readline {
            Ok(sql_cmd) => {
                let sql_cmd = sql_cmd.trim();
                if sql_cmd.len() > 0 {
                    if sql_cmd == "quit" {
                        break;
                    }
                    editor.add_history_entry(sql_cmd)?;
                    client.execute_sql(sql_cmd).await?;
                }
            }
            Err(ReadlineError::Interrupted) => break,
            Err(ReadlineError::Eof) => break,
            Err(err) => {
                println!("Error: {:?}", err);
                break;
            }
        }
    }

    Ok(())
}
