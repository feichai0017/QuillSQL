use futures::SinkExt;
use sqldb_rs::error::Result;
use sqldb_rs::sql;
use sqldb_rs::sql::engine::kv::KVEngine;
use sqldb_rs::storage::disk::DiskEngine;
use tokio::net::{TcpListener, TcpStream};
use tokio_stream::StreamExt;
use tokio_util::codec::{Framed, LinesCodec};

use std::env;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard};

const DB_PATH: &str = "/tmp/sqldb-test/sqldb-log";
const RESPONSE_END: &str = "!!!end!!!";

/// Possible requests our clients can send us
enum SqlRequest {
    SQL(String),
    ListTables,
    TableInfo(String),
}

impl SqlRequest {
    pub fn parse(cmd: &str) -> Self {
        let upper_cmd = cmd.to_uppercase();
        if upper_cmd == "SHOW TABLES" {
            return SqlRequest::ListTables;
        }
        if upper_cmd.starts_with("SHOW TABLE") {
            let args = upper_cmd.split_ascii_whitespace().collect::<Vec<_>>();
            if args.len() == 3 {
                return SqlRequest::TableInfo(args[2].to_lowercase());
            }
        }
        SqlRequest::SQL(cmd.into())
    }
}

pub struct ServerSession<E: sql::engine::Engine> {
    session: sql::engine::Session<E>,
}

impl<E: sql::engine::Engine + 'static> ServerSession<E> {
    pub fn new(eng: MutexGuard<E>) -> Result<Self> {
        Ok(Self {
            session: eng.session()?,
        })
    }

    pub async fn handle_request(&mut self, socket: TcpStream) -> Result<()> {
        let mut lines = Framed::new(socket, LinesCodec::new());
        while let Some(result) = lines.next().await {
            match result {
                Ok(line) => {
                    // 解析并得到 SqlRequest
                    let req = SqlRequest::parse(&line);

                    // 执行请求
                    let response = match req {
                        SqlRequest::SQL(sql) => match self.session.execute(&sql) {
                            Ok(rs) => rs.to_string(),
                            Err(e) => e.to_string(),
                        },
                        SqlRequest::ListTables => match self.session.get_table_names() {
                            Ok(names) => names,
                            Err(e) => e.to_string(),
                        },
                        SqlRequest::TableInfo(table_name) => {
                            match self.session.get_table(table_name) {
                                Ok(tbinfo) => tbinfo,
                                Err(e) => e.to_string(),
                            }
                        }
                    };

                    // 发送执行结果
                    if let Err(e) = lines.send(response.as_str()).await {
                        println!("error on sending response; error = {e:?}");
                    }
                    if let Err(e) = lines.send(RESPONSE_END).await {
                        println!("error on sending response; error = {e:?}");
                    }
                }
                Err(e) => {
                    println!("error on decoding from socket; error = {e:?}");
                }
            }
        }

        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // 启动 TCP 服务
    let addr = env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:8080".to_string());

    let listener = TcpListener::bind(&addr).await?;
    println!("sqldb server starts, listening on: {addr}");

    // 初始化 DB
    let p = PathBuf::from(DB_PATH);
    let kvengine = KVEngine::new(DiskEngine::new(p.clone())?);
    let shared_engine = Arc::new(Mutex::new(kvengine));

    loop {
        match listener.accept().await {
            Ok((socket, _)) => {
                let db = shared_engine.clone();
                let mut ss = ServerSession::new(db.lock()?)?;

                tokio::spawn(async move {
                    match ss.handle_request(socket).await {
                        Ok(_) => {}
                        Err(e) => {
                            println!("internal server error {:?}", e);
                        }
                    }
                });
            }
            Err(e) => println!("error accepting socket; error = {e:?}"),
        }
    }
}
