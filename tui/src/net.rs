//! Feed client: connect to `pmx serve`'s Unix socket, forward parsed messages,
//! reconnect forever. The server resends full state on every connect.

use std::path::PathBuf;
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::UnixStream;
use tokio::sync::mpsc::Sender;

use crate::proto::{self, Msg};

#[derive(Debug)]
pub enum NetEvent {
    Up,
    Down(String),
    Msg(Msg),
}

pub async fn run(path: PathBuf, tx: Sender<NetEvent>) {
    loop {
        let reason = match UnixStream::connect(&path).await {
            Ok(stream) => {
                if tx.send(NetEvent::Up).await.is_err() {
                    return;
                }
                let mut lines = BufReader::new(stream).lines();
                loop {
                    match lines.next_line().await {
                        Ok(Some(line)) => match proto::parse(&line) {
                            Ok(msg) => {
                                if tx.send(NetEvent::Msg(msg)).await.is_err() {
                                    return;
                                }
                            }
                            Err(e) => break format!("bad message: {e}"),
                        },
                        Ok(None) => break "server closed".to_string(),
                        Err(e) => break e.to_string(),
                    }
                }
            }
            Err(e) => e.to_string(),
        };
        if tx.send(NetEvent::Down(reason)).await.is_err() {
            return;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}
