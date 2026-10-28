//! pmx-tui: watch-only dashboard for `pmx serve` / the engine — markets with their L2
//! books, portfolio risk, strategy performance, slots, and the event log.

mod app;
mod book;
mod charts;
mod feed;
mod fmt;
mod net;
mod proto;
mod theme;
mod ui;
mod views;

use std::path::PathBuf;
use std::time::{Duration, Instant};

use clap::Parser;
use futures_util::StreamExt;
use ratatui::crossterm::event::{Event, EventStream, KeyEventKind};
use tokio::sync::mpsc;

use app::{App, Link};
use net::NetEvent;
use theme::Theme;

#[derive(Parser)]
#[command(about = "Watch-only prediction-market dashboard fed by `pmx serve` or the engine")]
struct Args {
    /// Feed socket (default: $PMX_SOCK, else data/pmx.sock)
    #[arg(long)]
    socket: Option<PathBuf>,
}

fn default_socket() -> PathBuf {
    std::env::var_os("PMX_SOCK")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../data/pmx.sock"))
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let socket = args.socket.unwrap_or_else(default_socket);
    let socket = std::path::absolute(&socket).unwrap_or(socket);
    let (tx, mut rx) = mpsc::channel(16_384);
    tokio::spawn(net::run(socket.clone(), tx));

    let mut app = App::new(socket.display().to_string());
    let theme = Theme::from_env();
    let mut terminal = ratatui::init();
    let mut events = EventStream::new();
    let mut frame = tokio::time::interval(Duration::from_millis(33));
    frame.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let result: anyhow::Result<()> = async {
        while !app.quit {
            tokio::select! {
                ev = events.next() => match ev {
                    Some(Ok(Event::Key(k))) if k.kind == KeyEventKind::Press => {
                        let page = ui::page_size(terminal.get_frame().area());
                        app.on_key(k, page);
                    }
                    Some(Ok(Event::Resize(..))) => app.dirty = true,
                    Some(Err(e)) => return Err(e.into()),
                    None => break,
                    _ => {}
                },
                Some(ev) = rx.recv() => {
                    // Drain everything queued so a burst costs one frame.
                    let (started, mut n) = (Instant::now(), 0);
                    let mut next = Some(ev);
                    while let Some(ev) = next {
                        match ev {
                            NetEvent::Up => app.set_link(Link::Up),
                            NetEvent::Down(e) => app.set_link(Link::Down(e)),
                            NetEvent::Msg(m) => {
                                app.on_msg(m, now_ms());
                                n += 1;
                            }
                        }
                        next = rx.try_recv().ok();
                    }
                    app.note_apply(started.elapsed(), n);
                }
                _ = frame.tick() => {
                    app.tick(now_ms());
                    if app.dirty {
                        let now = now_ms();
                        let started = Instant::now();
                        terminal.draw(|f| ui::draw(f, &app, &theme, now))?;
                        app.note_frame(started.elapsed());
                        app.dirty = false;
                    }
                }
            }
        }
        Ok(())
    }
    .await;
    ratatui::restore();
    result
}
