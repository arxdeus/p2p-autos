//! The `share` subcommand: a CLI client that plays the browser tab's role
//! from src/index.html, but for one local file. Connects to a running
//! p2p-autos server, offers the file, and answers pulls straight off
//! disk. The link only works while this process is running — killing it
//! drops the socket, the server tears down the share, same as closing the tab.

use std::path::Path;

use serde::Deserialize;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio_tungstenite::tungstenite::Message;

const DEFAULT_SERVER: &str = "p2p.autos:8080";

/// Server -> client messages this client understands (mirrors uploader.rs's `Up`,
/// reversed). `pull.f` is unused (this client only ever offers one file, so
/// there's only one thing a pull could mean) but must still be present to match
/// the JSON shape.
#[derive(Deserialize)]
#[serde(tag = "t")]
enum Down {
    #[serde(rename = "ready")]
    Ready { id: String },
    #[serde(rename = "full")]
    Full,
    #[serde(rename = "pull")]
    Pull {
        r: u32,
        #[serde(rename = "f")]
        _file: String,
        s: u64,
        e: u64,
    },
    #[serde(rename = "cancel")]
    Cancel {
        #[serde(rename = "r")]
        _r: u32,
    },
}

pub(crate) async fn run(args: impl Iterator<Item = String>) {
    let (path, server) = parse_args(args);

    let meta = tokio::fs::metadata(&path)
        .await
        .unwrap_or_else(|e| panic!("can't read {}: {e}", path.display()));
    let name = Path::new(&path)
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| "download".into());
    let mut file = tokio::fs::File::open(&path)
        .await
        .unwrap_or_else(|e| panic!("can't open {}: {e}", path.display()));

    let ws_url = format!("{}://{}/ws", server.ws_scheme(), server.host);
    let (ws, _) = tokio_tungstenite::connect_async(&ws_url)
        .await
        .unwrap_or_else(|e| panic!("connect {ws_url}: {e}"));
    let (mut tx, mut rx) = futures_util::StreamExt::split(ws);

    // ponytail: no MIME sniffing — server already defaults empty/invalid
    // mime to application/octet-stream via sanitize_mime. Add sniffing
    // here (e.g. the `mime_guess` crate) if the download's Content-Type
    // needs to be meaningful.
    let offer = serde_json::json!({"t":"offer","name":name,"size":meta.len(),"mime":""}).to_string();
    futures_util::SinkExt::send(&mut tx, Message::Text(offer))
        .await
        .unwrap_or_else(|e| panic!("send offer: {e}"));

    // ponytail: pulls are handled strictly sequentially (read -> disk ->
    // send -> read next) instead of pipelining like the browser's
    // per-request promise chaining. Simpler, and fine for one downloader;
    // add concurrent reads per request if disk latency starves the
    // server's window under many simultaneous downloaders.
    while let Some(msg) = futures_util::StreamExt::next(&mut rx).await {
        let Ok(Message::Text(text)) = msg else { continue };
        let Ok(down) = serde_json::from_str::<Down>(&text) else { continue };
        match down {
            Down::Ready { id } => {
                println!("{}://{}/d/{id}", server.link_scheme(), server.host);
            }
            Down::Full => {
                eprintln!("server rejected the offer: too many shares already");
                break;
            }
            Down::Pull { r, s, e, .. } => {
                let reply = match read_range(&mut file, s, e).await {
                    Ok(data) => {
                        let mut frame = Vec::with_capacity(4 + data.len());
                        frame.extend_from_slice(&r.to_le_bytes());
                        frame.extend_from_slice(&data);
                        Message::Binary(frame)
                    }
                    Err(_) => Message::Text(serde_json::json!({"t":"err","r":r}).to_string()),
                };
                if futures_util::SinkExt::send(&mut tx, reply).await.is_err() {
                    break;
                }
            }
            Down::Cancel { .. } => {} // sequential client: nothing in flight to cancel
        }
    }
}

async fn read_range(file: &mut tokio::fs::File, start: u64, end: u64) -> std::io::Result<Vec<u8>> {
    file.seek(std::io::SeekFrom::Start(start)).await?;
    let mut buf = vec![0u8; (end - start) as usize];
    file.read_exact(&mut buf).await?;
    Ok(buf)
}

struct Server {
    host: String,
    secure: bool,
}

impl Server {
    fn ws_scheme(&self) -> &'static str {
        if self.secure {
            "wss"
        } else {
            "ws"
        }
    }

    fn link_scheme(&self) -> &'static str {
        if self.secure {
            "https"
        } else {
            "http"
        }
    }
}

fn parse_server(raw: &str) -> Server {
    let (secure, rest) = if let Some(host) = raw.strip_prefix("wss://") {
        (true, host)
    } else if let Some(host) = raw.strip_prefix("https://") {
        (true, host)
    } else if let Some(host) = raw.strip_prefix("ws://") {
        (false, host)
    } else if let Some(host) = raw.strip_prefix("http://") {
        (false, host)
    } else {
        (false, raw)
    };
    Server {
        host: rest.trim_end_matches('/').to_string(),
        secure,
    }
}

fn parse_args(mut args: impl Iterator<Item = String>) -> (std::path::PathBuf, Server) {
    let mut path: Option<std::path::PathBuf> = None;
    let mut server = DEFAULT_SERVER.to_string();

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--server" => {
                server = args.next().unwrap_or_else(|| panic!("--server needs a value"));
            }
            other if path.is_none() => path = Some(other.into()),
            other => panic!("unknown argument: {other} (expected a file path and/or --server)"),
        }
    }

    let path = path.unwrap_or_else(|| panic!("usage: p2p-autos share <file> [--server host[:port]]"));
    (path, parse_server(&server))
}
