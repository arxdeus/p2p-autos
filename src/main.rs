//! directshare — browser-held file relay, with two modes:
//!
//! `serve` (default, also invoked with no subcommand at all): the relay
//! server. The uploader's tab holds the bytes; the server holds only a
//! routing table. A download pulls chunks over the uploader's WebSocket
//! and streams them out as a plain HTTP body, so any client (curl, wget,
//! a browser, a video player) can use the link with no JS on the consumer
//! side. Tab closes -> socket closes -> registry entry gone -> 404.
//!
//! `share <file>`: a CLI client that offers one local file to a remote
//! directshare server (playing the browser tab's role) and prints the
//! download link. Link dies when the process does, same as a closed tab.
mod client;
mod download;
mod sanitize;
mod share;
mod uploader;

use std::{net::SocketAddr, sync::Arc, time::Duration};

use axum::{
    http::header,
    response::IntoResponse,
    routing::{any, get},
    Router,
};

use share::App;

/// Bytes per pull. One pull -> one WebSocket frame -> one body chunk.
const CHUNK: u64 = 1 << 20;
/// Largest inbound WebSocket frame: one CHUNK plus the 4-byte request header.
const MAX_FRAME: usize = CHUNK as usize + 64;
/// Ceiling on pulls in flight per download; a fresh download starts at 1
/// and grows toward this only when evidence (a starved read, see the
/// Relay stream in download.rs) shows the pipe needs more depth to stay
/// full. Caps RAM per transfer at MAX_WINDOW * CHUNK no matter how far it
/// actually grows — and see share::App::budget_chunks for how that ceiling
/// itself shrinks under load rather than ever rejecting a download.
const MAX_WINDOW: u32 = 4;
/// Default `--memory-budget`, in MiB (== CHUNKs). Sized to fit a small
/// 512 MiB box comfortably even at full tilt, with room to spare for the
/// OS, the binary itself, and (if used) a reverse proxy in front.
const DEFAULT_MEMORY_BUDGET_MIB: usize = 256;

struct Config {
    bind: SocketAddr,
    memory_budget_mib: usize,
}

impl Config {
    /// `--address <ip>` / `--port <n>` override the default bind address
    /// and port (0.0.0.0:8080); `BIND=host:port` is the older equivalent,
    /// used only when neither flag is given. `--memory-budget <mib>` sets
    /// the total download-buffer budget divided live across active
    /// downloads — see share::App::budget_chunks.
    fn parse(mut args: impl Iterator<Item = String>) -> Self {
        let mut address: Option<String> = None;
        let mut port: Option<u16> = None;
        let mut memory_budget_mib = DEFAULT_MEMORY_BUDGET_MIB;

        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--address" => {
                    address = Some(args.next().unwrap_or_else(|| panic!("--address needs a value")));
                }
                "--port" => {
                    let v = args.next().unwrap_or_else(|| panic!("--port needs a value"));
                    port = Some(v.parse().expect("--port must be a number"));
                }
                "--memory-budget" => {
                    let v = args
                        .next()
                        .unwrap_or_else(|| panic!("--memory-budget needs a value (MiB)"));
                    memory_budget_mib = v.parse().expect("--memory-budget must be a number");
                }
                other => panic!(
                    "unknown argument: {other} (expected --address, --port, or --memory-budget)"
                ),
            }
        }

        let bind = if address.is_some() || port.is_some() {
            let address = address.unwrap_or_else(|| "0.0.0.0".into());
            let port = port.unwrap_or(8080);
            format!("{address}:{port}")
        } else {
            std::env::var("BIND").unwrap_or_else(|_| "0.0.0.0:8080".into())
        }
        .parse()
        .expect("invalid address/port");

        Self { bind, memory_budget_mib }
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "directshare=info,tower_http=warn".into()),
        )
        .init();

    let mut args = std::env::args().skip(1).peekable();
    if args.peek().map(String::as_str) == Some("share") {
        args.next();
        return client::run(args).await;
    }
    if args.peek().map(String::as_str) == Some("serve") {
        args.next();
    }

    let config = Config::parse(args);
    let app = Arc::new(App::new(config.memory_budget_mib));

    let router = Router::new()
        .route("/", get(index))
        .route("/ws", any(uploader::ws_upgrade))
        .route("/d/{id}", get(download::download))
        .with_state(app);

    let bind = config.bind;
    let listener = tokio::net::TcpListener::bind(bind)
        .await
        .unwrap_or_else(|e| panic!("bind {bind}: {e}"));
    tracing::info!(
        "listening on http://{bind} (memory budget: {} MiB)",
        config.memory_budget_mib
    );

    axum::serve(listener, router.into_make_service())
        .with_graceful_shutdown(shutdown_signal())
        .await
        .expect("server error");
}

/// Resolves once ctrl_c fires — and only then, i.e. this never bounds
/// normal operation. On fire it arms a watchdog: a response stuck writing
/// to a peer that stopped reading could otherwise hang the graceful drain
/// (which begins the instant this function returns) forever, so force the
/// exit if draining in-flight connections takes too long.
async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
    tracing::info!("shutdown signal received; draining for up to 30s");
    tokio::spawn(async {
        tokio::time::sleep(Duration::from_secs(30)).await;
        tracing::warn!("graceful shutdown timed out after 30s; forcing exit");
        std::process::exit(0);
    });
}

async fn index() -> impl IntoResponse {
    (
        [
            (header::CONTENT_TYPE, "text/html; charset=utf-8"),
            (header::CACHE_CONTROL, "no-store"),
        ],
        include_str!("index.html"),
    )
}
