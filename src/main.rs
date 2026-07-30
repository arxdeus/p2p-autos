//! directshare — browser-held file relay.
//!
//! The uploader's tab holds the bytes. The server holds only a routing table.
//! A download pulls chunks over the uploader's WebSocket and streams them out
//! as a plain HTTP body, so any client (curl, wget, a browser, a video player)
//! can use the link with no JS on the consumer side.
//!
//! Tab closes -> socket closes -> registry entry gone -> 404.
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
use dashmap::DashMap;

use share::App;

/// Bytes per pull. One pull -> one WebSocket frame -> one body chunk.
const CHUNK: u64 = 1 << 20;
/// Largest inbound WebSocket frame: one CHUNK plus the 4-byte request header.
const MAX_FRAME: usize = CHUNK as usize + 64;
/// Ceiling on pulls in flight per download; a fresh download starts at 1
/// and grows toward this only when evidence (a starved read, see the
/// Relay stream in download.rs) shows the pipe needs more depth to stay
/// full. Caps RAM per transfer at MAX_WINDOW * CHUNK no matter how far it
/// actually grows.
const MAX_WINDOW: u32 = 4;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "directshare=info,tower_http=warn".into()),
        )
        .init();

    let app = Arc::new(App {
        shares: DashMap::new(),
    });

    let router = Router::new()
        .route("/", get(index))
        .route("/ws", any(uploader::ws_upgrade))
        .route("/d/{id}", get(download::download))
        .with_state(app);

    let bind: SocketAddr = std::env::var("BIND")
        .unwrap_or_else(|_| "0.0.0.0:8080".into())
        .parse()
        .expect("BIND must be host:port");

    let listener = tokio::net::TcpListener::bind(bind)
        .await
        .unwrap_or_else(|e| panic!("bind {bind}: {e}"));
    tracing::info!("listening on http://{bind}");

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
