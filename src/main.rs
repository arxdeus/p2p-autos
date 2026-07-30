//! directshare — browser-held file relay.
//!
//! The uploader's tab holds the bytes. The server holds only a routing table.
//! A download pulls chunks over the uploader's WebSocket and streams them out
//! as a plain HTTP body, so any client (curl, wget, a browser, a video player)
//! can use the link with no JS on the consumer side.
//!
//! Tab closes -> socket closes -> registry entry gone -> 404.

use std::{
    collections::HashMap,
    convert::Infallible,
    net::SocketAddr,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::{Duration, Instant},
};

use axum::{
    body::Body,
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Path, State,
    },
    http::{header, HeaderMap, HeaderValue, Method, StatusCode},
    response::{IntoResponse, Response},
    routing::{any, get},
    Router,
};
use bytes::Bytes;
use dashmap::DashMap;
use futures_util::{SinkExt, Stream, StreamExt};
use rand::Rng;
use serde::Deserialize;
use tokio::sync::mpsc;

/// Bytes per pull. One pull -> one WebSocket frame -> one body chunk.
const CHUNK: u64 = 1 << 20;
/// Pulls in flight per download. Caps RAM per transfer at WINDOW * CHUNK.
const WINDOW: u32 = 4;
/// Largest inbound WebSocket frame: one CHUNK plus the 4-byte request header.
const MAX_FRAME: usize = CHUNK as usize + 64;
/// Shares one tab may register. Bounds a misbehaving client.
const MAX_SHARES_PER_CONN: usize = 64;
const MAX_NAME: usize = 255;
const PING_EVERY: Duration = Duration::from_secs(25);
/// A single outbound frame that takes longer than this stalls the whole
/// per-tab actor loop (`select!` doesn't poll other branches while one is
/// mid-await), so it doubles as that connection's death sentence.
const SEND_TIMEOUT: Duration = Duration::from_secs(10);
/// A download with zero forward progress (no pull answered, no chunk
/// consumed downstream) for this long is presumed abandoned and cancelled.
/// Generous on purpose — progress is tracked per CHUNK, not per byte, so
/// this must never fire on a merely slow connection, only a truly stuck
/// one. Checked on the existing ping tick; no separate timer.
const REQ_IDLE_TIMEOUT: Duration = Duration::from_secs(120);
/// A connection with no inbound traffic at all (not even an automatic
/// Pong) for this long is presumed dead. Unlike REQ_IDLE_TIMEOUT this is
/// not bandwidth-sensitive — pings/pongs are a few bytes — so it can be tight.
const CONN_IDLE_TIMEOUT: Duration = Duration::from_secs(90);
/// Concurrent downloads one uploader tab will serve at once. Bounds memory
/// (WINDOW * CHUNK per download) against one link being hit with many
/// connections that never drain their body.
const MAX_REQS_PER_CONN: usize = 32;
/// Unambiguous base-56: no 0/O, 1/l/I.
const ALPHABET: &[u8] = b"abcdefghijkmnopqrstuvwxyzABCDEFGHJKLMNPQRSTUVWXYZ23456789";
const ID_LEN: usize = 8;

// ---------------------------------------------------------------- state

/// Control messages from HTTP handlers to the socket task that owns a file.
enum Cmd {
    /// Start streaming `[start, end)` of share `file` into `sink`.
    Open {
        req: u32,
        file: Arc<str>,
        start: u64,
        end: u64,
        sink: mpsc::Sender<Bytes>,
    },
    /// One chunk left the body stream; the window has room for another pull.
    Credit(u32),
    /// The HTTP body was dropped (client disconnected, or the transfer ended).
    Close(u32),
}

#[derive(Clone)]
struct Share {
    name: Arc<str>,
    mime: Arc<str>,
    size: u64,
    cmd: mpsc::UnboundedSender<Cmd>,
}

struct App {
    shares: DashMap<String, Share>,
}

// ---------------------------------------------------------------- main

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
        .route("/ws", any(ws_upgrade))
        .route("/d/{id}", get(download))
        .with_state(app);

    let bind: SocketAddr = std::env::var("BIND")
        .unwrap_or_else(|_| "0.0.0.0:8080".into())
        .parse()
        .expect("BIND must be host:port");

    let listener = tokio::net::TcpListener::bind(bind)
        .await
        .unwrap_or_else(|e| panic!("bind {bind}: {e}"));
    tracing::info!("listening on http://{bind}");

    axum::serve(
        listener,
        router.into_make_service_with_connect_info::<SocketAddr>(),
    )
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

// ---------------------------------------------------------------- uploader socket

async fn ws_upgrade(State(app): State<Arc<App>>, ws: WebSocketUpgrade) -> Response {
    ws.max_message_size(MAX_FRAME)
        .max_write_buffer_size(MAX_FRAME * (WINDOW as usize + 2))
        .on_upgrade(move |socket| host(socket, app))
}

#[derive(Deserialize)]
#[serde(tag = "t")]
enum Up {
    /// Register a file this tab is willing to serve.
    #[serde(rename = "offer")]
    Offer {
        name: String,
        size: u64,
        #[serde(default)]
        mime: String,
    },
    /// The tab could not read the file (moved, permissions, eject).
    #[serde(rename = "err")]
    Err { r: u32 },
}

/// Per-download bookkeeping, owned by the socket task.
struct Req {
    /// Which of the tab's shares to read from.
    file: Arc<str>,
    sink: mpsc::Sender<Bytes>,
    /// Next byte offset to ask the tab for.
    next: u64,
    /// Exclusive end of the requested range.
    end: u64,
    /// Pulls issued whose chunk has not yet been consumed downstream.
    /// Never exceeds WINDOW, which is also the sink capacity.
    outstanding: u32,
    /// Pulls issued whose frame has not yet arrived.
    pending: u32,
    /// Last time a pull was answered or a chunk was consumed downstream.
    /// Swept on the ping tick; see REQ_IDLE_TIMEOUT.
    last_progress: Instant,
}

/// Owns one uploader tab: its registered shares and every download pulling
/// from them. Single task, no locks — the socket is only ever touched here.
async fn host(socket: WebSocket, app: Arc<App>) {
    let (mut tx, mut rx) = socket.split();
    let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel::<Cmd>();

    let mut ids: Vec<String> = Vec::new();
    let mut reqs: HashMap<u32, Req> = HashMap::new();
    let mut last_seen = Instant::now();

    let mut ping = tokio::time::interval(PING_EVERY);
    ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ping.tick().await; // interval fires immediately; skip that one

    loop {
        tokio::select! {
            incoming = rx.next() => {
                let Some(Ok(msg)) = incoming else { break };
                last_seen = Instant::now();
                match msg {
                    Message::Binary(buf) => {
                        // [u32 LE request id][chunk bytes]
                        if buf.len() < 4 {
                            continue;
                        }
                        let req = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]);
                        let data = buf.slice(4..);
                        let Some(st) = reqs.get_mut(&req) else { continue }; // cancelled
                        if st.pending == 0 {
                            // Unsolicited frame: the tab is not following the protocol.
                            reqs.remove(&req);
                            continue;
                        }
                        st.pending -= 1;
                        st.last_progress = Instant::now();
                        // Capacity is guaranteed: outstanding <= WINDOW = sink capacity.
                        if st.sink.try_send(data).is_err() {
                            reqs.remove(&req); // receiver gone; Close will follow
                            continue;
                        }
                        if st.next >= st.end && st.pending == 0 {
                            reqs.remove(&req); // dropping the sink ends the body
                        }
                    }
                    Message::Text(text) => {
                        match serde_json::from_str::<Up>(&text) {
                            Ok(Up::Offer { name, size, mime }) => {
                                if ids.len() >= MAX_SHARES_PER_CONN {
                                    // Always answer an offer: the tab pairs
                                    // replies with files by arrival order.
                                    if send_msg(&mut tx, Message::Text(r#"{"t":"full"}"#.into())).await.is_err() {
                                        break;
                                    }
                                    continue;
                                }
                                let name = sanitize_name(&name);
                                let mime = sanitize_mime(&mime);
                                let id = insert_share(&app, Share {
                                    name: name.into(),
                                    mime: mime.into(),
                                    size,
                                    cmd: cmd_tx.clone(),
                                });
                                ids.push(id.clone());
                                tracing::info!(%id, size, "share opened");
                                let msg = format!(r#"{{"t":"ready","id":"{id}"}}"#);
                                if send_msg(&mut tx, Message::Text(msg.into())).await.is_err() {
                                    break;
                                }
                            }
                            Ok(Up::Err { r }) => {
                                reqs.remove(&r); // truncates the body; client sees a short read
                            }
                            Err(_) => continue,
                        }
                    }
                    Message::Close(_) => break,
                    Message::Ping(_) | Message::Pong(_) => {}
                }
            }

            Some(cmd) = cmd_rx.recv() => {
                match cmd {
                    Cmd::Open { req, file, start, end, sink } => {
                        if reqs.len() >= MAX_REQS_PER_CONN {
                            // Drop `sink` without inserting: the receiver sees
                            // the channel close and the body ends immediately,
                            // same as any other cancellation path below.
                            continue;
                        }
                        reqs.insert(req, Req {
                            file, sink, next: start, end,
                            outstanding: 0, pending: 0,
                            last_progress: Instant::now(),
                        });
                        if pump(&mut tx, &mut reqs, req).await.is_err() {
                            break;
                        }
                    }
                    Cmd::Credit(req) => {
                        if let Some(st) = reqs.get_mut(&req) {
                            st.outstanding = st.outstanding.saturating_sub(1);
                            st.last_progress = Instant::now();
                        }
                        if pump(&mut tx, &mut reqs, req).await.is_err() {
                            break;
                        }
                    }
                    Cmd::Close(req) => {
                        if reqs.remove(&req).is_some() {
                            // Tell the tab to stop reading; unmatched frames are dropped above.
                            let msg = format!(r#"{{"t":"cancel","r":{req}}}"#);
                            if send_msg(&mut tx, Message::Text(msg.into())).await.is_err() {
                                break;
                            }
                        }
                    }
                }
            }

            _ = ping.tick() => {
                if last_seen.elapsed() > CONN_IDLE_TIMEOUT {
                    break; // no traffic at all in 3+ ping intervals: presumed dead
                }
                let stale: Vec<u32> = reqs.iter()
                    .filter(|(_, st)| st.last_progress.elapsed() > REQ_IDLE_TIMEOUT)
                    .map(|(&req, _)| req)
                    .collect();
                let mut peer_gone = false;
                for req in stale {
                    reqs.remove(&req);
                    let msg = format!(r#"{{"t":"cancel","r":{req}}}"#);
                    if send_msg(&mut tx, Message::Text(msg.into())).await.is_err() {
                        peer_gone = true;
                        break;
                    }
                }
                if peer_gone || send_msg(&mut tx, Message::Ping(Bytes::new())).await.is_err() {
                    break;
                }
            }
        }

    }

    for id in ids {
        app.shares.remove(&id);
        tracing::info!(%id, "share closed");
    }
    // reqs drops here: every sink closes, every in-flight download ends.
}

/// Send one WebSocket frame with a hard deadline. `select!` in `host` only
/// re-polls its other branches once the current branch's block fully
/// resolves, so an unbounded `.send().await` here would freeze every other
/// download sharing this tab — and the keepalive — if the peer stops reading.
async fn send_msg<S>(tx: &mut S, msg: Message) -> Result<(), ()>
where
    S: SinkExt<Message> + Unpin,
{
    match tokio::time::timeout(SEND_TIMEOUT, tx.send(msg)).await {
        Ok(Ok(())) => Ok(()),
        _ => Err(()),
    }
}

/// Issue pulls until the flow-control window is full or the range is done.
async fn pump<S>(tx: &mut S, reqs: &mut HashMap<u32, Req>, req: u32) -> Result<(), ()>
where
    S: SinkExt<Message> + Unpin,
{
    let Some(st) = reqs.get_mut(&req) else {
        return Ok(());
    };
    while st.outstanding < WINDOW && st.next < st.end {
        let start = st.next;
        let end = start.saturating_add(CHUNK).min(st.end);
        st.next = end;
        st.outstanding += 1;
        st.pending += 1;
        let msg = format!(
            r#"{{"t":"pull","r":{req},"f":"{}","s":{start},"e":{end}}}"#,
            st.file
        );
        if send_msg(tx, Message::Text(msg.into())).await.is_err() {
            return Err(());
        }
    }
    Ok(())
}

fn insert_share(app: &App, share: Share) -> String {
    loop {
        let id = gen_id();
        if let dashmap::mapref::entry::Entry::Vacant(e) = app.shares.entry(id.clone()) {
            e.insert(share);
            return id;
        }
    }
}

fn gen_id() -> String {
    let mut rng = rand::rng();
    (0..ID_LEN)
        .map(|_| ALPHABET[rng.random_range(0..ALPHABET.len())] as char)
        .collect()
}

// ---------------------------------------------------------------- download

/// Body backed by the uploader's socket. Yielding a chunk returns a credit so
/// the socket task issues the next pull; dropping it cancels the transfer.
struct Relay {
    rx: mpsc::Receiver<Bytes>,
    cmd: mpsc::UnboundedSender<Cmd>,
    req: u32,
}

impl Stream for Relay {
    type Item = Result<Bytes, Infallible>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.rx.poll_recv(cx) {
            Poll::Ready(Some(b)) => {
                let _ = self.cmd.send(Cmd::Credit(self.req));
                Poll::Ready(Some(Ok(b)))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Drop for Relay {
    fn drop(&mut self) {
        let _ = self.cmd.send(Cmd::Close(self.req));
    }
}

async fn download(
    State(app): State<Arc<App>>,
    Path(id): Path<String>,
    method: Method,
    headers: HeaderMap,
) -> Response {
    let Some(share) = app.shares.get(&id).map(|s| s.clone()) else {
        return (StatusCode::NOT_FOUND, "410 gone: the sender closed the tab\n").into_response();
    };

    let range = match parse_range(headers.get(header::RANGE), share.size) {
        Ok(r) => r,
        Err(()) => {
            return (
                StatusCode::RANGE_NOT_SATISFIABLE,
                [(header::CONTENT_RANGE, format!("bytes */{}", share.size))],
            )
                .into_response()
        }
    };
    let (start, end) = range.unwrap_or((0, share.size));
    let partial = range.is_some();
    let len = end - start;

    let mut res = Response::builder()
        .status(if partial {
            StatusCode::PARTIAL_CONTENT
        } else {
            StatusCode::OK
        })
        .header(header::CONTENT_TYPE, &*share.mime)
        .header(header::CONTENT_LENGTH, len)
        .header(header::ACCEPT_RANGES, "bytes")
        .header(header::CACHE_CONTROL, "no-store")
        .header(header::CONTENT_DISPOSITION, disposition(&share.name));
    if partial {
        res = res.header(
            header::CONTENT_RANGE,
            format!("bytes {start}-{}/{}", end - 1, share.size),
        );
    }

    // HEAD and empty ranges need no bytes from the tab.
    if method == Method::HEAD || len == 0 {
        return res.body(Body::empty()).unwrap().into_response();
    }

    let req = next_req_id();
    let (sink, rx) = mpsc::channel::<Bytes>(WINDOW as usize);
    if share
        .cmd
        .send(Cmd::Open {
            req,
            file: id.as_str().into(),
            start,
            end,
            sink,
        })
        .is_err()
    {
        return (StatusCode::NOT_FOUND, "410 gone: the sender closed the tab\n").into_response();
    }

    let body = Body::from_stream(Relay {
        rx,
        cmd: share.cmd.clone(),
        req,
    });
    res.body(body).unwrap().into_response()
}

fn next_req_id() -> u32 {
    use std::sync::atomic::{AtomicU32, Ordering};
    static N: AtomicU32 = AtomicU32::new(1);
    N.fetch_add(1, Ordering::Relaxed)
}

/// `None` = no (or ignorable) Range header, serve the whole file.
/// `Err` = the header was present, well-formed, and unsatisfiable -> 416.
#[allow(clippy::type_complexity)]
fn parse_range(h: Option<&HeaderValue>, size: u64) -> Result<Option<(u64, u64)>, ()> {
    let Some(h) = h.and_then(|v| v.to_str().ok()) else {
        return Ok(None);
    };
    let Some(spec) = h.trim().strip_prefix("bytes=") else {
        return Ok(None); // other units: a server may ignore the header
    };
    if spec.contains(',') {
        return Ok(None); // multipart ranges: serving the whole file is allowed
    }
    let (a, b) = spec.split_once('-').ok_or(())?;
    let (a, b) = (a.trim(), b.trim());

    let (start, end) = match (a.is_empty(), b.is_empty()) {
        // bytes=-N — the final N bytes
        (true, false) => {
            let n: u64 = b.parse().map_err(|_| ())?;
            if n == 0 {
                return Err(());
            }
            (size.saturating_sub(n), size)
        }
        // bytes=N-
        (false, true) => (a.parse().map_err(|_| ())?, size),
        // bytes=N-M (inclusive)
        (false, false) => {
            let s: u64 = a.parse().map_err(|_| ())?;
            let e: u64 = b.parse().map_err(|_| ())?;
            if e < s {
                return Err(());
            }
            (s, e.saturating_add(1).min(size))
        }
        (true, true) => return Err(()),
    };

    if start >= size {
        return Err(()); // unsatisfiable; also covers size == 0
    }
    Ok(Some((start, end.min(size))))
}

fn disposition(name: &str) -> String {
    // Two forms, per RFC 6266: a quoted ASCII fallback and the UTF-8 original.
    let ascii: String = name
        .chars()
        .map(|c| {
            if c.is_ascii() && !c.is_ascii_control() && c != '"' && c != '\\' {
                c
            } else {
                '_'
            }
        })
        .collect();
    format!(
        "attachment; filename=\"{}\"; filename*=UTF-8''{}",
        ascii,
        pct(name)
    )
}

fn pct(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// Strip path separators and control characters — the name reaches a header
/// and a client's filesystem, so it is a trust boundary.
fn sanitize_name(raw: &str) -> String {
    let base = raw.rsplit(['/', '\\']).next().unwrap_or("");
    let cleaned: String = base
        .chars()
        .filter(|c| !c.is_control())
        .take(MAX_NAME)
        .collect();
    match cleaned.trim().trim_matches('.') {
        "" => "download".into(),
        s => s.into(),
    }
}

/// Only a bare `type/subtype` survives; anything else becomes a safe default.
/// A reflected `Content-Type` is an XSS vector, so no parameters are kept.
fn sanitize_mime(raw: &str) -> String {
    let ok = |c: char| c.is_ascii_alphanumeric() || matches!(c, '!' | '#'..='\'' | '*' | '+' | '-' | '.' | '^' | '_' | '`' | '|' | '~');
    let mut parts = raw.trim().split('/');
    match (parts.next(), parts.next(), parts.next()) {
        (Some(t), Some(s), None)
            if !t.is_empty()
                && !s.is_empty()
                && t.len() + s.len() < 128
                && t.chars().all(ok)
                && s.chars().all(ok) =>
        {
            format!("{}/{}", t.to_ascii_lowercase(), s.to_ascii_lowercase())
        }
        _ => "application/octet-stream".into(),
    }
}

// ---------------------------------------------------------------- checks

#[cfg(test)]
mod tests {
    use super::*;

    fn hv(s: &str) -> HeaderValue {
        HeaderValue::from_str(s).unwrap()
    }

    #[test]
    fn ranges() {
        assert_eq!(parse_range(None, 100), Ok(None));
        assert_eq!(parse_range(Some(&hv("bytes=0-9")), 100), Ok(Some((0, 10))));
        assert_eq!(parse_range(Some(&hv("bytes=10-")), 100), Ok(Some((10, 100))));
        assert_eq!(parse_range(Some(&hv("bytes=-10")), 100), Ok(Some((90, 100))));
        // clamped to size, not an error
        assert_eq!(parse_range(Some(&hv("bytes=0-999")), 100), Ok(Some((0, 100))));
        assert_eq!(parse_range(Some(&hv("bytes=-999")), 100), Ok(Some((0, 100))));
        // unsatisfiable
        assert_eq!(parse_range(Some(&hv("bytes=100-")), 100), Err(()));
        assert_eq!(parse_range(Some(&hv("bytes=9-2")), 100), Err(()));
        assert_eq!(parse_range(Some(&hv("bytes=0-")), 0), Err(()));
        assert_eq!(parse_range(Some(&hv("bytes=x-y")), 100), Err(()));
        // ignorable
        assert_eq!(parse_range(Some(&hv("bytes=0-1,5-6")), 100), Ok(None));
        assert_eq!(parse_range(Some(&hv("items=0-1")), 100), Ok(None));
    }

    #[test]
    fn names() {
        assert_eq!(sanitize_name("../../etc/passwd"), "passwd");
        assert_eq!(sanitize_name("C:\\Users\\a\\b.txt"), "b.txt");
        assert_eq!(sanitize_name("ok\r\nX-Evil: 1"), "okX-Evil: 1");
        assert_eq!(sanitize_name("   "), "download");
        assert_eq!(sanitize_name(".."), "download");
        assert_eq!(sanitize_name("hé.txt"), "hé.txt");
    }

    #[test]
    fn dispositions_never_break_the_header() {
        let d = disposition("a\"b\\c é.bin");
        assert!(!d.contains('\\') || d.contains("%5C"));
        assert_eq!(d.matches('"').count(), 2);
        assert!(d.contains("filename*=UTF-8''a%22b%5Cc%20%C3%A9.bin"));
    }

    #[test]
    fn mimes() {
        assert_eq!(sanitize_mime("video/MP4"), "video/mp4");
        assert_eq!(sanitize_mime(""), "application/octet-stream");
        assert_eq!(
            sanitize_mime("text/html; charset=x"),
            "application/octet-stream"
        );
        assert_eq!(sanitize_mime("a/b/c"), "application/octet-stream");
        assert_eq!(sanitize_mime("te xt/plain"), "application/octet-stream");
    }

    #[test]
    fn ids_are_unambiguous() {
        let id = gen_id();
        assert_eq!(id.len(), ID_LEN);
        assert!(!id.contains(['0', 'O', '1', 'l', 'I']));
    }
}
