//! The uploader's side of the protocol: one WebSocket connection per tab,
//! driven by a single-task actor with no locks. It owns every share the
//! tab has registered and every download currently pulling bytes from it.
//!
//! `host()` is deliberately thin — it just dispatches each of the three
//! things that can happen (a frame from the tab, a command from an HTTP
//! handler, the periodic heartbeat) to a named `Actor` method below.

use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    response::Response,
};
use bytes::Bytes;
use futures_util::{stream::SplitSink, SinkExt, StreamExt};
use serde::Deserialize;
use tokio::sync::mpsc;

use crate::{
    sanitize::{sanitize_mime, sanitize_name},
    share::{insert_share, App, Share},
    CHUNK, MAX_FRAME, MAX_WINDOW,
};

/// Shares one tab may register. Bounds a misbehaving client.
const MAX_SHARES_PER_CONN: usize = 64;
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
/// (MAX_WINDOW * CHUNK per download) against one link being hit with many
/// connections that never drain their body.
const MAX_REQS_PER_CONN: usize = 32;

/// Control messages from HTTP handlers to the socket task that owns a file.
pub(crate) enum Cmd {
    /// Start streaming `[start, end)` of share `file` into `sink`.
    Open {
        req: u32,
        file: Arc<str>,
        start: u64,
        end: u64,
        sink: mpsc::Sender<Bytes>,
    },
    /// One chunk left the body stream. `bool` is whether the stream had to
    /// wait for it (a starved read) — the signal `pump` uses to grow the
    /// window for this request.
    Credit(u32, bool),
    /// The HTTP body was dropped (client disconnected, or the transfer ended).
    Close(u32),
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
    /// Never exceeds `window`, which is also the sink capacity's ceiling.
    outstanding: u32,
    /// Pulls issued whose frame has not yet arrived.
    pending: u32,
    /// Current pipeline depth for this request. Starts at 1, grows toward
    /// MAX_WINDOW on demand — see MAX_WINDOW's doc.
    window: u32,
    /// Last time a pull was answered or a chunk was consumed downstream.
    /// Swept on the ping tick; see REQ_IDLE_TIMEOUT.
    last_progress: Instant,
}

pub(crate) async fn ws_upgrade(State(app): State<Arc<App>>, ws: WebSocketUpgrade) -> Response {
    ws.max_message_size(MAX_FRAME)
        .max_write_buffer_size(MAX_FRAME * (MAX_WINDOW as usize + 2))
        .on_upgrade(move |socket| host(socket, app))
}

/// Drives one uploader tab for its whole lifetime: dispatch each event to
/// `Actor`, stop on the first `Err`, then drop every share and in-flight
/// download that belonged to this tab.
async fn host(socket: WebSocket, app: Arc<App>) {
    let (tx, mut rx) = socket.split();
    let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel::<Cmd>();
    let mut actor = Actor::new(tx);

    let mut ping = tokio::time::interval(PING_EVERY);
    ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ping.tick().await; // interval fires immediately; skip that one

    loop {
        let alive = tokio::select! {
            incoming = rx.next() => match incoming {
                Some(Ok(msg)) => actor.on_message(msg, &app, &cmd_tx).await,
                _ => Err(()),
            },
            Some(cmd) = cmd_rx.recv() => actor.on_cmd(cmd).await,
            _ = ping.tick() => actor.on_ping_tick().await,
        };
        if alive.is_err() {
            break;
        }
    }

    actor.shutdown(&app);
    // actor.reqs drops here: every sink closes, every in-flight download ends.
}

/// Owns one uploader tab's live state: its WebSocket sink, the shares it
/// has registered, and every download currently pulling from them. Single
/// task, no locks — `host` is the only caller of every method here.
struct Actor {
    tx: SplitSink<WebSocket, Message>,
    ids: Vec<String>,
    reqs: HashMap<u32, Req>,
    last_seen: Instant,
}

impl Actor {
    fn new(tx: SplitSink<WebSocket, Message>) -> Self {
        Self {
            tx,
            ids: Vec::new(),
            reqs: HashMap::new(),
            last_seen: Instant::now(),
        }
    }

    /// One frame arrived from the tab. `Err` means stop serving this connection.
    async fn on_message(
        &mut self,
        msg: Message,
        app: &App,
        cmd_tx: &mpsc::UnboundedSender<Cmd>,
    ) -> Result<(), ()> {
        self.last_seen = Instant::now();
        match msg {
            Message::Binary(buf) => {
                self.on_pull_reply(buf);
                Ok(())
            }
            Message::Text(text) => self.on_text(&text, app, cmd_tx).await,
            Message::Close(_) => Err(()),
            Message::Ping(_) | Message::Pong(_) => Ok(()),
        }
    }

    /// `[u32 LE request id][chunk bytes]`, answering an earlier pull. Never
    /// ends the connection — a bad frame just gets ignored or cancels the
    /// one request it names.
    fn on_pull_reply(&mut self, buf: Bytes) {
        if buf.len() < 4 {
            return;
        }
        let req = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]);
        let data = buf.slice(4..);
        let Some(st) = self.reqs.get_mut(&req) else {
            return; // cancelled
        };
        if st.pending == 0 {
            // Unsolicited frame: the tab is not following the protocol.
            self.reqs.remove(&req);
            return;
        }
        st.pending -= 1;
        st.last_progress = Instant::now();
        // Capacity is guaranteed: outstanding <= window <= MAX_WINDOW = sink capacity.
        if st.sink.try_send(data).is_err() {
            self.reqs.remove(&req); // receiver gone; Close will follow
            return;
        }
        if st.next >= st.end && st.pending == 0 {
            self.reqs.remove(&req); // dropping the sink ends the body
        }
    }

    async fn on_text(
        &mut self,
        text: &str,
        app: &App,
        cmd_tx: &mpsc::UnboundedSender<Cmd>,
    ) -> Result<(), ()> {
        match serde_json::from_str::<Up>(text) {
            Ok(Up::Offer { name, size, mime }) => {
                self.on_offer(name, size, mime, app, cmd_tx).await
            }
            Ok(Up::Err { r }) => {
                self.reqs.remove(&r); // truncates the body; client sees a short read
                Ok(())
            }
            Err(_) => Ok(()), // not JSON we understand; ignore
        }
    }

    async fn on_offer(
        &mut self,
        name: String,
        size: u64,
        mime: String,
        app: &App,
        cmd_tx: &mpsc::UnboundedSender<Cmd>,
    ) -> Result<(), ()> {
        if self.ids.len() >= MAX_SHARES_PER_CONN {
            // Always answer an offer: the tab pairs replies with files by
            // arrival order.
            return send_msg(&mut self.tx, Message::Text(r#"{"t":"full"}"#.into())).await;
        }
        let id = insert_share(
            app,
            Share {
                name: sanitize_name(&name).into(),
                mime: sanitize_mime(&mime).into(),
                size,
                cmd: cmd_tx.clone(),
            },
        );
        self.ids.push(id.clone());
        tracing::info!(%id, size, "share opened");
        let msg = format!(r#"{{"t":"ready","id":"{id}"}}"#);
        send_msg(&mut self.tx, Message::Text(msg.into())).await
    }

    /// A download opened, made progress, or ended.
    async fn on_cmd(&mut self, cmd: Cmd) -> Result<(), ()> {
        match cmd {
            Cmd::Open {
                req,
                file,
                start,
                end,
                sink,
            } => {
                if self.reqs.len() >= MAX_REQS_PER_CONN {
                    // Drop `sink` without inserting: the receiver sees the
                    // channel close and the body ends immediately, same as
                    // any other cancellation path below.
                    return Ok(());
                }
                self.reqs.insert(
                    req,
                    Req {
                        file,
                        sink,
                        next: start,
                        end,
                        outstanding: 0,
                        pending: 0,
                        window: 1,
                        last_progress: Instant::now(),
                    },
                );
                pump(&mut self.tx, &mut self.reqs, req).await
            }
            Cmd::Credit(req, starved) => {
                if let Some(st) = self.reqs.get_mut(&req) {
                    st.outstanding = st.outstanding.saturating_sub(1);
                    st.last_progress = Instant::now();
                    if starved {
                        // The pipe ran dry once; one more chunk of depth
                        // earns its keep. Never shrinks back — a
                        // download's needs don't usually change
                        // mid-transfer, and this keeps it simple.
                        st.window = (st.window + 1).min(MAX_WINDOW);
                    }
                }
                pump(&mut self.tx, &mut self.reqs, req).await
            }
            Cmd::Close(req) => {
                if self.reqs.remove(&req).is_none() {
                    return Ok(());
                }
                // Tell the tab to stop reading; unmatched frames are dropped above.
                let msg = format!(r#"{{"t":"cancel","r":{req}}}"#);
                send_msg(&mut self.tx, Message::Text(msg.into())).await
            }
        }
    }

    /// The periodic heartbeat: declare the connection dead if it has gone
    /// silent, cancel any download that has stopped making progress, then ping.
    async fn on_ping_tick(&mut self) -> Result<(), ()> {
        if self.last_seen.elapsed() > CONN_IDLE_TIMEOUT {
            return Err(()); // no traffic at all in 3+ ping intervals: presumed dead
        }
        let stale: Vec<u32> = self
            .reqs
            .iter()
            .filter(|(_, st)| st.last_progress.elapsed() > REQ_IDLE_TIMEOUT)
            .map(|(&req, _)| req)
            .collect();
        for req in stale {
            self.reqs.remove(&req);
            let msg = format!(r#"{{"t":"cancel","r":{req}}}"#);
            send_msg(&mut self.tx, Message::Text(msg.into())).await?;
        }
        send_msg(&mut self.tx, Message::Ping(Bytes::new())).await
    }

    /// Every share this tab registered is now gone; every in-flight
    /// download against them ends when `self.reqs` drops with `self`.
    fn shutdown(&self, app: &App) {
        for id in &self.ids {
            app.shares.remove(id);
            tracing::info!(%id, "share closed");
        }
    }
}

/// Send one WebSocket frame with a hard deadline. `select!` in `host` only
/// re-polls its other branches once the current branch's block fully
/// resolves, so an unbounded `.send().await` here would freeze every other
/// download sharing this tab — and the keepalive — if the peer stops reading.
async fn send_msg(tx: &mut SplitSink<WebSocket, Message>, msg: Message) -> Result<(), ()> {
    match tokio::time::timeout(SEND_TIMEOUT, tx.send(msg)).await {
        Ok(Ok(())) => Ok(()),
        _ => Err(()),
    }
}

/// Issue pulls until this request's current window is full or the range is done.
async fn pump(
    tx: &mut SplitSink<WebSocket, Message>,
    reqs: &mut HashMap<u32, Req>,
    req: u32,
) -> Result<(), ()> {
    let Some(st) = reqs.get_mut(&req) else {
        return Ok(());
    };
    while st.outstanding < st.window && st.next < st.end {
        let start = st.next;
        let end = start.saturating_add(CHUNK).min(st.end);
        st.next = end;
        st.outstanding += 1;
        st.pending += 1;
        let msg = format!(
            r#"{{"t":"pull","r":{req},"f":"{}","s":{start},"e":{end}}}"#,
            st.file
        );
        send_msg(tx, Message::Text(msg.into())).await?;
    }
    Ok(())
}
