//! The consumer's side of the protocol: a plain HTTP GET, streamed straight
//! from the uploader's tab, with no JS or WebSocket required on this end.

use std::{
    convert::Infallible,
    pin::Pin,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
    task::{Context, Poll},
};

use axum::{
    body::Body,
    extract::{Path, State},
    http::{header, HeaderMap, HeaderValue, Method, StatusCode},
    response::{IntoResponse, Response},
};
use bytes::Bytes;
use futures_util::Stream;
use tokio::sync::mpsc;

use crate::{share::App, uploader::Cmd, MAX_WINDOW};

pub(crate) async fn download(
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
    let (sink, rx) = mpsc::channel::<Bytes>(MAX_WINDOW as usize);
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
        starved: false,
    });
    res.body(body).unwrap().into_response()
}

fn next_req_id() -> u32 {
    static N: AtomicU32 = AtomicU32::new(1);
    N.fetch_add(1, Ordering::Relaxed)
}

/// Body backed by the uploader's socket. Yielding a chunk returns a credit so
/// the socket task issues the next pull; dropping it cancels the transfer.
struct Relay {
    rx: mpsc::Receiver<Bytes>,
    cmd: mpsc::UnboundedSender<Cmd>,
    req: u32,
    /// Set when hyper wanted the next chunk before one was buffered — the
    /// window was too shallow to keep this download fed. Reported on the
    /// next yield so the uploader's actor can grow it, then cleared.
    starved: bool,
}

impl Stream for Relay {
    type Item = Result<Bytes, Infallible>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.rx.poll_recv(cx) {
            Poll::Ready(Some(b)) => {
                let starved = std::mem::take(&mut self.starved);
                let _ = self.cmd.send(Cmd::Credit(self.req, starved));
                Poll::Ready(Some(Ok(b)))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => {
                self.starved = true;
                Poll::Pending
            }
        }
    }
}

impl Drop for Relay {
    fn drop(&mut self) {
        let _ = self.cmd.send(Cmd::Close(self.req));
    }
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
    fn dispositions_never_break_the_header() {
        let d = disposition("a\"b\\c é.bin");
        assert!(!d.contains('\\') || d.contains("%5C"));
        assert_eq!(d.matches('"').count(), 2);
        assert!(d.contains("filename*=UTF-8''a%22b%5Cc%20%C3%A9.bin"));
    }
}
