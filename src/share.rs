//! The share registry: files an uploader tab has offered, keyed by a short id.

use std::sync::{atomic::AtomicUsize, Arc};

use dashmap::DashMap;
use rand::Rng;
use tokio::sync::mpsc;

use crate::uploader::Cmd;

/// Unambiguous base-56: no 0/O, 1/l/I.
const ALPHABET: &[u8] = b"abcdefghijkmnopqrstuvwxyzABCDEFGHJKLMNPQRSTUVWXYZ23456789";
const ID_LEN: usize = 8;

#[derive(Clone)]
pub(crate) struct Share {
    pub(crate) name: Arc<str>,
    pub(crate) mime: Arc<str>,
    pub(crate) size: u64,
    pub(crate) cmd: mpsc::UnboundedSender<Cmd>,
}

pub(crate) struct App {
    pub(crate) shares: DashMap<String, Share>,
    /// Target total memory, in CHUNKs (== MiB, since CHUNK is exactly
    /// 1 MiB), for every download's buffer combined — across every share
    /// and every uploader tab. Never used to reject a download: pump()
    /// divides it live across however many downloads are active right
    /// now (see fair_share_window in uploader.rs), so more concurrent
    /// downloads means a smaller window each, automatically, not a cutoff.
    pub(crate) budget_chunks: usize,
    /// How many downloads are active right now, across every uploader tab.
    /// Incremented when a Req is created, decremented (via Req's Drop) the
    /// instant one ends, whatever the reason — so fair_share_window always
    /// divides by the true current count.
    pub(crate) active_downloads: Arc<AtomicUsize>,
}

impl App {
    pub(crate) fn new(budget_chunks: usize) -> Self {
        Self {
            shares: DashMap::new(),
            budget_chunks,
            active_downloads: Arc::new(AtomicUsize::new(0)),
        }
    }
}

/// Registers `share` under a fresh id, retrying on the (astronomically
/// unlikely) collision.
pub(crate) fn insert_share(app: &App, share: Share) -> String {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_are_unambiguous() {
        let id = gen_id();
        assert_eq!(id.len(), ID_LEN);
        assert!(!id.contains(['0', 'O', '1', 'l', 'I']));
    }
}
