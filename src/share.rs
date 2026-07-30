//! The share registry: files an uploader tab has offered, keyed by a short id.

use std::sync::Arc;

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
