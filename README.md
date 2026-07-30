<div align="center">

# p2p-autos

*Share a file straight from a browser tab or CLI — no upload, no storage, no signup*

[![Release](https://img.shields.io/github/v/release/arxdeus/p2p-autos?style=flat-square)](https://github.com/arxdeus/p2p-autos/releases)
[![Rust](https://img.shields.io/badge/Rust-2021-orange?style=flat-square&logo=rust)](Cargo.toml)

**[p2p.autos](http://p2p.autos/)**

[Installation](#installation) • [Usage](#usage) • [How it works](#how-it-works) • [Configuration](#configuration)

</div>

`p2p-autos` hands out a plain HTTP download link for a file that never leaves the sender's machine. The sender keeps a browser tab (or the bundled CLI) open; the server only holds a routing table and relays bytes as they're read. Close the tab, kill the process, and the link 404s — nothing was ever written to disk on the server.

## Installation

```sh
curl -fsSL https://raw.githubusercontent.com/arxdeus/p2p-autos/main/install.sh | bash
```

Installs the latest release to `/usr/local/bin` (Linux and macOS, x86_64/arm64). On Windows, grab the archive from the [Releases page](https://github.com/arxdeus/p2p-autos/releases).

### Build from source

```sh
cargo build --release
```

## Usage

### Run the server

```sh
p2p-autos                       # binds 0.0.0.0:8080
p2p-autos --address 0.0.0.0 --port 3000
```

Open the printed URL in a browser, drop a file, and share the link it gives you. Keep the tab open — closing it ends every share from that tab.

### Share from the CLI

No browser needed: offer one local file to a running server and get a link back.

```sh
p2p-autos share ./video.mp4
p2p-autos share ./video.mp4 --server p2p.example.com:8080
```

The link works for as long as this command keeps running.

## How it works

1. A sender opens the web UI (or runs `p2p-autos share <file>`) and connects over WebSocket, offering one or more files.
2. The server hands back a short link (`/d/<id>`) per file.
3. Anyone requests that link with a plain `GET` — `curl`, `wget`, a browser, a video player, anything that speaks HTTP.
4. The server asks the sender's socket to "pull" the next chunk, the sender reads it (from a `File` object in the tab, or from disk in the CLI) and pushes it back, and the server streams it straight into the HTTP response body.

No bytes are buffered to disk and no file ever touches the server's filesystem — only enough chunks to keep the pipe full sit in memory at once. `Range` requests are honored, so downloads are resumable and seekable (e.g. scrubbing a video).

> [!NOTE]
> This isn't WebRTC-style peer-to-peer — browsers can't accept inbound connections. The server is the relay every byte passes through; the "peer" is the sender's live session (tab or process), which is what makes the link ephemeral.

## Features

- **Nothing stored** — the server is a router and a pipe, never a disk.
- **Ephemeral by design** — a share only exists as long as its tab or process does.
- **Plain HTTP downloads** — resumable, seekable, no client-side JS required.
- **CLI or browser** — share from a terminal with no GUI, or drag-and-drop in a tab.
- **Adaptive windowing** — pull depth grows only when a download is actually starved, and the server's whole memory budget is divided live across active downloads, so load degrades throughput instead of rejecting connections.

## Configuration

| Flag / env | Default | Description |
| --- | --- | --- |
| `--address` | `0.0.0.0` | Bind address |
| `--port` | `8080` | Bind port |
| `BIND` | — | `host:port`, used only if `--address`/`--port` are both omitted |
| `--memory-budget` | `256` (MiB) | Total buffer budget for in-flight downloads, shared fairly across all of them |
| `RUST_LOG` | `p2p_autos=info,tower_http=warn` | [`tracing-subscriber`](https://docs.rs/tracing-subscriber) filter |

> [!TIP]
> `--memory-budget` is never a hard cap that rejects downloads — it's divided live across whatever's active right now, so more concurrent downloads just means a smaller pull window each.

## Resources

- [Releases](https://github.com/arxdeus/p2p-autos/releases) — prebuilt binaries for Linux, macOS, and Windows
