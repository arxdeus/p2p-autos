#!/usr/bin/env bash
set -euo pipefail

REPO="arxdeus/p2p-autos"
BIN="p2p-autos"
INSTALL_DIR="${INSTALL_DIR:-/usr/local/bin}"

os="$(uname -s)"
arch="$(uname -m)"

case "$os" in
Linux) target="x86_64-unknown-linux-gnu" ;;
Darwin)
	case "$arch" in
	arm64) target="aarch64-apple-darwin" ;;
	*) target="x86_64-apple-darwin" ;;
	esac
	;;
*)
	echo "error: unsupported OS: $os (use install.ps1 or build from source on Windows)" >&2
	exit 1
	;;
esac

tag="$(curl -fsSL "https://api.github.com/repos/$REPO/releases/latest" | grep '"tag_name"' | cut -d'"' -f4)"
[ -n "$tag" ] || {
	echo "error: could not resolve latest release" >&2
	exit 1
}

url="https://github.com/$REPO/releases/download/$tag/$BIN-$tag-$target.tar.gz"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

echo "Downloading $BIN $tag ($target)..."
curl -fsSL "$url" | tar -xz -C "$tmp"

if [ -w "$INSTALL_DIR" ]; then
	mv "$tmp/$BIN" "$INSTALL_DIR/$BIN"
else
	sudo mv "$tmp/$BIN" "$INSTALL_DIR/$BIN"
fi
chmod +x "$INSTALL_DIR/$BIN"

echo "Installed $BIN to $INSTALL_DIR/$BIN"
