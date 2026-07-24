#!/bin/bash

# skills-cassette install script for Linux and macOS.
# Requirements:
# * curl
# * uname
# * install
# * sha256sum or shasum
# * sudo (when the install directory is not writable)
# * /tmp directory

set -euo pipefail

VERSION="${SKILLS_CASSETTE_VERSION:-latest}"
BASE_URL="${SKILLS_CASSETTE_BASE_URL:-https://download.tapes.dev}"

# Detect OS
OS="$(uname -s | tr '[:upper:]' '[:lower:]')"
case "$OS" in
  linux*) OS="linux" ;;
  darwin*) OS="darwin" ;;
  *) echo "Unsupported OS: $OS"; exit 1 ;;
esac

# Detect architecture
ARCH="$(uname -m)"
case "$ARCH" in
  x86_64) ARCH="amd64" ;;
  aarch64|arm64) ARCH="arm64" ;;
  *) echo "Unsupported architecture: $ARCH"; exit 1 ;;
esac

INSTALL_DIR="${SKILLS_CASSETTE_INSTALL_DIR:-/usr/local/bin}"
TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT
DOWNLOAD_URL="$BASE_URL/skills-cassette/$VERSION/$OS/$ARCH/skills-cassette"

echo "Downloading skills-cassette $VERSION for $OS/$ARCH ..."
curl -fsSL "$DOWNLOAD_URL" -o "$TMP_DIR/skills-cassette"
curl -fsSL "$DOWNLOAD_URL.sha256" -o "$TMP_DIR/skills-cassette.sha256"

if command -v sha256sum >/dev/null 2>&1; then
  (cd "$TMP_DIR" && sha256sum -c skills-cassette.sha256)
elif command -v shasum >/dev/null 2>&1; then
  (cd "$TMP_DIR" && shasum -a 256 -c skills-cassette.sha256)
else
  echo "Cannot verify download: install sha256sum or shasum" >&2
  exit 1
fi

echo "Installing to $INSTALL_DIR ..."
if [ -w "$INSTALL_DIR" ]; then
  install -m 0755 "$TMP_DIR/skills-cassette" "$INSTALL_DIR/skills-cassette"
else
  sudo install -m 0755 "$TMP_DIR/skills-cassette" "$INSTALL_DIR/skills-cassette"
fi

echo "Installed skills-cassette:"
"$INSTALL_DIR/skills-cassette"
