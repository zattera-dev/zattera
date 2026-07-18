#!/bin/sh
# get.zattera.dev — install or upgrade the zattera binary.
#
#   curl -sfL https://get.zattera.dev | sh -
#
# Idempotent: re-running upgrades in place (stopping/restarting the systemd
# unit around the swap when one is running). Environment knobs:
#
#   INSTALL_ZATTERA_VERSION   pin a release, e.g. v0.1.0 (default: latest)
#   INSTALL_ZATTERA_BIN_DIR   install dir (default: /usr/local/bin)
#   INSTALL_ZATTERA_BASE_URL  override the asset base URL (testing/mirrors)
#
# Linux gets the full binary (server + CLI); macOS gets the CLI-only build.
# Windows: download zattera-windows-amd64.exe from the releases page.
set -eu

# Keep in step with upgrade.DefaultBaseURL (internal/daemon/upgrade/release.go):
# this script and `zt cluster upgrade` must install the same artifact.
GITHUB_REPO="adileo/zattera.dev"

info()  { echo "[zattera] $*"; }
fatal() { echo "[zattera] ERROR: $*" 1>&2; exit 1; }

# --- platform detection -----------------------------------------------------
os=$(uname -s)
case "$os" in
    Linux)  os=linux ;;
    Darwin) os=darwin ;;
    *) fatal "unsupported OS: $os (for Windows, download zattera-windows-amd64.exe from https://github.com/adileo/zattera.dev/releases)" ;;
esac
arch=$(uname -m)
case "$arch" in
    x86_64|amd64)  arch=amd64 ;;
    aarch64|arm64) arch=arm64 ;;
    *) fatal "unsupported architecture: $arch" ;;
esac
asset="zattera-$os-$arch"

# --- release location --------------------------------------------------------
version="${INSTALL_ZATTERA_VERSION:-}"
if [ -n "$version" ]; then
    base="https://github.com/$GITHUB_REPO/releases/download/$version"
else
    base="https://github.com/$GITHUB_REPO/releases/latest/download"
    version="latest"
fi
base="${INSTALL_ZATTERA_BASE_URL:-$base}"

# --- install dir -------------------------------------------------------------
bin_dir="${INSTALL_ZATTERA_BIN_DIR:-/usr/local/bin}"
mkdir -p "$bin_dir" 2>/dev/null || fatal "cannot create $bin_dir; run as root (curl ... | sudo sh -) or set INSTALL_ZATTERA_BIN_DIR"
[ -w "$bin_dir" ] || fatal "$bin_dir is not writable; run as root (curl ... | sudo sh -) or set INSTALL_ZATTERA_BIN_DIR"

# --- download + verify -------------------------------------------------------
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

download() { # url dest
    if command -v curl >/dev/null 2>&1; then
        curl -fsSL -o "$2" "$1" || fatal "download failed: $1"
    elif command -v wget >/dev/null 2>&1; then
        wget -qO "$2" "$1" || fatal "download failed: $1"
    else
        fatal "curl or wget is required"
    fi
}

info "downloading $asset ($version)"
download "$base/$asset" "$tmp/zattera"
download "$base/sha256sums.txt" "$tmp/sha256sums.txt"

expected=$(awk -v a="$asset" '$2 == a {print $1}' "$tmp/sha256sums.txt")
[ -n "$expected" ] || fatal "$asset not found in sha256sums.txt"
if command -v sha256sum >/dev/null 2>&1; then
    actual=$(sha256sum "$tmp/zattera" | awk '{print $1}')
else
    actual=$(shasum -a 256 "$tmp/zattera" | awk '{print $1}')
fi
[ "$expected" = "$actual" ] || fatal "checksum mismatch for $asset (expected $expected, got $actual)"
chmod 755 "$tmp/zattera"

# --- swap in place (upgrade-safe) ---------------------------------------------
# Stop a running node around the swap; systemd restarts cleanly on the new
# binary. cp+mv keeps the final rename atomic on $bin_dir's filesystem.
restart=0
if command -v systemctl >/dev/null 2>&1 && systemctl is-active --quiet zattera 2>/dev/null; then
    info "stopping zattera.service for upgrade"
    systemctl stop zattera
    restart=1
fi
# Keep the outgoing binary so an upgrade can be undone without a download
# (the same .prev the in-cluster upgrade writes).
if [ -f "$bin_dir/zattera" ]; then
    cp "$bin_dir/zattera" "$bin_dir/zattera.prev" || true
fi
cp "$tmp/zattera" "$bin_dir/.zattera.new"
mv "$bin_dir/.zattera.new" "$bin_dir/zattera"
ln -sf zattera "$bin_dir/zt"
if [ "$restart" = 1 ]; then
    systemctl start zattera
    info "zattera.service restarted"
fi

info "installed $("$bin_dir/zattera" version) to $bin_dir/zattera (alias: zt)"
