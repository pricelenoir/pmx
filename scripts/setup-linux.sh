#!/usr/bin/env bash
# Set up a Linux server (Ubuntu/Debian) to run pmx unattended: system packages, uv + the locked venv, the `pmx`
# command, time sync, and lingering (user services start at boot). Run it from the repo, as the user pmx will run
# as (not root); it asks for sudo where needed and is safe to run again. Then follow docs/runbooks/linux-vm.md.
#
#   scripts/setup-linux.sh             # the server: no dashboard toolchain (watch with `pmx remote HOST tui`)
#   scripts/setup-linux.sh --with-tui  # also Rust, to run the dashboard on the server itself
set -euo pipefail

with_tui=0
for arg in "$@"; do
  case "$arg" in
    --with-tui) with_tui=1 ;;
    -h|--help) sed -n '2,7p' "$0"; exit 0 ;;
    *) echo "unknown option: $arg" >&2; exit 2 ;;
  esac
done

repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
say() { printf '\n== %s\n' "$*"; }

if [[ "$(id -u)" -eq 0 ]]; then
  echo "run this as the user pmx will run as, not root (it uses sudo where it must)" >&2
  exit 1
fi
if ! command -v apt-get >/dev/null; then
  echo "this script knows apt (Ubuntu/Debian); elsewhere install git, curl, and uv, then run: uv sync --locked" >&2
  exit 1
fi

say "system packages"
sudo apt-get update -qq
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -qq git curl ca-certificates sqlite3 tmux >/dev/null

say "uv"
export PATH="$HOME/.local/bin:$PATH"
if ! command -v uv >/dev/null; then
  curl -LsSf https://astral.sh/uv/install.sh | sh
fi
uv --version

say "the venv (uv.lock)"
cd "$repo"
uv sync --locked --python 3.13
mkdir -p "$HOME/.local/bin"
ln -sf "$repo/.venv/bin/pmx" "$HOME/.local/bin/pmx"
# shellcheck disable=SC2016  # written literally: the login shell expands it
grep -qs 'HOME/.local/bin' "$HOME/.profile" || echo 'export PATH="$HOME/.local/bin:$PATH"' >> "$HOME/.profile"

if [[ "$with_tui" -eq 1 ]]; then
  say "Rust (the dashboard)"
  sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -qq build-essential >/dev/null
  if ! command -v cargo >/dev/null && [[ ! -x "$HOME/.cargo/bin/cargo" ]]; then
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal
  fi
  "$HOME/.cargo/bin/cargo" build --release --manifest-path "$repo/tui/Cargo.toml"
fi

say "time sync"
sudo timedatectl set-ntp true || echo "couldn't turn on NTP (timedatectl); check the clock another way"

say "lingering: user services start at boot and survive logout"
sudo loginctl enable-linger "$USER"

say "done"
cat <<EOF
pmx is at $HOME/.local/bin/pmx (open a new shell, or: export PATH="\$HOME/.local/bin:\$PATH")
next (docs/runbooks/linux-vm.md):
  pmx keygen --env prod && pmx setkey --env prod KEY_ID    # this server's own Kalshi key
  echo 'PMX_ALERT_WEBHOOK=https://ntfy.sh/<your-topic>' >> .env && pmx ops alert-test
  pmx doctor
  pmx ops install --record --backup [--run]
EOF
