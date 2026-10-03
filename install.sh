#!/bin/sh
# Install takt on macOS or Linux:
#   curl -fsSL https://raw.githubusercontent.com/damsleth/takt/main/install.sh | sh
# Puts takt.py in ~/.local/share/takt/ and links ~/.local/bin/takt to it.
# TAKT_REF pins a tag or commit instead of main. TAKT_SOURCE installs a local file.
set -eu

REF="${TAKT_REF:-main}"
URL="https://raw.githubusercontent.com/damsleth/takt/$REF/takt.py"
DIR="$HOME/.local/share/takt"
BIN="$HOME/.local/bin"

PY="$(command -v python3 || true)"
if [ -z "$PY" ] || ! "$PY" -c 'import sys; sys.exit(sys.version_info < (3, 11))'; then
  echo "takt needs Python 3.11 or later as python3 on PATH." >&2
  exit 1
fi

mkdir -p "$DIR" "$BIN"
if [ -n "${TAKT_SOURCE:-}" ]; then
  cp "$TAKT_SOURCE" "$DIR/takt.py.new"
else
  curl -fsSL "$URL" -o "$DIR/takt.py.new"
fi
mv "$DIR/takt.py.new" "$DIR/takt.py"
chmod +x "$DIR/takt.py"
ln -sf "$DIR/takt.py" "$BIN/takt"

echo "installed $("$BIN/takt" --version) to $BIN/takt"
case ":$PATH:" in
  *":$BIN:"*) ;;
  *) echo "add $BIN to PATH, for example: echo 'export PATH=\"\$HOME/.local/bin:\$PATH\"' >> ~/.profile" ;;
esac
echo "next: takt init"
