#!/usr/bin/env bash
# Fails if any `image:` in docker-compose.yml is not pinned by @sha256 digest.
# Usage: scripts/verify-pins.sh [compose-file]
set -uo pipefail
cd "$(dirname "$0")/.."
f=${1:-docker-compose.yml}
unpinned=$(grep -E '^[[:space:]]*image:' "$f" | grep -vE '@sha256:[0-9a-f]{64}')
if [ -n "$unpinned" ]; then
  echo "unpinned images in $f:" >&2; echo "$unpinned" >&2; exit 1
fi
echo "all $(grep -cE '^[[:space:]]*image:' "$f") images digest-pinned"
