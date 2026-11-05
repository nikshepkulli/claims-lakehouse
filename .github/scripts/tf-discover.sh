#!/usr/bin/env bash
# Discover terraform root dirs: infra/<cloud>/envs/<env> and infra/<cloud>/modules/<mod>.
# Only dirs that directly contain *.tf are returned. Emits GITHUB_OUTPUT keys:
#   dirs    JSON array of dirs (matrix)
#   any     true if matrix non-empty
#   has_tf  true if infra/ contains any *.tf file at all (gate uses this)
set -euo pipefail
out="${GITHUB_OUTPUT:-/dev/stdout}"
dirs=()
if [ -d infra ]; then
  while IFS= read -r d; do
    if compgen -G "$d/*.tf" >/dev/null; then dirs+=("$d"); fi
  done < <(find infra -mindepth 3 -maxdepth 3 -type d \( -path 'infra/*/envs/*' -o -path 'infra/*/modules/*' \) | LC_ALL=C sort)
fi
has_tf=false
if [ -d infra ] && [ -n "$(find infra -name '*.tf' -type f -print -quit)" ]; then has_tf=true; fi
if [ "${#dirs[@]}" -eq 0 ]; then json='[]'; any=false
else json=$(printf '%s\n' "${dirs[@]}" | jq -R . | jq -sc .); any=true; fi
{ echo "dirs=$json"; echo "any=$any"; echo "has_tf=$has_tf"; } >> "$out"
echo "terraform dirs: $json (has_tf=$has_tf)" >&2
