#!/usr/bin/env bash
# Format-path .expect() gate.
#
# Segment format parsers are corruption boundaries — a panic here means a
# corrupted on-disk byte takes the process down instead of being routed to
# the quarantine registry. Production code in these paths must propagate
# errors via `?`, never `.expect()`.
#
# Excluded: bodies of inline `#[cfg(test)] mod tests` blocks.

set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
paths=(
    nodedb-columnar/src/format.rs
    nodedb-fts/src/lsm/segment
    nodedb-array/src/segment/format
)

violations=()
for rel in "${paths[@]}"; do
    target="$ROOT/$rel"
    [ -e "$target" ] || continue
    while IFS= read -r match; do
        file="${match%%:*}"
        rest="${match#*:}"
        lineno="${rest%%:*}"
        last_cfg=$(awk -v n="$lineno" 'NR<=n && /^#\[cfg\(test\)\]/ {x=NR} END{print x+0}' "$file")
        if [ "$last_cfg" -gt 0 ]; then
            close_after=$(awk -v a="$last_cfg" -v b="$lineno" 'NR>a && NR<b && /^}/ {x=NR} END{print x+0}' "$file")
            [ "$close_after" -eq 0 ] && continue
        fi
        violations+=("${file#$ROOT/}:$lineno")
    done < <(grep -rHEn '\.expect\(' "$target" 2>/dev/null \
             | grep -v ':[[:space:]]*//' \
             | grep -v ':[[:space:]]*\*' \
             || true)
done

if [ ${#violations[@]} -gt 0 ]; then
    echo "FAIL: ${#violations[@]} .expect() in format-path production code:"
    printf '  %s\n' "${violations[@]}"
    echo
    echo "Format parsers are corruption boundaries. Use '?' propagation so the"
    echo "quarantine registry can record + isolate the bad segment."
    exit 1
fi
echo "OK: format-path .expect() gate clean."
