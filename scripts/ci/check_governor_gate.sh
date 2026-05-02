#!/usr/bin/env bash
# Memory-governor gate.
#
# Every non-test `Vec::with_capacity` and `Mmap::map` call in the five
# governed engine crates must either:
#   1. have a `.reserve(` or `.try_reserve(` invocation on a governor handle
#      within the 5 preceding source lines, or
#   2. carry a `// no-governor: <reason>` marker on a preceding line.
#
# The marker form documents an intentional exemption (hot-path bounded
# allocation, structural cold path governed by an outer caller, etc).
# Any unmarked, ungoverned site fails the gate.
#
# Excluded: doc-comment lines, `tests.rs` files (included via
# `#[cfg(test)] mod tests;`), and bodies of inline `#[cfg(test)] mod tests`
# blocks.

set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
crates=(nodedb-vector nodedb-graph nodedb-fts nodedb-columnar nodedb-spatial)

violations=()
for crate in "${crates[@]}"; do
    while IFS= read -r match; do
        file="${match%%:*}"
        rest="${match#*:}"
        lineno="${rest%%:*}"
        case "$file" in
            */tests.rs|*/tests/*) continue ;;
        esac
        last_cfg=$(awk -v n="$lineno" 'NR<=n && /^#\[cfg\(test\)\]/ {x=NR} END{print x+0}' "$file")
        if [ "$last_cfg" -gt 0 ]; then
            close_after=$(awk -v a="$last_cfg" -v b="$lineno" 'NR>a && NR<b && /^}/ {x=NR} END{print x+0}' "$file")
            [ "$close_after" -eq 0 ] && continue
        fi
        start=$((lineno > 5 ? lineno - 5 : 1))
        window=$(sed -n "${start},${lineno}p" "$file")
        echo "$window" | grep -qE '\.(reserve|try_reserve)\(' && continue
        echo "$window" | grep -q 'no-governor:' && continue
        violations+=("$file:$lineno")
    done < <(grep -rEn '(Vec::with_capacity|Mmap::map)\b' "$ROOT/$crate/src" 2>/dev/null \
             | grep -v ':[[:space:]]*//' \
             | grep -v ':[[:space:]]*\*' \
             || true)
done

if [ ${#violations[@]} -gt 0 ]; then
    echo "FAIL: ${#violations[@]} ungoverned allocation site(s):"
    printf '  %s\n' "${violations[@]}"
    echo
    echo "Each site must either:"
    echo "  - have a governor.reserve() call on a preceding line (within 5 lines), or"
    echo "  - carry a '// no-governor: <reason>' marker on the preceding line."
    exit 1
fi
echo "OK: governor gate clean across ${#crates[@]} crates."
