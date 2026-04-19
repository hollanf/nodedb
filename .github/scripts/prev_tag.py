#!/usr/bin/env python3
"""Print the previous semver tag relative to the given tag.

For full releases (no pre-release suffix) only other full releases are
considered, so the changelog spans stable -> stable. For pre-releases,
all tags are candidates.
"""

import re
import subprocess
import sys

TAG_RE = re.compile(r"^v(\d+)\.(\d+)\.(\d+)(?:-(.+))?$")


def parse(tag: str):
    m = TAG_RE.match(tag)
    if not m:
        return None
    return (
        int(m.group(1)),
        int(m.group(2)),
        int(m.group(3)),
        1 if m.group(4) is None else 0,
        m.group(4) or "",
    )


def main() -> int:
    if len(sys.argv) < 2:
        return 0
    cur = sys.argv[1]
    cur_key = parse(cur)
    if cur_key is None:
        return 0
    cur_is_release = cur_key[3] == 1

    tags = subprocess.check_output(["git", "tag", "--list", "v*"]).decode().split()
    parsed = [(t, parse(t)) for t in tags]
    parsed = [(t, k) for t, k in parsed if k is not None]
    if cur_is_release:
        parsed = [(t, k) for t, k in parsed if k[3] == 1]
    parsed.sort(key=lambda x: x[1], reverse=True)
    ordered = [t for t, _ in parsed]
    if cur in ordered:
        i = ordered.index(cur)
        if i + 1 < len(ordered):
            print(ordered[i + 1])
    return 0


if __name__ == "__main__":
    sys.exit(main())
