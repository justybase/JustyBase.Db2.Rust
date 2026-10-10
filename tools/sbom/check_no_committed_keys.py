#!/usr/bin/env python3
"""Fail if private key/certificate material is tracked in git.

TLS fixtures for tests are generated ephemerally by
scripts/generate-test-tls-fixtures.sh and must never be committed.
Scans tracked files (git ls-files) for key/certificate suffixes.
"""
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
FORBIDDEN_SUFFIXES = (".pem", ".key", ".p12", ".pfx")


def main() -> int:
    result = subprocess.run(
        ["git", "ls-files", "-z"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    tracked = [path for path in result.stdout.split("\0") if path]
    bad = [path for path in tracked if path.lower().endswith(FORBIDDEN_SUFFIXES)]
    if bad:
        print("FAIL: key/certificate files must not be committed:", file=sys.stderr)
        for path in bad:
            print(f"  - {path}", file=sys.stderr)
        print(
            "Generate ephemeral fixtures with scripts/generate-test-tls-fixtures.sh instead.",
            file=sys.stderr,
        )
        return 1
    print(f"PASS: no committed key material ({len(tracked)} tracked files scanned)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
