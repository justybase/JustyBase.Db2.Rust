#!/usr/bin/env python3
"""Validate the publishable crate contents without publishing."""
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
REQUIRED = (
    "Cargo.toml",
    "Cargo.lock",
    "LICENSE",
    "README.md",
    "THIRD-PARTY-NOTICES.md",
    "API-COMPATIBILITY.md",
    "docs/DRIVER-GUIDE.md",
    "docs/QUALIFICATION.md",
    "docs/KERBEROS-STATUS.md",
    "src/lib.rs",
)
FORBIDDEN_SUFFIXES = (".pem", ".key", ".p12", ".pfx", ".sth", ".env")
FORBIDDEN_PREFIXES = (
    ".github/",
    "assets/",
    "tests/",
    "tools/",
    "target/",
    "release-out/",
)
FORBIDDEN_SUBSTRINGS = ("clidriver",)


def main() -> int:
    result = subprocess.run(
        ["cargo", "package", "--list", "--allow-dirty", "--offline"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    entries = [line.strip() for line in result.stdout.splitlines() if line.strip()]
    errors = []

    for name in REQUIRED:
        if name not in entries:
            errors.append(f"missing required entry: {name}")
    for entry in entries:
        lower = entry.lower()
        if lower.endswith(FORBIDDEN_SUFFIXES):
            errors.append(f"key/certificate/env file must not be packaged: {entry}")
        if entry.startswith(FORBIDDEN_PREFIXES):
            errors.append(f"development-only path must not be packaged: {entry}")
        if any(token in lower for token in FORBIDDEN_SUBSTRINGS):
            errors.append(f"forbidden client payload: {entry}")

    if errors:
        print("FAIL: cargo package contents", file=sys.stderr)
        for error in errors:
            print(f"  - {error}", file=sys.stderr)
        return 1
    print(f"PASS: cargo package contents ({len(entries)} entries, publishable tree is clean)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
