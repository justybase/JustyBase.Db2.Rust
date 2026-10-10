#!/usr/bin/env python3
"""Assemble third-party license notices for the Rust driver's locked deps.

For every non-workspace crate in the locked graph, locate the crate source
(rooted at Cargo's registry `src` directory reported by `cargo metadata`) and
concatenate the declared license expression plus any LICENSE/COPYING/NOTICE
files shipped in the crate. Output is a single Markdown notice file usable in
compiled-binary distributions. This is a mechanical inventory, not legal advice.
"""
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

# Usage: assemble_rust_notices.py [output-directory]   (default: <repo>/sbom-out)
NOTICE_NAMES = ("LICENSE", "LICENCE", "COPYING", "NOTICE", "UNLICENSE")


def license_files(crate_dir: Path) -> list[Path]:
    found = []
    if not crate_dir.is_dir():
        return found
    for entry in sorted(crate_dir.iterdir()):
        if entry.is_file() and entry.name.upper().startswith(NOTICE_NAMES):
            found.append(entry)
    return found


def main() -> int:
    out_dir = Path(sys.argv[1]).resolve() if len(sys.argv) > 1 else ROOT / "sbom-out"
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / "justybase-db2-third-party-notices.md"
    raw = subprocess.run(
        [
            "cargo",
            "metadata",
            "--locked",
            "--offline",
            "--format-version",
            "1",
            "--manifest-path",
            str(ROOT / "Cargo.toml"),
        ],
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    metadata = json.loads(raw)
    workspace_ids = set(metadata["workspace_members"])

    lines = [
        "# Rust driver third-party notices",
        "",
        "Generated mechanically from `cargo metadata --locked --offline`. Each section",
        "lists the crate version, its declared SPDX license expression, and any license",
        "or notice files shipped in the crate source. This file must accompany compiled",
        "binary distributions of `justybase-db2`; it is not legal advice.",
        "",
    ]
    missing = []
    for package in sorted(
        (p for p in metadata["packages"] if p["id"] not in workspace_ids),
        key=lambda p: (p["name"], p["version"]),
    ):
        lines.append(f"## {package['name']} {package['version']}")
        lines.append("")
        lines.append(f"- License: {package.get('license') or package.get('license_file') or 'UNKNOWN'}")
        lines.append(f"- Repository: {package.get('repository') or 'n/a'}")
        crate_dir = Path(package["manifest_path"]).parent
        files = license_files(crate_dir)
        if not files:
            missing.append(f"{package['name']} {package['version']}")
            lines.append("- License files: none found in crate source")
        else:
            for path in files:
                lines.append(f"- License file: `{path.name}`")
                lines.append("")
                try:
                    text = path.read_text(encoding="utf-8", errors="replace").strip()
                except OSError as error:  # pragma: no cover - filesystem dependent
                    text = f"<unable to read: {error}>"
                lines.append("```")
                lines.append(text)
                lines.append("```")
        lines.append("")

    if missing:
        lines.append("## Crates without an embedded license file")
        lines.append("")
        lines.append("The following crates declare a license expression but ship no")
        lines.append("LICENSE/COPYING/NOTICE file in their source archive:")
        lines.append("")
        for name in missing:
            lines.append(f"- {name}")
        lines.append("")

    out.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
    print(f"wrote {out}; {len(missing)} crates without an embedded license file")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
