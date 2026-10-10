#!/usr/bin/env python3
"""Summarize replay block-size probe output.

Usage: summarize-block-counters.py <results-dir> <out.csv>
Expects per-size directories (16384/32767/65535) with Criterion
`estimates.json` files. Emits one row per benchmark with mean/median ms so
block-size scaling can be compared without inferring a new default.
"""
import csv
import json
import sys
from pathlib import Path


def main() -> int:
    if len(sys.argv) != 3:
        print(f"usage: {sys.argv[0]} <results-dir> <out.csv>", file=sys.stderr)
        return 2
    root = Path(sys.argv[1])
    out = Path(sys.argv[2])
    rows = []
    for estimates in sorted(root.rglob("estimates.json")):
        try:
            data = json.loads(estimates.read_text())
        except (OSError, json.JSONDecodeError):
            continue
        mean_ns = data.get("mean", {}).get("point_estimate")
        median_ns = data.get("median", {}).get("point_estimate")
        if mean_ns is None:
            continue
        rel = estimates.relative_to(root).as_posix()
        rows.append(
            {
                "path": rel,
                "mean_ms": f"{mean_ns / 1e6:.3f}",
                "median_ms": f"{median_ns / 1e6:.3f}",
            }
        )
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=["path", "mean_ms", "median_ms"])
        writer.writeheader()
        writer.writerows(rows)
    print(f"wrote {len(rows)} rows to {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
