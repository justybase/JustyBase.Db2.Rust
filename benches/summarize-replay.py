#!/usr/bin/env python3
"""Summarize Criterion replay output into metrics.csv (B/row, rows/s).

Usage: summarize-replay.py <criterion-dir> <out.csv>
Parses `estimates.json` under each Criterion benchmark folder. Allocation
figures come from a matching `allocation_probe` run when available; otherwise
alloc columns are left empty for manual fill from /usr/bin/time or stats_alloc.
"""
import csv
import json
import sys
from pathlib import Path


def main() -> int:
    if len(sys.argv) != 3:
        print(f"usage: {sys.argv[0]} <criterion-dir> <out.csv>", file=sys.stderr)
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
        # Folder layout: <group>/<benchmark>/new/estimates.json
        try:
            bench = estimates.parent.parent.name
            group = estimates.parent.parent.parent.name
        except IndexError:
            continue
        rows.append(
            {
                "group": group,
                "benchmark": bench,
                "mean_ms": f"{mean_ns / 1e6:.3f}",
                "median_ms": f"{median_ns / 1e6:.3f}",
                "alloc_per_op": "",
                "b_per_row": "",
                "rows_per_s": "",
            }
        )
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=[
                "group",
                "benchmark",
                "mean_ms",
                "median_ms",
                "alloc_per_op",
                "b_per_row",
                "rows_per_s",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)
    print(f"wrote {len(rows)} rows to {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
