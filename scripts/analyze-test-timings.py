#!/usr/bin/env python3
"""
analyze-test-timings.py

Parse TRX test result files, aggregate execution time per class, and
bin-pack classes into N balanced shards using a greedy algorithm.

Writes scripts/shard-profile.json (relative to the repo root inferred from
the script location) so the CI workflow can read it with `jq` at runtime —
keeping filter expressions out of the YAML matrix entirely.

Usage:
    python scripts/analyze-test-timings.py <file.trx> [options]
    python scripts/analyze-test-timings.py results/*.trx --shards 3

Options:
    --shards N    Number of shards to produce (default: 3)
    --dry-run     Print the profile but do not write shard-profile.json

The last shard always uses a negative filter (everything NOT in the
earlier shards) so any future test class is automatically captured.
"""

import argparse
import json
import sys
import xml.etree.ElementTree as ET
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path


TRX_NS = {"t": "http://microsoft.com/schemas/VisualStudio/TeamTest/2010"}
PROFILE_PATH = Path(__file__).parent / "shard-profile.json"


def parse_duration(s: str) -> float:
    """Parse HH:MM:SS.fffffff into total seconds."""
    h, m, rest = s.split(":")
    return int(h) * 3600 + int(m) * 60 + float(rest)


def parse_trx(path: Path) -> dict:
    """Return {fully_qualified_class_name: total_seconds} from one TRX file."""
    root = ET.parse(path).getroot()

    id_to_class: dict = {}
    for unit_test in root.findall(".//t:UnitTest", TRX_NS):
        test_id = unit_test.get("id")
        method = unit_test.find("t:TestMethod", TRX_NS)
        if method is not None and test_id:
            cls = method.get("className", "")
            if cls:
                id_to_class[test_id] = cls

    durations: dict = defaultdict(float)
    for result in root.findall(".//t:UnitTestResult", TRX_NS):
        test_id = result.get("testId", "")
        d = result.get("duration", "")
        if d and test_id in id_to_class:
            durations[id_to_class[test_id]] += parse_duration(d)

    return dict(durations)


def bin_pack(class_durations: dict, n: int):
    """Greedy bin-packing: heaviest class first → least-full shard."""
    items = sorted(class_durations.items(), key=lambda x: x[1], reverse=True)
    shards = [[] for _ in range(n)]
    totals = [0.0] * n
    for cls, dur in items:
        idx = min(range(n), key=lambda i: totals[i])
        shards[idx].append((cls, dur))
        totals[idx] += dur
    return shards, totals


def filter_segment(fqn: str) -> str:
    parts = fqn.split(".")
    tail = "." + ".".join(parts[-2:]) if len(parts) >= 2 else fqn
    return f"FullyQualifiedName~{tail}"


def build_filter(shard, positive_filters: list, is_last: bool) -> str:
    if is_last:
        return "&".join(f.replace("~", "!~") for f in positive_filters)
    return "|".join(filter_segment(c) for c, _ in shard)


def fmt(s: float) -> str:
    m, sec = divmod(int(s), 60)
    return f"{m}m{sec:02d}s"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Analyse TRX timings and write scripts/shard-profile.json"
    )
    parser.add_argument("trx_files", nargs="+", help="TRX result file(s)")
    parser.add_argument("--shards", type=int, default=3, help="Number of shards (default: 3)")
    parser.add_argument("--dry-run", action="store_true", help="Print only, do not write JSON")
    args = parser.parse_args()

    merged: dict = defaultdict(float)
    for path in args.trx_files:
        p = Path(path)
        if not p.exists():
            print(f"ERROR: File not found: {path}", file=sys.stderr)
            return 1
        for cls, dur in parse_trx(p).items():
            merged[cls] += dur

    if not merged:
        print("ERROR: No test timings found.", file=sys.stderr)
        return 1

    total = sum(merged.values())
    shards, totals = bin_pack(dict(merged), args.shards)
    ideal = total / args.shards

    # Build positive-filter list for all-but-last shards
    positive_filters: list = []
    for shard in shards[:-1]:
        for cls, _ in shard:
            positive_filters.append(filter_segment(cls))

    # ── Summary ──────────────────────────────────────────────────────────────
    print(f"{'='*64}")
    print(f"  Shard profile  ({args.shards} shards, measured total: {fmt(total)})")
    print(f"{'='*64}")
    print(f"  Ideal per shard: {fmt(ideal)}\n")

    profile_shards = []
    for i, (shard, t) in enumerate(zip(shards, totals)):
        is_last = i == len(shards) - 1
        deviation = abs(t - ideal) / ideal * 100
        filt = build_filter(shard, positive_filters, is_last)
        top = sorted(shard, key=lambda x: x[1], reverse=True)
        preview = ", ".join(f"{c.split('.')[-1]} ({fmt(d)})" for c, d in top[:4])
        if len(top) > 4:
            preview += f", +{len(top)-4} more"
        print(f"  Shard {i}  {fmt(t)}  (±{deviation:.0f}% from ideal)  {len(shard)} classes")
        print(f"    {preview}\n")
        profile_shards.append({
            "id": i,
            "estimate": fmt(t),
            "classes": len(shard),
            "top_classes": [c.split(".")[-1] for c, _ in top[:5]],
            "filter": filt,
        })

    profile = {
        "_comment": "Auto-generated by scripts/analyze-test-timings.py — do not edit by hand",
        "generated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "shards": args.shards,
        "total_estimate": fmt(total),
        "ideal_per_shard": fmt(ideal),
        "shard_profiles": profile_shards,
    }

    if args.dry_run:
        print("(dry-run: skipping write)")
        return 0

    PROFILE_PATH.write_text(json.dumps(profile, indent=2) + "\n", encoding="utf-8")
    print(f"Written: {PROFILE_PATH}")
    print(f"\nConfigure _build-and-test.yml with shard_index: {list(range(args.shards))}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
