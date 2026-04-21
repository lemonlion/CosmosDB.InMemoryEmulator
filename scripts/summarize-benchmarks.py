"""
Summarize benchmark results from multiple runs into an averaged markdown table.

Reads test-output.txt files from the results/ directory (downloaded artifacts)
and produces a per-scenario summary with averaged metrics across runs.

Usage:
    python3 scripts/summarize-benchmarks.py [results_dir]

The results directory should contain subdirectories matching the pattern:
    bench-<framework>-<rps>rps-<duration>s-run<N>
  or:
    bench-emulator-<framework>-<rps>rps-<duration>s-run<N>
"""

import os
import re
import sys
from collections import defaultdict

SCENARIO_PATTERN = re.compile(r'║\s+(.+?(?:\(\d+/\d+\)))\s+║')
METRICS_PATTERN = re.compile(
    r'║\s+(Throughput|Mean latency|P50 latency|P95 latency|P99 latency|Max latency)\s*:\s*([\d.]+)'
)
ARTIFACT_PATTERN = re.compile(
    r'bench-(?:emulator-)?(?P<framework>net[\d.]+)-(?P<rps>\d+)rps-(?P<duration>\d+)s-run(?P<run>\d+)'
)
METRIC_ORDER = [
    "Throughput", "Mean latency", "P50 latency",
    "P95 latency", "P99 latency", "Max latency",
]


def parse_results(results_dir):
    """Parse all test-output.txt files and return grouped metrics."""
    configs = defaultdict(lambda: defaultdict(list))

    for artifact_dir in sorted(os.listdir(results_dir)):
        match = ARTIFACT_PATTERN.match(artifact_dir)
        if not match:
            continue

        framework = match.group("framework")
        rps = match.group("rps")
        duration = match.group("duration")
        config_key = f"{framework} / {rps}rps / {duration}s"

        filepath = os.path.join(results_dir, artifact_dir, "test-output.txt")
        if not os.path.exists(filepath):
            print(f"WARNING: Missing {filepath}", file=sys.stderr)
            continue

        with open(filepath) as f:
            lines = f.readlines()

        current_scenario = "Unknown"
        for line in lines:
            sm = SCENARIO_PATTERN.search(line)
            if sm:
                current_scenario = sm.group(1).strip()
            mm = METRICS_PATTERN.search(line)
            if mm:
                metric_name = mm.group(1).strip()
                value = float(mm.group(2))
                key = (config_key, current_scenario)
                configs[key][metric_name].append(value)

    return configs


def write_summary(configs, results_dir):
    """Write averaged summary tables and per-run details."""
    scenarios = sorted(set(s for _, s in configs.keys()))
    run_count = 0
    for values_by_metric in configs.values():
        for vals in values_by_metric.values():
            run_count = max(run_count, len(vals))
            break
        break

    for scenario in scenarios:
        print(f"# {scenario} (Averaged over {run_count} run{'s' if run_count != 1 else ''})")
        print()
        print("| Config | Throughput (ops/s) | Mean (ms) | P50 (ms) | P95 (ms) | P99 (ms) | Max (ms) |")
        print("|--------|-------------------|-----------|----------|----------|----------|----------|")

        for key in sorted(configs.keys()):
            cfg, scn = key
            if scn != scenario:
                continue
            metrics = configs[key]
            row = [cfg]
            for metric in METRIC_ORDER:
                values = metrics.get(metric, [])
                if values:
                    avg = sum(values) / len(values)
                    row.append(f"{avg:.3f}" if "latency" in metric.lower() else f"{avg:.1f}")
                else:
                    row.append("N/A")
            print("| " + " | ".join(row) + " |")
        print()

    # Per-run details
    print("## Per-Run Details")
    print()
    for artifact_dir in sorted(os.listdir(results_dir)):
        match = ARTIFACT_PATTERN.match(artifact_dir)
        if not match:
            continue
        framework = match.group("framework")
        rps = match.group("rps")
        duration = match.group("duration")
        run = match.group("run")
        filepath = os.path.join(results_dir, artifact_dir, "test-output.txt")
        if not os.path.exists(filepath):
            continue
        with open(filepath) as f:
            lines = f.readlines()

        current_scenario = "Unknown"
        scenario_metrics = {}
        for line in lines:
            sm = SCENARIO_PATTERN.search(line)
            if sm:
                if scenario_metrics:
                    detail = ", ".join(f"{k}: {v}" for k, v in scenario_metrics.items())
                    print(f"- **{framework} {rps}rps {duration}s run{run} — {current_scenario}**: {detail}")
                current_scenario = sm.group(1).strip()
                scenario_metrics = {}
            mm = METRICS_PATTERN.search(line)
            if mm:
                scenario_metrics[mm.group(1).strip()] = float(mm.group(2))
        if scenario_metrics:
            detail = ", ".join(f"{k}: {v}" for k, v in scenario_metrics.items())
            print(f"- **{framework} {rps}rps {duration}s run{run} — {current_scenario}**: {detail}")


def main():
    results_dir = sys.argv[1] if len(sys.argv) > 1 else "results"
    if not os.path.isdir(results_dir):
        print(f"ERROR: Results directory '{results_dir}' not found", file=sys.stderr)
        sys.exit(1)

    configs = parse_results(results_dir)
    if not configs:
        print("WARNING: No benchmark results found to summarize", file=sys.stderr)
        sys.exit(0)

    write_summary(configs, results_dir)


if __name__ == "__main__":
    main()
