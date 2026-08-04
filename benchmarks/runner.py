"""
Benchmark runner: executes suites from ``benchmarks.suites`` and persists
results to ``benchmarks/results/<tag>_<date>.json``.

Usage:
    pixi run python -m benchmarks.runner --suite parse,cold_import,memory \\
        --tag m1-native
    pixi run python -m benchmarks.runner --suite parse --tag docker-2cpu \\
        --limits "2cpu/4g"
    pixi run python -m benchmarks.runner --suite s3_pull --live

Offline suites run by default. Suites that need live S3 (``s3_pull``) only
run with ``--live``. The ``footprint`` suite is slow (builds two venvs and
hits PyPI), so it only runs when named explicitly in ``--suite``.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import subprocess
import sys
from datetime import date
from pathlib import Path

from benchmarks import suites

REPO_ROOT = Path(__file__).resolve().parents[1]
RESULTS_DIR = Path(__file__).resolve().parent / "results"

SUITE_FUNCS = {
    "parse": suites.bench_parse,
    "cold_import": suites.bench_cold_import,
    "memory": suites.bench_memory,
    "footprint": suites.bench_footprint,
    "s3_pull": suites.bench_s3_pull,
}
LIVE_SUITES = {"s3_pull"}
DEFAULT_SUITES = "parse,cold_import,memory"


def _cgroup_cpus():
    """Effective CPU limit under cgroup v2 (containers), else None.

    os.cpu_count() reports the HOST core count inside a limited container,
    which mislabeled the machine matrix rows (critique hygiene)."""
    try:
        quota, period = open("/sys/fs/cgroup/cpu.max").read().split()
        if quota != "max":
            return round(int(quota) / int(period), 2)
    except OSError:
        pass
    return None


def _git_sha() -> str:
    try:
        proc = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True,
            text=True,
            cwd=REPO_ROOT,
            check=True,
        )
        return proc.stdout.strip()
    except Exception:
        return "unknown"


def _pkg_version(name: str):
    try:
        mod = __import__(name)
        return mod.__version__
    except ImportError:
        return None


def machine_info(tag: str, limits: str | None) -> dict:
    return {
        "tag": tag,
        "platform": platform.platform(),
        "machine": platform.machine(),
        "cpu_count": os.cpu_count(),
        "effective_cpus": _cgroup_cpus(),
        "python": sys.version.split()[0],
        "seisfetch_sha": os.environ.get("SEISFETCH_SHA") or _git_sha(),
        "obspy": _pkg_version("obspy"),
        "pymseed": _pkg_version("pymseed"),
        "numpy": _pkg_version("numpy"),
        "container_limits": limits,
    }


def main():
    parser = argparse.ArgumentParser(description="seisfetch benchmark runner")
    parser.add_argument(
        "--suite",
        default=DEFAULT_SUITES,
        help=f"Comma-separated suites (default: {DEFAULT_SUITES}). "
        f"Available: {', '.join(SUITE_FUNCS)}",
    )
    parser.add_argument("--tag", default="local", help="Machine tag for the JSON name")
    parser.add_argument(
        "--limits", default=None, help='Container limits label, e.g. "2cpu/4g"'
    )
    parser.add_argument(
        "--date", default=date.today().isoformat(), help="Timestamp (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--live", action="store_true", help="Allow suites that hit live S3"
    )
    args = parser.parse_args()

    requested = [s.strip() for s in args.suite.split(",") if s.strip()]
    unknown = [s for s in requested if s not in SUITE_FUNCS]
    if unknown:
        parser.error(f"unknown suite(s): {', '.join(unknown)}")
    if args.live and not any(s in LIVE_SUITES for s in requested):
        requested.extend(sorted(LIVE_SUITES))

    results: dict = {}
    for name in requested:
        if name in LIVE_SUITES and not args.live:
            print(f"[skip] {name}: requires --live")
            results[name] = {"skipped": "requires --live"}
            continue
        print(f"[run ] {name} ...")
        results[name] = SUITE_FUNCS[name]()

    payload = {
        "machine": machine_info(args.tag, args.limits),
        "timestamp": args.date,
        "suites": results,
    }

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    out_path = RESULTS_DIR / f"{args.tag}_{args.date}.json"
    out_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    print(f"[done] wrote {out_path}")


if __name__ == "__main__":
    main()
