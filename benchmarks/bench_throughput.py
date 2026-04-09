#!/usr/bin/env python
"""
Benchmark suite for seisfetch.

Measures:
  1. Raw S3 download throughput (per datacenter)
  2. miniSEED parse speed: pymseed vs ObsPy
  3. End-to-end: download + parse → numpy
  4. Parallel multi-day download
  5. Bulk request throughput (multi-station)
  6. Cross-datacenter comparison (EarthScope vs SCEDC vs NCEDC)

No ObsPy in the core benchmark path.  ObsPy is only imported
for the parse comparison benchmark.

Usage:
    python -m benchmarks.bench_throughput
    python -m benchmarks.bench_throughput --suite all
    python -m benchmarks.bench_throughput --suite parse
    python -m benchmarks.bench_throughput --suite download \
        --network CI --station SDD --channel BHZ
    python -m benchmarks.bench_throughput --suite bulk
"""

from __future__ import annotations

import argparse
import time

import numpy as np

# =========================================================================== #
#  Utilities
# =========================================================================== #


def _hline(char="=", width=64):
    print(char * width)


def _header(title: str):
    print()
    _hline()
    print(f"  {title}")
    _hline()


def _row(label: str, value, unit: str = ""):
    if isinstance(value, float):
        print(f"  {label:30s}  {value:>12.3f} {unit}")
    else:
        print(f"  {label:30s}  {str(value):>12s} {unit}")


def _trial_rows(trials: list[dict]):
    for t in trials:
        extras = []
        if "throughput_mbps" in t:
            extras.append(f"{t['throughput_mbps']:.1f} Mbps")
        if "total_samples" in t:
            extras.append(f"{t['total_samples']:,} samples")
        extra_s = ", ".join(extras)
        print(f"    Trial {t['trial']:2d}: {t['elapsed_s']:.3f}s  ({extra_s})")


# =========================================================================== #
#  1. Raw S3 download throughput
# =========================================================================== #


def bench_s3_download(
    network, station, start, end, channel="*", datacenter=None, n_trials=3
):
    """Download raw bytes, measure throughput."""
    from seisfetch.client import SeisfetchClient

    client = SeisfetchClient(backend="s3_open", datacenter=datacenter, max_workers=8)
    trials = []
    for i in range(n_trials):
        t0 = time.perf_counter()
        raw = client.get_raw(
            network, station, starttime=start, endtime=end, channel=channel
        )
        elapsed = time.perf_counter() - t0
        nbytes = len(raw)
        trials.append(
            {
                "trial": i + 1,
                "bytes": nbytes,
                "elapsed_s": elapsed,
                "throughput_mbps": (nbytes * 8 / 1e6) / max(elapsed, 1e-9),
            }
        )

    _header(f"S3 Download: {network}.{station} ({datacenter or 'auto'})")
    _row("Object size", trials[0]["bytes"], "bytes")
    _trial_rows(trials)
    _row("Mean throughput", np.mean([t["throughput_mbps"] for t in trials]), "Mbps")
    _row("Mean elapsed", np.mean([t["elapsed_s"] for t in trials]), "s")
    return trials


# =========================================================================== #
#  2. Parse benchmark: pymseed vs ObsPy
# =========================================================================== #


def bench_parse(raw_bytes: bytes, n_trials=5):
    """Compare pymseed and ObsPy parse speed on the same data."""
    import io

    _header(f"miniSEED Parse Comparison ({len(raw_bytes):,} bytes)")

    # -- pymseed (primary) --
    from seisfetch.convert import parse_mseed

    times_py = []
    for _ in range(n_trials):
        t0 = time.perf_counter()
        bundle = parse_mseed(raw_bytes)
        times_py.append(time.perf_counter() - t0)

    total_samples = sum(t.npts for t in bundle.traces)
    n_traces = len(bundle.traces)

    py_mean = np.mean(times_py)
    py_mbps = (len(raw_bytes) * 8 / 1e6) / max(py_mean, 1e-9)
    _row("pymseed mean", py_mean * 1000, "ms")
    _row("pymseed throughput", py_mbps, "Mbps")
    _row("  traces", n_traces)
    _row("  total samples", f"{total_samples:,}")

    # -- ObsPy (optional comparison) --
    try:
        from obspy import read as obspy_read

        times_ob = []
        for _ in range(n_trials):
            t0 = time.perf_counter()
            obspy_read(io.BytesIO(raw_bytes), format="MSEED")
            times_ob.append(time.perf_counter() - t0)
        ob_mean = np.mean(times_ob)
        ob_mbps = (len(raw_bytes) * 8 / 1e6) / max(ob_mean, 1e-9)
        _row("ObsPy mean", ob_mean * 1000, "ms")
        _row("ObsPy throughput", ob_mbps, "Mbps")
        _row("Speedup (pymseed/ObsPy)", ob_mean / max(py_mean, 1e-12), "×")
    except ImportError:
        print("  (ObsPy not installed — skipping comparison)")

    return {
        "pymseed_ms": py_mean * 1000,
        "pymseed_mbps": py_mbps,
        "traces": n_traces,
        "samples": total_samples,
    }


# =========================================================================== #
#  3. End-to-end: download + parse → numpy
# =========================================================================== #


def bench_end_to_end(
    network, station, start, end, channel="*", location="*", datacenter=None, n_trials=3
):
    """Full pipeline: S3/FDSN → raw → pymseed → TraceBundle."""
    from seisfetch.client import SeisfetchClient

    client = SeisfetchClient(backend="s3_open", datacenter=datacenter, max_workers=8)
    trials = []
    for i in range(n_trials):
        t0 = time.perf_counter()
        bundle = client.get_numpy(
            network,
            station,
            starttime=start,
            endtime=end,
            channel=channel,
            location=location,
        )
        elapsed = time.perf_counter() - t0
        npts = sum(t.npts for t in bundle.traces)
        trials.append(
            {
                "trial": i + 1,
                "elapsed_s": elapsed,
                "total_samples": npts,
                "n_traces": len(bundle),
            }
        )

    _header(f"End-to-End (S3 → pymseed → numpy): {network}.{station}")
    _trial_rows(trials)
    _row("Mean elapsed", np.mean([t["elapsed_s"] for t in trials]), "s")
    _row("Mean samples", f"{int(np.mean([t['total_samples'] for t in trials])):,}")
    return trials


# =========================================================================== #
#  4. Parallel multi-day download
# =========================================================================== #


def bench_multi_day(
    network, station, start, n_days, channel="*", max_workers=8, datacenter=None
):
    """Download N consecutive days in parallel."""
    from seisfetch.client import SeisfetchClient
    from seisfetch.utils import to_epoch

    end_epoch = to_epoch(start) + n_days * 86400
    client = SeisfetchClient(
        backend="s3_open", datacenter=datacenter, max_workers=max_workers
    )

    t0 = time.perf_counter()
    bundle = client.get_numpy(
        network, station, starttime=start, endtime=end_epoch, channel=channel
    )
    elapsed = time.perf_counter() - t0
    npts = sum(t.npts for t in bundle.traces)

    _header(f"Parallel {n_days}-Day Download: {network}.{station}")
    _row("Days", n_days)
    _row("Workers", max_workers)
    _row("Elapsed", elapsed, "s")
    _row("Total samples", f"{npts:,}")
    _row("Days/second", n_days / max(elapsed, 1e-9))
    _row("Traces", len(bundle))
    return {"elapsed_s": elapsed, "npts": npts, "days": n_days}


# =========================================================================== #
#  5. Bulk request throughput
# =========================================================================== #


def bench_bulk(requests_spec: list[tuple], max_workers=16):
    """Benchmark get_numpy_bulk with multiple stations."""
    from seisfetch.client import SeisfetchClient

    client = SeisfetchClient(backend="s3_open", max_workers=max_workers)

    _header(f"Bulk Download: {len(requests_spec)} requests, {max_workers} workers")

    t0 = time.perf_counter()
    summary = client.get_numpy_bulk(
        requests_spec, max_workers=max_workers, progress=None
    )
    elapsed = time.perf_counter() - t0

    _row("Total requests", summary.total)
    _row("Succeeded", summary.succeeded)
    _row("Failed", summary.failed)
    _row("Total bytes", f"{summary.total_bytes:,}")
    _row("Wall-clock time", elapsed, "s")
    _row(
        "Aggregate throughput",
        (summary.total_bytes * 8 / 1e6) / max(elapsed, 1e-9),
        "Mbps",
    )
    _row("Requests/second", summary.total / max(elapsed, 1e-9))

    total_samples = 0
    for r in summary.successful_results:
        if r.bundle:
            total_samples += sum(t.npts for t in r.bundle.traces)
    _row("Total samples", f"{total_samples:,}")

    return summary


# =========================================================================== #
#  6. Cross-datacenter comparison
# =========================================================================== #


def bench_cross_datacenter(n_trials=2):
    """Compare download speed across EarthScope, SCEDC, NCEDC."""
    _header("Cross-Datacenter Comparison")

    targets = [
        ("IU", "ANMO", "2024-01-15", "2024-01-15T01:00:00", "*", "earthscope"),
        ("CI", "SDD", "2024-06-01", "2024-06-01T01:00:00", "BHZ", "scedc"),
        ("BK", "BRK", "2024-06-01", "2024-06-01T01:00:00", "BHZ", "ncedc"),
    ]

    results = []
    for net, sta, start, end, cha, dc in targets:
        try:
            trials = bench_s3_download(
                net, sta, start, end, channel=cha, datacenter=dc, n_trials=n_trials
            )
            mean_mbps = np.mean([t["throughput_mbps"] for t in trials])
            results.append(
                {
                    "dc": dc,
                    "net": net,
                    "sta": sta,
                    "mbps": mean_mbps,
                    "bytes": trials[0]["bytes"],
                }
            )
        except Exception as e:
            print(f"  {dc} ({net}.{sta}): FAILED — {e}")
            results.append({"dc": dc, "net": net, "sta": sta, "mbps": 0, "bytes": 0})

    _header("Summary: Cross-Datacenter")
    print(f"  {'Datacenter':12s} {'Net.Sta':10s} {'Size':>12s} {'Throughput':>12s}")
    print(f"  {'-'*12:12s} {'-'*10:10s} {'-'*12:12s} {'-'*12:12s}")
    for r in results:
        size_s = f"{r['bytes']/1e6:.1f} MB" if r["bytes"] else "—"
        mbps_s = f"{r['mbps']:.1f} Mbps" if r["mbps"] else "FAILED"
        print(f"  {r['dc']:12s} {r['net']}.{r['sta']:6s} {size_s:>12s} {mbps_s:>12s}")
    return results


# =========================================================================== #
#  CLI
# =========================================================================== #

SUITES = {
    "download": "Raw S3 download throughput",
    "parse": "miniSEED parse comparison (pymseed vs ObsPy)",
    "e2e": "End-to-end: S3 → pymseed → numpy",
    "multiday": "Parallel multi-day download",
    "bulk": "Bulk multi-station throughput",
    "crossdc": "Cross-datacenter comparison",
    "all": "Run all benchmarks",
}


def main():
    parser = argparse.ArgumentParser(
        description="seisfetch benchmarks",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="Suites:\n" + "\n".join(f"  {k:10s}  {v}" for k, v in SUITES.items()),
    )
    parser.add_argument(
        "--suite",
        default="all",
        choices=list(SUITES.keys()),
        help="Which benchmark suite to run (default: all)",
    )
    parser.add_argument("--network", default="IU")
    parser.add_argument("--station", default="ANMO")
    parser.add_argument("--channel", default="*")
    parser.add_argument("--location", default="*")
    parser.add_argument("--start", default="2024-01-15T00:00:00")
    parser.add_argument(
        "--end", default=None, help="End time (default: start + 1 hour)"
    )
    parser.add_argument(
        "--datacenter", default=None, choices=["earthscope", "scedc", "ncedc"]
    )
    parser.add_argument("--days", type=int, default=3)
    parser.add_argument("--trials", type=int, default=3)
    parser.add_argument("--workers", type=int, default=8)
    args = parser.parse_args()

    start = args.start
    from seisfetch.utils import to_epoch

    end = args.end or str(to_epoch(start) + 3600)

    suite = args.suite
    run_all = suite == "all"

    # ── 1. Download ───────────────────────────────────────────────── #
    raw_bytes = None
    if run_all or suite == "download":
        bench_s3_download(
            args.network,
            args.station,
            start,
            end,
            channel=args.channel,
            datacenter=args.datacenter,
            n_trials=args.trials,
        )

    # ── 2. Parse comparison ───────────────────────────────────────── #
    if run_all or suite == "parse":
        # Need raw data to parse
        if raw_bytes is None:
            from seisfetch.client import SeisfetchClient

            client = SeisfetchClient(backend="s3_open", datacenter=args.datacenter)
            raw_bytes = client.get_raw(
                args.network,
                args.station,
                starttime=start,
                endtime=end,
                channel=args.channel,
            )
        if raw_bytes:
            bench_parse(raw_bytes, n_trials=args.trials)
        else:
            print("\n  (no data available for parse benchmark)")

    # ── 3. End-to-end ─────────────────────────────────────────────── #
    if run_all or suite == "e2e":
        bench_end_to_end(
            args.network,
            args.station,
            start,
            end,
            channel=args.channel,
            location=args.location,
            datacenter=args.datacenter,
            n_trials=args.trials,
        )

    # ── 4. Multi-day ──────────────────────────────────────────────── #
    if (run_all or suite == "multiday") and args.days > 1:
        bench_multi_day(
            args.network,
            args.station,
            start,
            args.days,
            channel=args.channel,
            max_workers=args.workers,
            datacenter=args.datacenter,
        )

    # ── 5. Bulk ───────────────────────────────────────────────────── #
    if run_all or suite == "bulk":
        bulk_requests = [
            ("IU", "ANMO", "00", "BHZ", "2024-01-15", "2024-01-15T01:00:00"),
            ("IU", "ANMO", "00", "BHN", "2024-01-15", "2024-01-15T01:00:00"),
            ("IU", "ANMO", "00", "BHE", "2024-01-15", "2024-01-15T01:00:00"),
        ]
        bench_bulk(bulk_requests, max_workers=args.workers)

    # ── 6. Cross-datacenter ───────────────────────────────────────── #
    if run_all or suite == "crossdc":
        bench_cross_datacenter(n_trials=min(args.trials, 2))

    print("\nBenchmarks complete.")


if __name__ == "__main__":
    main()
