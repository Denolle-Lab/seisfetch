"""
Offline (and one live) benchmark suites for seisfetch.

Each suite function returns a plain dict of measurements so that
``benchmarks.runner`` can persist them to JSON. Nothing here prints.

Suites
------
bench_parse        miniSEED parse speed: seisfetch vs ObsPy vs bare pymseed
bench_cold_import  cold ``import seisfetch`` / ``import obspy`` time
bench_memory       peak RSS + tracemalloc peak while parsing a day file
bench_footprint    installed size of seisfetch core vs obspy (fresh venvs)
bench_s3_pull      live S3 day-file pull (seisfetch vs bare boto3) — needs
                   network, only run with ``--live``
"""

from __future__ import annotations

import glob
import io
import json
import os
import shutil
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]

PARSE_FILES = [
    "tests/bench.mseed",
    "tests/test_local.mseed",
    "tests/fixtures/gap_3seg.mseed",
    "tests/fixtures/enc_float32.mseed",
]

# =========================================================================== #
#  Helpers
# =========================================================================== #


def _time_trials(fn, n_trials: int) -> list[float]:
    """Run ``fn`` ``n_trials`` times, return elapsed seconds per trial.

    One untimed warmup call first: lazy imports, JIT'd allocator pools, and
    first-touch page faults belong to neither stack's steady state.
    """
    fn()
    times = []
    for _ in range(n_trials):
        t0 = time.perf_counter()
        fn()
        times.append(time.perf_counter() - t0)
    return times


def _stats(times: list[float], nbytes: int) -> dict:
    tmin = min(times)
    return {
        "min_ms": round(tmin * 1000, 3),
        "mean_ms": round(statistics.mean(times) * 1000, 3),
        "mb_per_s": round((nbytes / 1e6) / max(tmin, 1e-12), 1),
    }


# =========================================================================== #
#  1. Parse
# =========================================================================== #


def _pymseed_bare(raw: bytes) -> list:
    """Bare pymseed decode: from_buffer + copy every segment's samples."""
    from pymseed import MS3TraceList

    out = []
    tl = MS3TraceList.from_buffer(raw, unpack_data=True)
    for tid in tl:
        for seg in tid:
            out.append(seg.np_datasamples.copy())
    return out


def bench_parse(n_trials: int = 5) -> dict:
    """Time seisfetch.parse_mseed vs ObsPy vs bare pymseed on fixed files."""
    from seisfetch.convert import parse_mseed

    results: dict = {}
    for rel in PARSE_FILES:
        path = REPO_ROOT / rel
        raw = path.read_bytes()
        nbytes = len(raw)
        entry: dict = {"bytes": nbytes}

        entry["seisfetch"] = _stats(
            _time_trials(lambda: parse_mseed(raw), n_trials), nbytes
        )
        entry["pymseed_bare"] = _stats(
            _time_trials(lambda: _pymseed_bare(raw), n_trials), nbytes
        )
        try:
            from obspy import read as obspy_read

            entry["obspy"] = _stats(
                _time_trials(lambda: obspy_read(io.BytesIO(raw)), n_trials),
                nbytes,
            )
        except ImportError:
            entry["obspy"] = {"skipped": "obspy not installed"}

        results[rel] = entry
    return results


# =========================================================================== #
#  2. Cold import
# =========================================================================== #


def _cold_import_once(module: str) -> float:
    code = (
        "import time;t=time.perf_counter();"
        f"import {module};print(time.perf_counter()-t)"
    )
    proc = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        cwd=REPO_ROOT,
        check=True,
    )
    return float(proc.stdout.strip().splitlines()[-1])


def bench_cold_import(n_trials: int = 5) -> dict:
    """Cold-start import time of seisfetch and obspy in fresh interpreters."""
    results: dict = {}
    for module in ("seisfetch", "obspy"):
        try:
            times = [_cold_import_once(module) for _ in range(n_trials)]
            results[module] = {
                "min_s": round(min(times), 4),
                "mean_s": round(statistics.mean(times), 4),
            }
        except Exception as e:  # module missing, subprocess failure, ...
            results[module] = {"error": str(e)}
    return results


# =========================================================================== #
#  3. Memory
# =========================================================================== #

_MEM_SCRIPT = """\
import io, json, resource, sys, tracemalloc
path, which = sys.argv[1], sys.argv[2]
raw = open(path, "rb").read()
if which == "seisfetch":
    from seisfetch.convert import parse_mseed

    def work():
        return parse_mseed(raw)
else:
    from obspy import read

    def work():
        return read(io.BytesIO(raw))
tracemalloc.start()
result = work()
_, peak = tracemalloc.get_traced_memory()
tracemalloc.stop()
rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
if sys.platform != "darwin":
    rss *= 1024  # linux reports KiB, macOS reports bytes
print(json.dumps({
    "tracemalloc_peak_mb": round(peak / 1e6, 3),
    "peak_rss_mb": round(rss / 1e6, 1),
}))
"""


def bench_memory(day_file: str = "tests/bench.mseed") -> dict:
    """Peak RSS and tracemalloc peak while parsing a day file, per parser."""
    path = REPO_ROOT / day_file
    results: dict = {"file": day_file, "bytes": path.stat().st_size}
    for which in ("seisfetch", "obspy"):
        try:
            proc = subprocess.run(
                [sys.executable, "-c", _MEM_SCRIPT, str(path), which],
                capture_output=True,
                text=True,
                cwd=REPO_ROOT,
                check=True,
            )
            results[which] = json.loads(proc.stdout.strip().splitlines()[-1])
        except Exception as e:
            results[which] = {"error": str(e)}
    return results


# =========================================================================== #
#  4. Install footprint
# =========================================================================== #


def _venv_install_size_mb(target: str) -> float:
    """Create a temp venv, pip install ``target``, return site-packages MB."""
    tmp = tempfile.mkdtemp(prefix="seisfetch_bench_venv_")
    try:
        subprocess.run(
            [sys.executable, "-m", "venv", tmp],
            capture_output=True,
            text=True,
            check=True,
        )
        pip = os.path.join(tmp, "bin", "pip")
        subprocess.run(
            [pip, "install", "--quiet", target],
            capture_output=True,
            text=True,
            check=True,
            timeout=900,
        )
        site_pkgs = glob.glob(os.path.join(tmp, "lib", "python*", "site-packages"))[0]
        du = subprocess.run(
            ["du", "-sk", site_pkgs], capture_output=True, text=True, check=True
        )
        return round(int(du.stdout.split()[0]) / 1024, 1)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def bench_footprint() -> dict:
    """Installed size (MB) of seisfetch core vs obspy in fresh venvs. Slow."""
    results: dict = {}
    for name, target in (
        ("seisfetch_core", str(REPO_ROOT)),
        ("obspy", "obspy"),
    ):
        try:
            results[name] = {"installed_mb": _venv_install_size_mb(target)}
        except Exception as e:
            results[name] = {"error": str(e)}
    return results


# =========================================================================== #
#  5. Live S3 pull
# =========================================================================== #

_S3_TARGET = {
    "network": "CI",
    "station": "PASC",
    "date": "2022-01-02",
    "year": 2022,
    "doy": 2,
    "channel": "BHZ",
    "location": "00",
}


def bench_s3_pull() -> dict:
    """Pull one SCEDC day file: seisfetch S3OpenClient vs bare boto3."""
    results: dict = {"target": dict(_S3_TARGET)}

    try:
        from seisfetch.s3 import S3OpenClient

        client = S3OpenClient(datacenter="scedc")
        t0 = time.perf_counter()
        raw = client.get_raw(
            _S3_TARGET["network"],
            _S3_TARGET["station"],
            starttime=_S3_TARGET["date"],
            channel=_S3_TARGET["channel"],
            location=_S3_TARGET["location"],
        )
        elapsed = time.perf_counter() - t0
        results["seisfetch"] = {
            "bytes": len(raw),
            "elapsed_s": round(elapsed, 3),
            "mbps": round((len(raw) * 8 / 1e6) / max(elapsed, 1e-9), 1),
        }
    except Exception as e:
        results["seisfetch"] = {"error": str(e)}

    try:
        import boto3
        from botocore import UNSIGNED
        from botocore.config import Config

        from seisfetch.s3 import DATACENTERS, _scedc_key

        dc = DATACENTERS["scedc"]
        key = _scedc_key(
            _S3_TARGET["network"],
            _S3_TARGET["station"],
            _S3_TARGET["year"],
            _S3_TARGET["doy"],
            location=_S3_TARGET["location"],
            channel=_S3_TARGET["channel"],
        )
        s3 = boto3.client(
            "s3",
            region_name=dc["region"],
            config=Config(signature_version=UNSIGNED),
        )
        t0 = time.perf_counter()
        data = s3.get_object(Bucket=dc["bucket"], Key=key)["Body"].read()
        elapsed = time.perf_counter() - t0
        results["boto3_baseline"] = {
            "bytes": len(data),
            "elapsed_s": round(elapsed, 3),
            "mbps": round((len(data) * 8 / 1e6) / max(elapsed, 1e-9), 1),
        }
    except Exception as e:
        results["boto3_baseline"] = {"error": str(e)}

    return results
