"""Focused A/B of pymseed decode paths under cgroup limits.

Only the four cases that matter, interleaved in a shuffled order each round
so host drift and CPU-quota refill cancel instead of biasing whichever case
runs first.

Usage:
  python benchmarks/ab_pymseed_paths.py [REPS] [GLOB]

With no GLOB the single-segment 11 MB Steim2 channel-day in tests/ is used.
Pass a glob (e.g. '/data/earthscope.*.ms') to time the whole set per round
instead — station-day objects carry tens of traceids and segments, and the
per-segment cost structure differs from one big contiguous segment.
"""

import gc
import json
import random
import statistics as st
import sys
import time
from pathlib import Path

import pymseed
from pymseed import MS3TraceList

REPS = int(sys.argv[1]) if len(sys.argv) > 1 else 15
if len(sys.argv) > 2:
    import glob as _glob

    PATHS = sorted(Path(p) for p in _glob.glob(sys.argv[2]))
    if not PATHS:
        sys.exit(f"no files matched {sys.argv[2]!r}")
else:
    PATHS = [Path(__file__).resolve().parent.parent / "tests" / "bench.mseed"]
BUFFERS = [p.read_bytes() for p in PATHS]  # read once; this measures decode
RAW = BUFFERS[0]


def reclist_np(raw):
    tl = MS3TraceList.from_buffer(raw, unpack_data=False, record_list=True)
    return sum(
        seg.create_numpy_array_from_recordlist().shape[0] for tid in tl for seg in tid
    )


def view(raw):
    tl = MS3TraceList.from_buffer(raw, unpack_data=True)
    return sum(seg.np_datasamples.shape[0] for tid in tl for seg in tid)


def take_np(raw):
    tl = MS3TraceList.from_buffer(raw, unpack_data=True)
    return sum(seg.take_np_datasamples().shape[0] for tid in tl for seg in tid)


def copy(raw):
    tl = MS3TraceList.from_buffer(raw, unpack_data=True)
    return sum(seg.np_datasamples.copy().shape[0] for tid in tl for seg in tid)


CASES = {"reclist->np": reclist_np, "view": view, "take_np": take_np, "copy": copy}
if not hasattr(
    next(iter(next(iter(MS3TraceList.from_buffer(RAW, unpack_data=True))))),
    "take_np_datasamples",
):
    del CASES["take_np"]


def run_case(fn):
    """One timed round = the whole file set, so many-segment objects count."""
    return sum(fn(b) for b in BUFFERS)


for fn in CASES.values():  # warm up every case before timing any
    run_case(fn)
    gc.collect()

times = {k: [] for k in CASES}
order = list(CASES)
rng = random.Random(20260816)
for _ in range(REPS):
    rng.shuffle(order)
    for name in order:
        gc.collect()
        t0 = time.perf_counter()
        run_case(CASES[name])
        times[name].append((time.perf_counter() - t0) * 1e3)

nseg = sum(
    len(seg_list)
    for b in BUFFERS
    for seg_list in [[s for t in MS3TraceList.from_buffer(b) for s in t]]
)
out = {
    "pymseed": pymseed.__version__,
    "reps": REPS,
    "files": len(BUFFERS),
    "mb": round(sum(len(b) for b in BUFFERS) / 1e6, 1),
    "segments": nseg,
    "cases": {},
}
print(
    f"pymseed {pymseed.__version__}  reps={REPS}  files={len(BUFFERS)} "
    f"({out['mb']} MB, {nseg} segments)"
)
print(f"{'case':<14}{'min':>8}{'p25':>8}{'median':>8}")
for k, v in times.items():
    v.sort()
    rec = {"min": v[0], "p25": v[len(v) // 4], "median": st.median(v)}
    out["cases"][k] = rec
    print(f"{k:<14}{rec['min']:>8.1f}{rec['p25']:>8.1f}{rec['median']:>8.1f}")
print("JSON " + json.dumps(out))
