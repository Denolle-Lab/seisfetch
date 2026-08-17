"""Focused A/B of pymseed decode paths under cgroup limits.

Only the four cases that matter, interleaved in a shuffled order each round
so host drift and CPU-quota refill cancel instead of biasing whichever case
runs first.
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

RAW = (Path(__file__).resolve().parent.parent / "tests" / "bench.mseed").read_bytes()
REPS = int(sys.argv[1]) if len(sys.argv) > 1 else 15


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

for fn in CASES.values():  # warm up every case before timing any
    fn(RAW)
    gc.collect()

times = {k: [] for k in CASES}
order = list(CASES)
rng = random.Random(20260816)
for _ in range(REPS):
    rng.shuffle(order)
    for name in order:
        gc.collect()
        t0 = time.perf_counter()
        CASES[name](RAW)
        times[name].append((time.perf_counter() - t0) * 1e3)

out = {"pymseed": pymseed.__version__, "reps": REPS, "cases": {}}
print(f"pymseed {pymseed.__version__}  reps={REPS}")
print(f"{'case':<14}{'min':>8}{'p25':>8}{'median':>8}")
for k, v in times.items():
    v.sort()
    rec = {"min": v[0], "p25": v[len(v) // 4], "median": st.median(v)}
    out["cases"][k] = rec
    print(f"{k:<14}{rec['min']:>8.1f}{rec['p25']:>8.1f}{rec['median']:>8.1f}")
print("JSON " + json.dumps(out))
