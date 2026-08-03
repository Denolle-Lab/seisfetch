"""Micro-profile of parse_mseed components vs obspy.read.

Times each layer of the pymseed/libmseed parse pipeline separately so we can
see which component degrades under cgroup limits (container) vs native.

Usage:
    python -m benchmarks.profile_parse                # timing table (min of 7)
    python -m benchmarks.profile_parse --rss CASE     # run one case, print RSS
    python -m benchmarks.profile_parse --ownership    # np_datasamples probe

CASE is one of the names printed in the table.
"""

from __future__ import annotations

import gc
import io
import resource
import sys
import time
from pathlib import Path

import numpy as np

BENCH = Path(__file__).resolve().parent.parent / "tests" / "bench.mseed"
REPEATS = 7


def _load() -> bytes:
    return BENCH.read_bytes()


# --------------------------------------------------------------------------- #
# cases — each returns a callable taking raw bytes
# --------------------------------------------------------------------------- #


def case_tracelist_unpack(raw):
    from pymseed import MS3TraceList

    tl = MS3TraceList.from_buffer(raw, unpack_data=True)
    return sum(seg.numsamples for tid in tl for seg in tid)


def case_tracelist_unpack_nocrc(raw):
    from pymseed import MS3TraceList

    tl = MS3TraceList.from_buffer(raw, unpack_data=True, validate_crc=False)
    return sum(seg.numsamples for tid in tl for seg in tid)


def case_tracelist_unpack_reclist(raw):
    from pymseed import MS3TraceList

    tl = MS3TraceList.from_buffer(raw, unpack_data=True, record_list=True)
    return sum(seg.numsamples for tid in tl for seg in tid)


def case_tracelist_reclist_only(raw):
    from pymseed import MS3TraceList

    tl = MS3TraceList.from_buffer(raw, unpack_data=False, record_list=True)
    return sum(seg.samplecnt for tid in tl for seg in tid)


def case_tracelist_reclist_np(raw):
    """record_list only, then decode straight into numpy-owned arrays."""
    from pymseed import MS3TraceList

    tl = MS3TraceList.from_buffer(raw, unpack_data=False, record_list=True)
    n = 0
    for tid in tl:
        for seg in tid:
            arr = seg.create_numpy_array_from_recordlist()
            n += arr.shape[0]
    return n


def case_tracelist_unpack_copy(raw):
    """unpack in C, then np view + copy per segment (current data path)."""
    from pymseed import MS3TraceList

    tl = MS3TraceList.from_buffer(raw, unpack_data=True)
    n = 0
    for tid in tl:
        for seg in tid:
            arr = seg.np_datasamples.copy()
            n += arr.shape[0]
    return n


def case_parse_mseed(raw):
    from seisfetch.convert import parse_mseed

    b = parse_mseed(raw)
    return sum(t.npts for t in b.traces)


def case_parse_mseed_flags(raw):
    from seisfetch.convert import parse_mseed

    b = parse_mseed(raw, collect_flags=True)
    return sum(t.npts for t in b.traces)


def case_parse_records_fallback(raw):
    from seisfetch.convert import _parse_records

    b = _parse_records(raw, False)
    return sum(t.npts for t in b.traces)


def case_records_unpack(raw):
    from pymseed import MS3Record

    n = 0
    for msr in MS3Record.from_buffer(raw, unpack_data=True):
        n += msr.numsamples
    return n


def case_records_headers(raw):
    from pymseed import MS3Record

    n = 0
    for _ in MS3Record.from_buffer(raw, unpack_data=False):
        n += 1
    return n


def case_first_record(raw):
    from pymseed import MS3Record

    msr = next(iter(MS3Record.from_buffer(raw, unpack_data=False)))
    return msr.encoding


def case_obspy_read(raw):
    from obspy import read

    st = read(io.BytesIO(raw), format="MSEED")
    return sum(tr.stats.npts for tr in st)


CASES = {
    "tl_unpack": case_tracelist_unpack,
    "tl_unpack_nocrc": case_tracelist_unpack_nocrc,
    "tl_unpack+reclist": case_tracelist_unpack_reclist,
    "tl_reclist_only": case_tracelist_reclist_only,
    "tl_reclist->np": case_tracelist_reclist_np,
    "tl_unpack+copy": case_tracelist_unpack_copy,
    "parse_mseed": case_parse_mseed,
    "parse_mseed_flags": case_parse_mseed_flags,
    "parse_records_fb": case_parse_records_fallback,
    "rec_iter_unpack": case_records_unpack,
    "rec_iter_headers": case_records_headers,
    "first_record": case_first_record,
    "obspy_read": case_obspy_read,
}


def run_table():
    raw = _load()
    print(f"file: {BENCH.name}  size={len(raw)/1e6:.1f} MB")
    from pymseed import MS3TraceList

    tl = MS3TraceList.from_buffer(raw, unpack_data=True)
    nseg = sum(len(tid) for tid in tl)
    print(f"traceids={len(tl)} segments={nseg}")
    del tl
    print(f"{'case':<20} {'min ms':>9} {'mean ms':>9}")
    for name, fn in CASES.items():
        try:
            fn(raw)  # warmup
        except Exception as exc:
            print(f"{name:<20} FAILED: {exc}")
            continue
        times = []
        for _ in range(REPEATS):
            gc.collect()
            t0 = time.perf_counter()
            fn(raw)
            times.append((time.perf_counter() - t0) * 1e3)
        print(f"{name:<20} {min(times):>9.2f} {sum(times)/len(times):>9.2f}")


def run_rss(case: str):
    raw = _load()
    fn = CASES[case]
    before = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    fn(raw)
    after = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    scale = 1024 if sys.platform == "darwin" else 1  # ru_maxrss: B on mac, KiB on linux
    print(
        f"{case}: peak RSS {after/1024/scale*1024/1024:.1f} MiB "
        f"(delta {(after-before)/1024/scale*1024/1024:.1f} MiB)"
    )


def run_ownership():
    """Does np_datasamples survive the MS3TraceList? (no-copy safety probe)"""
    from pymseed import MS3TraceList

    raw = _load()
    tl = MS3TraceList.from_buffer(raw, unpack_data=True)
    views = []
    for tid in tl:
        for seg in tid:
            views.append(seg.np_datasamples)  # no copy
    checks = [v[:1000].copy() for v in views]
    del tl
    gc.collect()
    # scribble over the allocator to expose use-after-free
    junk = [np.random.randint(0, 2**31, 1 << 20, dtype=np.int32) for _ in range(64)]
    changed = any(not np.array_equal(v[:1000], c) for v, c in zip(views, checks))
    del junk
    print(f"np_datasamples values changed after del tracelist: {changed}")
    print(
        "=> view is NOT safe without keeping the tracelist alive"
        if changed
        else "=> values unchanged in this run (still unsafe by contract)"
    )


if __name__ == "__main__":
    if "--rss" in sys.argv:
        run_rss(sys.argv[sys.argv.index("--rss") + 1])
    elif "--ownership" in sys.argv:
        run_ownership()
    else:
        run_table()
