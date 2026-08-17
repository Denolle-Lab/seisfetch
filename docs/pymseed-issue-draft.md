# GitHub issue draft for EarthScope/pymseed — SUBMITTED

Status: submitted 2026-08-06 as https://github.com/EarthScope/pymseed/issues/6
(with an added "Why this matters downstream" section). RESOLVED 2026-08-07:
Chad Trabant implemented proposal #1 in pymseed 0.9.4 (commit 3e06380, "Hold
the trace list from its data sample views") — the np_datasamples view now
pins the trace list and survives GC, so the copy idiom is unnecessary.
Verified: safe view = 21.8 ms in the 0.5cpu/512m container (was 94.4 ms with
the copy), matching the record-list path; seisfetch tests pass on 0.9.4
(already inside the >=0.6,<0.10 pin). seisfetch stays on the record-list
path for the per-record encoding metadata. Kept here for the record.

### Follow-up: pymseed 0.9.5 and `take_np_datasamples()` (2026-08-16)

Chad released 0.9.5 on 2026-08-07 adding
`MS3TraceSeg.take_np_datasamples()` — transfers the decoded buffer to numpy
with no copy, so the array outlives the trace list without relying on the
0.9.4 keepalive — and asked on the issue for help testing it, recommending it
over `record_list=True` + `create_numpy_array_from_recordlist()` for the
common read-and-get-arrays case. Issue #6 is still open on that request.

Evaluated on 0.9.5 (`benchmarks/profile_parse.py`, cases `tl_unpack+view`
and `tl_unpack+take_np`; same 11.3 MB CI.PASC.00.BHZ Steim2 channel-day,
min of 7, native macOS arm64):

| case | 0.9.4 (ms) | 0.9.5 (ms) |
|---|---|---|
| `record_list=True` → `create_numpy_array_from_recordlist()` | 21.0 | 21.5 |
| `unpack_data=True` → `np_datasamples` (no copy, safe since 0.9.4) | 23.2 | 22.8 |
| `unpack_data=True` → `take_np_datasamples()` | n/a | **22.4** |
| `unpack_data=True` → `np_datasamples.copy()` (old idiom) | 25.0 | 26.8 |
| `seisfetch.parse_mseed` (record-list path) | 20.9 | 21.4 |

Findings:

- **Correct.** `take_np_datasamples()` output is bit-identical to the
  record-list decode on the channel-day, survives `del tracelist` + GC, and
  is `int32` as expected. Semantics are one-shot and destructive: after
  taking, the segment reports `numsamples == 0` and a second take returns an
  empty array.
- **Not faster than the record-list path here.** 22.4 ms vs 21.5 ms natively
  — the two are within noise of each other, with the record-list path
  marginally ahead. Chad's "huge win" framing is against the *copy* idiom and
  the record-list *setup cost*; on this file the record-list build is only
  ~4.5 ms and it is amortized by giving us what we need anyway. The container
  tiers below widen this gap rather than closing it.
- **seisfetch stays on the record-list path.** Not for speed but for
  metadata: we want the per-record encoding, which `record_list=True`
  provides in the same pass. Reaching the same place via `take_np` would mean
  `unpack_data=True, record_list=True` (27.8 ms — slower than either) or
  giving up the encoding.
Under cgroup limits (`benchmarks/ab_pymseed_paths.py`, the four cases
interleaved in shuffled order, 2 containers x 15 reps per version per tier,
same channel-day; Docker Desktop linux/arm64 on M1). Minimum ms — medians are
2-3x higher at the Lambda tier from CPU-quota throttling and are reported in
the script output:

| case | fargate 2cpu/4g, 0.9.4 | 0.9.5 | lambda 0.5cpu/512m, 0.9.4 | 0.9.5 |
|---|---|---|---|---|
| `record_list` → `create_numpy_array_from_recordlist()` | **21.7** | **21.6** | **21.5** | **21.2** |
| `unpack_data=True` → `np_datasamples` (safe view) | 28.1 | 27.7 | 28.1 | 28.3 |
| `unpack_data=True` → `take_np_datasamples()` | n/a | 27.6 | n/a | 28.6 |
| `unpack_data=True` → `np_datasamples.copy()` | 35.0 | 35.2 | 36.4 | 47.6 |

This sharpens the native result rather than overturning it:

- **`take_np_datasamples()` is indistinguishable from the plain 0.9.4 safe
  view** — 27.6 vs 27.7 ms at the Fargate tier, 28.6 vs 28.3 at Lambda.
  Expected in hindsight: since the 0.9.4 keepalive the view is already
  zero-copy, so `take_np` changes *ownership*, not data movement. Its value
  is lifetime semantics — the array no longer pins the trace list, so the
  rest of that memory can be released earlier — not throughput.
- **The record-list path is the fastest in every tier**, and by a wider
  margin than native: ~6-7 ms (about 25 %) ahead of both unpack-then-take
  paths at both tiers, where natively it led by ~1 ms.
- The copy idiom remains the worst path and is the only one that degrades
  with tighter limits, reproducing the original issue's finding.

### Does it change with realistic station-day objects? Yes — it doubles

The tests above use one 11 MB channel-day that merges to a single
trace/segment, which is the friendliest possible case for the unpack paths.
Re-run over real EarthScope station-day objects from the xcorr cache
(`earthscope.II.PFO.all.*`, 44 traceids / ~50 segments each), which is what
the archive actually serves:

| case | fargate 2cpu/4g, 4 files (106 MB, 194 seg) | lambda-1g, 2 files (52 MB, 97 seg) |
|---|---|---|
| | 0.9.4 / 0.9.5 | 0.9.4 / 0.9.5 |
| `record_list` → `create_numpy_array_from_recordlist()` | **216.9 / 215.7** | **155.9 / 151.6** |
| `unpack_data=True` → `np_datasamples` (safe view) | 451.2 / 447.4 | 391.4 / 384.2 |
| `unpack_data=True` → `take_np_datasamples()` | n/a / 450.6 | n/a / 341.6 |
| `unpack_data=True` → `np_datasamples.copy()` | 483.1 / 481.1 | 407.6 / 409.3 |

Minimum ms per round, where a round decodes every file in the set. The
fargate column is 2 containers x 7 reps per version; the lambda-1g column is
a single container x 7 reps.

- **The record-list lead grows from ~1.3x to ~2.1x.** On the single-segment
  file it was 21.6 vs 27.7 ms; on 194-segment station-days it is 216 vs 447.
  The advantage scales with segment count, which fits the mechanism:
  `mstl3_unpack_recordlist` decodes straight into one numpy-owned allocation
  per segment, while `unpack_data=True` decodes into libmseed's own buffers
  and then wraps each segment, paying per-segment overhead that the
  record-list path never incurs.
- **This is the case that matters for us.** EarthScope objects are
  station-days with tens of channels; the single-segment channel-day is the
  SCEDC/NCEDC shape. So the path seisfetch already uses wins by the largest
  margin exactly where our heaviest objects live.
- **One place `take_np` does appear to help**: at lambda-1g with 97 segments
  it came in at 341.6 ms against 384.2 for the plain view, consistent across
  min/p25/median. Plausible mechanism — releasing the trace list earlier
  relieves memory pressure when many segments are live at once. Single run,
  so treat as suggestive rather than established. It is still ~2.2x slower
  than the record-list path.

Caveats on all of the above: Docker Desktop on macOS runs a Linux VM, so this
is cgroup-in-VM rather than bare Linux — the same property the original
issue's measurements had, which makes them comparable to each other but not a
substitute for real Fargate/Lambda. Files are read into memory before timing,
so these measure decode only, not S3 transfer.

Release safety, checked separately: `parse_mseed` output is **bit-identical
between 0.9.4 and 0.9.5** across every fixture (Steim2 day, float32, float64,
int16, 3-segment gap, overlap) — sample SHA-256, npts, dtype, rate and
integer-ns start times all equal. The full unit suite (270 passed) and the
precision suite (53 passed) are green on 0.9.5. The noisepy equivalence
results therefore carry over by construction: path B of both harnesses starts
from `parse_mseed`, whose output does not change. The `>=0.6,<0.10` pin
stands.

---

## Title

`np_datasamples` forces a full-size copy that dominates parse time in
cgroup-limited containers — proposal for a safe owned-array accessor

## Body

### Summary

`MS3TraceSeg.np_datasamples` returns a borrowed view over the C-owned sample
buffer (`np.frombuffer` over an `ffi.buffer`, `mstracelist.py:390` in 0.8.1)
whose lifetime is tied to the `MS3TraceList`. The docstring is explicit that
"if the data are needed beyond the lifetime of this instance, a copy must be
made" (`mstracelist.py:360-364`), so every consumer that keeps the arrays
does `seg.np_datasamples.copy()`.

That copy is nearly free on bare metal but is the single dominant cost in
memory-cgroup-limited containers (Fargate/Lambda-class): it is a second
full-size allocation touched once (fresh-page memcpy), and under a cgroup
plus VM it costs 25–75 ms per 11 MB channel-day — more than the entire
libmseed parse+decode. Meanwhile the library already contains the efficient
alternative (`record_list=True` + `create_numpy_array_from_recordlist()`,
`mstracelist.py:392-431`), but it is marked "for advanced use only"
(`add_buffer` docstring, `mstracelist.py:1155-1156`) and is easy to miss.

### Micro-benchmark

11.3 MB SCEDC Steim2 channel-day (CI.PASC.00.BHZ, 21,984 records, merging to
1 trace / 1 segment, 6,912,000 int32 samples). `time.perf_counter`, min of 7,
pymseed 0.8.1, numpy 2.5, CPython 3.12, arm64. Container = `python:3.12-slim`
(linux/arm64) under Docker with `--cpus=2 --memory=4g`.

| case | native macOS (ms) | container 2cpu/4g (ms) | container 0.5cpu/512m (ms) |
|---|---|---|---|
| `from_buffer(unpack_data=True)` | 24.1 | 21.5 | 21.4 |
| `from_buffer(unpack_data=True, record_list=True)` | 28.6 | 24.0 | 24.6 |
| `from_buffer(record_list=True)` (headers only) | 9.1 | 5.9 | 6.0 |
| `record_list=True` → `create_numpy_array_from_recordlist()` | 21.2 | 20.1 | 20.2 |
| `unpack_data=True` → `np_datasamples.copy()` | 25.4 | **48.3** | **95.2** |
| per-record `MS3Record.from_buffer(unpack_data=True)` loop | 31.8 | 32.6 | 32.8 |
| obspy 1.4 `read()` (reference) | 33.7 | 31.0 | 32.6 |

Observations:

- The C parse/decode itself (`unpack_data=True`, first row) does not degrade
  under cgroup limits at all.
- The **only** component that degrades is the numpy copy of the C buffer:
  +1.3 ms native, +27 ms at 2 cpu/4 GB, +74 ms at 0.5 cpu/512 MB. Two
  full-size buffers (libmseed internal + numpy copy) and a memcpy over fresh
  pages is the difference; peak RSS for the copy path is ~92 MB vs ~37 MB
  for the decode-into-numpy path.
- `create_numpy_array_from_recordlist()` (decode directly into a numpy-owned
  array via `mstl3_unpack_recordlist`) is the fastest correct path in every
  environment — but it requires `record_list=True` (+~4.5 ms and ~6 MB of
  per-record header duplicates for a 22k-record day file) and lives behind
  an "advanced use only" warning.
- `validate_crc=False` is worth ~0.5–0.7 ms on v2 data (no CRCs present).

### Proposal

Any of these would let downstream users hit the fast path without private
API or "advanced" workflows:

1. **Safe owned accessor** — e.g. `MS3TraceSeg.to_numpy()` (or an
   `np_datasamples` keepalive) where the returned array holds a reference to
   the parent `MS3TraceList` (`arr.base` chain or a small owner capsule), so
   the zero-copy view is safe to use after the trace list goes out of scope.
   Zero cost, removes the reason the copy idiom exists.

2. **Bulk decode-to-numpy without a record list** — a mode such as
   `MS3TraceList.from_buffer(buffer, unpack='numpy')` that decodes each
   segment directly into a numpy-owned allocation during the read (what
   `create_numpy_array_from_recordlist()` does, minus the record-list
   requirement), and retains the first-record encoding on the segment so
   callers don't need `record_list=True` just to learn "STEIM2".

3. **Docs** — promote `record_list=True` +
   `create_numpy_array_from_recordlist()` as the recommended
   high-performance buffer workflow; the current "advanced use only" note
   steers users toward `unpack_data=True` + copy, which is the pathological
   pattern in serverless/container deployments.

Happy to share the benchmark script (self-contained, reads one channel-day
file) or PR any of the above.

### Environment

- pymseed 0.8.1 (same API verified present and working in 0.6.0/0.7.0 and in the current release 0.9.3 — the benchmark table numbers are from 0.8.1)
- libmseed (bundled), CPython 3.12.x, numpy 2.5.1
- native: macOS 15 / Apple Silicon; container: Docker `python:3.12-slim`
  linux/arm64 with `--cpus`/`--memory` cgroup limits
