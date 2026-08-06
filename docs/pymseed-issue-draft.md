# GitHub issue draft for EarthScope/pymseed — SUBMITTED

Status: submitted 2026-08-06 as https://github.com/EarthScope/pymseed/issues/6
(with an added "Why this matters downstream" section). Kept here for the record.

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
