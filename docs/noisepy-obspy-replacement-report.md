# Replacing obspy with seisfetch in NoisePy's data path — evaluation report

Date: 2026-08-03 · seisfetch branch `feature/noisepy-eval` · noisepy-seis 0.9.93 ·
obspy 1.5.0 · pymseed 0.8.1

## Verdict

**Justified.** All four go/no-go criteria set before the evaluation pass:

| Criterion | Threshold | Result |
|---|---|---|
| Parse speed | within 2× of `obspy.read` | **faster than obspy in every environment** after the owned-buffer decode fix (21.4 vs 37.2 ms native; ~20 vs ~33 ms in all containers) |
| CCF equivalence | waveform correlation > 0.99999 | **bit-identical** (max abs diff = 0.0 on EN, EZ, NZ, ZZ) |
| dv/v equivalence | same stretching grid cell (±5%, 161 cells) | identical on all pairs |
| Lambda 512 MB | day-file parse+preprocess without OOM | **pass** — 156 MB peak RSS |

Every number in this report traces to a committed JSON under
`benchmarks/results/` or a test in `tests/precision/`.

## Headline numbers (m1-native, `benchmarks/results/m1-native_2026-08-03.json`)

| Metric | seisfetch | obspy stack | Note |
|---|---|---|---|
| Parse 11 MB Steim2 channel-day | **21.4 ms** | 37.2 ms | was 217 ms at v0.2.0; the fix chain: C tracelist assembly, then decoding into a numpy-owned buffer (no borrowed-view copy) |
| Cold `import` | **0.10 s** | 0.24 s | after making transport imports lazy (was 0.27 s) |
| Installed footprint | **80 MB** | 311 MB | pip venvs, `du -s site-packages`; Lambda layer limit is 250 MB — obspy does not fit, seisfetch does |
| arm64 Linux install | wheels only | **requires gcc** | obspy publishes no linux/aarch64 wheels — on Graviton Fargate/Lambda (AWS's cheaper arm64 tier) it must compile from source; seisfetch+pymseed install from wheels |
| Parse peak memory (tracemalloc) | **27.7 MB** | 52.0 MB | 11 MB day file |
| Removable from NoisePy | — | **~145 MB** | obspy 41 + lxml 19 + sqlalchemy 16 + matplotlib stack 59 + pyasdf/prov 9 |

## Precision evidence

The claim that matters — *the science does not change* — is tested at three levels,
all in `tests/precision/`:

1. **Decode identity** (`test_parse_identity.py`): seisfetch segments are
   bit-identical to obspy traces (`np.array_equal`) across Steim2, float32,
   float64, int16 encodings and gapped/overlapping topologies, on the 11 MB
   real SCEDC fixtures and synthetic ones. Two test-harness normalizations are
   documented there: obspy leaves exactly-contiguous record runs split where
   libmseed's trace list joins them, and obspy's float-backed `UTCDateTime`
   rounds start times by ~32 ns; seisfetch's integer nanoseconds are exact.
2. **Preprocessing ports** (`test_preprocess_equivalence.py`): the five obspy
   operations NoisePy's `preprocess_raw` needs at `rm_resp=NO` — hann taper,
   `merge(method=1, fill_value=0)`, zero-phase Butterworth bandpass, Fourier
   resample, `trim(pad=True, fill_value=0)` — are reimplemented in
   numpy/scipy (`seisfetch/contrib/noisepy_adapter.py`) and each asserts
   **exact** equality (`assert_array_equal`, not `allclose`) against obspy on
   identical inputs. The Fourier resample reproduces obspy's
   `scipy.fftpack` recipe to the last ulp, including its float-op order. The
   full chain (gap check → detrend → taper → merge → taper → bandpass →
   resample → trim) is also exactly equal on gapped and float32 fixtures.
   Two dtype subtleties were required and are encoded in tests: obspy's taper
   multiplies in place (float32 stays float32), and merge must not promote
   float32 to float64 when the fill value fits.
3. **End-to-end CCFs through real NoisePy** (`test_ccf_equivalence.py`,
   `benchmarks/noisepy_eval/run_ccf_eval.py`): identical SCEDC bytes
   (CI.PASC 2022-01-02, three components) fed through (A) `obspy.read` +
   noisepy's own `preprocess_raw` and (B) seisfetch `parse_mseed` + the
   adapter, then noisepy's own `compute_fft` and `correlate` for the daily
   EN/EZ/NZ cross-components and ZZ autocorrelation. Result
   (`benchmarks/results/ccf_equivalence_m1_2026-08-03.json`): **max abs
   difference 0.0 — the CCFs are bit-identical**, and stretching dv/v lands
   in the same grid cell on all pairs.

The two-env caveat, honestly: NoisePy imports obspy at module level, so the
equivalence runs necessarily had obspy installed. What the evaluation proves is
that the *data path* never uses it; footprint and cold-start numbers come from
the seisfetch-only environment.

## What had to be fixed in seisfetch first (all on this branch)

The evaluation began by measuring seisfetch v0.2.0 honestly: `parse_mseed` was
**5× slower** than `obspy.read` (one Python dataclass per miniSEED record,
NSLC/flags re-parsed 22k times per day file), `to_dict()` silently concatenated
across gaps with a wrong time axis, S3 multi-day byte order was
non-deterministic (`as_completed` joins), requested time windows were ignored
on the S3 path, and `num_segments` counted records. Fixes:

- parse fast path via libmseed's C trace list (`MS3TraceList`), with the
  per-record path kept as fallback for malformed v2 headers → 217 → 34 ms;
- gap-aware API: `segments()`, `to_dict(fill_value=)` with true sample
  placement (exactly equal to obspy merge), `trim()`, `overlaps()`, and a
  warning on the legacy gap-blind default;
- deterministic submission-order S3/FDSN joins; client-level sample-precise
  window trim (`trim=True` default on `get_numpy`/`get_xarray`);
- `pymseed>=0.6,<0.9` pin and the private-API sid fallback isolated behind a
  guard; lazy transport imports (`import seisfetch` no longer pulls boto3);
- CI (there was none), five small committed fixtures, 20+ new tests.

## Architecture recommendation

1. **seisfetch becomes the sole owner of data-center knowledge.** Today
   `noisepy-io/s3store.py` and `seisfetch/s3.py` both hard-code the
   scedc-pds/ncedc-pds/EarthScope layouts. The adapter's
   `SeisfetchS3RawStore` already demonstrates the target shape: noisepy-io
   stores hold NO key builders and call seisfetch (`route_network()` as the
   routing authority; FDSN waveform fallback via `seisfetch.fdsn`'s 37
   providers — S3 default, FDSN fallback, exactly the desired policy).
2. **Split the "I" out of noisepy-io.** The input layer (RawDataStore +
   catalogs + ChannelData) becomes a thin package depending on seisfetch, with
   no obspy; the output layer (CCF/stack stores; pyasdf-dependent ASDF store)
   stays put. Note obspy is today an *undeclared* direct dependency of both
   noisepy packages — only pyasdf declares it.
3. **NoisePy migration path** (the follow-on PR this report justifies):
   (i) adopt the five numpy ports into `noise_module.py` behind the existing
   `rm_resp` switch; (ii) make `ChannelData` array-backed with `stream` as a
   lazily built compatibility property (its single consumer is
   `correlate.py:437`); (iii) demote obspy to an extra required only for
   `rm_resp != NO` and ASDF output. With `rm_resp=NO`, the whole
   Inventory/StationXML/FDSN branch is dead code and coordinates are
   metadata-only (zero-coordinate precedent already exists in noisepy's own
   h5store).

## Risks

- `rm_resp != NO` still needs obspy (response removal): out of scope here;
  it is the remaining obspy surface after a migration.
- pymseed is young and seisfetch touches one private API (isolated + guarded
  + version-pinned on this branch; upstreaming a public accessor to pymseed
  would remove the risk).
- Overlap semantics: later-segment-overwrites matches obspy on the committed
  overlap fixture; exotic overlap patterns (interpolation_samples != 0) are
  not used by NoisePy and not covered.
- The sub-sample start alignment (`segment_interpolate`) runs only inside
  NoisePy's resample branch; the port mirrors that placement. BH-native
  40 Hz pipelines never hit it; HH→40 Hz pipelines do, and the port is
  covered by the resample tests.
- seisfetch had no CI before this branch; the new workflow runs the offline
  suite on ubuntu+macos, 3.10/3.12.

## Machine matrix

Generated by `benchmarks/docker/run_matrix.sh` (cgroup-limited containers,
linux/arm64 native on the M1 host — ratios are the portable claim; JSONs in
`benchmarks/results/`):

| Machine | Parse 11 MB (sf / obspy, ms) | Cold import (sf / obspy, s) | Peak RSS (MB, both) |
|---|---|---|---|
| m1-native | **21.4** / 37.2 | **0.06** / 0.13 | 148–152 |
| fargate-class (2 cpu / 4 GB) | **20.2** / 32.1 | **0.07** / 0.13 | 156 |
| lambda-1g (0.6 cpu / 1 GB) | **20.2** / 33.7 | **0.13** / 0.26 | 170 |
| lambda-512m (0.5 cpu / 512 MB) | **20.4** / 32.9 | **0.18** / 0.26 | 156 |

Reading the matrix honestly:

- **Both stacks complete a day-file parse in a 512 MB container** (156 MB
  peak RSS) — the Lambda-feasibility question is settled by memory and by
  *installability* (250 MB layer limit + no aarch64 obspy wheels), not speed.
- ~~Under CPU throttling seisfetch's parse loses its native-hardware edge~~
  **Superseded (profiled, fixed).** The container slowdown was *not* "Python
  time per segment" (there are 1–3 segments) and not CPU throttling — a
  component micro-profile (`benchmarks/profile_parse.py`) isolated it to the
  `np_datasamples.copy()` data path: with `unpack_data=True` libmseed decodes
  into a C-owned sample buffer, and the full-size numpy copy of it (a second
  ~35 MB allocation, fresh-page memcpy) cost ~1.3 ms native but 27–74 ms
  under cgroup limits (page-fault/accounting cost in the VM; the C parse
  itself, 21 ms, does not degrade at all). `parse_mseed` now builds the
  trace list with `record_list=True, unpack_data=False` and decodes each
  segment directly into a numpy-owned array
  (`create_numpy_array_from_recordlist`), i.e. one big allocation instead of
  two and no memcpy. Post-fix (min of 7): native 21.1 ms vs obspy 34.1;
  fargate-class (2 cpu/4 GB) 21.1 vs 31.5; lambda-1g 20.5 vs 31.3;
  lambda-512m 20.5 vs 32.6 — seisfetch is now fastest in every cell, and
  peak parse RSS dropped from ~92 MB to ~37 MB over baseline. The machine
  matrix above pre-dates this fix; the parse column is stale pending a
  matrix re-run. A safe zero-copy / bulk accessor upstream in pymseed would
  make this the default path for everyone (issue draft:
  `docs/pymseed-issue-draft.md`).
- Parse is ~1–3% of a NoisePy station-day budget (the 2026 compute audit
  measured 4.3–6.3 s/station-day), so a 25–60 ms swing is negligible against
  the 145 MB dependency cut and the S3 pull time (seconds per file).

## Reproduce

```bash
pixi run pytest tests/precision -q                  # decode + port equivalence
python benchmarks/noisepy_eval/run_ccf_eval.py ...  # CCF bit-identity (needs noisepy env)
pixi run python -m benchmarks.runner --suite parse,cold_import,memory,footprint --tag <tag>
./benchmarks/docker/run_matrix.sh                   # fargate/lambda-class
pixi run python -m benchmarks.render_results        # regenerate RESULTS.md
```
