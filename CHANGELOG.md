# Changelog

All notable changes to seisfetch are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions follow
[Semantic Versioning](https://semver.org).

## 0.4.1 — 2026-08-17

The release NoisePy's obspy-free data path depends on: it makes seisfetch
installable alongside noisepy's pinned S3 stack, and closes the last two gaps
between the numpy pre-processing chain and noisepy's obspy one.

### Added

- `contrib.obspy_ports.gps2dist_azimuth_np`: port of
  `obspy.geodetics.base.gps2dist_azimuth` (the Vincenty-inverse branch, the
  one that runs in a default install). noisepy's `cc_parameters` writes
  dist/azi/baz into every saved cross-correlation, so this was the last
  obspy call on the cross-correlation path. Exactly equal to obspy's own
  Vincenty on 2000 random coordinate pairs plus station-like, equatorial,
  identical-point and longitude-wrapping cases; see the geographiclib note
  under Fixed for the one install where "exactly" becomes "to ~5e-6 m".

### Fixed

- `contrib.noisepy_adapter.preprocess_raw_np` now rounds segment start times
  to whole microseconds, as obspy's miniSEED reader does. pymseed keeps exact
  nanoseconds; noisepy derives its sub-sample correction from
  `UTCDateTime.microsecond`, so on a day file starting at
  00:00:00.019537920 the two chains computed `nfric` 1.6e-6 apart and the
  output differed by ~1e-8 relative — small, but not bit-identical, and
  bit-identity is the contract.
- The `gps2dist_azimuth_np` equivalence tests asserted exact equality against
  whichever branch obspy happened to take, so they failed on any machine with
  `geographiclib` installed — obspy delegates to it when present and only
  falls back to its own Vincenty otherwise. The exact-equality tests now skip
  when `HAS_GEOGRAPHICLIB`, and a tolerance test runs either way so coverage
  is not silently lost. The two branches differ by ~5e-6 m over ~500 km, and
  geographiclib spells a due-south back-azimuth `0.0` where Vincenty says
  `360.0`; both are far below what CCF metadata resolves, but it means
  bit-identity holds against a default obspy install rather than every one.
- `preprocess_raw_np(..., pretrimmed=False)` skips the sample-grid alignment
  guard. The guard exists for callers that pre-trim segments with
  `TraceBundle.trim` (inside-window) before pre-processing; a caller that
  passes whole segments — exactly what `obspy.read` hands noisepy — is
  trimmed only by `trim_pad0_np` at the end of the chain, which IS obspy's
  nearest-sample trim, so unaligned windows are handled identically. NoisePy
  asserts bit-identity on a window 0.218 samples off the grid.

### Changed

- `boto3` lower bound relaxed to `>=1.26` (was `>=1.28`). NoisePy cannot
  install seisfetch otherwise: `noisepy-seis-io` pins `s3fs==2023.4.0`,
  which forces `aiobotocore 2.5.2` and `botocore<1.29.162`, a range
  `boto3>=1.28` excludes. `seisfetch/s3.py` uses only `boto3.client`,
  `boto3.Session`, list-objects paginators, `botocore.UNSIGNED` and
  `ClientError` — all long-stable. Verified on boto3 1.26.161: live
  SCEDC/NCEDC/EarthScope day-file pulls and the full precision suite
  (56 passed, including both NoisePy CCF equivalence harnesses,
  bit-identical).

## 0.4.0 — 2026-08-16

Four-archive release: GeoNet joins SCEDC, NCEDC and EarthScope, and the
Earth2Studio integration becomes a live, time-indexed source that fetches on
demand and always returns physical units (PR #5).

### Added

- **GeoNet (New Zealand)** open-data S3 archive: `geonet-open-data`
  (ap-southeast-2), per-channel day objects, wildcard discovery and
  station listing; `NZ` routes there automatically. Verified live.
- **`SeisfetchLiveSource`** (`seisfetch.earth2`): a time-indexed
  Earth2Studio `DataSource` that fetches on demand from the four archives
  with a day-bundle cache — physical units are required (`calibrate="gain"`
  by default via the FDSN station text service; `"response"` for full
  deconvolution via `contrib.response`; raw counts are never returned).
- `ChannelEpoch` + `parse_channel_text` (`seisfetch.fdsn`): stdlib parsing
  of `format=text&level=channel` — gain, coordinates and epochs in one
  HTTP request, no StationXML.
- `SeismicDataFrameSource(auto_coords=True)` fills station lat/lon from
  the FDSN station service automatically.

### Changed

- Earth2Studio adapters' `fetch` is now genuinely async
  (`asyncio.to_thread`), so pipelines can overlap sources.
- NCEDC S3 region corrected to us-west-2 (was us-east-2; worked via
  redirect but paid a cross-region hop per request).

## 0.3.1 — 2026-08-06

Cross-station validation release: the noisepy equivalence evidence now
covers cross-station cross-correlation across the three S3 archives
(SCEDC × NCEDC × EarthScope), bit-identical through the resample and
sub-sample-alignment branches, with instrument response removal
demonstrated obspy-free end to end (PR #4).

### Fixed

- `contrib.noisepy_adapter.segment_interpolate_np` now reproduces noisepy's
  numba mixed-precision semantics exactly on every numpy version
  (`np.multiply(..., dtype=np.float64)`; the previous vectorization was
  1 ulp off on ~30% of samples, ~2e-5 of CCF peak after stacking, and a
  plain float64-scalar expression silently demotes to float32 under
  numpy 1.x value-based casting).
- `contrib.noisepy_adapter.preprocess_raw_np` keeps the chain's natural
  dtype instead of force-casting float32 — noisepy leaves float64 after
  resample whenever the sub-sample (fric) branch does not fire.
- `contrib.response._parse_iso_utc` accepts fractional seconds of any
  width (EarthScope II StationXML carries `.0000`, rejected by
  `fromisoformat` before Python 3.11).

### Added

- Cross-station CCF equivalence harness
  (`benchmarks/noisepy_eval/run_xcorr_eval.py`) with strict bit-identity
  pass criterion, integration test, and a "NoisePy equivalence" section in
  `benchmarks/RESULTS.md`/`.html` (tables + validation figures).
- `notebooks/06_cross_correlation_three_archives.ipynb`: four stations,
  three archives, two months, response removed with `contrib.response`
  (7.6e-16 of peak vs obspy on real data), CCF stack convergence and a
  distance record section.
- Precision bank: numba-semantics reference test for
  `segment_interpolate_np`, resample-branch chain-equivalence case,
  StationXML fraction-width regression test.

## 0.3.0 — 2026-08-04

The obspy-replacement evaluation release: parse rewrite, gap-aware API,
typed failure contract, instrument-response removal, and the full
three-persona external critique with every blocker, correctness major, and
operations finding resolved (`docs/reviews/2026-08-external-critique.md`).

### Changed (breaking or behavior-visible)

- **License** is now the compound expression `MIT AND LGPL-3.0-only`:
  ObsPy-derived numerical translations live in
  `seisfetch/contrib/obspy_ports.py` under LGPL-3.0-only; everything else
  stays MIT. See THIRD_PARTY_NOTICES.md.
- **Failure contract**: fetch paths no longer swallow errors into empty
  bytes. Clean 404s are tolerated per key; any other failure raises
  `FetchError` (`on_error="warn"` opts out); nothing-found raises
  `NoDataError` unless `missing_ok=True`. FDSN maps 204/404 to no-data and
  raises `FDSNError` on real HTTP errors.
- **`location="*"` (the default) now works**: resolved by paginated LIST
  discovery on SCEDC/NCEDC (finds location-coded channels instead of
  guessing blank-location keys); FDSN passes `*` through and spells blank
  as `--`.
- **Day windows are half-open** `[start, end)`: a request ending at
  midnight no longer fetches the following day's objects.
- **`get_numpy`/`get_xarray` trim to the requested window by default**
  (`trim=False` restores whole-object behavior) and filter station-day
  objects to the requested channel/location after parse.
- **`bundle_to_obspy`/`get_waveforms` return one Trace per segment**
  (obspy-read parity; no masked arrays). `merge=1` restores the old
  force-merge.
- **`to_dict()` warns on gappy channels** (the legacy concatenation has a
  wrong time axis after the first gap); `to_dict(fill_value=)` places
  segments at true offsets with obspy `merge(method=1)` overlap semantics.
- **Mixed sampling rates under one NSLC raise `MixedSamplingRateError`**
  from `to_dict`/`metadata` (`segments()` is the per-rate escape hatch).
- **`FDSNMultiClient` defaults to failover** (first non-empty provider
  wins); the old broadcast is `strategy="broadcast"`.
- **`fetch_bulk_numpy` drops raw bytes after parsing** (`keep_raw=True`
  restores; byte accounting preserved).
- **`parse_mseed(collect_flags=False)`**: per-record quality flags are now
  opt-in; `TraceBundle.traces` holds true continuous segments (one per
  contiguous run, not one per miniSEED record) and `num_segments` counts
  real segments.
- pymseed pinned `>=0.6,<0.10` (verified against 0.9.3); the private-API
  sid fallback is isolated behind a guard.

### Added

- **Parse fast path** on libmseed's `MS3TraceList` with decode into
  numpy-owned memory: 11 MB Steim2 channel-day 217 ms (v0.2.0) → ~21 ms,
  faster than `obspy.read` in every measured environment, at ~1/3 the
  parse memory.
- **Gap-aware API**: `TraceBundle.segments()`, `trim()`, `overlaps()`,
  `to_dict(fill_value=)`.
- **`seisfetch.exceptions`**: `SeisfetchError`, `FetchError`,
  `NoDataError`, `FDSNError`, `MixedSamplingRateError`.
- **Instrument response removal without obspy**
  (`seisfetch/contrib/response.py`): evalresp-equivalent evaluator
  (1.6e-10 vs compiled evalresp, including its conditional A0 rule),
  obspy-`remove_response` port (machine precision on real data, ~2x
  faster), SeisIO.jl-style translation, stdlib StationXML parsing, loud
  failures on defective metadata.
- **NoisePy adapter** (`seisfetch/contrib/noisepy_adapter.py`): numpy
  ports of NoisePy's `rm_resp=NO` preprocessing chain, validated exact
  against obspy; end-to-end CCFs through real NoisePy are bit-identical.
- **Operations hardening**: adaptive retries, connect/read timeouts,
  pool sized to fan-out, one shared executor per client, paginated
  listings, EarthScope credential refresh, `iter_bulk_raw` streaming.
- **Benchmarks with committed results** (`benchmarks/results/*.json`,
  `RESULTS.md`), docker machine matrix (Fargate/Lambda-class), CI
  (offline pytest matrix + lint), tutorial notebook
  `notebooks/05_response_removal.ipynb`.

### Fixed

- Silent gap concatenation in `to_dict`/`bundle_to_xarray` time axes.
- Non-deterministic multi-day/multi-provider byte order (`as_completed`
  joins → submission order).
- Contained-segment crash in `to_dict(fill_value=)`.
- Out-of-order contiguous records now heal identically on the fast and
  fallback parse paths; truncated buffers warn about unparsed bytes.
- BG (The Geysers) routed to NCEDC (was SCEDC).
- Timezone-offset timestamps in response epoch selection.

## 0.2.0 — 2026-04-27

Initial public state: S3 clients for EarthScope/SCEDC/NCEDC, FDSN HTTP
client, pymseed-based `parse_mseed`, bulk engine, xarray/zarr/obspy
exporters, Earth2Studio adapters, CLI.
