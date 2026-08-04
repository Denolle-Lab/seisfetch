# Changelog

All notable changes to seisfetch are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions follow
[Semantic Versioning](https://semver.org).

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
