# External critique — three-persona review of seisfetch

Date: 2026-08-04 · branch `feature/noisepy-eval` @ `2e4f100` · every finding
below was **reproduced by the reviewer** (marked CONFIRMED) unless tagged
SUSPECTED. Reviewers: (1) an obspy core-team RSE, (2) an EarthScope cloud
RSE (pymseed + S3 archive operator perspective), (3) a seismic network
engineer specialized in response metadata and digit-level accuracy.

This file is the deduplicated synthesis; fix order at the end.

**Resolution log:** B1 (licensing) resolved 2026-08-04 — the ObsPy-derived
translations were isolated into `seisfetch/contrib/obspy_ports.py` under
LGPL-3.0-only with SPDX header and derivation notice; project license
expression is now `MIT AND LGPL-3.0-only`; THIRD_PARTY_NOTICES gained a
"Derived code" section covering ObsPy (LGPL), NoisePy (MIT), and SeisIO.jl
(MIT) provenance. Remaining blockers B2-B4 unfixed as of this note.

---

## Blockers

### B1 · Licensing: the obspy ports in `contrib/` are LGPL derivative works shipped as MIT  *(reviewer 1)*
`resample_fourier_np`, `taper_np`, `_npts2nfft`, `invert_spectrum_np`,
`sac_cosine_taper`, `cosine_sac_taper_np` and the `remove_response_np`
pipeline order are variable-renamed translations of obspy source — the
`spec.dtype.type(0)` numpy-quirk dodge is a fingerprint of translation, and
the docstrings say "float-op order mirrors obspy exactly." Translation of
LGPL-3.0 source is a "work based on the Library"; the package metadata says
MIT and THIRD_PARTY_NOTICES does not mention derivation. `bandpass_np`
(standard scipy Butterworth) and the evalresp-mode formulas in `response.py`
(black-box empirical verification — legitimate clean-room practice) are fine.
**Resolve before any PyPI distribution**: either (a) mark those files
LGPL-3.0 with SPDX headers + "Derived from ObsPy" notices and compound the
project license expression, or (b) clean-room rewrite the ports against the
behavioral test bank (which makes reimplementation verifiable without
consulting obspy source) and document the process.

### B2 · Silent-empty-bytes failure contract = silent data loss  *(reviewers 1+2, independently)*
Every fetch path swallows every exception into a log warning and returns
fewer/zero bytes: `s3.py:285-292`, `s3.py:410-416` (auth), `fdsn.py:326-334`
(multi), `fdsn.py:429-441`. 404, 403, throttling-after-retry-exhaustion, and
expired credentials are indistinguishable from a quiet station. Live-proven:
the committed benchmark's own S3 target is a nonexistent key reported as a
successful 0-byte pull (boto3 baseline correctly raises NoSuchKey). For a
mining campaign this manufactures gaps that look like gaps in the ground.

### B3 · The default `location="*"` matches nothing on either backend  *(reviewers 1+2, independently)*
S3: `s3.py:261` collapses `*` to blank-location only — real SCEDC keys are
`CIPASC_BHZ00_...`; the client builds `CIPASC_BHZ___...` → 404 (live HEAD),
swallowed by B2. FDSN: `fdsn.py:186` maps `*` → `loc=--`, which in FDSN
semantics is *blank only* — live-proven 404 on IU.ANMO (works with `00`).
The integration tests pass `location="00"` in ~14 places — working around
the broken default. Most of GSN and CI/BK broadband is location-coded:
the documented default usage returns nothing.

### B4 · Response module: silent failure family on dirty metadata  *(reviewer 3, all CONFIRMED numerically)*
The numerical core is evalresp-equivalent at 1e-15 on clean metadata, but QC
exists for the dirty tail, where failures are consistently silent:
- **A0 recompute is unconditional; evalresp's is conditional.** evalresp
  renormalizes at the stage-gain frequency **only when `NormalizationFrequency
  != StageGain/Frequency`**; when equal it uses the XML A0 as-is. seisfetch
  recomputes always — silently "fixing" stale A0s the reference reproduces
  (2.0× divergence demonstrated on synthetic; digital-PZ case diverged 61×).
  The design-doc claim "evalresp ignores the XML NormalizationFactor" is
  falsified as stated — it ignores it *only* in the fn≠fg branch (which the
  CI.PASC fixture happens to sit in).
- **Timezone-offset timestamps select the wrong epoch**: `[:19]` lexicographic
  compare discards offsets; `2011-11-22T20:00:00-08:00` (after epoch close in
  UTC) returned the closed epoch. A Pacific-time QC box producing
  `datetime.now().astimezone().isoformat()` hits this.
- **Silent all-NaN paths**: gain frequency 0.0 (evalresp errors loudly),
  gain frequency at a spectral zero, zero-sum FIR, and `sac_cosine_taper`
  on npts<20 (0/0 edges) → `remove_response_np` on a 19-sample stub returns
  100% NaN.
- **Absent `InstrumentSensitivity` → silent unity response** in paz mode
  (amplitudes wrong by 3–10 orders); absent sensitivity `Frequency` →
  silent renorm at the 1.0 Hz default (0.26% bias on an STS-1-like epoch;
  degenerate f_sens produced a plausible-looking 6.9e20).
- **Falsy `or` defaults rewrite legal zeros**: `StageGain/Value=0.0`,
  `NormalizationFactor=0.0`, `Frequency=0.0` all silently become 1.0 —
  exactly the broken-metadata sentinels a QC tool should flag.
- **Polynomial / ResponseList stages silently degrade to `GainStage`**
  (SUSPECTED by code-read): a MacLaurin channel would be mis-deconvolved
  without error. IIR-with-denominators correctly raises; extend that.

---

## Major

### Correctness / semantics
- **`to_dict(fill_value=)` crashes on contained segments** (`convert.py:231`
  sizes from the last-by-start segment) — and after fixing the size, the
  later-segment-overwrites policy still deviates from obspy `merge(method=1)`
  for *contained* traces (obspy keeps the surrounding trace). Partial tail
  overlap matches obspy exactly. `merge_fill0_np`/`bundle_to_xarray`/
  `to_zarr` inherit the crash. *(rev 1, CONFIRMED)*
- **Mixed sampling rates under one NSLC**: opaque broadcast crash with fill,
  silent concatenation without, `metadata()` reports the first rate with no
  conflict flag. obspy raises a clear typed error. *(rev 1, CONFIRMED)*
- **Adapter diverges totally on non-sample-aligned windows**: no runtime
  guard enforces the "day files start on integer seconds" assumption;
  a 0.4-sample-offset window produced max abs diff 687.9 filtered counts vs
  the obspy chain (pre-trim removes a sample obspy keeps). Also two trim
  conventions coexist: `TraceBundle.trim` (inside-window) vs `trim_pad0_np`
  (nearest-sample). *(rev 1, CONFIRMED)*
- **Fast-path/fallback topology divergence**: out-of-order contiguous v2
  records → 1 segment via tracelist, 2 via the per-record fallback;
  `check_sample_gaps_np`'s >100-segment rejection can then disagree between
  paths on identical bytes. *(rev 1, CONFIRMED)*
- **Truncated miniSEED buffers silently drop the trailing partial record** —
  a live failure mode for a network-fetch library. *(rev 1, CONFIRMED)*
- **`get_waveforms()` returns masked force-merged Streams** that break
  `filter`/`detrend`/`remove_response` for obspy users on the first gappy
  file; obspy's own `read()` deliberately returns split traces. *(rev 1,
  CONFIRMED)*
- **FDSN no-data raises a raw transport exception** despite requesting
  `nodata=404`; needs the 204/404 mapping + a typed exception. *(rev 1,
  CONFIRMED)*

### Cloud / operations *(reviewer 2; several confirmed live)*
- **No deliberate retry/timeout/backoff anywhere**; anonymous-EarthScope
  route (default for ALL unknown networks, BG mis-routed to SCEDC) can hang
  for minutes with no boto3 timeouts *(rev 1 confirmed a >2 min hang)*.
- **Day-window off-by-one**: inclusive `date_range` + `start+86400` default
  fetches two day objects per one-day request — ~2× GETs and egress at
  campaign scale.
- **Wildcard channel expansion is guess-based GET amplification**: `BH?` →
  5 guessed channels × 2 days = 10 GETs of which ≤3 exist; ~60% waste at
  scale; `BH3`/`BHU` unfetchable.
- **Unpaginated `list_objects_v2`** truncates at 1000 keys (live-proven on a
  SCEDC day prefix) — poisonous as bulk-job discovery input.
- **Thread fan-out vs pool mismatch**: bulk 16 workers × per-call executors
  of 8 = up to 128 concurrent GETs through a 10-connection urllib3 pool.
- **`S3AuthClient`**: credentials fetched once, never refreshed (short-lived
  EarthScope creds die mid-campaign → B2 silences it); hardcoded
  access-point alias as a constant; no region pinning.
- **`FDSNMultiClient` broadcasts to 4 providers and concatenates** —
  duplicate records + 4× load on community services; should be failover.
- **Bulk results accumulate raw bytes + parsed bundles in RAM** (~10–30 GB
  per 1000 day-files); no streaming/chunking/checkpointing.
- **EarthScope station-day objects ignore channel/location filters** after
  parse — all channels returned regardless of request.

### Claims, versioning, sustainability *(reviewers 1+2)*
- **Report traceability**: "every number traces to a committed JSON" is
  violated twice — the native cold-import row shows a lambda-container
  number (committed JSON says 0.068/0.13 s), and no footprint JSON exists
  for the 80/311 MB claim. The arm64-wheel claim was independently verified
  TRUE; the 512 MB RSS pass is real.
- **Version hygiene**: hardcoded 0.2.0 while this branch changes published
  behavior (`get_numpy` trims by default; `to_dict` warns) — needs 0.3.0 +
  CHANGELOG; setuptools-scm declared but unused; zero tags; no release
  workflow; not on PyPI; bus factor 1.
- **pymseed pin `<0.9` already excludes upstream 0.9.3**; no CI leg tests
  the bound; issue draft should be re-verified against 0.9.3 before
  submission (draft otherwise judged fair and accurate by the operator
  persona).
- **CI thin**: claims py≥3.9, tests 3.10/3.12 only; no lint job; no windows
  despite pixi win-64; committed 11 MB fixture is the SAME blob twice
  (bench.mseed = test_local.mseed), overstating benchmark fixture diversity;
  `.DS_Store` and ~15 diagnostic scripts inside `tests/`.
- **README drift**: "37+ FDSN servers" → 33 distinct entries with aliases;
  "No ObsPy required" framing doesn't inherit the report's caveats.

---

## Minor / nits (abbreviated)
`TraceArray.endtime_ns` truncates (−0.84 ns worst; use round) · segment
`record_flags` holds first-record flags only (document or OR-aggregate) ·
`water_level=None` zero-guard deviates (unobservably) from obspy's inf ·
`"--"` location not normalized in response epoch lookup · `output="DEF"`
raises bare KeyError · `'M/S/S'` unit alias rejected · machine-precision
claim should be scoped "with pre_filt" (4.7e-11 without, from evalresp's
float32 internals — seisfetch is the more accurate side) · PEP 639 license
string needs setuptools≥77 vs declared ≥68 floor · benchmark container rows
lack commit sha and true cgroup cpu count · stale "unknown" section in
RESULTS.md.

---

## What all three reviewers praised (verified, not vibes)
The precision methodology — exact equality against obspy on committed
fixtures, harness normalizations documented, bit-identical CCFs through real
NoisePy with committed JSON — was called "the gold standard for replacement
claims" and "the right methodology" by the two engineering reviewers. The
owned-buffer parse path is "the best pymseed usage seen downstream" per the
operator persona, and the container page-fault profiling that found it was
called genuinely novel systems work. The response module's FIR handling
(ODD/EVEN symmetry, multi-rate chains, DC normalization, CorrectionApplied)
and digital-PZ convention were verified correct at 1e-13–1e-16; the A0
fixture design was praised as the reason the evalresp behavior was
discovered at all. The reports' habit of publicly superseding their own
wrong conclusions was singled out twice. Verdict shared by all three:
strong numerical core and unusual measurement discipline; needs a
correctness-on-dirty-inputs pass, an operations pass, and a project shell
(license resolution, releases, second maintainer) — not a rewrite.

---

## Recommended fix order
1. **B1 licensing decision** (blocks any distribution; choose relicense vs
   clean-room now, since it determines whether the ports can be touched
   freely).
2. **B2+B3 failure contract and default semantics** (silent data loss +
   dead defaults; one coherent PR: typed errors/result statuses, location
   wildcard via LIST-discovery or hard error, FDSN `*` passthrough +
   nodata mapping, half-open day windows).
3. **B4 response dirty-metadata pass** (conditional A0 + docstring fix,
   UTC-aware epoch parsing, raise on degenerate renorm/zero-sum FIR/tiny
   taper, `is None` instead of falsy-or, raise on polynomial/ResponseList).
4. **Correctness majors**: contained-segment sizing + containment policy,
   mixed-rate typed error, adapter sub-sample guard, out-of-order fallback
   fixture, truncation warning, get_waveforms merge default.
5. **Operations pass**: retries/timeouts/pool, pagination, fail-fast
   EarthScope routing, credential refresh, failover-not-broadcast, bulk
   streaming.
6. **Project shell**: version bump + CHANGELOG + tags + PyPI + lint CI +
   pymseed 0.9 leg; fix the two report numbers; de-duplicate the 11 MB
   fixture.
