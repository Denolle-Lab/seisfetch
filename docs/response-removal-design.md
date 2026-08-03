# Lean instrument-response removal without obspy — design and validation

Date: 2026-08-03 · module `seisfetch/contrib/response.py` · tests
`tests/precision/test_response_equivalence.py` · fixture
`tests/fixtures/CI_PASC_00_BHZ.xml` (two real CI.PASC.00.BHZ epochs)

## Goal

Response removal is the one obspy capability the NoisePy migration still
needed. This module provides it in ~450 lines of numpy + stdlib XML: an
evalresp-equivalent response evaluator, an obspy-`remove_response` port, and a
SeisIO.jl-style translation operator. Dependencies: numpy (scipy nowhere in
this module). No evalresp C library, no obspy, no lxml.

## Validation results (all measured)

| Test | Result |
|---|---|
| `evaluate_response(mode="full")` vs compiled evalresp (both epochs, VEL/ACC/DISP, 1 mHz–19.9 Hz) | max rel diff **1.6e-10** |
| `remove_response_np` vs `Trace.remove_response` (real 6.9M-sample Tohoku day, water_level=60, pre_filt) | max diff **6.6e-16 of peak** — machine precision |
| Speed on that day | **1.9 s** vs obspy 3.6 s |
| `mode="paz"` (stage-1 PZ x sensitivity) error vs full | 0.7–1.3% below 4 Hz; ~23% by 16 Hz; unusable ≥ 16 Hz (FIR roll-off unmodeled) |

## What obspy/evalresp actually does (dissected + verified by perturbation)

`Trace.remove_response` = demean → SAC quarter-cosine taper (5% total) →
rfft at `_npts2nfft(npts)` → evaluate H on the rfft grid → optional
`pre_filt` raised-cosine applied to the DATA spectrum → water-level inversion
of H (clip |H| below `max|H|·10^(-wl/20)`, phase preserved, then 1/H) →
multiply → force Nyquist bin real → irfft → truncate. All ported verbatim.

Response evaluation is compiled evalresp (no pure-Python path exists in
obspy). Per-stage formulas, verified empirically to ~1e-16 per stage:

- analog PZ (rad/s): `H = A0·Π(iω−z)/Π(iω−p)`; (Hz): same with `s = i·f`
- digital PZ: `z = exp(+iω·dt)`
- FIR/Coefficients: `Σ c_k·exp(−iω·k·dt) / Σ c_k` (DC-normalized) times
  `exp(+iω·CorrectionApplied)` — `Decimation/Delay` is ignored
- stage gains multiply; **InstrumentSensitivity is never applied** (cross-check
  only); units via `H·(iω)^(n_native − n_requested)`; CM/MM/NM prefixes scale
  by 1e2/1e3/1e9

**The discovery that mattered:** evalresp **ignores the XML
`NormalizationFactor` and `NormalizationFrequency` entirely** and recomputes
A0 so that |PZ shape| = 1 at the **stage gain's frequency**. Proven by
perturbation (doubling A0 or changing f_norm: no effect) and by the CI.PASC
2007 epoch, where f_norm (0.03 Hz) ≠ gain frequency (1.0 Hz) and the recompute
reproduces evalresp to 9 digits (ratio 0.997788169). Implementations that use
the XML A0 as given (SeisIO light mode, naive SACPZ) inherit whatever error
the metadata author left — 0.22% on this real channel.

## SeisIO.jl comparison (from-scratch prior art)

SeisIO's `translate_resp!`/`remove_resp!` avoid the water level by
*translating*: multiply the spectrum by
`H_new·conj(H_old)/(|H_old|² + wl·max|H_old|²)` with `wl ≈ eps(Float32)` —
stabilization comes from the target response's own roll-off (damped-oscillator
`fctoresp(fc, damping=1/√2)`), not from spectral clipping. PZ-only (FIR
stages never evaluated), sensitivity applied separately, caller
detrends/tapers. Ported here as `translate_resp_np` +
`damped_oscillator_response`; band-limited comparison against water-level
removal agrees to corr > 0.999 in the passband.

## Choosing a mode

- **`mode="full"`** whenever StationXML with all stages is available — it IS
  evalresp, to 1e-10, and costs nothing extra.
- **`mode="paz"`** for SACPZ-style metadata or when only stage-1 PZ +
  sensitivity exist. Fine below ~Nyquist/10 (classic 0.05–4 Hz monitoring
  bands); do not use above ~Nyquist/3.
- **Water level (obspy-compatible)** for drop-in equivalence;
  **translation (SeisIO-compatible)** when you want a common target
  instrument across a network instead of flat-to-count deconvolution.

## Metadata path

`parse_stationxml_response(xml_bytes, net, sta, loc, cha, time_iso)` reads the
needed subset with stdlib `xml.etree`: PolesZeros (type, poles, zeros),
StageGain (value + frequency — the frequency is load-bearing, see A0 above),
Coefficients/FIR numerators (+ Symmetry expansion), Decimation
InputSampleRate + Correction, InstrumentSensitivity, stage-1 InputUnits.
FDSN station services return exactly this by default at `level=response`, and
SCEDC/NCEDC mirror the XMLs in their public buckets — one small GET per
station, epoch selection included.

## Limitations (explicit)

- Not implemented: IIR `Coefficients` stages with denominators, polynomial
  (blockette-62) responses, `ResponseList` stages — all raise or are absent.
  Rare in modern broadband/strong-motion metadata; add on demand.
- RESP / SACPZ file parsing not included (StationXML only). SACPZ users can
  build a `ChannelResponse` with one `PZStage` + sensitivity by hand.
- The evalresp A0-recompute is applied per PZ stage only when
  `StageGain/Frequency` is present (matching evalresp's requirement that the
  gain blockette exist).

## Follow-on

Wire into the NoisePy adapter as the `rm_resp="inv"` equivalent: with this
module, the migration's "obspy stays for response removal" caveat disappears
and the obspy extra becomes needed only for ASDF output via pyasdf.
