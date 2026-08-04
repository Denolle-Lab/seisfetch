"""Lean instrument-response removal — numpy + stdlib XML, no obspy, no evalresp.

Two evaluation modes over one StationXML subset:

- ``mode="full"`` — evalresp-equivalent: every stage evaluated (analog and
  digital poles/zeros, FIR/Coefficients with DC normalization and the
  ``Decimation/CorrectionApplied`` phase advance, per-stage gains). Formulas
  verified empirically against obspy 1.5.0's compiled evalresp to ~1e-16
  relative per stage (see docs/response-removal-design.md). A0 handling
  follows evalresp's CONDITIONAL rule: when a PZ stage's
  ``NormalizationFrequency`` differs from its ``StageGain/Frequency``, the
  XML ``NormalizationFactor`` is ignored and A0 is recomputed at the gain
  frequency; when they are equal, the XML A0 is used as-is.

Failure posture (2026-08 critique, B4): defective metadata fails LOUDLY.
Zero or missing gains, degenerate normalization references, zero-sum FIR
stages, polynomial/ResponseList stages, and absent sensitivity in paz mode
all raise with the stage number named — never a silent NaN or unity.
- ``mode="paz"`` — SACPZ/SeisIO-style shortcut: stage-1 poles/zeros shape
  (A0-normalized) times the overall InstrumentSensitivity. Accurate to
  <0.1% below ~Nyquist/10 and ~2% at Nyquist/2; the FIR anti-alias
  roll-off near Nyquist is not modeled.

Two deconvolution styles:

- :func:`remove_response_np` — obspy ``Trace.remove_response`` port:
  demean, SAC quarter-cosine taper, rfft at ``_npts2nfft`` length,
  optional ``pre_filt`` raised-cosine on the data spectrum, water-level
  inversion of the response (default 60 dB), multiply, irfft, truncate.
- :func:`translate_resp_np` — SeisIO.jl-style translation: multiply by
  ``H_target * conj(H) / (|H|^2 + eps * max|H|^2)``; no water level, the
  target response's own roll-off provides the stabilization. Target
  ``H=1`` (flat) reproduces SeisIO's ``remove_resp``.

Metadata comes from StationXML (``parse_stationxml_response``) — the format
every FDSN station service returns by default (``level=response``) and that
the SCEDC/NCEDC public buckets mirror.
"""

from __future__ import annotations

import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from datetime import datetime, timezone

import numpy as np

# ObsPy-derived numerical primitives live in obspy_ports (LGPL-3.0-only);
# re-exported here for backward compatibility of the public API
from seisfetch.contrib.obspy_ports import (  # noqa: F401
    _npts2nfft,
    cosine_sac_taper_np,
    invert_spectrum_np,
    sac_cosine_taper,
)

_NS = {"s": "http://www.fdsn.org/xml/station/1"}

# frequency-domain differentiation exponent by quantity
_UNIT_EXPONENT = {"M": 0, "M/S": 1, "M/S**2": 2, "M/S/S": 2}
_UNIT_SCALE = {"CM": 1e2, "MM": 1e3, "NM": 1e9}  # prefix -> SI scale factor
_OUTPUT_EXPONENT = {"DISP": 0, "VEL": 1, "ACC": 2}


@dataclass
class PZStage:
    transfer_type: (
        str  # "LAPLACE (RADIANS/SECOND)" | "LAPLACE (HERTZ)" | "DIGITAL (Z-TRANSFORM)"
    )
    a0: float | None
    poles: np.ndarray
    zeros: np.ndarray
    gain: float
    gain_frequency: float | None = None  # StageGain/Frequency
    normalization_frequency: float | None = None  # XML NormalizationFrequency
    input_sample_rate: float | None = None  # for digital PZ


@dataclass
class FIRStage:
    coefficients: np.ndarray
    gain: float
    input_sample_rate: float
    correction_applied: float  # seconds; the exp(+i*w*corr) phase advance


@dataclass
class GainStage:
    gain: float


@dataclass
class ChannelResponse:
    stages: list = field(default_factory=list)
    sensitivity: float | None = None  # None when the XML has no InstrumentSensitivity
    sensitivity_frequency: float | None = None
    input_units: str = "M/S"  # stage-1 input units, drives (iw)^n and scale

    @property
    def unit_scale(self) -> float:
        prefix = self.input_units.split("/")[0].strip().upper()
        return _UNIT_SCALE.get(prefix, 1.0)

    @property
    def native_exponent(self) -> int:
        units = self.input_units.upper().replace(" ", "")
        for prefix, base in (("CM", "M"), ("MM", "M"), ("NM", "M")):
            if units.startswith(prefix + "/") or units == prefix:
                units = base + units[len(prefix) :]
                break
        try:
            return _UNIT_EXPONENT[units]
        except KeyError:
            raise ValueError(f"unsupported input units {self.input_units!r}")


# --------------------------------------------------------------------------- #
#  StationXML parsing (stdlib only)
# --------------------------------------------------------------------------- #


def _text(el, path, cast=float):
    node = el.find(path, _NS)
    return cast(node.text) if node is not None and node.text is not None else None


def _complex_list(stage_el, tag) -> np.ndarray:
    out = []
    for el in stage_el.findall(f"s:{tag}", _NS):
        out.append(complex(_text(el, "s:Real"), _text(el, "s:Imaginary")))
    return np.asarray(out, dtype=complex)


def _parse_iso_utc(s: str) -> datetime:
    """ISO 8601 -> aware UTC datetime. Naive times are taken as UTC;
    offsets (including the epoch-selection bug class ``-08:00``) are
    honored and converted."""
    s = s.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _norm_loc(loc) -> str:
    """Normalize a location code: None, "", and the FDSN blank spelling
    "--" all mean the blank location."""
    loc = (loc or "").strip()
    return "" if loc == "--" else loc


def parse_stationxml_response(
    xml_bytes: bytes,
    network: str,
    station: str,
    location: str,
    channel: str,
    time_iso: str,
) -> ChannelResponse:
    """Extract one channel epoch's response from StationXML bytes.

    Selects the epoch whose [startDate, endDate] covers ``time_iso``.
    All timestamps are parsed to timezone-aware UTC datetimes; naive
    strings are taken as UTC, and non-UTC offsets are converted (a
    ``[:19]`` string comparison used to select the wrong epoch for
    offset-bearing timestamps at epoch boundaries).
    """
    t = _parse_iso_utc(time_iso)
    root = ET.fromstring(xml_bytes)
    for net in root.findall("s:Network", _NS):
        if net.get("code") != network:
            continue
        for sta in net.findall("s:Station", _NS):
            if sta.get("code") != station:
                continue
            for ch in sta.findall("s:Channel", _NS):
                if ch.get("code") != channel:
                    continue
                if _norm_loc(ch.get("locationCode")) != _norm_loc(location):
                    continue
                start = _parse_iso_utc(ch.get("startDate") or "1900-01-01")
                end_str = ch.get("endDate")
                end = (
                    _parse_iso_utc(end_str)
                    if end_str
                    else datetime.max.replace(tzinfo=timezone.utc)
                )
                if not (start <= t <= end):
                    continue
                return _parse_response(ch.find("s:Response", _NS))
    raise LookupError(
        f"no response epoch for {network}.{station}.{location}.{channel} @ {time_iso}"
    )


def _parse_response(resp_el) -> ChannelResponse:
    out = ChannelResponse()
    sens = resp_el.find("s:InstrumentSensitivity", _NS)
    if sens is not None:
        out.sensitivity = _text(sens, "s:Value")
        if out.sensitivity == 0.0:
            raise ValueError(
                "InstrumentSensitivity/Value is 0.0 — broken metadata "
                "(a zero sensitivity cannot be deconvolved)"
            )
        out.sensitivity_frequency = _text(sens, "s:Frequency")
        units = sens.find("s:InputUnits/s:Name", _NS)
        if units is not None:
            out.input_units = units.text

    for st in sorted(
        resp_el.findall("s:Stage", _NS), key=lambda s: int(s.get("number"))
    ):
        n = st.get("number")
        for bad in ("Polynomial", "ResponseList"):
            if st.find(f"s:{bad}", _NS) is not None:
                raise NotImplementedError(
                    f"stage {n}: {bad} response stages are not supported "
                    "(refusing to silently mis-deconvolve; use obspy for "
                    "polynomial/response-list channels)"
                )
        gain = _text(st, "s:StageGain/s:Value")
        if gain is None:
            raise ValueError(
                f"stage {n}: missing StageGain/Value (evalresp rejects this "
                "metadata too)"
            )
        if gain == 0.0:
            raise ValueError(f"stage {n}: StageGain/Value is 0.0 — broken metadata")
        gain_freq = _text(st, "s:StageGain/s:Frequency")
        pz = st.find("s:PolesZeros", _NS)
        coeff = st.find("s:Coefficients", _NS)
        fir = st.find("s:FIR", _NS)
        deci = st.find("s:Decimation", _NS)
        in_sr = _text(deci, "s:InputSampleRate") if deci is not None else None
        corr = _text(deci, "s:Correction") if deci is not None else None
        if corr is None:
            corr = 0.0

        if pz is not None:
            if int(st.get("number")) == 1:
                units = pz.find("s:InputUnits/s:Name", _NS)
                if units is not None:
                    out.input_units = units.text
            a0 = _text(pz, "s:NormalizationFactor")
            if a0 == 0.0:
                raise ValueError(
                    f"stage {n}: NormalizationFactor is 0.0 — broken metadata"
                )
            tt = _text(pz, "s:PzTransferFunctionType", str) or ""
            if ("DIGITAL" in tt.upper() or "Z-TRANSFORM" in tt.upper()) and (
                in_sr is None
            ):
                raise ValueError(
                    f"stage {n}: digital (Z-transform) PolesZeros stage has "
                    "no Decimation/InputSampleRate — cannot evaluate"
                )
            out.stages.append(
                PZStage(
                    transfer_type=tt,
                    a0=a0,
                    poles=_complex_list(pz, "Pole"),
                    zeros=_complex_list(pz, "Zero"),
                    gain=gain,
                    gain_frequency=gain_freq,
                    normalization_frequency=_text(pz, "s:NormalizationFrequency"),
                    input_sample_rate=in_sr,
                )
            )
        elif coeff is not None:
            nums = [float(n.text) for n in coeff.findall("s:Numerator", _NS)]
            dens = coeff.findall("s:Denominator", _NS)
            if dens:
                raise NotImplementedError("IIR Coefficients stages not supported")
            if nums:
                if in_sr is None:
                    raise ValueError(
                        f"stage {n}: Coefficients stage has no "
                        "Decimation/InputSampleRate — cannot evaluate"
                    )
                out.stages.append(
                    FIRStage(
                        coefficients=np.asarray(nums),
                        gain=gain,
                        input_sample_rate=in_sr,
                        correction_applied=corr,
                    )
                )
            else:  # gain-only stage
                out.stages.append(GainStage(gain))
        elif fir is not None:
            nums = [float(n.text) for n in fir.findall("s:NumeratorCoefficient", _NS)]
            sym = _text(fir, "s:Symmetry", str) or "NONE"
            c = np.asarray(nums)
            if sym.upper() == "ODD":  # c + reversed c[:-1]
                c = np.concatenate([c, c[-2::-1]])
            elif sym.upper() == "EVEN":
                c = np.concatenate([c, c[::-1]])
            if in_sr is None:
                raise ValueError(
                    f"stage {n}: FIR stage has no Decimation/InputSampleRate "
                    "— cannot evaluate"
                )
            out.stages.append(
                FIRStage(
                    coefficients=c,
                    gain=gain,
                    input_sample_rate=in_sr,
                    correction_applied=corr,
                )
            )
        else:
            out.stages.append(GainStage(gain))
    return out


# --------------------------------------------------------------------------- #
#  Response evaluation
# --------------------------------------------------------------------------- #


def evaluate_response(
    freqs: np.ndarray,
    resp: ChannelResponse,
    output: str = "VEL",
    mode: str = "full",
) -> np.ndarray:
    """H(f) in counts per (output unit), on arbitrary frequencies (Hz).

    ``mode="full"``: product over all stages of shape x gain — the
    evalresp-verified formulas. ``mode="paz"``: stage-1 A0-normalized
    poles/zeros shape times the overall InstrumentSensitivity.
    """
    freqs = np.asarray(freqs, dtype=float)
    w = 2.0 * np.pi * freqs

    if mode == "paz":
        if resp.sensitivity is None or resp.sensitivity_frequency is None:
            raise ValueError(
                "paz mode needs InstrumentSensitivity Value AND Frequency; "
                "this StationXML has none (mode='full' uses stage gains and "
                "does not need it)"
            )
        pz = next(s for s in resp.stages if isinstance(s, PZStage))
        shape = _pz_shape(pz, freqs, w)
        # renormalize at the sensitivity frequency: InstrumentSensitivity is
        # DEFINED as |H| there, so the composite must equal it exactly at
        # f_sens. This removes the scalar bias a stale XML A0 would inject
        # and leaves only the genuine FIR ripple as paz-mode error. (This is
        # a deliberate seisfetch choice, distinct from evalresp's
        # conditional rule used in mode='full'.)
        h = (
            shape
            / _ref_magnitude(pz, resp.sensitivity_frequency, "paz")
            * (resp.sensitivity)
        )
    elif mode == "full":
        h = np.ones_like(freqs, dtype=complex)
        for s in resp.stages:
            if isinstance(s, PZStage):
                # evalresp's A0 rule, verified by perturbation experiments
                # (2026-08 critique, B4): the XML NormalizationFactor is
                # used AS-IS when NormalizationFrequency equals the
                # StageGain/Frequency; only when they DIFFER (or A0/f_norm
                # is missing) does evalresp ignore it and renormalize
                # |shape| = 1 at the gain frequency.
                shape = _pz_shape(s, freqs, w)
                recompute = s.gain_frequency is not None and (
                    s.a0 is None
                    or s.normalization_frequency is None
                    or s.normalization_frequency != s.gain_frequency
                )
                if s.a0 is None and s.gain_frequency is None:
                    raise ValueError(
                        "PZ stage has neither NormalizationFactor nor "
                        "StageGain/Frequency — cannot normalize"
                    )
                if recompute:
                    shape = shape / _ref_magnitude(s, s.gain_frequency, "full")
                h = h * shape * s.gain
            elif isinstance(s, FIRStage):
                if s.coefficients.sum() == 0:
                    raise ValueError(
                        "FIR stage coefficients sum to zero — cannot "
                        "DC-normalize (broken metadata)"
                    )
                h = h * _fir_shape(s, w) * s.gain
            else:
                h = h * s.gain
    else:
        raise ValueError(mode)

    out_key = output.upper()
    if out_key == "DEF":
        # native quantity: no frequency-domain differentiation/integration
        return h * resp.unit_scale
    n = resp.native_exponent - _OUTPUT_EXPONENT[out_key]
    if n:
        with np.errstate(divide="ignore", invalid="ignore"):
            h = h * (1j * w) ** n
        if n < 0:
            h[w == 0] = 0.0
    return h * resp.unit_scale


def _ref_magnitude(pz: PZStage, f0: float, context: str) -> float:
    """|shape(f0)| for renormalization, with degeneracy made LOUD.

    A gain/sensitivity frequency of 0.0 on a velocity response (zeros at
    the origin), or one sitting on a spectral zero/pole, makes the
    reference 0 or non-finite — the silent all-NaN family from the
    2026-08 critique. Raise instead."""
    f_arr = np.asarray([float(f0)])
    ref = abs(_pz_shape(pz, f_arr, 2.0 * np.pi * f_arr)[0])
    if not np.isfinite(ref) or ref == 0.0:
        raise ValueError(
            f"cannot renormalize ({context} mode): |PZ shape({f0} Hz)| = "
            f"{ref} — the reference frequency sits on a zero/pole of the "
            "stage (often a bogus 0.0 Hz frequency in the metadata)"
        )
    return ref


def _pz_shape(s: PZStage, freqs: np.ndarray, w: np.ndarray) -> np.ndarray:
    tt = (s.transfer_type or "").upper()
    if "HERTZ" in tt:
        x = 1j * freqs
    elif "DIGITAL" in tt or "Z-TRANSFORM" in tt:
        dt = 1.0 / s.input_sample_rate
        x = np.exp(1j * w * dt)
    else:  # LAPLACE (RADIANS/SECOND)
        x = 1j * w
    num = np.ones_like(x)
    for z in s.zeros:
        num = num * (x - z)
    den = np.ones_like(x)
    for p in s.poles:
        den = den * (x - p)
    a0 = 1.0 if s.a0 is None else s.a0
    with np.errstate(divide="ignore", invalid="ignore"):
        out = a0 * num / den
    # per-bin singularities (poles/zeros at DC or Nyquist) are physical and
    # zeroed; GLOBAL degeneracy is guarded loudly in _ref_magnitude
    return np.nan_to_num(out, nan=0.0, posinf=0.0, neginf=0.0)


def _fir_shape(s: FIRStage, w: np.ndarray) -> np.ndarray:
    # evalresp: DC-normalized coefficient sum, CorrectionApplied phase advance.
    # Accumulate with a running power of exp(-i*w*dt) instead of an
    # (nfreq x ntaps) outer product — same result, O(ntaps) passes over the
    # frequency vector and O(nfreq) memory.
    dt = 1.0 / s.input_sample_rate
    unit = np.exp(-1j * w * dt)
    h = np.zeros_like(unit)
    zk = np.ones_like(unit)
    for c in s.coefficients:
        h += c * zk
        zk *= unit
    return h / s.coefficients.sum() * np.exp(1j * w * s.correction_applied)


# --------------------------------------------------------------------------- #
#  obspy remove_response port (pure numpy)
# --------------------------------------------------------------------------- #


def remove_response_np(
    data: np.ndarray,
    sampling_rate: float,
    resp: ChannelResponse,
    output: str = "VEL",
    water_level: float | None = 60.0,
    pre_filt=None,
    zero_mean: bool = True,
    taper: bool = True,
    taper_fraction: float = 0.05,
    mode: str = "full",
) -> np.ndarray:
    """Port of obspy ``Trace.remove_response`` (non-polynomial path)."""
    x = np.asarray(data, dtype=np.float64).copy()
    npts = x.shape[0]
    if zero_mean:
        x -= x.mean()
    if taper:
        x *= sac_cosine_taper(npts, taper_fraction)

    nfft = _npts2nfft(npts)
    spec = np.fft.rfft(x, n=nfft)
    freqs = np.linspace(0.0, sampling_rate / 2.0, nfft // 2 + 1)
    h = evaluate_response(freqs, resp, output=output, mode=mode)

    if pre_filt is not None:
        spec *= cosine_sac_taper_np(freqs, pre_filt)

    if water_level is None:
        h = h.copy()
        h[0] = 0.0
        nz = np.abs(h[1:]) > 0
        inv = np.zeros_like(h)
        inv[1:][nz] = 1.0 / h[1:][nz]
        h = inv
    else:
        h = invert_spectrum_np(h, water_level)

    spec *= h
    spec[-1] = abs(spec[-1]) + 0.0j
    return np.fft.irfft(spec)[:npts]


# --------------------------------------------------------------------------- #
#  SeisIO.jl-style translation
# --------------------------------------------------------------------------- #


def damped_oscillator_response(
    freqs: np.ndarray, fc: float, damping: float = 1.0 / np.sqrt(2.0)
) -> np.ndarray:
    """SeisIO ``fctoresp``: one zero at origin, damped pole pair at fc."""
    c = damping
    root = np.sqrt(complex(c * c - 1.0))
    p1 = 2 * np.pi * fc * (-c + root)
    p2 = 2 * np.pi * fc * (-c - root)
    s = 1j * 2 * np.pi * np.asarray(freqs, dtype=float)
    h = s / ((s - p1) * (s - p2))
    # normalize |H| = 1 at fc (SeisIO resp_a0!)
    s0 = 1j * 2 * np.pi * fc
    a0 = 1.0 / abs(s0 / ((s0 - p1) * (s0 - p2)))
    return h * a0


def translate_resp_np(
    data: np.ndarray,
    sampling_rate: float,
    resp: ChannelResponse,
    target: np.ndarray | None = None,
    mode: str = "paz",
    output: str = "VEL",
    wl: float = np.finfo(np.float32).eps,
    pre_filt=None,
) -> np.ndarray:
    """SeisIO.jl-style response translation (no water level).

    Multiplies the spectrum by ``H_new * conj(H_old) / (|H_old|^2 + wl*gamma)``
    with ``gamma = max|H_old|^2``. ``target=None`` means flat (full removal,
    SeisIO ``remove_resp``); pass :func:`damped_oscillator_response` values on
    the rfft frequency grid to translate to a common instrument instead.
    ``pre_filt`` (optional 4-corner raised cosine) is applied to the data
    spectrum in the same pass, at the same point of the pipeline as obspy's
    ``remove_response`` — use it when comparing the two one-to-one.
    Caller is responsible for detrend/taper (SeisIO convention).
    """
    x = np.asarray(data, dtype=np.float64)
    npts = x.shape[0]
    nfft = _npts2nfft(npts)
    freqs = np.linspace(0.0, sampling_rate / 2.0, nfft // 2 + 1)

    h_old = evaluate_response(freqs, resp, output=output, mode=mode)
    h_new = np.ones_like(h_old) if target is None else np.asarray(target)

    mag2 = np.abs(h_old) ** 2
    gamma = mag2.max()
    op = h_new * np.conj(h_old) / (mag2 + wl * gamma)

    spec = np.fft.rfft(x, n=nfft)
    if pre_filt is not None:
        spec *= cosine_sac_taper_np(freqs, pre_filt)
    spec *= op
    return np.fft.irfft(spec)[:npts]
