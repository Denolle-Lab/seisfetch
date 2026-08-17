"""obspy-free NoisePy data-path adapter (evaluation grade).

Reproduces, in numpy/scipy only, the obspy operations NoisePy's
``preprocess_raw`` (noisepy/seis/noise_module.py:77-231) performs on the
``rm_resp=NO`` path, plus a ``ChannelData``-compatible container and a
``RawDataStore``-shaped S3 store whose URLs come from :mod:`seisfetch.s3`.

Requires numpy + scipy + seisfetch only — install with ``seisfetch[noisepy]``.
This module must never import obspy (enforced by a test). It exists to prove
numerical equivalence of a seisfetch-fed NoisePy pipeline; the footprint and
cold-start numbers in the evaluation report come from a seisfetch-only
environment that never imports noisepy.

The two-env caveat, stated plainly: NoisePy itself imports obspy at module
level, so any process that runs ``noisepy.seis.cross_correlate`` still has
obspy installed. What this adapter demonstrates is that the DATA PATH — read,
gap handling, preprocessing — does not need it; the follow-on NoisePy PR
would adopt these ports behind the existing ``rm_resp`` switch and demote
obspy to an extra.

Port provenance (obspy 1.5.0 sources, verbatim semantics):
  - taper: obspy.core.trace.Trace.taper (hann via scipy, half-length =
    min(pct*npts, max_length*sr, npts/2), sides from hann(2*wlen+1))
  - resample: obspy.core.trace.Trace.resample(no_filter=True) — packed
    scipy.fftpack.rfft spectrum, hann window in frequency, linear spectral
    interpolation, irfft scaling num/npts
  - bandpass: obspy.signal.filter.bandpass — scipy iirfilter sos + sosfilt,
    zerophase = forward + reversed pass
  - merge(method=1, fill_value=0): TraceBundle.to_dict(fill_value=0)
    placement (validated exactly equal on gapped fixtures)
  - trim(pad=True, fill_value=0): nearest-sample cut/pad
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

import numpy as np

from seisfetch.contrib.obspy_ports import (  # noqa: F401  (LGPL-3.0-only file)
    resample_fourier_np,
    taper_np,
)
from seisfetch.convert import TraceArray, TraceBundle

logger = logging.getLogger(__name__)

__all__ = [
    "NpChannelData",
    "check_sample_gaps_np",
    "taper_np",
    "merge_fill0_np",
    "bandpass_np",
    "resample_fourier_np",
    "trim_pad0_np",
    "preprocess_raw_np",
    "SeisfetchS3RawStore",
]


# --------------------------------------------------------------------------- #
#  ChannelData duck type
# --------------------------------------------------------------------------- #


@dataclass
class NpChannelData:
    """Array-backed stand-in for ``noisepy.seis.io.datatypes.ChannelData``.

    Exposes the three attributes NoisePy reads after preprocessing:
    ``data`` (1-D; dtype follows noisepy's chain — float32, or float64 out
    of the resample branch), ``sampling_rate``, ``start_timestamp`` (epoch
    seconds). The one remaining obspy seam in noisepy-seis is
    ``correlate.py:437`` (``ch_data.stream.copy()`` inside ``preprocess``);
    the evaluation harness replaces that call with :func:`preprocess_raw_np`,
    so ``stream`` here raises with a pointer to that seam.
    """

    data: np.ndarray = field(default_factory=lambda: np.empty(0, dtype=np.float32))
    sampling_rate: float = 0.0
    start_timestamp: float = 0.0

    @property
    def stream(self):
        raise NotImplementedError(
            "NpChannelData carries numpy arrays, not an obspy Stream. The one "
            "consumer is noisepy correlate.py:437 (preprocess); route it "
            "through seisfetch.contrib.noisepy_adapter.preprocess_raw_np."
        )

    @staticmethod
    def empty() -> "NpChannelData":
        return NpChannelData()


# --------------------------------------------------------------------------- #
#  Numpy ports of the obspy operations used at rm_resp=NO
# --------------------------------------------------------------------------- #


def check_sample_gaps_np(
    segments: list[TraceArray], start_ns: int, end_ns: int
) -> list[TraceArray]:
    """Port of noisepy ``check_sample_gaps`` + ``portion_gaps``.

    Rejects the whole channel (returns []) when there are no segments, more
    than 100 segments, or the summed inter-segment gap exceeds 30% of the
    requested window. Segments with a lower integer rate than the channel
    max are resampled up (Fourier, like obspy ``tr.resample``); segments
    shorter than 10 samples are dropped.
    """
    if len(segments) == 0 or len(segments) > 100:
        return []

    sr0 = segments[0].sampling_rate
    window_npts = (end_ns - start_ns) / 1e9 * sr0
    pgaps = 0.0
    for a, b in zip(segments[:-1], segments[1:]):
        pgaps += (b.starttime_ns - a.endtime_ns) / 1e9 * a.sampling_rate
    pgaps = pgaps / window_npts if window_npts != 0 else 1.0
    if pgaps > 0.3:
        return []

    freq = max(int(s.sampling_rate) for s in segments)
    out = []
    for s in segments:
        if s.sampling_rate != freq:
            data = resample_fourier_np(
                np.asarray(s.data, dtype=np.float64), s.sampling_rate, freq
            )
            s = TraceArray(
                network=s.network,
                station=s.station,
                location=s.location,
                channel=s.channel,
                starttime_ns=s.starttime_ns,
                sampling_rate=float(freq),
                data=data,
                encoding=s.encoding,
                record_flags=s.record_flags,
            )
        if s.npts >= 10:
            out.append(s)
    return out


def merge_fill0_np(segments: list[TraceArray]) -> tuple[np.ndarray, int]:
    """Port of ``Stream.merge(method=1, fill_value=0)``.

    Returns (merged array, starttime_ns). Placement is delegated to
    ``TraceBundle.to_dict(fill_value=0)`` — validated exactly equal to
    obspy's merge on gapped fixtures; overlaps resolve later-segment-wins.
    """
    bundle = TraceBundle(list(segments))
    (arr,) = bundle.to_dict(fill_value=0).values()
    return arr, segments[0].starttime_ns


def bandpass_np(
    data: np.ndarray,
    freqmin: float,
    freqmax: float,
    df: float,
    corners: int = 4,
    zerophase: bool = True,
) -> np.ndarray:
    """Port of ``obspy.signal.filter.bandpass`` (butterworth, sos)."""
    from scipy.signal import iirfilter, sosfilt

    fe = 0.5 * df
    low = freqmin / fe
    high = freqmax / fe
    if high - 1.0 > -1e-6:
        raise ValueError(
            f"high corner {freqmax} at/above Nyquist {fe}; "
            "match noisepy's pre_filt construction instead"
        )
    if low > 1:
        raise ValueError("low corner above Nyquist")
    sos = iirfilter(corners, [low, high], btype="band", ftype="butter", output="sos")
    firstpass = sosfilt(sos, data)
    if zerophase:
        return sosfilt(sos, firstpass[::-1])[::-1]
    return firstpass


def trim_pad0_np(
    x: np.ndarray, t0_ns: int, sampling_rate: float, start_ns: int, end_ns: int
) -> tuple[np.ndarray, int]:
    """Port of ``Trace.trim(starttime, endtime, pad=True, fill_value=0)``.

    Nearest-sample semantics: the output grid stays on the input's sample
    grid; the window is cut/padded to the nearest sample of [start, end].
    Returns (array, new_t0_ns).
    """
    dt_ns = 1e9 / sampling_rate
    i0 = int(round((start_ns - t0_ns) / dt_ns))
    i1 = int(round((end_ns - t0_ns) / dt_ns))
    npts_out = i1 - i0 + 1
    out = np.zeros(npts_out, dtype=x.dtype)
    src0 = max(i0, 0)
    src1 = min(i1 + 1, x.shape[0])
    if src1 > src0:
        out[src0 - i0 : src0 - i0 + (src1 - src0)] = x[src0:src1]
    return out, t0_ns + int(round(i0 * dt_ns))


def segment_interpolate_np(sig1: np.ndarray, nfric: float) -> np.ndarray:
    """Port of noisepy ``segment_interpolate`` (numba hat-function interp).

    Shifts samples onto integer multiples of the sampling interval when the
    trace start time falls between sample points. The formula mirrors
    noisepy verbatim, including its edge handling.
    """
    sig1 = np.asarray(sig1, dtype=np.float32)
    sig2 = np.empty_like(sig1)
    sig2[0] = sig1[0]
    sig2[-1] = sig1[-1]
    # noisepy: sig2[ii] = (1-nfric)*sig1[ii+1] + nfric*sig1[ii], under numba's
    # "float32[:](float32[:],float32)" signature. Numba's type unification
    # makes (1 - nfric) FLOAT64 (int literal + float32 -> float64) so the
    # first product runs in float64, while nfric*sig1[ii] stays float32; the
    # sum is float64 and the store rounds to float32. Anything else (all-
    # float32, all-float64) is off by 1 ulp on ~30% of samples, which
    # amplifies to ~2e-5 of peak in a stacked CCF.
    # the first product MUST run in float64. Do not write it as
    # `np.float64(...) * sig1[2:]`: under numpy 1.x value-based casting a
    # float64 SCALAR does not upcast a float32 array and the product silently
    # runs in float32 (numpy 2 promotes; the difference is invisible there).
    # The ufunc dtype argument forces float64 on every numpy without an
    # intermediate .astype copy. The second product must stay float32; the
    # sum then upcasts (array-array promotion is version-stable).
    nf32 = np.float32(nfric)
    sig2[1:-1] = (
        np.multiply(sig1[2:], 1.0 - np.float64(nf32), dtype=np.float64)
        + nf32 * sig1[1:-1]
    )
    return sig2


def preprocess_raw_np(
    segments: list[TraceArray],
    start_ns: int,
    end_ns: int,
    freqmin: float,
    freqmax: float,
    sampling_rate: float,
    pretrimmed: bool = True,
) -> NpChannelData:
    """The full ``preprocess_raw`` chain at ``rm_resp=NO``, obspy-free.

    Order mirrors noisepy noise_module.py:128-227: gap check -> per-segment
    nan/inf zeroing, float32 cast, demean, detrend, 5% taper -> merge with
    zero fill -> 5%/50 s taper -> bandpass pre-filter -> Fourier resample if
    needed (with the sub-sample segment_interpolate alignment noisepy runs
    inside that branch) -> trim/pad to [start, end].
    """
    from scipy.signal import detrend

    segments = check_sample_gaps_np(segments, start_ns, end_ns)
    if not segments:
        return NpChannelData.empty()
    sps = int(segments[0].sampling_rate)

    # Runtime guard (2026-08 critique). It applies only to PRE-TRIMMED input:
    # if the caller already cut the segments to the window with
    # TraceBundle.trim (inside-window), a sub-sample offset removes different
    # samples than obspy's nearest-sample trim would, BEFORE detrend/taper/
    # filter, and the outputs diverge. Refuse rather than diverge silently.
    #
    # With pretrimmed=False the caller passes whole segments, exactly what
    # obspy.read hands noisepy, and the only trim is trim_pad0_np at the end
    # of this chain — which IS obspy's nearest-sample trim. Unaligned windows
    # are then fine, and noisepy's own test suite asserts bit-identity on a
    # window 0.218 samples off the grid.
    if pretrimmed:
        dt_ns = 1e9 / segments[0].sampling_rate
        for label, t_ns in (("start", start_ns), ("end", end_ns)):
            off = (t_ns - segments[0].starttime_ns) % dt_ns
            frac = min(off, dt_ns - off) / dt_ns
            if frac > 1e-3:
                raise ValueError(
                    f"window {label} is {frac:.3f} samples off the data grid; "
                    "pre-trimmed input requires sample-aligned windows (obspy's "
                    "chain would trim nearest-sample here and the two paths "
                    "diverge). Pass whole segments with pretrimmed=False, align "
                    "the window to the sample grid, or use the obspy path."
                )

    # pre_filt corners as noise_module.py:118-126 builds them; noisepy makes
    # [f1, f2, f3, f4] but only f1/f4 reach bandpass on the rm_resp=NO path
    f1 = 0.9 * freqmin
    if 1.1 * freqmax > 0.45 * sampling_rate:
        f4 = 0.45 * sampling_rate
    else:
        f4 = 1.1 * freqmax

    cleaned = []
    for s in segments:
        d = np.asarray(s.data, dtype=np.float32)
        d[~np.isfinite(d)] = 0
        d = detrend(d, type="constant")
        d = detrend(d, type="linear")
        d = taper_np(d, s.sampling_rate, max_percentage=0.05)
        cleaned.append(
            TraceArray(
                network=s.network,
                station=s.station,
                location=s.location,
                channel=s.channel,
                starttime_ns=s.starttime_ns,
                sampling_rate=s.sampling_rate,
                data=d,
            )
        )

    merged, t0_ns = merge_fill0_np(cleaned)
    # obspy's miniSEED reader rounds sample times to whole microseconds, and
    # noisepy derives the sub-sample correction below from
    # UTCDateTime.microsecond. pymseed keeps exact nanoseconds, so round here
    # to carry the same start time obspy would: an 80 ns difference shifts
    # nfric by ~1.6e-6 and the interpolated output by ~1e-8 relative, which
    # is small but NOT bit-identical, and bit-identity is the contract.
    t0_ns = int(round(t0_ns / 1000.0)) * 1000
    merged = taper_np(merged, sps, max_percentage=0.05, max_length=50)
    merged = np.float32(bandpass_np(merged, f1, f4, df=sps, corners=4, zerophase=True))

    sr = float(sps)
    if abs(sampling_rate - sps) > 1e-4:
        merged = resample_fourier_np(merged, sps, sampling_rate)
        sr = sampling_rate
        # sub-sample start alignment — noisepy runs this only inside the
        # resample branch (noise_module.py:158-167), so we do too
        delta = 1.0 / sr
        micro = (t0_ns % 1_000_000_000) / 1000.0  # whole microseconds, see above
        fric = micro % (delta * 1e6)
        if fric > 1e-4:
            merged = segment_interpolate_np(
                np.float32(merged), float(fric / (delta * 1e6))
            )
            t0_ns -= int(round(fric * 1000))

    out, out_t0_ns = trim_pad0_np(merged, t0_ns, sr, start_ns, end_ns)
    # keep the chain's natural dtype, mirroring noisepy exactly: float32
    # without resample (the bandpass cast), FLOAT64 after resample (obspy
    # resample returns float64 and noisepy never casts back), float32 again
    # only when the fric branch's segment_interpolate cast fires. Forcing
    # float32 here diverges from noisepy at 6e-8 whenever fric == 0.
    return NpChannelData(
        data=np.asarray(out),
        sampling_rate=sr,
        start_timestamp=out_t0_ns / 1e9,
    )


# --------------------------------------------------------------------------- #
#  RawDataStore-shaped S3 store (URLs owned by seisfetch.s3)
# --------------------------------------------------------------------------- #


class SeisfetchS3RawStore:
    """Duck-typed ``RawDataStore`` reading SCEDC/NCEDC/EarthScope via seisfetch.

    Demonstrates "seisfetch as sole URL owner": no S3 key construction here —
    routing and layouts come from :mod:`seisfetch.s3`. Coordinates are finite
    placeholders (0.0), the pattern noisepy's own h5store uses; they are
    metadata-only for single-station work.

    Interface subset: ``read_data(timespan, channel) -> NpChannelData`` plus
    ``get_timespans/get_channels`` hooks the evaluation harness fills from
    its station list (full catalog support is follow-on work).
    """

    def __init__(self, max_workers: int = 8):
        from seisfetch.s3 import S3OpenClient

        self._client = S3OpenClient(max_workers=max_workers)

    def read_channel(
        self,
        network: str,
        station: str,
        location: str,
        channel: str,
        start_ns: int,
        end_ns: int,
    ) -> list[TraceArray]:
        """Fetch + parse one channel-day; returns time-sorted segments."""
        from seisfetch.convert import parse_mseed

        raw = self._client.get_raw(
            network,
            station,
            start_ns / 1e9,
            end_ns / 1e9,
            location=location or "",
            channel=channel,
        )
        if not raw:
            return []
        bundle = parse_mseed(raw).trim(start_ns, end_ns)
        nslc = f"{network}.{station}.{location or ''}.{channel}"
        return bundle.segments().get(nslc, [])

    def read_data(
        self,
        network: str,
        station: str,
        location: str,
        channel: str,
        start_ns: int,
        end_ns: int,
        freqmin: float,
        freqmax: float,
        sampling_rate: float,
    ) -> NpChannelData:
        segs = self.read_channel(network, station, location, channel, start_ns, end_ns)
        if not segs:
            return NpChannelData.empty()
        return preprocess_raw_np(
            segs, start_ns, end_ns, freqmin, freqmax, sampling_rate
        )
