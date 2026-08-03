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
    ``data`` (1-D float32), ``sampling_rate``, ``start_timestamp`` (epoch
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


def taper_np(
    x: np.ndarray,
    sampling_rate: float,
    max_percentage: float = 0.05,
    max_length: float | None = None,
) -> np.ndarray:
    """Port of obspy ``Trace.taper(type='hann', side='both')``."""
    from scipy.signal.windows import hann

    npts = x.shape[0]
    half = [int(max_percentage * npts)]
    if max_length is not None:
        half.append(int(max_length * sampling_rate))
    half.append(int(npts / 2))
    wlen = min(half)

    if 2 * wlen == npts:
        sides = hann(2 * wlen)
    else:
        sides = hann(2 * wlen + 1)
    taper = np.hstack(
        (sides[:wlen], np.ones(npts - 2 * wlen), sides[len(sides) - wlen :])
    )
    if not np.issubdtype(x.dtype, np.floating):
        x = np.require(x, dtype=np.float64)
    # obspy multiplies in place (self.data *= taper), so the input float
    # dtype is preserved — match that exactly (float32 stays float32)
    return (x * taper).astype(x.dtype, copy=False)


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


def resample_fourier_np(
    x: np.ndarray, sr_in: float, sr_out: float, window: str = "hann"
) -> np.ndarray:
    """Port of ``obspy.core.trace.Trace.resample(no_filter=True)``.

    Uses the same packed real FFT (scipy.fftpack) and linear spectral
    interpolation as obspy, so output should match to machine precision.
    """
    from scipy.fftpack import irfft, rfft
    from scipy.signal import get_window

    npts = x.shape[0]
    factor = sr_in / float(sr_out)

    spec = rfft(x.view(x.dtype.newbyteorder("=")))
    spec = np.insert(spec, 1, spec.dtype.type(0))
    if npts % 2 == 0:
        spec = np.append(spec, [0])
    x_r = spec[::2]
    x_i = spec[1::2]

    if window is not None:
        large_w = np.fft.ifftshift(get_window(window, npts))
        x_r = x_r * large_w[: npts // 2 + 1]
        x_i = x_i * large_w[: npts // 2 + 1]

    num = int(npts / factor)
    if num == 0:
        num = 1

    # float-op order mirrors obspy exactly (delta = 1/sr, df = 1/(npts*delta))
    # so results match to the last ulp even for odd npts
    delta = 1.0 / sr_in
    df = 1.0 / (npts * delta)
    d_large_f = 1.0 / num * sr_out
    f = df * np.arange(0, npts // 2 + 1, dtype=np.int32)
    n_large_f = num // 2 + 1
    large_f = d_large_f * np.arange(0, n_large_f, dtype=np.int32)
    large_y = np.zeros(2 * n_large_f)
    large_y[::2] = np.interp(large_f, f, x_r)
    large_y[1::2] = np.interp(large_f, f, x_i)

    large_y = np.delete(large_y, 1)
    if num % 2 == 0:
        large_y = np.delete(large_y, -1)
    return irfft(large_y) * (float(num) / float(npts))


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


def preprocess_raw_np(
    segments: list[TraceArray],
    start_ns: int,
    end_ns: int,
    freqmin: float,
    freqmax: float,
    sampling_rate: float,
) -> NpChannelData:
    """The full ``preprocess_raw`` chain at ``rm_resp=NO``, obspy-free.

    Order mirrors noisepy noise_module.py:128-227: gap check -> per-segment
    nan/inf zeroing, float32 cast, demean, detrend, 5% taper -> merge with
    zero fill -> 5%/50 s taper -> bandpass pre-filter -> Fourier resample if
    needed -> trim/pad to [start, end]. Sub-sample start alignment
    (segment_interpolate) is omitted: S3 archive day files start on integer
    seconds, and the harness asserts this holds for evaluated data.
    """
    from scipy.signal import detrend

    segments = check_sample_gaps_np(segments, start_ns, end_ns)
    if not segments:
        return NpChannelData.empty()
    sps = int(segments[0].sampling_rate)

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
    merged = taper_np(merged, sps, max_percentage=0.05, max_length=50)
    merged = np.float32(bandpass_np(merged, f1, f4, df=sps, corners=4, zerophase=True))

    sr = float(sps)
    if abs(sampling_rate - sps) > 1e-4:
        merged = resample_fourier_np(merged, sps, sampling_rate)
        sr = sampling_rate

    out, out_t0_ns = trim_pad0_np(merged, t0_ns, sr, start_ns, end_ns)
    return NpChannelData(
        data=np.asarray(out, dtype=np.float32),
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
