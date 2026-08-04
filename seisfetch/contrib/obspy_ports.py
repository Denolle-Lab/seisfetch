# SPDX-License-Identifier: LGPL-3.0-only
#
# Derived from ObsPy (https://github.com/obspy/obspy),
# Copyright (C) The ObsPy Development Team (devs@obspy.org),
# GNU Lesser General Public License, Version 3.
#
# The functions in THIS FILE are Python translations of ObsPy routines
# (obspy.core.trace.Trace.resample/taper, obspy.signal.invsim.cosine_taper /
# cosine_sac_taper / invert_spectrum, obspy.signal.util._npts2nfft),
# preserving their exact numerical behavior including float-operation order.
# They are therefore works based on the Library and are distributed under
# LGPL-3.0, unlike the rest of seisfetch (MIT). See THIRD_PARTY_NOTICES.md.
"""Numerically exact ports of ObsPy signal-processing primitives (LGPL-3.0).

Every function here is validated against ObsPy by exact equality
(``np.testing.assert_array_equal``) in ``tests/precision/``. Keeping them in
one file keeps the LGPL surface of the package minimal and explicit.
"""

from __future__ import annotations

import numpy as np

__all__ = [
    "taper_np",
    "resample_fourier_np",
    "_npts2nfft",
    "sac_cosine_taper",
    "cosine_sac_taper_np",
    "invert_spectrum_np",
]


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


def resample_fourier_np(
    x: np.ndarray, sr_in: float, sr_out: float, window: str = "hann"
) -> np.ndarray:
    """Port of ``obspy.core.trace.Trace.resample(no_filter=True)``.

    Uses the same packed real FFT (scipy.fftpack) and linear spectral
    interpolation as obspy, so output matches to machine precision.
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


def _npts2nfft(npts: int) -> int:
    """Port of obspy.signal.util._npts2nfft."""
    nfft = 2 * npts if npts % 2 == 0 else 2 * (npts + 1)

    def max_prime(n):
        f = 2
        largest = 1
        while f * f <= n:
            while n % f == 0:
                largest, n = f, n // f
            f += 1
        return max(largest, n) if n > 1 else largest

    if nfft > 5000 and max_prime(nfft) >= 500:
        for cand in range(nfft + 2, nfft + 22, 2):
            if max_prime(cand) < 500:
                return cand
        return int(2 ** np.ceil(np.log2(nfft)))
    return nfft


def sac_cosine_taper(npts: int, p: float = 0.05) -> np.ndarray:
    """Port of obspy cosine_taper(..., sactaper=True, halfcosine=False).

    Degenerate short segments (taper half-width collapsing to a single
    sample, npts*p/2 < 1) used to produce 0/0 -> NaN edges that silently
    propagated through the FFT; the guards below pin those edge samples to
    obspy's observed values instead.
    """
    frac = int(npts * p / 2.0 + 0.5)
    idx1, idx2 = 0, frac - 1
    idx3, idx4 = npts - frac, npts - 1
    idx2 += 1
    idx3 -= 1
    w = np.ones(npts)
    if idx2 > idx1:
        k = np.arange(idx1, idx2 + 1)
        w[idx1 : idx2 + 1] = np.cos(-(np.pi / 2.0) * (idx2 - k) / (idx2 - idx1))
    else:
        w[idx1] = np.cos(np.pi / 2.0)  # obspy's observed edge value (~6e-17)
    if idx4 > idx3:
        k = np.arange(idx3, idx4 + 1)
        w[idx3 : idx4 + 1] = np.cos((np.pi / 2.0) * (idx3 - k) / (idx4 - idx3))
    else:
        w[idx4] = np.cos(np.pi / 2.0)
    return w


def cosine_sac_taper_np(freqs: np.ndarray, flimit) -> np.ndarray:
    """Port of obspy cosine_sac_taper: raised-cosine band flanks."""
    fl1, fl2, fl3, fl4 = flimit
    t = np.zeros_like(freqs)
    left = (fl1 <= freqs) & (freqs <= fl2)
    t[left] = 0.5 * (1.0 - np.cos(np.pi * (freqs[left] - fl1) / (fl2 - fl1)))
    t[(fl2 < freqs) & (freqs < fl3)] = 1.0
    right = (fl3 <= freqs) & (freqs <= fl4)
    t[right] = 0.5 * (1.0 + np.cos(np.pi * (freqs[right] - fl3) / (fl4 - fl3)))
    return t


def invert_spectrum_np(h: np.ndarray, water_level_db: float) -> np.ndarray:
    """Port of obspy invert_spectrum: water-level-regularized 1/H."""
    h = h.copy()
    swamp = np.abs(h).max() * 10.0 ** (-water_level_db / 20.0)
    mag = np.abs(h)
    idx = (mag < swamp) & (mag > 0.0)
    h[idx] *= swamp / mag[idx]
    nonzero = np.abs(h) > 0.0
    h[nonzero] = 1.0 / h[nonzero]
    h[~nonzero] = 0.0
    return h
