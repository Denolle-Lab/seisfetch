# SPDX-License-Identifier: LGPL-3.0-only
#
# Derived from ObsPy (https://github.com/obspy/obspy),
# Copyright (C) The ObsPy Development Team (devs@obspy.org),
# GNU Lesser General Public License, Version 3.
#
# The functions in THIS FILE are Python translations of ObsPy routines
# (obspy.core.trace.Trace.resample/taper, obspy.signal.invsim.cosine_taper /
# cosine_sac_taper / invert_spectrum, obspy.signal.util._npts2nfft,
# obspy.geodetics.base.calc_vincenty_inverse/gps2dist_azimuth),
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
    "gps2dist_azimuth_np",
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


# WGS84 ellipsoid, as obspy.geodetics.base defines it
WGS84_A = 6378137.0
WGS84_F = 1 / 298.257223563


def gps2dist_azimuth_np(
    lat1: float,
    lon1: float,
    lat2: float,
    lon2: float,
    a: float = WGS84_A,
    f: float = WGS84_F,
) -> tuple[float, float, float]:
    """Port of ``obspy.geodetics.base.gps2dist_azimuth`` (Vincenty inverse).

    Returns ``(distance_m, azimuth_A_to_B_deg, azimuth_B_to_A_deg)`` on the
    WGS84 ellipsoid.

    obspy calls geographiclib when it is installed and falls back to its own
    Vincenty otherwise. This port is the Vincenty branch — the one that runs
    in a default install — reproduced statement for statement, including its
    iteration bound and its degenerate-case handling, so distances/azimuths
    written into CCF metadata are unchanged.

    Against an obspy that DOES have geographiclib the two disagree by ~5e-6 m
    over ~500 km (and geographiclib spells a due-south back-azimuth 0.0 where
    Vincenty says 360.0). Both are far below anything CCF metadata resolves,
    but it means bit-identity holds against a default obspy install, not
    against every obspy install. Nearly-antipodal pairs, where
    Vincenty fails to converge, raise ValueError (obspy warns and returns
    NaNs after falling through); noise cross-correlation pairs are never
    antipodal in practice.
    """
    import math

    for name, lat in (("lat1", lat1), ("lat2", lat2)):
        if lat > 90 or lat < -90:
            raise ValueError(f"{name} out of bounds! (-90 <= {name} <= 90)")

    # obspy's _normalize_longitude: repeated +-360 subtraction, NOT a modulo.
    # The two differ in the last ulp for some inputs, which propagates into
    # the returned distance, so reproduce the loop exactly.
    def _normalize_longitude(longitude):
        while longitude > 180:
            longitude -= 360
        while longitude < -180:
            longitude += 360
        return longitude

    lon1 = _normalize_longitude(lon1)
    lon2 = _normalize_longitude(lon2)

    b = a * (1 - f)  # semiminor axis

    if math.isclose(lat1, lat2) and math.isclose(lon1, lon2):
        return 0.0, 0.0, 0.0

    lat1 = math.radians(lat1)
    lon1 = math.radians(lon1)
    lat2 = math.radians(lat2)
    lon2 = math.radians(lon2)

    tan_u1 = (1 - f) * math.tan(lat1)
    tan_u2 = (1 - f) * math.tan(lat2)

    u_1 = math.atan(tan_u1)
    u_2 = math.atan(tan_u2)

    dlon = lon2 - lon1
    last_dlon = -4000000.0  # an impossible value
    omega = dlon

    iterlimit = 100
    try:
        while (
            last_dlon < -3000000.0
            or dlon != 0
            and abs((last_dlon - dlon) / dlon) > 1.0e-9
        ):
            sqr_sin_sigma = pow(math.cos(u_2) * math.sin(dlon), 2) + pow(
                (
                    math.cos(u_1) * math.sin(u_2)
                    - math.sin(u_1) * math.cos(u_2) * math.cos(dlon)
                ),
                2,
            )
            sin_sigma = math.sqrt(sqr_sin_sigma)
            cos_sigma = math.sin(u_1) * math.sin(u_2) + math.cos(u_1) * math.cos(
                u_2
            ) * math.cos(dlon)
            sigma = math.atan2(sin_sigma, cos_sigma)
            sin_alpha = math.cos(u_1) * math.cos(u_2) * math.sin(dlon) / sin_sigma

            sqr_cos_alpha = 1 - sin_alpha * sin_alpha
            if math.isclose(sqr_cos_alpha, 0):
                # Equatorial line
                cos2sigma_m = 0
            else:
                cos2sigma_m = cos_sigma - (
                    2 * math.sin(u_1) * math.sin(u_2) / sqr_cos_alpha
                )

            c = (f / 16) * sqr_cos_alpha * (4 + f * (4 - 3 * sqr_cos_alpha))
            last_dlon = dlon
            dlon = omega + (1 - c) * f * sin_alpha * (
                sigma
                + c
                * sin_sigma
                * (cos2sigma_m + c * cos_sigma * (-1 + 2 * pow(cos2sigma_m, 2)))
            )

            iterlimit -= 1
            if iterlimit < 0:
                raise StopIteration
    except (ValueError, StopIteration):
        raise ValueError(
            "Vincenty's inverse formula did not converge (nearly antipodal points); "
            "obspy has the same limitation."
        )

    u2 = sqr_cos_alpha * (a * a - b * b) / (b * b)
    _a = 1 + (u2 / 16384) * (4096 + u2 * (-768 + u2 * (320 - 175 * u2)))
    _b = (u2 / 1024) * (256 + u2 * (-128 + u2 * (74 - 47 * u2)))
    delta_sigma = (
        _b
        * sin_sigma
        * (
            cos2sigma_m
            + (_b / 4)
            * (
                cos_sigma * (-1 + 2 * pow(cos2sigma_m, 2))
                - (_b / 6)
                * cos2sigma_m
                * (-3 + 4 * sqr_sin_sigma)
                * (-3 + 4 * pow(cos2sigma_m, 2))
            )
        )
    )

    dist = b * _a * (sigma - delta_sigma)
    alpha12 = math.atan2(
        (math.cos(u_2) * math.sin(dlon)),
        (
            math.cos(u_1) * math.sin(u_2)
            - math.sin(u_1) * math.cos(u_2) * math.cos(dlon)
        ),
    )
    alpha21 = math.atan2(
        (math.cos(u_1) * math.sin(dlon)),
        (
            -math.sin(u_1) * math.cos(u_2)
            + math.cos(u_1) * math.sin(u_2) * math.cos(dlon)
        ),
    )

    if alpha12 < 0.0:
        alpha12 = alpha12 + (2.0 * math.pi)
    if alpha12 > (2.0 * math.pi):
        alpha12 = alpha12 - (2.0 * math.pi)

    alpha21 = alpha21 + math.pi

    if alpha21 < 0.0:
        alpha21 = alpha21 + (2.0 * math.pi)
    if alpha21 > (2.0 * math.pi):
        alpha21 = alpha21 - (2.0 * math.pi)

    alpha12 = alpha12 * 360 / (2.0 * math.pi)
    alpha21 = alpha21 * 360 / (2.0 * math.pi)

    return dist, alpha12, alpha21
