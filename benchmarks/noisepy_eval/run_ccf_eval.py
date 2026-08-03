"""CCF equivalence harness: obspy path vs seisfetch path through REAL noisepy.

Feeds identical miniSEED bytes through:
  A. obspy.read -> noisepy.seis.noise_module.preprocess_raw (rm_resp=NO)
     -> ChannelData -> noisepy compute_fft -> noisepy correlate
  B. seisfetch parse_mseed -> seisfetch.contrib.noisepy_adapter
     .preprocess_raw_np -> NpChannelData -> the SAME noisepy compute_fft
     and correlate calls
and compares the daily cross-component CCFs (EN, EZ, NZ) and the ZZ
autocorrelation: max abs difference, normalized waveform correlation, and
stretching-dv/v grid argmax on +-5%.

Needs an env with noisepy-seis, obspy AND seisfetch installed (the two-env
caveat is documented in the adapter module: this proves numerical
equivalence; footprint numbers come from the seisfetch-only env).

Usage:
  python benchmarks/noisepy_eval/run_ccf_eval.py \
      --cache /tmp/ccf_eval_cache [--day 2022-01-02] [--station CI.PASC]
"""

import argparse
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np


def fetch_bytes(cache: Path, network, station, location, channel, day) -> bytes:
    from seisfetch.s3 import S3OpenClient

    cache.mkdir(parents=True, exist_ok=True)
    f = cache / f"{network}.{station}.{location}.{channel}.{day}.ms"
    if f.exists():
        return f.read_bytes()
    raw = S3OpenClient().get_raw(
        network, station, day, location=location, channel=channel
    )
    if not raw:
        raise RuntimeError(f"no data for {network}.{station} {channel} {day}")
    f.write_bytes(raw)
    return raw


def make_config():
    from noisepy.seis.io.datatypes import (
        CCMethod,
        ConfigParameters,
        FreqNorm,
        RmResp,
        TimeNorm,
    )

    return ConfigParameters(
        sampling_rate=40.0,
        cc_len=1800,
        step=450,
        maxlag=32.0,
        freqmin=0.5,
        freqmax=19.0,
        acorr_only=True,
        rm_resp=RmResp.NO,
        cc_method=CCMethod.XCORR,
        freq_norm=FreqNorm.RMA,
        time_norm=TimeNorm.NO,
        substack=False,
    )


def path_a(raw: bytes, cfg, start, end):
    """obspy + noisepy's own preprocess_raw."""
    import io

    import obspy
    from noisepy.seis import noise_module
    from noisepy.seis.correlate import compute_fft
    from noisepy.seis.io.datatypes import ChannelData

    st = obspy.read(io.BytesIO(raw))
    st_p = noise_module.preprocess_raw(
        st.copy(),
        obspy.Inventory(),
        cfg,
        obspy.UTCDateTime(start),
        obspy.UTCDateTime(end),
    )
    if len(st_p) == 0:
        raise RuntimeError("path A: preprocess_raw rejected the stream")
    return compute_fft(cfg, ChannelData(st_p))


def path_b(raw: bytes, cfg, start, end, nslc: str):
    """seisfetch + adapter ports; same noisepy compute_fft."""
    from noisepy.seis.correlate import compute_fft

    from seisfetch.contrib.noisepy_adapter import preprocess_raw_np
    from seisfetch.convert import parse_mseed

    segs = parse_mseed(raw).segments()[nslc]
    start_ns = int(start.timestamp() * 1e9)
    end_ns = int(end.timestamp() * 1e9)
    npcd = preprocess_raw_np(
        segs, start_ns, end_ns, cfg.freqmin, cfg.freqmax, cfg.sampling_rate
    )
    if npcd.data.size == 0:
        raise RuntimeError("path B: preprocess_raw_np rejected the channel")
    return compute_fft(cfg, npcd)


def daily_ccf(cfg, fft_src, fft_rec):
    """Mirror noisepy cross_corr for one channel pair (XCORR method)."""
    from noisepy.seis import noise_module

    Nfft = fft_src.length
    sfft1 = np.conj(fft_src.fft).reshape(fft_src.window_count, fft_src.length // 2)
    sfft2 = fft_rec.fft.reshape(fft_rec.window_count, fft_rec.length // 2)
    good = lambda std: np.where(  # noqa: E731
        (std < cfg.max_over_std) & (std > 0) & (~np.isnan(std))
    )[0]
    bb = np.intersect1d(good(fft_src.std), good(fft_rec.std))
    corr, tcorr, ncorr = noise_module.correlate(
        sfft1[bb, :], sfft2[bb, :], cfg, Nfft, fft_src.fft_time[bb]
    )
    return corr, ncorr


def stretch_argmax(ccf, fs, eps_grid):
    """Cell index of the best-fitting stretch of ccf vs itself perturbed —
    used only to check A and B land in the same grid cell."""
    n = ccf.shape[0]
    t = (np.arange(n) - n // 2) / fs
    win = (np.abs(t) > 5) & (np.abs(t) < 25)
    ref = np.interp(t, t / 1.002, ccf)
    ccs = []
    for e in eps_grid:
        s = np.interp(t, t / (1 + e), ccf)
        a, b = s[win], ref[win]
        ccs.append(np.dot(a, b) / np.sqrt(np.dot(a, a) * np.dot(b, b) + 1e-30))
    return int(np.argmax(ccs))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--cache", default="/tmp/ccf_eval_cache")
    ap.add_argument("--day", default="2022-01-02")
    ap.add_argument("--station", default="CI.PASC")
    ap.add_argument("--location", default="00")
    ap.add_argument("--band", default="BH")
    ap.add_argument("--json", default=None, help="write results JSON here")
    args = ap.parse_args()

    network, station = args.station.split(".")
    day0 = datetime.fromisoformat(args.day).replace(tzinfo=timezone.utc)
    day1 = day0 + timedelta(days=1)
    cfg = make_config()
    cache = Path(args.cache)

    comps = ["E", "N", "Z"]
    ffts_a, ffts_b = {}, {}
    for c in comps:
        chan = f"{args.band}{c}"
        raw = fetch_bytes(cache, network, station, args.location, chan, args.day)
        nslc = f"{network}.{station}.{args.location}.{chan}"
        ffts_a[c] = path_a(raw, cfg, day0, day1)
        ffts_b[c] = path_b(raw, cfg, day0, day1, nslc)

    pairs = [("E", "N"), ("E", "Z"), ("N", "Z"), ("Z", "Z")]
    eps_grid = np.linspace(-0.05, 0.05, 161)
    results = {}
    ok = True
    for s, r in pairs:
        ca, na = daily_ccf(cfg, ffts_a[s], ffts_a[r])
        cb, nb = daily_ccf(cfg, ffts_b[s], ffts_b[r])
        ca, cb = np.atleast_2d(ca), np.atleast_2d(cb)
        a = ca.mean(axis=0)
        b = cb.mean(axis=0)
        max_abs = float(np.abs(a - b).max())
        denom = float(np.abs(a).max())
        wf_corr = float(np.corrcoef(a, b)[0, 1])
        cell_a = stretch_argmax(a, cfg.sampling_rate, eps_grid)
        cell_b = stretch_argmax(b, cfg.sampling_rate, eps_grid)
        pair_ok = wf_corr > 0.99999 and cell_a == cell_b
        ok &= pair_ok
        results[f"{s}{r}"] = {
            "windows_a": int(na if np.isscalar(na) else len(np.atleast_1d(na))),
            "max_abs_diff": max_abs,
            "max_abs_rel_to_peak": max_abs / denom if denom else 0.0,
            "waveform_corr": wf_corr,
            "dvv_cell_a": cell_a,
            "dvv_cell_b": cell_b,
            "pass": bool(pair_ok),
        }
        print(
            f"{s}{r}: corr={wf_corr:.9f} max|diff|/peak={max_abs / denom:.2e} "
            f"dvv cell {cell_a} vs {cell_b} -> {'PASS' if pair_ok else 'FAIL'}"
        )

    if args.json:
        Path(args.json).write_text(json.dumps(results, indent=2, sort_keys=True))
    print("OVERALL:", "PASS" if ok else "FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
