"""CROSS-STATION CCF equivalence through real noisepy, three archives.

Extends run_ccf_eval (single-station) to the cross-station case the dv/v
evaluation never covered: three stations pulled from the three S3 archives —

    CI.PASC (SCEDC)  ·  BK.PKD (NCEDC)  ·  II.PFO (EarthScope)

— fed through (A) obspy.read + noisepy's preprocess_raw and (B) seisfetch
parse + adapter ports, then noisepy's own compute_fft/correlate for the
three interstation pairs (~150/230/380 km). All BHZ run at 40 sps; the
config targets 20 sps so the Fourier-resample port is exercised inside the
cross-station chain. The EarthScope object is a full station-day file
(48 channels), so path A must channel-select — run_ccf_eval's path_a
cannot, hence the local variant here.

Usage:
  python benchmarks/noisepy_eval/run_xcorr_eval.py --cache DIR [--day 2022-01-02]
"""

import argparse
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np

HERE = Path(__file__).parent
sys.path.insert(0, str(HERE))
from run_ccf_eval import daily_ccf, path_b, snap_window  # noqa: E402

STATIONS = [
    # (datacenter, net, sta, loc, cha)
    ("scedc", "CI", "PASC", "00", "BHZ"),
    ("ncedc", "BK", "PKD", "00", "BHZ"),
    ("earthscope", "II", "PFO", "00", "BHZ"),
]

PAIRS = [("PASC", "PKD"), ("PASC", "PFO"), ("PKD", "PFO")]


def make_config():
    from noisepy.seis.io.datatypes import (
        CCMethod,
        ConfigParameters,
        FreqNorm,
        RmResp,
        TimeNorm,
    )

    # cross-station ambient noise: downsample 40 -> 20 sps, keep the
    # secondary microseism band that carries interstation surface waves
    return ConfigParameters(
        sampling_rate=20.0,
        cc_len=1800,
        step=450,
        maxlag=150.0,
        freqmin=0.05,
        freqmax=9.0,
        acorr_only=False,
        rm_resp=RmResp.NO,
        cc_method=CCMethod.XCORR,
        freq_norm=FreqNorm.RMA,
        time_norm=TimeNorm.NO,
        substack=False,
    )


def path_a_select(raw: bytes, cfg, start, end, nslc: str):
    """obspy + noisepy preprocess_raw, restricted to one channel (the
    EarthScope station-day object carries every channel of the station)."""
    import io

    import obspy
    from noisepy.seis import noise_module
    from noisepy.seis.correlate import compute_fft
    from noisepy.seis.io.datatypes import ChannelData

    net, sta, loc, cha = nslc.split(".")
    st = obspy.read(io.BytesIO(raw)).select(
        network=net, station=sta, location=loc, channel=cha
    )
    if len(st) == 0:
        raise RuntimeError(f"path A: {nslc} not in file")
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


def cache_name(dc, net, sta, loc, cha, day) -> str:
    """Cache key encodes everything the fetched bytes depend on: the
    datacenter and, for per-channel archives, the location/channel
    (EarthScope objects are whole station-days). A bare net.sta.day name
    would silently reuse the wrong file across archives or channels."""
    sel = "all" if dc == "earthscope" else f"{loc or '--'}.{cha}"
    return f"{dc}.{net}.{sta}.{sel}.{day}.ms"


def fetch(cache: Path, dc, net, sta, loc, cha, day) -> bytes:
    from seisfetch.s3 import S3OpenClient

    cache.mkdir(parents=True, exist_ok=True)
    f = cache / cache_name(dc, net, sta, loc, cha, day)
    if f.exists():
        return f.read_bytes()
    kwargs = {} if dc == "earthscope" else {"location": loc, "channel": cha}
    raw = S3OpenClient(datacenter=dc).get_raw(net, sta, day, **kwargs)
    f.write_bytes(raw)
    return raw


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--cache", default="/tmp/xcorr_eval_cache")
    ap.add_argument("--day", default="2022-01-02")
    ap.add_argument("--json", default=None)
    args = ap.parse_args()

    day0 = datetime.fromisoformat(args.day).replace(tzinfo=timezone.utc)
    day1 = day0 + timedelta(days=1)
    cfg = make_config()
    cache = Path(args.cache)

    ffts_a, ffts_b = {}, {}
    for dc, net, sta, loc, cha in STATIONS:
        raw = fetch(cache, dc, net, sta, loc, cha, args.day)
        nslc = f"{net}.{sta}.{loc}.{cha}"
        w0, w1 = snap_window(raw, nslc, day0, day1)
        ffts_a[sta] = path_a_select(raw, cfg, w0, w1, nslc)
        ffts_b[sta] = path_b(raw, cfg, w0, w1, nslc)
        print(
            f"{dc:11s} {nslc}: windows A={ffts_a[sta].window_count} "
            f"B={ffts_b[sta].window_count}"
        )

    results, ok = {}, True
    for s, r in PAIRS:
        ca, _ = daily_ccf(cfg, ffts_a[s], ffts_a[r])
        cb, _ = daily_ccf(cfg, ffts_b[s], ffts_b[r])
        a = np.atleast_2d(ca).mean(axis=0)
        b = np.atleast_2d(cb).mean(axis=0)
        max_abs = float(np.abs(a - b).max())
        peak = float(np.abs(a).max())
        if peak == 0.0:
            raise RuntimeError(f"{s}-{r}: all-zero CCF on path A — nothing to compare")
        corr = float(np.corrcoef(a, b)[0, 1])
        # the claim under test is BIT-identity; any nonzero diff is a fail
        # (precision-bank policy: future drift must be seen and justified)
        pair_ok = max_abs == 0.0
        ok &= pair_ok
        results[f"{s}-{r}"] = {
            "max_abs_diff": max_abs,
            "max_abs_rel_to_peak": max_abs / peak,
            "waveform_corr": corr,
            "pass": bool(pair_ok),
        }
        print(
            f"{s}-{r}: corr={corr:.9f} max|diff|/peak={max_abs / peak:.2e} "
            f"-> {'PASS' if pair_ok else 'FAIL'}"
        )

    if args.json:
        Path(args.json).write_text(json.dumps(results, indent=2, sort_keys=True))
    print("OVERALL:", "PASS" if ok else "FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
