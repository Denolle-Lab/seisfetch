"""
Compare seisfetch (S3 + pymseed) vs obspy across datacenters.

All use anonymous S3 access — no credentials needed.
  SCEDC:  CI.PASC.00.BHZ   (2011-03-11, Tohoku day)
  NCEDC:  BK.BKS.00.BHZ    (2011-03-11, Tohoku day)

Note: EarthScope open bucket requires earthscope-sdk credentials
(S3AuthClient), so it is not tested here.
"""

import io
import time

import matplotlib.pyplot as plt
import numpy as np
import obspy

from seisfetch import parse_mseed
from seisfetch.s3 import S3OpenClient, route_network

# --------------------------------------------------------------------------- #
#  Test cases: (network, station, location, channel, date)
# --------------------------------------------------------------------------- #
CASES = [
    # SCEDC
    ("CI", "PASC", "00", "BHZ", "2011-03-11"),
    # NCEDC
    ("BK", "BKS", "00", "BHZ", "2011-03-11"),
]


def test_one(client, net, sta, loc, cha, date):
    """Download + parse with both seisfetch and obspy; return results dict."""
    nslc = f"{net}.{sta}.{loc}.{cha}"
    dc = route_network(net)
    end = f"{date[:4]}-{int(date[5:7]):02d}-{int(date[8:10])+1:02d}"
    print(f"\n{'='*60}")
    print(f"  {nslc}  {date}  →  {dc}")
    print(f"{'='*60}")

    # Download
    t0 = time.perf_counter()
    raw = client.get_raw(net, sta, date, end, channel=cha, location=loc)
    t_dl = time.perf_counter() - t0
    print(f"  Download:  {t_dl:.2f}s  ({len(raw)/1e6:.1f} MB)")

    if not raw:
        print("  *** NO DATA ***")
        return None

    # seisfetch / pymseed
    t0 = time.perf_counter()
    bundle = parse_mseed(raw)
    data_sf = bundle.to_dict().get(nslc, np.array([]))
    t_sf = time.perf_counter() - t0
    print(f"  pymseed:   {t_sf:.4f}s  |  {len(data_sf)} samples")

    # obspy
    t0 = time.perf_counter()
    st = obspy.read(io.BytesIO(raw))
    st_sel = st.select(network=net, station=sta, location=loc, channel=cha)
    if len(st_sel) > 1:
        st_sel.merge(fill_value="latest")
    if len(st_sel) == 0:
        print("  *** obspy found no matching traces ***")
        print(f"  obspy IDs: {[tr.id for tr in st]}")
        return None
    data_ob = st_sel[0].data
    sr = st_sel[0].stats.sampling_rate
    t_ob = time.perf_counter() - t0
    print(f"  obspy:     {t_ob:.4f}s  |  {len(data_ob)} samples")

    # Validate
    n = min(len(data_sf), len(data_ob))
    diff = data_sf[:n].astype(np.int64) - data_ob[:n].astype(np.int64)
    max_diff = int(np.max(np.abs(diff)))
    n_mismatch = int(np.count_nonzero(diff))
    identical = max_diff == 0
    print(
        f"  Max |diff|:  {max_diff}   Mismatches: {n_mismatch}/{n}   Identical: {identical}"
    )

    return {
        "nslc": nslc,
        "dc": dc,
        "date": date,
        "data_sf": data_sf[:n],
        "data_ob": data_ob[:n],
        "diff": diff,
        "max_diff": max_diff,
        "identical": identical,
        "sr": sr,
        "t_dl": t_dl,
        "t_sf": t_sf,
        "t_ob": t_ob,
    }


def main():
    client = S3OpenClient()
    results = []

    for net, sta, loc, cha, date in CASES:
        r = test_one(client, net, sta, loc, cha, date)
        if r is not None:
            results.append(r)

    if not results:
        print("\nNo successful downloads.")
        return

    # ---- Summary ----------------------------------------------------------
    print(f"\n{'='*60}")
    print("  SUMMARY")
    print(f"{'='*60}")
    all_ok = True
    for r in results:
        status = "PASS" if r["identical"] else "FAIL"
        if not r["identical"]:
            all_ok = False
        print(
            f"  [{status}]  {r['nslc']:20s}  ({r['dc']:11s})  "
            f"dl={r['t_dl']:.1f}s  pymseed={r['t_sf']:.3f}s  obspy={r['t_ob']:.3f}s  "
            f"max_diff={r['max_diff']}"
        )
    print(f"\n  Overall: {'ALL PASSED' if all_ok else 'SOME FAILED'}")

    # ---- Plot -------------------------------------------------------------
    n_cases = len(results)
    fig, axes = plt.subplots(n_cases, 2, figsize=(16, 4 * n_cases), squeeze=False)

    for i, r in enumerate(results):
        n = len(r["data_sf"])
        t_sec = np.arange(n) / r["sr"]

        # Overlay
        ax = axes[i, 0]
        ax.plot(t_sec, r["data_ob"], linewidth=0.3, color="C1", label="obspy")
        ax.plot(
            t_sec, r["data_sf"], linewidth=0.3, color="C0", alpha=0.7, label="seisfetch"
        )
        ax.set_ylabel("Counts")
        ax.set_title(f"{r['nslc']}  ({r['dc']})  —  {r['date']}")
        ax.legend(loc="upper right", fontsize=8)

        # Difference
        ax2 = axes[i, 1]
        ax2.plot(t_sec, r["diff"], linewidth=0.3, color="C3")
        ax2.set_ylabel("Difference")
        ax2.set_title(f"max |diff| = {r['max_diff']}")

    axes[-1, 0].set_xlabel("Time (s)")
    axes[-1, 1].set_xlabel("Time (s)")
    fig.suptitle("seisfetch vs obspy — SCEDC / NCEDC", fontsize=14)
    fig.tight_layout()
    import os

    plot_path = os.path.join(os.path.dirname(__file__), "compare_readers.png")
    fig.savefig(plot_path, dpi=150)
    print(f"\nPlot saved to {plot_path}")
    plt.show()


if __name__ == "__main__":
    main()
