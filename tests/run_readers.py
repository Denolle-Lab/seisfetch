import io
import time

import numpy as np
import obspy

from seisfetch import parse_mseed
from seisfetch.s3 import S3OpenClient


def main():
    print("Downloading bench data...")
    raw = S3OpenClient().get_raw(
        "CI", "PASC", "2011-03-11", "2011-03-12", channel="BHZ", location="00"
    )

    print("Parsing pymseed...")
    t0 = time.perf_counter()
    bundle = parse_mseed(raw)
    data_pm = bundle.to_dict().get("CI.PASC.00.BHZ", np.array([]))
    t1 = time.perf_counter() - t0

    print("Parsing obspy bytesio...")
    t0 = time.perf_counter()
    st = obspy.read(io.BytesIO(raw))
    if len(st) > 1:
        st.merge(fill_value="latest")
    data_io = st[0].data
    t2 = time.perf_counter() - t0

    with open("bench.mseed", "wb") as f:
        f.write(raw)

    print("Parsing obspy local file...")
    t0 = time.perf_counter()
    st2 = obspy.read("bench.mseed")
    if len(st2) > 1:
        st2.merge(fill_value="latest")
    t3 = time.perf_counter() - t0

    print("--- Validation ---")
    print(f"seisfetch pymseed read : {t1:.4f}s")
    print(f"obspy from io.BytesIO  : {t2:.4f}s")
    print(f"obspy from local file  : {t3:.4f}s")
    print("Length pymseed         :", len(data_pm))
    print("Length obspy           :", len(data_io))

    if len(data_pm) == len(data_io):
        diff = data_pm - data_io
        print("Max amplitude diff     :", np.max(np.abs(diff)))


if __name__ == "__main__":
    main()
