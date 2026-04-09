import io
import time

import numpy as np
import obspy

from seisfetch import parse_mseed
from seisfetch.s3 import S3OpenClient


def main():
    client = S3OpenClient()
    print("Downloading raw miniSEED from SCEDC S3...")
    t0 = time.perf_counter()
    raw_bytes = client.get_raw(
        "CI", "PASC", "2011-03-11", "2011-03-12", channel="BHZ", location="00"
    )
    t_dl = time.perf_counter() - t0
    if len(raw_bytes) == 0:
        print("Warning: Retrieved 0 bytes. Check station parameters.")
        return

    print(f"Downloaded {len(raw_bytes) / 1024 / 1024:.2f} MB in {t_dl:.2f}s")

    with open("test_local.mseed", "wb") as f:
        f.write(raw_bytes)

    print("\n--- Method 1: seisfetch (pymseed) from memory ---")
    t0 = time.perf_counter()
    bundle = parse_mseed(raw_bytes)
    arrays = bundle.to_dict()
    data_pymseed = arrays.get("CI.PASC.00.BHZ", np.array([]))
    t_pymseed = time.perf_counter() - t0
    print(f"pymseed parsing took {t_pymseed:.4f} seconds")

    print("\n--- Method 2: obspy + io.BytesIO from memory (QuakeScope s3_helper) ---")
    t0 = time.perf_counter()
    buff = io.BytesIO(raw_bytes)
    st_io = obspy.read(buff)
    if len(st_io) > 1:
        st_io.merge(fill_value="latest")
    data_io = st_io[0].data if len(st_io) > 0 else np.array([])
    t_io = time.perf_counter() - t0
    print(f"obspy (BytesIO) parsing took {t_io:.4f} seconds")

    print("\n--- Method 3: obspy from local disk file ---")
    t0 = time.perf_counter()
    st_file = obspy.read("test_local.mseed")
    if len(st_file) > 1:
        st_file.merge(fill_value="latest")
    data_file = st_file[0].data if len(st_file) > 0 else np.array([])
    t_file = time.perf_counter() - t0
    print(f"obspy (local file) parsing took {t_file:.4f} seconds")

    print("\n--- Results Validation ---")
    print(f"pymseed array shape: {data_pymseed.shape}, dtype: {data_pymseed.dtype}")
    print(f"obspy array shape:   {data_io.shape}, dtype: {data_io.dtype}")

    if data_pymseed.shape == data_io.shape:
        if np.array_equal(data_pymseed, data_io):
            print("SUCCESS! Both parsers produced identical numpy arrays!")
        else:
            print("WARNING! The arrays differ in values.")
    else:
        print("WARNING! The arrays differ in shape.")


if __name__ == "__main__":
    main()
