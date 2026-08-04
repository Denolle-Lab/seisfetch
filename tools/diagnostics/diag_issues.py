"""Diagnose BKS segment ordering and EarthScope key format."""

import io

import numpy as np
import obspy

from seisfetch import parse_mseed
from seisfetch.s3 import S3OpenClient, _earthscope_key
from seisfetch.utils import date_to_year_doy

# ---- NCEDC BKS: check segment ordering -----------------------------------
print("=" * 60)
print("NCEDC BKS: checking segment ordering")
print("=" * 60)
client = S3OpenClient()
raw = client.get_raw(
    "BK", "BKS", "2011-03-11", "2011-03-12", channel="BHZ", location="00"
)

bundle = parse_mseed(raw)
traces = [t for t in bundle.traces if t.id == "BK.BKS.00.BHZ"]
print(f"Number of trace segments: {len(traces)}")
for i, t in enumerate(traces[:10]):
    print(f"  seg {i}: starttime_ns={t.starttime_ns}  npts={t.npts}  first={t.data[0]}")

# Check if segments are sorted by starttime
starttimes = [t.starttime_ns for t in traces]
is_sorted = all(starttimes[i] <= starttimes[i + 1] for i in range(len(starttimes) - 1))
print(f"Segments sorted by starttime? {is_sorted}")

# Compare: sorted concat vs unsorted concat
sorted_traces = sorted(traces, key=lambda t: t.starttime_ns)
data_sorted = np.concatenate([t.data for t in sorted_traces])
data_unsorted = np.concatenate([t.data for t in traces])
print(f"Sorted == Unsorted? {np.array_equal(data_sorted, data_unsorted)}")

# Compare sorted concat vs obspy
st = obspy.read(io.BytesIO(raw))
st_sel = st.select(network="BK", station="BKS", location="00", channel="BHZ")
st_sel.merge(fill_value="latest")
data_ob = st_sel[0].data
n = min(len(data_sorted), len(data_ob))
diff = data_sorted[:n].astype(np.int64) - data_ob[:n].astype(np.int64)
max_diff = int(np.max(np.abs(diff)))
print(f"Sorted concat vs obspy: max_diff={max_diff}, identical={max_diff==0}")

# ---- EarthScope: check key format ----------------------------------------
print("\n" + "=" * 60)
print("EarthScope: checking key format")
print("=" * 60)
yr, doy = date_to_year_doy("2011-03-11")
key = _earthscope_key("IU", "ANMO", yr, doy)
print(f"Generated key: {key}")

# Try listing objects to find the real key
import boto3
from botocore import UNSIGNED
from botocore.config import Config

s3 = boto3.client(
    "s3", region_name="us-east-2", config=Config(signature_version=UNSIGNED)
)
prefix = f"miniseed/IU/{yr}/{doy:03d}/"
print(f"Listing prefix: {prefix}")
resp = s3.list_objects_v2(
    Bucket="earthscope-geophysical-data", Prefix=prefix, MaxKeys=20
)
for obj in resp.get("Contents", []):
    print(f"  {obj['Key']}  ({obj['Size']} bytes)")

if not resp.get("Contents"):
    print("  No objects found. Trying broader prefix...")
    prefix2 = f"miniseed/IU/{yr}/"
    resp2 = s3.list_objects_v2(
        Bucket="earthscope-geophysical-data", Prefix=prefix2, MaxKeys=10, Delimiter="/"
    )
    for p in resp2.get("CommonPrefixes", []):
        print(f"  {p['Prefix']}")
