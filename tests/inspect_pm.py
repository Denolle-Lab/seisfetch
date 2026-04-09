import numpy as np
import obspy
from pymseed import MS3Record

from seisfetch.s3 import S3OpenClient

raw = S3OpenClient().get_raw(
    "CI", "PASC", "2011-03-11", "2011-03-12", channel="BHZ", location="00"
)
print("Total size:", len(raw))

recs = []
for msr in MS3Record.from_buffer(raw, unpack_data=True):
    recs.append(msr.np_datasamples.copy())

pm_data = np.concatenate(recs)
print("PM dtype:", pm_data.dtype)

import io

st = obspy.read(io.BytesIO(raw))
st.merge(fill_value="latest")
obspy_data = st[0].data
print("Obspy dtype:", obspy_data.dtype)

print("First 10 of pm_data:", pm_data[:10])
print("First 10 of obspy_data:", obspy_data[:10])
