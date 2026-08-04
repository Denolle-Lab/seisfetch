import io

import numpy as np
import obspy
from pymseed import MS3Record

from seisfetch.s3 import S3OpenClient

raw = S3OpenClient().get_raw(
    "CI", "PASC", "2011-03-11", "2011-03-12", channel="BHZ", location="00"
)

recs = []
for msr in MS3Record.from_buffer(raw, unpack_data=True):
    recs.append(msr.np_datasamples.copy())

pm_data = np.concatenate(recs)

st = obspy.read(io.BytesIO(raw))
st.merge(fill_value="latest")
obspy_data = st[0].data

print("Arrays identical?", np.array_equal(pm_data, obspy_data))
diff = pm_data - obspy_data
max_diff = np.max(np.abs(diff))
print("Max Diff:", max_diff)
