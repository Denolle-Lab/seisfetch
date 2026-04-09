import numpy as np
import obspy
from pymseed import MS3Record

from seisfetch.s3 import S3OpenClient

raw = S3OpenClient().get_raw(
    "CI", "PASC", "2011-03-11", "2011-03-12", channel="BHZ", location="00"
)

# Extract with pymseed manually
recs = []
for msr in MS3Record.from_buffer(raw, unpack_data=True):
    recs.append(msr.np_datasamples.copy())
pm_data = np.concatenate(recs)

# Extract with obspy
import io

st = obspy.read(io.BytesIO(raw))
st.merge(fill_value="latest")
obspy_data = st[0].data

print("Are arrays identical?", np.array_equal(pm_data, obspy_data))
diff = pm_data - obspy_data
idx = np.where(diff != 0)[0]
if len(idx) > 0:
    first_diff = idx[0]
    print(
        f"First mismatch at index {first_diff}: PM={pm_data[first_diff]}, Obspy={obspy_data[first_diff]}"
    )
