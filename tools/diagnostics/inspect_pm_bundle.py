import numpy as np

from seisfetch import parse_mseed
from seisfetch.s3 import S3OpenClient

raw = S3OpenClient().get_raw(
    "CI", "PASC", "2011-03-11", "2011-03-12", channel="BHZ", location="00"
)

bundle = parse_mseed(raw)
data_dict = bundle.to_dict()
pm_data = data_dict.get("CI.PASC.00.BHZ", np.array([]))

recs = []
from pymseed import MS3Record

for msr in MS3Record.from_buffer(raw, unpack_data=True):
    recs.append(msr.np_datasamples.copy())
raw_concat = np.concatenate(recs)

print("Are arrays identical?", np.array_equal(pm_data, raw_concat))
diff = pm_data - raw_concat
idx = np.where(diff != 0)[0]
if len(idx) > 0:
    first_diff = idx[0]
    print(
        f"First mismatch at index {first_diff}: PM={pm_data[first_diff]}, Obspy={raw_concat[first_diff]}"
    )
