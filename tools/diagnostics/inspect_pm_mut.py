from pymseed import MS3Record

from seisfetch.s3 import S3OpenClient

raw = S3OpenClient().get_raw(
    "CI", "PASC", "2011-03-11", "2011-03-12", channel="BHZ", location="00"
)

traces = []
t0_first = None
for i, msr in enumerate(MS3Record.from_buffer(raw, unpack_data=True)):
    if i == 0:
        t0_first = msr.np_datasamples[0]
        print(f"Index 0 inserted as {t0_first}")
    traces.append(msr.np_datasamples)

print(f"Index 0 after loop: {traces[0][0]}")

traces2 = []
for i, msr in enumerate(MS3Record.from_buffer(raw, unpack_data=True)):
    traces2.append(msr.np_datasamples.copy())

print(f"Index 0 copied after loop: {traces2[0][0]}")
