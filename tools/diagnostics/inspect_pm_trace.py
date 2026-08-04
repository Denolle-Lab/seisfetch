from pymseed import MS3Record

from seisfetch.s3 import S3OpenClient

raw = S3OpenClient().get_raw(
    "CI", "PASC", "2011-03-11", "2011-03-12", channel="BHZ", location="00"
)

traces = []
for msr in MS3Record.from_buffer(raw, unpack_data=True):
    arr = msr.np_datasamples
    # arr without copy?
    if len(traces) == 0:
        print("First msr np_datasamples type:", arr.dtype, arr.__class__)
        print("First msr numpy buffer id:", hex(id(arr)))
    traces.append(arr)

print("At append time:")
print("First item of first trace:", traces[0][0])
print("First item of last trace:", traces[-1][0])

from seisfetch import parse_mseed

bundle = parse_mseed(raw)
print("\nIn bundle:")
print("First item of first bundle trace:", bundle.traces[0].data[0])
