"""Generate small committed miniSEED fixtures (run manually, outputs committed).

Uses obspy only to WRITE the files; nothing at test time depends on obspy for
these fixtures. Each file is < 200 KB.

    pixi run python tests/fixtures/make_fixtures.py
"""

from pathlib import Path

import numpy as np

HERE = Path(__file__).parent
FS = 40.0
T0 = "2023-01-02T00:00:00.000000Z"


def _trace(data, starttime, dtype):
    from obspy import Trace, UTCDateTime

    tr = Trace(np.asarray(data, dtype=dtype))
    tr.stats.network = "XX"
    tr.stats.station = "FIX"
    tr.stats.location = "00"
    tr.stats.channel = "BHZ"
    tr.stats.sampling_rate = FS
    tr.stats.starttime = UTCDateTime(starttime)
    return tr


def main():
    from obspy import Stream, UTCDateTime

    rng = np.random.default_rng(7)
    t0 = UTCDateTime(T0)

    # gap_3seg: three segments, gaps of 10 s and 3.5 s
    n = int(60 * FS)
    segs = []
    offset = 0.0
    for gap_s in (0.0, 10.0, 3.5):
        offset += gap_s
        segs.append(_trace((rng.standard_normal(n) * 1000), t0 + offset, np.int32))
        offset += n / FS
    Stream(segs).write(str(HERE / "gap_3seg.mseed"), format="MSEED", encoding="STEIM2")

    # overlap: second segment starts 5 s before the first ends, different data
    a = _trace(rng.standard_normal(n) * 1000, t0, np.int32)
    b = _trace(rng.standard_normal(n) * 1000 + 5000, t0 + n / FS - 5.0, np.int32)
    Stream([a, b]).write(str(HERE / "overlap.mseed"), format="MSEED", encoding="STEIM2")

    # encodings
    for enc, dtype, name in (
        ("FLOAT32", np.float32, "enc_float32"),
        ("FLOAT64", np.float64, "enc_float64"),
        ("INT16", np.int16, "enc_int16"),
    ):
        data = rng.standard_normal(n) * (100 if enc != "INT16" else 30)
        tr = _trace(data, t0, dtype)
        Stream([tr]).write(str(HERE / f"{name}.mseed"), format="MSEED", encoding=enc)

    for f in sorted(HERE.glob("*.mseed")):
        print(f"{f.name}: {f.stat().st_size} bytes")


if __name__ == "__main__":
    main()
