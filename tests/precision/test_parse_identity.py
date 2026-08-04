"""4a: seisfetch decode must be bit-identical to obspy, per segment.

Matches seisfetch segments to obspy traces by (id, starttime, npts) and
asserts exact sample equality, identical dtypes, nanosecond start times, and
sampling rates — across encodings (Steim2, float32/64, int16) and gap
topologies.
"""

import io
from pathlib import Path

import numpy as np
import pytest

obspy = pytest.importorskip("obspy")

from seisfetch.convert import parse_mseed  # noqa: E402

TESTS = Path(__file__).parent.parent
FIXTURE_FILES = [
    TESTS / "bench.mseed",
    TESTS / "fixtures" / "gap_3seg.mseed",
    TESTS / "fixtures" / "overlap.mseed",
    TESTS / "fixtures" / "enc_float32.mseed",
    TESTS / "fixtures" / "enc_float64.mseed",
    TESTS / "fixtures" / "enc_int16.mseed",
]


# obspy's UTCDateTime is float-backed: nanosecond start times round to ~32 ns
NS_TOL = 1_000


def _obspy_contiguous_segments(st):
    """obspy.read returns one Trace per RECORD RUN and does not join runs
    that are exactly contiguous (libmseed's trace list does). Merge obspy
    traces whose next start falls within half a sample of the previous end,
    so both sides describe the same physical segments."""
    by_id = {}
    for tr in st:
        by_id.setdefault(tr.id, []).append(tr)
    segments = []
    for tid, trs in by_id.items():
        trs.sort(key=lambda t: t.stats.starttime)
        cur = [trs[0]]
        for tr in trs[1:]:
            prev = cur[-1]
            expected = prev.stats.endtime + prev.stats.delta
            if abs(tr.stats.starttime - expected) <= 0.5 * prev.stats.delta:
                cur.append(tr)
            else:
                segments.append(cur)
                cur = [tr]
        segments.append(cur)
    out = []
    for run in segments:
        data = np.concatenate([t.data for t in run])
        out.append(
            (
                run[0].id,
                int(round(run[0].stats.starttime.timestamp * 1e9)),
                run[0].stats.sampling_rate,
                data,
            )
        )
    return out


@pytest.mark.parametrize("path", FIXTURE_FILES, ids=lambda p: p.name)
def test_segments_bit_identical_to_obspy(path):
    raw = path.read_bytes()
    bundle = parse_mseed(raw)
    obspy_segments = _obspy_contiguous_segments(obspy.read(io.BytesIO(raw)))

    assert len(bundle.traces) == len(obspy_segments), (
        f"{path.name}: segment count mismatch "
        f"(seisfetch {len(bundle.traces)}, obspy {len(obspy_segments)})"
    )

    for seg in bundle.traces:
        match = [
            o
            for o in obspy_segments
            if o[0] == seg.id
            and abs(o[1] - seg.starttime_ns) <= NS_TOL
            and o[3].shape[0] == seg.npts
        ]
        assert len(match) == 1, f"{path.name}: no obspy match for {seg.id}"
        _, _, sr, data = match[0]
        assert seg.sampling_rate == sr
        # libmseed promotes int16 -> int32; obspy keeps int16 for INT16
        # encoding — require same numeric kind, then exact values
        if np.issubdtype(seg.data.dtype, np.integer):
            assert np.issubdtype(data.dtype, np.integer)
        else:
            assert seg.data.dtype == data.dtype
        np.testing.assert_array_equal(seg.data, data)


def test_bench_day_merged_identity():
    raw = (TESTS / "bench.mseed").read_bytes()
    st = obspy.read(io.BytesIO(raw)).merge(method=1, fill_value=0)
    d = parse_mseed(raw).to_dict(fill_value=0)["CI.PASC.00.BHZ"]
    np.testing.assert_array_equal(d, st[0].data)
