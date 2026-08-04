"""Tests for the segment-aware TraceBundle API and the tracelist fast path."""

from pathlib import Path

import numpy as np
import pytest

from seisfetch.convert import parse_mseed

FIXTURES = Path(__file__).parent / "fixtures"
BENCH = Path(__file__).parent / "bench.mseed"


def _load(name):
    return parse_mseed((FIXTURES / name).read_bytes())


class TestSegments:
    def test_true_segment_count_on_day_file(self):
        # 21,984 records must collapse to a handful of true segments
        b = parse_mseed(BENCH.read_bytes())
        assert len(b.traces) <= 3
        meta = b.metadata()["CI.PASC.00.BHZ"]
        assert meta.num_segments == len(b.traces)

    def test_gapped_file_has_three_segments(self):
        b = _load("gap_3seg.mseed")
        segs = b.segments()["XX.FIX.00.BHZ"]
        assert len(segs) == 3
        gaps = b.gaps()["XX.FIX.00.BHZ"]
        assert len(gaps) == 2
        assert gaps[0].duration_s == pytest.approx(10.0, abs=0.05)
        assert gaps[1].duration_s == pytest.approx(3.5, abs=0.05)

    def test_encodings_decoded(self):
        for name, dtype in (
            ("enc_float32.mseed", np.float32),
            ("enc_float64.mseed", np.float64),
            ("enc_int16.mseed", np.int32),  # libmseed promotes int16 -> int32
        ):
            b = _load(name)
            assert len(b.traces) == 1
            assert b.traces[0].data.dtype == dtype


class TestToDict:
    def test_gapless_default_unchanged(self):
        b = _load("enc_float32.mseed")
        d = b.to_dict()
        assert len(d["XX.FIX.00.BHZ"]) == b.traces[0].npts

    def test_gappy_default_warns(self):
        b = _load("gap_3seg.mseed")
        with pytest.warns(UserWarning, match="contain gaps"):
            d = b.to_dict()
        # historical behavior: plain concatenation, shorter than the span
        assert len(d["XX.FIX.00.BHZ"]) == sum(s.npts for s in b.traces)

    def test_fill_value_places_segments_at_true_offsets(self):
        b = _load("gap_3seg.mseed")
        fs = b.traces[0].sampling_rate
        d = b.to_dict(fill_value=0)["XX.FIX.00.BHZ"]
        segs = b.segments()["XX.FIX.00.BHZ"]
        t0 = segs[0].starttime_ns
        span = int(round((segs[-1].endtime_ns - t0) * fs / 1e9)) + 1
        assert len(d) == span
        for s in segs:
            i0 = int(round((s.starttime_ns - t0) * fs / 1e9))
            np.testing.assert_array_equal(d[i0 : i0 + s.npts], s.data)
        # gap region is filled
        g = b.gaps()["XX.FIX.00.BHZ"][0]
        gi0 = int(round((g.start_ns - t0) * fs / 1e9))
        assert np.all(d[gi0 : gi0 + g.samples_missing - 1] == 0)

    def test_fill_value_matches_obspy_merge(self):
        obspy = pytest.importorskip("obspy")
        import io

        raw = (FIXTURES / "gap_3seg.mseed").read_bytes()
        st = obspy.read(io.BytesIO(raw)).merge(method=1, fill_value=0)
        d = parse_mseed(raw).to_dict(fill_value=0)["XX.FIX.00.BHZ"]
        np.testing.assert_array_equal(d, st[0].data)


class TestOverlapAndTrim:
    def test_overlap_detected(self):
        b = _load("overlap.mseed")
        ov = b.overlaps()["XX.FIX.00.BHZ"]
        assert len(ov) == 1
        assert ov[0].duration_s == pytest.approx(-5.0, abs=0.05)

    def test_overlap_later_segment_wins(self):
        b = _load("overlap.mseed")
        d = b.to_dict(fill_value=0)["XX.FIX.00.BHZ"]
        segs = b.segments()["XX.FIX.00.BHZ"]
        fs = segs[0].sampling_rate
        i1 = int(round((segs[1].starttime_ns - segs[0].starttime_ns) * fs / 1e9))
        np.testing.assert_array_equal(d[i1 : i1 + segs[1].npts], segs[1].data)

    def test_trim_sample_precise(self):
        b = _load("enc_float32.mseed")
        t = b.traces[0]
        start_ns = t.starttime_ns + int(10e9)  # cut 10 s off the front
        end_ns = t.endtime_ns - int(20e9)  # and 20 s off the back
        cut = b.trim(start_ns, end_ns).traces[0]
        fs = t.sampling_rate
        assert cut.starttime_ns == t.starttime_ns + int(10e9)
        assert cut.npts == t.npts - int(30 * fs)
        np.testing.assert_array_equal(
            cut.data, t.data[int(10 * fs) : t.npts - int(20 * fs)]
        )

    def test_trim_drops_out_of_window_segments(self):
        b = _load("gap_3seg.mseed")
        segs = b.segments()["XX.FIX.00.BHZ"]
        cut = b.trim(segs[2].starttime_ns, segs[2].endtime_ns)
        assert len(cut.traces) == 1
        np.testing.assert_array_equal(cut.traces[0].data, segs[2].data)


class TestFallbackPath:
    def test_per_record_fallback_matches_fast_path(self):
        from seisfetch.convert import _parse_records

        raw = (FIXTURES / "gap_3seg.mseed").read_bytes()
        fast = parse_mseed(raw)
        slow = _parse_records(raw, collect_flags=False)
        assert len(fast.traces) == len(slow.traces)
        for a, b in zip(fast.traces, slow.traces):
            assert a.id == b.id and a.starttime_ns == b.starttime_ns
            np.testing.assert_array_equal(a.data, b.data)
