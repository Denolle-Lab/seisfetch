"""Regression tests for the 2026-08 critique's correctness majors:
contained-segment merge, mixed sampling rates, out-of-order fallback
parity, truncated buffers, obspy-parity Stream conversion, and the
adapter's sample-alignment guard. Reproductions mirror the reviewers'."""

import io
import tempfile

import numpy as np
import pytest

from seisfetch.convert import TraceArray, TraceBundle, parse_mseed
from seisfetch.exceptions import MixedSamplingRateError


def _seg(data, t0_s, sr=1.0, value=None):
    d = np.full(len(data), value, dtype=np.float64) if value is not None else data
    return TraceArray(
        network="XX",
        station="TST",
        location="",
        channel="BHZ",
        starttime_ns=int(t0_s * 1e9),
        sampling_rate=sr,
        data=np.asarray(d, dtype=np.float64),
    )


class TestContainedSegments:
    """Reviewer repro: big [0..9s]=10, inner [3..5s]=77 crashed to_dict and,
    once sized, obspy merge(method=1) keeps the SURROUNDING trace."""

    def test_contained_segment_no_crash_and_surrounding_wins(self):
        big = _seg(np.empty(10), 0.0, value=10.0)
        inner = _seg(np.empty(3), 3.0, value=77.0)
        d = TraceBundle([big, inner]).to_dict(fill_value=0)["XX.TST..BHZ"]
        np.testing.assert_array_equal(d, np.full(10, 10.0))

    def test_contained_matches_obspy_merge(self):
        obspy = pytest.importorskip("obspy")
        big = _seg(np.empty(10), 0.0, value=10.0)
        inner = _seg(np.empty(3), 3.0, value=77.0)
        st = obspy.Stream()
        for t in (big, inner):
            tr = obspy.Trace(t.data.copy())
            tr.stats.sampling_rate = t.sampling_rate
            tr.stats.starttime = obspy.UTCDateTime(t.starttime_ns / 1e9)
            tr.id = "XX.TST..BHZ"
            st.append(tr)
        st.merge(method=1, fill_value=0)
        d = TraceBundle([big, inner]).to_dict(fill_value=0)["XX.TST..BHZ"]
        np.testing.assert_array_equal(d, st[0].data)

    def test_partial_tail_overlap_still_matches_obspy(self):
        obspy = pytest.importorskip("obspy")
        a = _seg(np.empty(6), 0.0, value=1.0)
        b = _seg(np.empty(6), 4.0, value=2.0)
        st = obspy.Stream()
        for t in (a, b):
            tr = obspy.Trace(t.data.copy())
            tr.stats.sampling_rate = 1.0
            tr.stats.starttime = obspy.UTCDateTime(t.starttime_ns / 1e9)
            tr.id = "XX.TST..BHZ"
            st.append(tr)
        st.merge(method=1, fill_value=0)
        d = TraceBundle([a, b]).to_dict(fill_value=0)["XX.TST..BHZ"]
        np.testing.assert_array_equal(d, st[0].data)


class TestMixedSamplingRates:
    def _bundle(self):
        return TraceBundle(
            [_seg(np.ones(10), 0.0, sr=20.0), _seg(np.ones(10), 100.0, sr=40.0)]
        )

    def test_to_dict_fill_raises_typed(self):
        with pytest.raises(MixedSamplingRateError, match="20.0.*40.0"):
            self._bundle().to_dict(fill_value=0)

    def test_to_dict_plain_raises_typed(self):
        with pytest.raises(MixedSamplingRateError):
            self._bundle().to_dict()

    def test_metadata_raises_typed(self):
        with pytest.raises(MixedSamplingRateError):
            self._bundle().metadata()

    def test_segments_is_the_escape_hatch(self):
        segs = self._bundle().segments()["XX.TST..BHZ"]
        assert {s.sampling_rate for s in segs} == {20.0, 40.0}


class TestOutOfOrderFallbackParity:
    """Same bytes must yield the same segment topology on the fast
    (MS3TraceList) and per-record fallback paths, even when contiguous
    records are stored out of time order."""

    @staticmethod
    def _two_records_out_of_order() -> bytes:
        from pymseed import MS3TraceList, timestr2nstime

        def one_record(t0: str, values) -> bytes:
            tl = MS3TraceList()
            tl.add_data(
                sourceid="FDSN:XX_TST__B_H_Z",
                data_samples=list(values),
                sample_type="i",
                sample_rate=1.0,
                start_time=timestr2nstime(t0),
            )
            with tempfile.NamedTemporaryFile(suffix=".ms") as f:
                tl.to_file(f.name, format_version=2, max_reclen=512)
                f.seek(0)
                return f.read()

        first = one_record("2024-01-15T00:00:00Z", range(100))
        second = one_record("2024-01-15T00:01:40Z", range(100, 200))
        return second + first  # stored out of time order, exactly contiguous

    def test_both_paths_heal_to_one_segment(self):
        from seisfetch.convert import _parse_records

        raw = self._two_records_out_of_order()
        fast = parse_mseed(raw)
        slow = _parse_records(raw, collect_flags=False)
        assert len(fast.traces) == 1
        assert len(slow.traces) == 1
        np.testing.assert_array_equal(fast.traces[0].data, slow.traces[0].data)
        assert fast.traces[0].starttime_ns == slow.traces[0].starttime_ns


class TestTruncatedBuffer:
    def test_trailing_partial_record_warns(self):
        raw = TestOutOfOrderFallbackParity._two_records_out_of_order()
        cut = raw[: len(raw) - 300]  # cut into the final record
        with pytest.warns(UserWarning, match="trailing byte"):
            b = parse_mseed(cut)
        assert len(b.traces) == 1  # the intact record still parses


class TestBundleToObspyParity:
    def test_default_matches_obspy_read_shape(self):
        obspy = pytest.importorskip("obspy")
        from pathlib import Path

        from seisfetch.convert import bundle_to_obspy

        raw = (Path(__file__).parent / "fixtures" / "gap_3seg.mseed").read_bytes()
        st_sf = bundle_to_obspy(parse_mseed(raw))
        st_ob = obspy.read(io.BytesIO(raw))
        assert len(st_sf) == len(st_ob) == 3
        assert not any(np.ma.isMaskedArray(tr.data) for tr in st_sf)
        # standard obspy processing must work on the gappy result
        st_sf.filter("bandpass", freqmin=1.0, freqmax=10.0)

    def test_merge_1_restores_old_behavior(self):
        pytest.importorskip("obspy")
        from pathlib import Path

        from seisfetch.convert import bundle_to_obspy

        raw = (Path(__file__).parent / "fixtures" / "gap_3seg.mseed").read_bytes()
        st = bundle_to_obspy(parse_mseed(raw), merge=1)
        assert len(st) == 1
        assert np.ma.isMaskedArray(st[0].data)


class TestAdapterAlignmentGuard:
    def test_subsample_window_raises_instead_of_diverging(self):
        pytest.importorskip("scipy")
        from seisfetch.contrib.noisepy_adapter import preprocess_raw_np

        seg = _seg(np.random.default_rng(0).standard_normal(4000), 0.0, sr=40.0)
        start = int(0.4 / 40.0 * 1e9)  # 0.4 samples off the grid
        with pytest.raises(ValueError, match="off the data grid"):
            preprocess_raw_np([seg], start, start + int(50e9), 0.5, 19.0, 40.0)

    def test_aligned_window_passes(self):
        pytest.importorskip("scipy")
        from seisfetch.contrib.noisepy_adapter import preprocess_raw_np

        seg = _seg(np.random.default_rng(0).standard_normal(4000), 0.0, sr=40.0)
        out = preprocess_raw_np([seg], 0, int(50e9), 0.5, 19.0, 40.0)
        assert out.data.size > 0 and np.isfinite(out.data).all()
