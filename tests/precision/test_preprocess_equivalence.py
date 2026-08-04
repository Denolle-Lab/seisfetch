"""4b: numpy ports vs the obspy operations they replace, on identical inputs.

taper / merge / trim are pure arithmetic -> exact equality required.
Fourier resample reuses obspy's own scipy.fftpack recipe -> exact equality
expected; the assertion is exact and any future drift must be justified.
The full preprocess chain is compared against noisepy's ``preprocess_raw``
semantics reimplemented with obspy primitives here (noisepy itself is not
importable in this env; the chain below IS noise_module.py:128-227 at
rm_resp=NO, line for line).
"""

import io
from pathlib import Path

import numpy as np
import pytest

obspy = pytest.importorskip("obspy")
import scipy.signal  # noqa: E402
from obspy.signal.filter import bandpass  # noqa: E402

from seisfetch.contrib.noisepy_adapter import (  # noqa: E402
    bandpass_np,
    check_sample_gaps_np,
    merge_fill0_np,
    preprocess_raw_np,
    resample_fourier_np,
    taper_np,
    trim_pad0_np,
)
from seisfetch.convert import parse_mseed  # noqa: E402

TESTS = Path(__file__).parent.parent
FIXTURES = TESTS / "fixtures"
RNG = np.random.default_rng(11)


def _trace(data, sr=40.0, t0="2023-01-02T00:00:00"):
    tr = obspy.Trace(np.asarray(data))
    tr.stats.sampling_rate = sr
    tr.stats.starttime = obspy.UTCDateTime(t0)
    return tr


class TestTaper:
    @pytest.mark.parametrize("npts", [1000, 999, 72000])
    def test_hann_5pct_exact(self, npts):
        x = RNG.standard_normal(npts)
        tr = _trace(x.copy())
        tr.taper(max_percentage=0.05)
        np.testing.assert_array_equal(taper_np(x, 40.0, 0.05), tr.data)

    def test_max_length_cap_exact(self):
        x = RNG.standard_normal(72000)
        tr = _trace(x.copy())
        tr.taper(max_percentage=0.05, max_length=50)
        np.testing.assert_array_equal(taper_np(x, 40.0, 0.05, max_length=50), tr.data)


class TestMergeFill0:
    @pytest.mark.parametrize("name", ["gap_3seg.mseed", "overlap.mseed"])
    def test_matches_obspy_merge(self, name):
        raw = (FIXTURES / name).read_bytes()
        st = obspy.read(io.BytesIO(raw)).merge(method=1, fill_value=0)
        segs = parse_mseed(raw).segments()["XX.FIX.00.BHZ"]
        merged, t0_ns = merge_fill0_np(segs)
        np.testing.assert_array_equal(merged, st[0].data)
        assert t0_ns == int(round(st[0].stats.starttime.timestamp * 1e9))


class TestBandpass:
    def test_zerophase_exact(self):
        # corners as noisepy builds them at sampling_rate=40: f1=0.45, f4=18
        x = RNG.standard_normal(72000).astype(np.float32)
        ref = bandpass(x, 0.45, 18.0, df=40.0, corners=4, zerophase=True)
        got = bandpass_np(x, 0.45, 18.0, df=40.0, corners=4, zerophase=True)
        np.testing.assert_array_equal(got, ref)


class TestResample:
    # obspy Trace.resample(no_filter=True) vs port, exact
    @pytest.mark.parametrize(
        "npts,sr_in,sr_out",
        [
            (360000, 100.0, 40.0),  # HH -> 40 Hz, even npts
            (359999, 100.0, 40.0),  # odd npts
            (72000, 40.0, 20.0),
        ],
    )
    def test_fourier_resample_exact(self, npts, sr_in, sr_out):
        x = RNG.standard_normal(npts)
        tr = _trace(x.copy(), sr=sr_in)
        tr.resample(sr_out)  # no_filter=True is the default
        got = resample_fourier_np(x, sr_in, sr_out)
        # exact: same scipy.fftpack recipe. Record any observed drift here
        # before relaxing (observed max |diff| == 0.0 on scipy 1.16).
        np.testing.assert_array_equal(got, tr.data)


class TestTrim:
    def test_pad_both_sides_exact(self):
        x = RNG.standard_normal(4000).astype(np.float32)
        t0 = obspy.UTCDateTime("2023-01-02T00:00:10")
        tr = _trace(x.copy(), sr=40.0, t0=str(t0))
        start = t0 - 5.0
        end = t0 + 4000 / 40.0 + 3.0
        tr.trim(starttime=start, endtime=end, pad=True, fill_value=0)
        got, got_t0 = trim_pad0_np(
            x,
            int(t0.timestamp * 1e9),
            40.0,
            int(start.timestamp * 1e9),
            int(end.timestamp * 1e9),
        )
        np.testing.assert_array_equal(got, tr.data)
        assert got_t0 == int(round(tr.stats.starttime.timestamp * 1e9))

    def test_cut_interior_exact(self):
        x = RNG.standard_normal(4000).astype(np.float32)
        t0 = obspy.UTCDateTime("2023-01-02T00:00:00")
        tr = _trace(x.copy(), sr=40.0, t0=str(t0))
        start, end = t0 + 10.0, t0 + 60.0
        tr.trim(starttime=start, endtime=end, pad=True, fill_value=0)
        got, _ = trim_pad0_np(
            x,
            int(t0.timestamp * 1e9),
            40.0,
            int(start.timestamp * 1e9),
            int(end.timestamp * 1e9),
        )
        np.testing.assert_array_equal(got, tr.data)


class TestGapRejection:
    def test_same_decision_as_noisepy_logic(self):
        raw = (FIXTURES / "gap_3seg.mseed").read_bytes()
        segs = parse_mseed(raw).segments()["XX.FIX.00.BHZ"]
        t0 = segs[0].starttime_ns
        t1 = segs[-1].endtime_ns
        # over the true span the gap fraction is small -> accepted
        assert check_sample_gaps_np(segs, t0, t1)
        # over a day-long window the segments cover ~3 min -> gaps dominate?
        # noisepy's portion_gaps counts only INTER-SEGMENT gaps, not edge
        # gaps, so the decision must stay 'accepted' for a day window too
        day_ns = int(86400e9)
        assert check_sample_gaps_np(segs, t0, t0 + day_ns)

    def test_reject_over_100_segments(self):
        raw = (FIXTURES / "gap_3seg.mseed").read_bytes()
        seg = parse_mseed(raw).segments()["XX.FIX.00.BHZ"][0]
        assert check_sample_gaps_np([seg] * 101, 0, int(86400e9)) == []


class TestFullChain:
    """preprocess_raw_np vs the obspy chain from noise_module.py:128-227."""

    def _obspy_chain(self, raw, start, end, freqmin=0.5, freqmax=19.0, sr=40.0):
        st = obspy.read(io.BytesIO(raw))
        # check_sample_gaps: trivial for these fixtures (few segments,
        # small gap fraction over the fixture's own span)
        sps = int(st[0].stats.sampling_rate)
        f1 = 0.9 * freqmin
        if 1.1 * freqmax > 0.45 * sr:
            f4 = 0.45 * sr
        else:
            f4 = 1.1 * freqmax
        for ii in range(len(st)):
            st[ii].data[~np.isfinite(st[ii].data)] = 0
            st[ii].data = np.float32(st[ii].data)
            st[ii].data = scipy.signal.detrend(st[ii].data, type="constant")
            st[ii].data = scipy.signal.detrend(st[ii].data, type="linear")
            st[ii] = st[ii].taper(max_percentage=0.05)
        if len(st) > 1:
            st.merge(method=1, fill_value=0)
        st[0].taper(max_percentage=0.05, max_length=50)
        st[0].data = np.float32(
            bandpass(st[0].data, f1, f4, df=sps, corners=4, zerophase=True)
        )
        if abs(sr - sps) > 1e-4:
            st.resample(sr)
        st[0].trim(starttime=start, endtime=end, pad=True, fill_value=0)
        return st[0]

    @pytest.mark.parametrize("name", ["gap_3seg.mseed", "enc_float32.mseed"])
    def test_chain_equivalence(self, name):
        raw = (FIXTURES / name).read_bytes()
        segs = parse_mseed(raw).segments()["XX.FIX.00.BHZ"]
        t0 = segs[0].starttime_ns
        start_ns = t0 - int(2e9)
        end_ns = segs[-1].endtime_ns + int(3e9)

        start = obspy.UTCDateTime(start_ns / 1e9)
        end = obspy.UTCDateTime(end_ns / 1e9)
        ref = self._obspy_chain(raw, start, end)
        got = preprocess_raw_np(segs, start_ns, end_ns, 0.5, 19.0, 40.0)

        assert got.sampling_rate == ref.stats.sampling_rate
        assert got.data.shape == ref.data.shape
        assert got.start_timestamp == pytest.approx(
            ref.stats.starttime.timestamp, abs=1e-6
        )
        np.testing.assert_array_equal(got.data, ref.data)
