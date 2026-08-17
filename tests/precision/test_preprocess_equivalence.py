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
from obspy.geodetics.base import HAS_GEOGRAPHICLIB  # noqa: E402
from obspy.signal.filter import bandpass  # noqa: E402

from seisfetch.contrib.noisepy_adapter import (  # noqa: E402
    bandpass_np,
    check_sample_gaps_np,
    merge_fill0_np,
    preprocess_raw_np,
    resample_fourier_np,
    segment_interpolate_np,
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


class TestSegmentInterpolate:
    """segment_interpolate_np vs noisepy's numba-compiled original.

    The numba signature is float32[:](float32[:], float32), and numba's type
    unification makes (1 - nfric) FLOAT64 while nfric*sig1[ii] stays float32
    — a precision mix no uniform-dtype implementation reproduces (found on
    the 2026-08 three-archive cross-correlation run: all-float32 was 1 ulp
    off on ~30% of samples, 2e-5 of peak in the stacked CCF). The reference
    loop below encodes those exact semantics; run_xcorr_eval.py validates
    the port against actual numba end-to-end (bit-identical, 2026-08-05).
    """

    @staticmethod
    def _numba_reference(sig1, nfric):
        nf = np.float32(nfric)
        out = np.empty(len(sig1), np.float32)
        out[0], out[-1] = sig1[0], sig1[-1]
        for ii in range(1, len(sig1) - 1):
            out[ii] = np.float32(
                (1.0 - np.float64(nf)) * np.float64(sig1[ii + 1])
                + np.float64(nf * sig1[ii])
            )
        return out

    @pytest.mark.parametrize("nfric", [0.39076, 0.5, 0.0001, 0.9999])
    def test_matches_numba_semantics(self, nfric):
        x = (RNG.standard_normal(5001) * 1000).astype(np.float32)
        np.testing.assert_array_equal(
            segment_interpolate_np(x, nfric), self._numba_reference(x, nfric)
        )

    def test_matches_actual_numba_if_available(self):
        noise_module = pytest.importorskip("noisepy.seis.noise_module")
        x = (RNG.standard_normal(200001) * 1000).astype(np.float32)
        ref = noise_module.segment_interpolate(x.copy(), 0.39076)
        np.testing.assert_array_equal(segment_interpolate_np(x, 0.39076), ref)


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

    # sr=20 exercises the Fourier-resample branch inside the chain (fixtures
    # start on integer seconds, so the fric sub-sample branch stays off —
    # that branch is covered by TestSegmentInterpolate + run_xcorr_eval.py)
    @pytest.mark.parametrize(
        "name,sr",
        [
            ("gap_3seg.mseed", 40.0),
            ("enc_float32.mseed", 40.0),
            ("enc_float32.mseed", 20.0),
        ],
    )
    def test_chain_equivalence(self, name, sr):
        raw = (FIXTURES / name).read_bytes()
        segs = parse_mseed(raw).segments()["XX.FIX.00.BHZ"]
        t0 = segs[0].starttime_ns
        start_ns = t0 - int(2e9)
        end_ns = segs[-1].endtime_ns + int(3e9)

        start = obspy.UTCDateTime(start_ns / 1e9)
        end = obspy.UTCDateTime(end_ns / 1e9)
        freqmax = 19.0 if sr == 40.0 else 8.0
        ref = self._obspy_chain(raw, start, end, freqmax=freqmax, sr=sr)
        got = preprocess_raw_np(segs, start_ns, end_ns, 0.5, freqmax, sr)

        assert got.sampling_rate == ref.stats.sampling_rate
        assert got.data.shape == ref.data.shape
        assert got.start_timestamp == pytest.approx(
            ref.stats.starttime.timestamp, abs=1e-6
        )
        np.testing.assert_array_equal(got.data, ref.data)


class TestGps2DistAzimuth:
    """The CCF metadata path: every station pair's dist/azi/baz.

    noisepy writes these into every saved cross-correlation, so the port has
    to be EXACTLY equal to obspy's, not merely close.
    """

    @pytest.mark.skipif(
        HAS_GEOGRAPHICLIB,
        reason="obspy delegates to geographiclib when it is installed; this "
        "port reproduces obspy's own Vincenty branch, which is what a "
        "default obspy install runs. Tolerance coverage for the "
        "geographiclib case is in test_agrees_with_obspy_either_branch.",
    )
    def test_matches_obspy_exactly_on_random_pairs(self):
        import random

        from obspy.geodetics.base import gps2dist_azimuth

        from seisfetch.contrib.obspy_ports import gps2dist_azimuth_np

        random.seed(20260816)
        checked = 0
        for _ in range(2000):
            lat1, lon1 = random.uniform(-90, 90), random.uniform(-360, 360)
            lat2, lon2 = random.uniform(-90, 90), random.uniform(-360, 360)
            try:
                ref = gps2dist_azimuth(lat1, lon1, lat2, lon2)
            except Exception:
                continue  # antipodal non-convergence; noisepy pairs never hit this
            assert gps2dist_azimuth_np(lat1, lon1, lat2, lon2) == ref
            checked += 1
        assert checked > 1900

    def test_agrees_with_obspy_either_branch(self):
        """Runs whichever branch obspy takes, so CI cannot silently lose cover.

        Vincenty and geographiclib disagree by ~1e-5 m over ~500 km — five
        microns, against CCF metadata whose consumers round to metres. Exact
        equality is still the bar for the default (Vincenty) install; this is
        the floor that must hold either way.
        """
        from obspy.geodetics.base import gps2dist_azimuth

        from seisfetch.contrib.obspy_ports import gps2dist_azimuth_np

        for lat1, lon1, lat2, lon2 in [
            (34.1, -118.1, 37.9, -122.3),
            (33.61, -116.46, 35.82, -120.34),
            (0.0, 0.0, 0.0, 10.0),
            (10.0, 190.0, -10.0, -170.0),
        ]:
            got = gps2dist_azimuth_np(lat1, lon1, lat2, lon2)
            ref = gps2dist_azimuth(lat1, lon1, lat2, lon2)
            assert got[0] == pytest.approx(ref[0], abs=1e-3)  # metres
            # modulo 360: on a due-south, same-meridian pair obspy's Vincenty
            # reports back-azimuth 360.0 where geographiclib reports 0.0. The
            # port inherits Vincenty's spelling because it reproduces that
            # branch statement for statement.
            for g, r in zip(got[1:], ref[1:]):
                assert (g - r + 180) % 360 - 180 == pytest.approx(0.0, abs=1e-6)

    @pytest.mark.skipif(
        HAS_GEOGRAPHICLIB,
        reason="obspy delegates to geographiclib when installed; see "
        "test_agrees_with_obspy_either_branch",
    )
    def test_matches_obspy_on_station_pairs_and_degenerate_cases(self):
        from obspy.geodetics.base import gps2dist_azimuth

        from seisfetch.contrib.obspy_ports import gps2dist_azimuth_np

        pairs = [
            (34.1, -118.1, 37.9, -122.3),  # PASC - PKD class
            (33.61, -116.46, 35.82, -120.34),  # PFO - PKD class
            (34.0, -118.0, 34.0, -118.0),  # identical points -> (0, 0, 0)
            (0.0, 0.0, 0.0, 10.0),  # equatorial line (cos2sigma_m branch)
            (10.0, 190.0, -10.0, -170.0),  # longitudes needing normalization
        ]
        for lat1, lon1, lat2, lon2 in pairs:
            assert gps2dist_azimuth_np(lat1, lon1, lat2, lon2) == gps2dist_azimuth(
                lat1, lon1, lat2, lon2
            )

    def test_latitude_out_of_bounds_raises(self):
        import pytest

        from seisfetch.contrib.obspy_ports import gps2dist_azimuth_np

        with pytest.raises(ValueError, match="lat1"):
            gps2dist_azimuth_np(91.0, 0.0, 0.0, 0.0)
        with pytest.raises(ValueError, match="lat2"):
            gps2dist_azimuth_np(0.0, 0.0, -91.0, 0.0)
