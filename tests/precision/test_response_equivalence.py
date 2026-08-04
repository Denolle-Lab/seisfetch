"""Response module vs obspy's compiled evalresp, on real CI.PASC metadata.

Fixture: tests/fixtures/CI_PASC_00_BHZ.xml — two real epochs of CI.PASC.00.BHZ
(2007 STS-1-era and 2015 sensors), 4 stages each (analog PZ, gain, digitizer
Coefficients, 39-tap FIR). The 2007 epoch is the interesting one: its XML
NormalizationFrequency (0.03 Hz) differs from the stage gain frequency
(1.0 Hz), which exposed that evalresp IGNORES the XML NormalizationFactor and
recomputes A0 at the stage gain's frequency (verified by perturbation:
doubling A0 or changing f_norm has no effect on evalresp output).
"""

import io
from pathlib import Path

import numpy as np
import pytest

obspy = pytest.importorskip("obspy")

from seisfetch.contrib.response import (  # noqa: E402
    evaluate_response,
    parse_stationxml_response,
    remove_response_np,
    translate_resp_np,
)
from seisfetch.convert import parse_mseed  # noqa: E402

TESTS = Path(__file__).parent.parent
XML = (TESTS / "fixtures" / "CI_PASC_00_BHZ.xml").read_bytes()
EPOCHS = ["2011-03-11T12:00:00", "2022-01-02T12:00:00"]


@pytest.fixture(scope="module")
def inv():
    return obspy.read_inventory(io.BytesIO(XML))


class TestEvaluateResponse:
    @pytest.mark.parametrize("t", EPOCHS, ids=lambda t: t[:4])
    @pytest.mark.parametrize("output", ["VEL", "ACC", "DISP"])
    def test_full_mode_matches_evalresp(self, inv, t, output):
        resp = parse_stationxml_response(XML, "CI", "PASC", "00", "BHZ", t)
        ob = inv.get_response("CI.PASC.00.BHZ", obspy.UTCDateTime(t))
        h_ob, freqs = ob.get_evalresp_response(
            t_samp=1 / 40.0, nfft=8192, output=output
        )
        h_np = evaluate_response(freqs, resp, output=output, mode="full")
        band = (freqs > 1e-3) & (freqs < 19.9)
        rel = np.abs(h_np[band] - h_ob[band]) / np.abs(h_ob[band])
        # observed 4.6e-11 (evalresp's float32 internals); assert an order
        # of margin
        assert rel.max() < 1e-9

    def test_paz_mode_band_errors_documented(self, inv):
        """paz mode (stage-1 PZ x sensitivity): fine at low f, wrong near
        Nyquist — the documented trade-off."""
        t = EPOCHS[1]
        resp = parse_stationxml_response(XML, "CI", "PASC", "00", "BHZ", t)
        ob = inv.get_response("CI.PASC.00.BHZ", obspy.UTCDateTime(t))
        h_ob, freqs = ob.get_evalresp_response(t_samp=1 / 40.0, nfft=8192, output="VEL")
        h_paz = evaluate_response(freqs, resp, output="VEL", mode="paz")
        low = (freqs >= 0.05) & (freqs <= 4.0)
        rel_low = np.abs(np.abs(h_paz[low]) / np.abs(h_ob[low]) - 1).max()
        assert rel_low < 0.02  # observed 0.66-1.3% across epochs
        top = (freqs >= 16.0) & (freqs <= 19.0)
        rel_top = np.abs(np.abs(h_paz[top]) / np.abs(h_ob[top]) - 1).max()
        assert rel_top > 1.0  # the FIR roll-off is NOT modeled: >100% error


class TestRemoveResponse:
    def _slice(self, n=48000):
        raw = (TESTS / "bench.mseed").read_bytes()
        seg = parse_mseed(raw).segments()["CI.PASC.00.BHZ"][0]
        return seg.data[:n].astype(np.float64), seg.sampling_rate

    def test_waveform_machine_precision_vs_obspy(self, inv):
        data, fs = self._slice()
        pre_filt = (0.36, 0.4, 18.0, 19.8)
        tr = obspy.Trace(data.copy())
        tr.stats.sampling_rate = fs
        tr.stats.network, tr.stats.station = "CI", "PASC"
        tr.stats.location, tr.stats.channel = "00", "BHZ"
        tr.stats.starttime = obspy.UTCDateTime("2011-03-11T00:00:00")
        tr.remove_response(
            inventory=inv, output="VEL", water_level=60, pre_filt=pre_filt
        )
        resp = parse_stationxml_response(
            XML, "CI", "PASC", "00", "BHZ", "2011-03-11T12:00:00"
        )
        out = remove_response_np(
            data, fs, resp, output="VEL", water_level=60, pre_filt=pre_filt
        )
        rel = np.abs(tr.data - out).max() / np.abs(tr.data).max()
        # observed 6.6e-16 on the full day; slices stay at machine precision
        assert rel < 1e-12

    def test_water_level_none_branch(self, inv):
        data, fs = self._slice(8000)
        resp = parse_stationxml_response(
            XML, "CI", "PASC", "00", "BHZ", "2011-03-11T12:00:00"
        )
        out = remove_response_np(
            data,
            fs,
            resp,
            output="VEL",
            water_level=None,
            pre_filt=(0.36, 0.4, 18.0, 19.8),
        )
        assert np.isfinite(out).all()

    def test_translate_resp_flat_close_to_removal_in_band(self, inv):
        """SeisIO-style translation with a flat target, band-limited, should
        agree with water-level removal inside the passband."""
        from seisfetch.contrib.noisepy_adapter import bandpass_np

        data, fs = self._slice()
        resp = parse_stationxml_response(
            XML, "CI", "PASC", "00", "BHZ", "2011-03-11T12:00:00"
        )
        a = remove_response_np(
            data,
            fs,
            resp,
            output="VEL",
            water_level=60,
            pre_filt=(0.36, 0.4, 4.0, 6.0),
            taper=False,
            zero_mean=True,
        )
        b = translate_resp_np(data - data.mean(), fs, resp, mode="full")
        # compare in the common band, away from edges
        a_b = bandpass_np(a, 0.5, 3.5, df=fs)
        b_b = bandpass_np(b, 0.5, 3.5, df=fs)
        edge = 4000
        corr = np.corrcoef(a_b[edge:-edge], b_b[edge:-edge])[0, 1]
        assert corr > 0.999
