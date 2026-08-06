"""B4: the response module must fail LOUDLY on defective metadata, and its
A0 handling must match evalresp's CONDITIONAL rule (recompute at the stage
gain frequency only when it differs from NormalizationFrequency)."""

import io

import numpy as np
import pytest

obspy = pytest.importorskip("obspy")

from seisfetch.contrib.response import (  # noqa: E402
    ChannelResponse,
    FIRStage,
    PZStage,
    evaluate_response,
    parse_stationxml_response,
    remove_response_np,
)

XML_TMPL = """<?xml version="1.0" encoding="UTF-8"?>
<FDSNStationXML xmlns="http://www.fdsn.org/xml/station/1" schemaVersion="1.1">
 <Source>test</Source><Created>2020-01-01T00:00:00Z</Created>
 <Network code="XX"><Station code="TST" startDate="2000-01-01T00:00:00Z">
  <Latitude>0</Latitude><Longitude>0</Longitude><Elevation>0</Elevation>
  <Site><Name>t</Name></Site>
  <Channel code="BHZ" locationCode="" startDate="2000-01-01T00:00:00Z"
           endDate="2015-01-01T00:00:00Z">
   <Latitude>0</Latitude><Longitude>0</Longitude><Elevation>0</Elevation>
   <Depth>0</Depth><SampleRate>40</SampleRate>
   <Response>
    <InstrumentSensitivity><Value>{sens}</Value><Frequency>{sens_f}</Frequency>
     <InputUnits><Name>M/S</Name></InputUnits>
     <OutputUnits><Name>COUNTS</Name></OutputUnits>
    </InstrumentSensitivity>
    <Stage number="1"><PolesZeros>
      <InputUnits><Name>M/S</Name></InputUnits>
      <OutputUnits><Name>V</Name></OutputUnits>
      <PzTransferFunctionType>LAPLACE (RADIANS/SECOND)</PzTransferFunctionType>
      <NormalizationFactor>{a0}</NormalizationFactor>
      <NormalizationFrequency>{fn}</NormalizationFrequency>
      <Zero number="0"><Real>0</Real><Imaginary>0</Imaginary></Zero>
      <Zero number="1"><Real>0</Real><Imaginary>0</Imaginary></Zero>
      <Pole number="0"><Real>-0.037</Real><Imaginary>0.037</Imaginary></Pole>
      <Pole number="1"><Real>-0.037</Real><Imaginary>-0.037</Imaginary></Pole>
      <Pole number="2"><Real>-503</Real><Imaginary>0</Imaginary></Pole>
     </PolesZeros>
     <StageGain><Value>{gain}</Value><Frequency>{fg}</Frequency></StageGain>
    </Stage>
   </Response>
  </Channel>
 </Station></Network>
</FDSNStationXML>
"""


def _xml(a0=503.0, fn=1.0, fg=1.0, gain=1500.0, sens=1500.0, sens_f=1.0):
    return XML_TMPL.format(
        a0=a0, fn=fn, fg=fg, gain=gain, sens=sens, sens_f=sens_f
    ).encode()


def _evalresp(xml, output="VEL", nfft=1024):
    inv = obspy.read_inventory(io.BytesIO(xml))
    ob = inv.get_response("XX.TST..BHZ", obspy.UTCDateTime("2010-06-01"))
    return ob.get_evalresp_response(t_samp=1 / 40.0, nfft=nfft, output=output)


class TestConditionalA0:
    """evalresp uses the XML A0 as-is when fn == fg, and recomputes at fg
    only when they differ. Both branches must match evalresp — including on
    DELIBERATELY WRONG A0 values (the metadata-defect population)."""

    def test_fn_equals_fg_uses_wrong_a0_like_evalresp(self):
        xml = _xml(a0=1006.0, fn=1.0, fg=1.0)  # A0 ~2x the correct value
        h_ob, freqs = _evalresp(xml)
        resp = parse_stationxml_response(xml, "XX", "TST", "", "BHZ", "2010-06-01")
        h_np = evaluate_response(freqs, resp, output="VEL", mode="full")
        band = (freqs > 1e-2) & (freqs < 19)
        rel = np.abs(h_np[band] - h_ob[band]) / np.abs(h_ob[band])
        assert rel.max() < 1e-9  # both reproduce the defective A0

    def test_fn_differs_from_fg_recomputes_like_evalresp(self):
        xml = _xml(a0=1006.0, fn=0.03, fg=1.0)  # wrong A0, fn != fg
        h_ob, freqs = _evalresp(xml)
        resp = parse_stationxml_response(xml, "XX", "TST", "", "BHZ", "2010-06-01")
        h_np = evaluate_response(freqs, resp, output="VEL", mode="full")
        band = (freqs > 1e-2) & (freqs < 19)
        rel = np.abs(h_np[band] - h_ob[band]) / np.abs(h_ob[band])
        assert rel.max() < 1e-9  # both ignore A0 and renormalize at fg


class TestEpochTimezones:
    def test_offset_timestamp_converted_to_utc(self):
        xml = _xml()
        # 2014-12-31T20:00:00-08:00 == 2015-01-01T04:00Z, AFTER epoch close
        with pytest.raises(LookupError):
            parse_stationxml_response(
                xml, "XX", "TST", "", "BHZ", "2014-12-31T20:00:00-08:00"
            )

    def test_z_offset_and_naive_agree(self):
        xml = _xml()
        for t in (
            "2010-06-01T00:00:00",
            "2010-06-01T00:00:00Z",
            "2010-06-01T00:00:00+00:00",
        ):
            assert (
                parse_stationxml_response(xml, "XX", "TST", "", "BHZ", t).sensitivity
                == 1500.0
            )

    def test_arbitrary_fraction_widths_parse(self):
        # EarthScope II StationXML carries ".0000" (4 digits); pre-3.11
        # fromisoformat only accepts 3 or 6 -> fractions must be normalized
        from seisfetch.contrib.response import _parse_iso_utc

        base = _parse_iso_utc("2020-04-29T18:00:00")
        for frac in (".0000", ".0", ".000000000", ".123"):
            for suffix in ("", "Z", "+00:00"):
                got = _parse_iso_utc(f"2020-04-29T18:00:00{frac}{suffix}")
                if frac == ".123":
                    assert got.microsecond == 123000
                else:
                    assert got == base

    def test_dashes_location_means_blank(self):
        xml = _xml()
        assert (
            parse_stationxml_response(
                xml, "XX", "TST", "--", "BHZ", "2010-06-01"
            ).sensitivity
            == 1500.0
        )


class TestLoudFailures:
    def test_gain_frequency_zero_raises_not_nan(self):
        # zeros at the origin: |shape(0)| = 0 -> old code produced 100% NaN
        xml = _xml(fn=0.5, fg=0.0)
        resp = parse_stationxml_response(xml, "XX", "TST", "", "BHZ", "2010-06-01")
        with pytest.raises(ValueError, match="renormalize"):
            evaluate_response(np.array([1.0, 2.0]), resp, mode="full")

    def test_zero_sum_fir_raises(self):
        resp = ChannelResponse(
            stages=[
                PZStage(
                    "LAPLACE (RADIANS/SECOND)",
                    1.0,
                    np.array([-1 + 0j]),
                    np.array([]),
                    1.0,
                    gain_frequency=1.0,
                    normalization_frequency=1.0,
                ),
                FIRStage(np.array([0.5, -0.5]), 1.0, 40.0, 0.0),
            ],
            sensitivity=1.0,
            sensitivity_frequency=1.0,
        )
        with pytest.raises(ValueError, match="sum to zero"):
            evaluate_response(np.array([1.0]), resp, mode="full")

    def test_paz_without_sensitivity_raises(self):
        resp = ChannelResponse(
            stages=[
                PZStage(
                    "LAPLACE (RADIANS/SECOND)",
                    1.0,
                    np.array([-1 + 0j]),
                    np.array([]),
                    1500.0,
                    gain_frequency=1.0,
                    normalization_frequency=1.0,
                )
            ]
        )
        with pytest.raises(ValueError, match="InstrumentSensitivity"):
            evaluate_response(np.array([1.0]), resp, mode="paz")

    def test_zero_stage_gain_raises_at_parse(self):
        with pytest.raises(ValueError, match="StageGain/Value is 0.0"):
            parse_stationxml_response(
                _xml(gain=0.0), "XX", "TST", "", "BHZ", "2010-06-01"
            )

    def test_zero_sensitivity_raises_at_parse(self):
        with pytest.raises(ValueError, match="Sensitivity"):
            parse_stationxml_response(
                _xml(sens=0.0), "XX", "TST", "", "BHZ", "2010-06-01"
            )

    def test_polynomial_stage_raises(self):
        xml = (
            _xml()
            .decode()
            .replace(
                "<PolesZeros>",
                "<Polynomial><ApproximationType>MACLAURIN"
                "</ApproximationType></Polynomial><PolesZeros>",
                1,
            )
            .encode()
        )
        with pytest.raises(NotImplementedError, match="Polynomial"):
            parse_stationxml_response(xml, "XX", "TST", "", "BHZ", "2010-06-01")

    def test_tiny_segment_remove_response_is_finite(self):
        xml = _xml()
        resp = parse_stationxml_response(xml, "XX", "TST", "", "BHZ", "2010-06-01")
        rng = np.random.default_rng(3)
        out = remove_response_np(
            rng.standard_normal(19) * 1000,
            40.0,
            resp,
            output="VEL",
            water_level=60,
        )
        assert np.isfinite(out).all()


class TestSmallCompat:
    def test_def_output_is_native(self):
        xml = _xml()
        resp = parse_stationxml_response(xml, "XX", "TST", "", "BHZ", "2010-06-01")
        f = np.array([0.5, 1.0, 5.0])
        # native units are M/S, so DEF == VEL
        np.testing.assert_array_equal(
            evaluate_response(f, resp, output="DEF", mode="full"),
            evaluate_response(f, resp, output="VEL", mode="full"),
        )

    def test_m_s_s_unit_alias(self):
        resp = ChannelResponse(input_units="M/S/S")
        assert resp.native_exponent == 2
