"""Tests for the live Earth2Studio source: channel-text metadata parsing,
mandatory calibration, window assembly, and real async."""

import asyncio
from datetime import datetime, timezone

import numpy as np
import pytest

pytest.importorskip("xarray")
pytest.importorskip("pandas")

from seisfetch.earth2 import SeisfetchLiveSource
from seisfetch.fdsn import ChannelEpoch, parse_channel_text
from tests.helpers import make_mseed

TEXT = (
    "#Network|Station|Location|Channel|Latitude|Longitude|Elevation|Depth|"
    "Azimuth|Dip|SensorDescription|Scale|ScaleFreq|ScaleUnits|SampleRate|"
    "StartTime|EndTime\n"
    "NZ|WEL|10|HHZ|-41.2865|174.768|138.0|0.0|0.0|-90.0|Broadband|"
    "600000000.0|1.0|M/S|100.0|2010-01-01T00:00:00|\n"
    "NZ|WEL|10|HHZ|-41.2865|174.768|138.0|0.0|0.0|-90.0|Old sensor|"
    "300000000.0|1.0|M/S|100.0|2000-01-01T00:00:00|2009-12-31T23:59:59\n"
)


class TestChannelText:
    def test_parse(self):
        eps = parse_channel_text(TEXT)
        assert len(eps) == 2
        ep = eps[0]
        assert (ep.network, ep.station, ep.location, ep.channel) == (
            "NZ",
            "WEL",
            "10",
            "HHZ",
        )
        assert ep.scale == 600000000.0
        assert ep.latitude == pytest.approx(-41.2865)
        assert ep.end is None

    def test_covers_selects_epoch(self):
        eps = parse_channel_text(TEXT)
        now = [e for e in eps if e.covers("2022-01-02T00:00:00")]
        old = [e for e in eps if e.covers("2005-06-01T00:00:00")]
        assert len(now) == 1 and now[0].scale == 600000000.0
        assert len(old) == 1 and old[0].scale == 300000000.0

    def test_covers_is_timezone_safe(self):
        # lexicographic string comparison fails on offset/fraction variants;
        # these must all select the modern epoch (Copilot review, PR #5)
        eps = parse_channel_text(TEXT)
        modern = eps[0]
        for t in (
            "2022-01-02T00:00:00+00:00",
            "2022-01-02T00:00:00Z",
            "2022-01-01T19:00:00-05:00",
            "2022-01-02T00:00:00.0000",
        ):
            assert modern.covers(t), t
        # 2009-12-31T19:00-05:00 == 2010-01-01T00:00Z: modern epoch, not
        # the old one — exactly the case string comparison gets wrong
        assert modern.covers("2009-12-31T19:00:00-05:00")
        assert not eps[1].covers("2009-12-31T19:00:00-05:00")

    def test_blank_scale_is_none(self):
        row = TEXT.splitlines()[1].split("|")
        row[11] = ""
        eps = parse_channel_text("|".join(row))
        assert eps[0].scale is None


class TestLiveSource:
    NSLC = "IU.ANMO.00.BHZ"

    def _source(self, monkeypatch, calibrate="gain", raw=None):
        src = SeisfetchLiveSource([self.NSLC], window_s=5.0, calibrate=calibrate)
        # inject metadata (no network in unit tests)
        src._meta[self.NSLC] = [
            ChannelEpoch(
                network="IU",
                station="ANMO",
                location="00",
                channel="BHZ",
                latitude=34.9,
                longitude=-106.5,
                elevation=1700.0,
                depth=0.0,
                scale=2.0,
                scale_frequency=1.0,
                scale_units="M/S",
                sample_rate=100.0,
                start="1990-01-01T00:00:00",
                end=None,
            )
        ]
        if raw is None:
            raw = make_mseed()  # IU.ANMO.00.BHZ, 100 sps, 2024-01-15T00:00Z

        def fake_get_raw(self_client, net, sta, day, location="", channel=""):
            return raw

        from seisfetch.s3 import S3OpenClient

        monkeypatch.setattr(S3OpenClient, "get_raw", fake_get_raw)
        return src

    def test_requires_calibration(self):
        with pytest.raises(ValueError, match="physical units"):
            SeisfetchLiveSource(["IU.ANMO.00.BHZ"], calibrate="none")

    def test_gain_corrected_window(self, monkeypatch):
        raw = make_mseed()  # one payload shared by both sources
        src = self._source(monkeypatch, raw=raw)
        t0 = datetime(2024, 1, 15, 0, 0, 0, tzinfo=timezone.utc)
        da = src(t0)
        assert da.dims == ("time", "variable", "sample")
        assert da.shape == (1, 1, 500)  # 5 s at 100 sps
        assert da.attrs["calibration"] == "gain"
        # gain division: scale=2 output x2 must equal scale=1 output exactly
        src2 = self._source(monkeypatch, raw=raw)
        src2._meta[self.NSLC][0] = ChannelEpoch(
            **{**src2._meta[self.NSLC][0].__dict__, "scale": 1.0}
        )
        da2 = src2(t0)
        np.testing.assert_allclose(da.values * 2.0, da2.values)

    def test_window_count_and_coords(self, monkeypatch):
        src = self._source(monkeypatch)
        lat, lon, elev = src.coords(self.NSLC)
        assert lat == pytest.approx(34.9)
        t0 = datetime(2024, 1, 15, 0, 0, 0, tzinfo=timezone.utc)
        da = src([t0, t0])
        assert da.shape[0] == 2

    def test_zero_scale_raises(self, monkeypatch):
        src = self._source(monkeypatch)
        src._meta[self.NSLC][0] = ChannelEpoch(
            **{**src._meta[self.NSLC][0].__dict__, "scale": 0.0}
        )
        t0 = datetime(2024, 1, 15, 0, 0, 0, tzinfo=timezone.utc)
        with pytest.raises(LookupError, match="Scale=0"):
            src(t0)

    def test_units_from_metadata(self, monkeypatch):
        # an accelerometer channel must not be labeled m/s
        src = self._source(monkeypatch)
        src._meta[self.NSLC][0] = ChannelEpoch(
            **{**src._meta[self.NSLC][0].__dict__, "scale_units": "M/S**2"}
        )
        t0 = datetime(2024, 1, 15, 0, 0, 0, tzinfo=timezone.utc)
        da = src(t0)
        assert da.attrs["units"] == "M/S**2 (gain-corrected)"

    def test_datacenter_override_routes_metadata(self):
        src = SeisfetchLiveSource(["IU.ANMO.00.BHZ"], datacenter="geonet")
        assert src._provider("IU") == "GEONET"
        src2 = SeisfetchLiveSource(["IU.ANMO.00.BHZ"])
        assert src2._provider("IU") == "EARTHSCOPE"

    def test_missing_scale_raises(self, monkeypatch):
        src = self._source(monkeypatch)
        src._meta[self.NSLC][0] = ChannelEpoch(
            **{**src._meta[self.NSLC][0].__dict__, "scale": None}
        )
        t0 = datetime(2024, 1, 15, 0, 0, 0, tzinfo=timezone.utc)
        with pytest.raises(LookupError, match="Scale"):
            src(t0)

    def test_async_fetch_is_real(self, monkeypatch):
        src = self._source(monkeypatch)
        t0 = datetime(2024, 1, 15, 0, 0, 0, tzinfo=timezone.utc)

        async def go():
            return await src.fetch(t0)

        da = asyncio.run(go())
        assert da.shape == (1, 1, 500)
