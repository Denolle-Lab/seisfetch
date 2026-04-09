"""Tests for seisfetch.s3 — multi-datacenter S3 client."""

import boto3
import numpy as np
import pytest
from moto import mock_aws

from seisfetch.s3 import (
    S3OpenClient,
    _earthscope_key,
    _ncedc_key,
    _scedc_key,
    route_network,
)
from seisfetch.utils import OPEN_BUCKET
from tests.helpers import make_mseed

# ── Key builder tests ──────────────────────────────────────────────── #


class TestKeyBuilders:
    def test_earthscope(self):
        k = _earthscope_key("IU", "ANMO", 2024, 15)
        assert k == "miniseed/IU/2024/015/ANMO.IU.2024.015"

    def test_scedc(self):
        k = _scedc_key("CI", "SDD", 2016, 183, location="", channel="HHZ")
        assert k == "continuous_waveforms/2016/2016_183/CISDD__HHZ___2016183.ms"

    def test_ncedc(self):
        k = _ncedc_key("BK", "BRK", 2024, 100, location="00", channel="BHZ")
        assert k == "continuous_waveforms/BK/2024/2024.100/BRK.BK.BHZ.00.D.2024.100"

    def test_ncedc_no_location(self):
        k = _ncedc_key("NC", "JBL", 2023, 1, location="", channel="HHZ")
        assert "JBL.NC.HHZ..D.2023.001" in k


# ── Network routing ────────────────────────────────────────────────── #


class TestRouteNetwork:
    def test_ci_to_scedc(self):
        assert route_network("CI") == "scedc"

    def test_bk_to_ncedc(self):
        assert route_network("BK") == "ncedc"

    def test_iu_to_earthscope(self):
        assert route_network("IU") == "earthscope"

    def test_uw_to_earthscope(self):
        assert route_network("UW") == "earthscope"

    def test_ta_to_earthscope(self):
        assert route_network("TA") == "earthscope"

    def test_nc_shared_prefers_ncedc(self):
        # NC is in both SCEDC and NCEDC; should prefer NCEDC
        assert route_network("NC") == "ncedc"

    def test_case_insensitive(self):
        assert route_network("ci") == "scedc"


# ── S3OpenClient with EarthScope mock ─────────────────────────────── #


@mock_aws
class TestS3OpenClientEarthScope:
    def _setup(self, network="IU", station="ANMO", year=2024, doy=15):
        s3 = boto3.client("s3", region_name="us-east-2")
        s3.create_bucket(
            Bucket=OPEN_BUCKET,
            CreateBucketConfiguration={"LocationConstraint": "us-east-2"},
        )
        key = _earthscope_key(network, station, year, doy)
        s3.put_object(Bucket=OPEN_BUCKET, Key=key, Body=make_mseed(network, station))

    def _client(self):
        s3 = boto3.client("s3", region_name="us-east-2")
        return S3OpenClient(datacenter="earthscope", max_workers=1, _s3_client=s3)

    def test_get_raw(self):
        self._setup()
        assert (
            len(
                self._client().get_raw(
                    "IU", "ANMO", starttime="2024-01-15", endtime="2024-01-15T01:00:00"
                )
            )
            > 0
        )

    def test_get_raw_missing(self):
        boto3.client("s3", region_name="us-east-2").create_bucket(
            Bucket=OPEN_BUCKET,
            CreateBucketConfiguration={"LocationConstraint": "us-east-2"},
        )
        assert (
            self._client().get_raw(
                "XX", "NOPE", starttime="2024-01-15", endtime="2024-01-15T01:00:00"
            )
            == b""
        )

    def test_parse_roundtrip(self):
        self._setup()
        from seisfetch.convert import parse_mseed

        raw = self._client().get_raw(
            "IU", "ANMO", starttime="2024-01-15", endtime="2024-01-15T01:00:00"
        )
        b = parse_mseed(raw)
        assert len(b) >= 1 and isinstance(b.traces[0].data, np.ndarray)

    def test_starttime_required(self):
        with pytest.raises(ValueError):
            self._client().get_raw("IU", "ANMO", starttime=None)

    def test_list_networks(self):
        self._setup()
        assert "IU" in self._client().list_networks(datacenter="earthscope")

    def test_list_stations(self):
        self._setup(network="UW", station="MBW", year=2024, doy=300)
        assert "MBW" in self._client().list_stations(
            "UW", 2024, 300, datacenter="earthscope"
        )

    def test_multi_day(self):
        s3 = boto3.client("s3", region_name="us-east-2")
        s3.create_bucket(
            Bucket=OPEN_BUCKET,
            CreateBucketConfiguration={"LocationConstraint": "us-east-2"},
        )
        from seisfetch.utils import s3_key

        for doy in (15, 16):
            s3.put_object(
                Bucket=OPEN_BUCKET,
                Key=s3_key("IU", "ANMO", 2024, doy),
                Body=make_mseed(),
            )
        c = self._client()
        c._max_workers = 2
        assert (
            len(
                c.get_raw(
                    "IU", "ANMO", starttime="2024-01-15", endtime="2024-01-16T12:00:00"
                )
            )
            > 0
        )


# ── Channel expansion (SCEDC/NCEDC) ───────────────────────────────── #


class TestChannelExpansion:
    def test_explicit(self):
        assert S3OpenClient._expand_channels("BHZ") == ["BHZ"]

    def test_wildcard_question(self):
        expanded = S3OpenClient._expand_channels("BH?")
        assert "BHZ" in expanded and "BHN" in expanded and len(expanded) == 5

    def test_wildcard_star_partial(self):
        expanded = S3OpenClient._expand_channels("HH*")
        assert "HHZ" in expanded

    def test_bare_star_raises(self):
        with pytest.raises(ValueError, match="Per-channel"):
            S3OpenClient._expand_channels("*")

    def test_bare_empty_raises(self):
        with pytest.raises(ValueError, match="Per-channel"):
            S3OpenClient._expand_channels("")
