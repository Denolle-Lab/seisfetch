"""Fetch-semantics tests for critique blockers B2 (failure contract) and
B3 (location wildcards, half-open day windows). moto + respx, offline."""

import boto3
import pytest
from botocore.exceptions import ClientError

from seisfetch.exceptions import FDSNError, FetchError, NoDataError
from seisfetch.s3 import S3OpenClient
from tests.helpers import make_mseed

moto = pytest.importorskip("moto")
respx = pytest.importorskip("respx")
mock_aws = moto.mock_aws


def _scedc_setup():
    """moto scedc-pds with location-coded channel-days (the real layout)."""
    s3 = boto3.client("s3", region_name="us-west-2")
    s3.create_bucket(
        Bucket="scedc-pds",
        CreateBucketConfiguration={"LocationConstraint": "us-west-2"},
    )
    bodies = {}
    for doy, cha, loc in [
        (2, "BHZ", "00"),
        (2, "BHN", "00"),
        (2, "BHZ", "10"),
        (2, "HHZ", "00"),
        (3, "BHZ", "00"),
    ]:
        key = (
            f"continuous_waveforms/2022/2022_{doy:03d}/"
            f"CIPASC_{cha}{loc}_2022{doy:03d}.ms"
        )
        body = make_mseed(network="CI", station="PASC", channel=cha, location=loc)
        s3.put_object(Bucket="scedc-pds", Key=key, Body=body)
        bodies[(doy, cha, loc)] = body
    client = S3OpenClient(datacenter="scedc", max_workers=2, _s3_client=s3)
    return client, bodies


@mock_aws
class TestLocationWildcardDiscovery:
    def test_default_wildcard_finds_location_coded_channels(self):
        c, bodies = _scedc_setup()
        raw = c.get_raw("CI", "PASC", "2022-01-02", channel="BHZ")  # location="*"
        # discovery must find BOTH loc 00 and loc 10 BHZ objects
        assert raw == bodies[(2, "BHZ", "00")] + bodies[(2, "BHZ", "10")]

    def test_explicit_location_selects_one(self):
        c, bodies = _scedc_setup()
        raw = c.get_raw("CI", "PASC", "2022-01-02", channel="BHZ", location="10")
        assert raw == bodies[(2, "BHZ", "10")]

    def test_channel_wildcard_discovers_not_guesses(self):
        c, bodies = _scedc_setup()
        raw = c.get_raw("CI", "PASC", "2022-01-02", channel="BH?", location="00")
        # BHN + BHZ at loc 00, sorted key order (BHN < BHZ); HHZ excluded
        assert raw == bodies[(2, "BHN", "00")] + bodies[(2, "BHZ", "00")]

    def test_blank_location_still_means_blank(self):
        c, _ = _scedc_setup()
        with pytest.raises(NoDataError):
            c.get_raw("CI", "PASC", "2022-01-02", channel="BHZ", location="")


@mock_aws
class TestFailureContract:
    def test_all_missing_raises_nodata(self):
        c, _ = _scedc_setup()
        with pytest.raises(NoDataError):
            c.get_raw("CI", "NOPE", "2022-01-02", channel="BHZ", location="00")

    def test_missing_ok_returns_empty(self):
        c, _ = _scedc_setup()
        raw = c.get_raw(
            "CI", "NOPE", "2022-01-02", channel="BHZ", location="00", missing_ok=True
        )
        assert raw == b""

    def test_real_error_raises_fetch_error(self, monkeypatch):
        c, _ = _scedc_setup()

        def denied(bucket, key, region):
            raise ClientError(
                {
                    "Error": {"Code": "AccessDenied", "Message": "no"},
                    "ResponseMetadata": {"HTTPStatusCode": 403},
                },
                "GetObject",
            )

        monkeypatch.setattr(c, "_fetch_object", denied)
        with pytest.raises(FetchError) as exc:
            c.get_raw("CI", "PASC", "2022-01-02", channel="BHZ", location="00")
        assert exc.value.failures[0][1] == "AccessDenied"

    def test_on_error_warn_tolerates_partial(self, monkeypatch):
        c, bodies = _scedc_setup()
        real = c._fetch_object

        def flaky(bucket, key, region):
            if key.endswith("BHZ10_2022002.ms"):
                raise ClientError(
                    {
                        "Error": {"Code": "SlowDown", "Message": "throttled"},
                        "ResponseMetadata": {"HTTPStatusCode": 503},
                    },
                    "GetObject",
                )
            return real(bucket, key, region)

        monkeypatch.setattr(c, "_fetch_object", flaky)
        raw = c.get_raw("CI", "PASC", "2022-01-02", channel="BHZ", on_error="warn")
        assert raw == bodies[(2, "BHZ", "00")]  # partial, but explicit opt-in


@mock_aws
class TestHalfOpenWindows:
    def test_default_one_day_fetches_one_day(self):
        c, bodies = _scedc_setup()
        raw = c.get_raw("CI", "PASC", "2022-01-02", channel="BHZ", location="00")
        assert raw == bodies[(2, "BHZ", "00")]  # doy 3 NOT included

    def test_midnight_end_excludes_next_day(self):
        c, bodies = _scedc_setup()
        raw = c.get_raw(
            "CI",
            "PASC",
            "2022-01-02",
            "2022-01-03",
            channel="BHZ",
            location="00",
        )
        assert raw == bodies[(2, "BHZ", "00")]

    def test_midday_end_includes_that_day(self):
        c, bodies = _scedc_setup()
        raw = c.get_raw(
            "CI",
            "PASC",
            "2022-01-02",
            "2022-01-03T12:00:00",
            channel="BHZ",
            location="00",
        )
        assert raw == bodies[(2, "BHZ", "00")] + bodies[(3, "BHZ", "00")]


class TestFDSNSemantics:
    URL = "https://service.earthscope.org/fdsnws/dataselect/1/query"

    def test_wildcard_location_passes_through(self):
        from seisfetch.fdsn import FDSNClient

        with respx.mock:
            route = respx.get(self.URL).respond(200, content=make_mseed())
            FDSNClient().get_raw(
                "IU", "ANMO", starttime="2024-01-15", endtime="2024-01-15T01:00:00"
            )
            assert route.calls[0].request.url.params["loc"] == "*"

    def test_blank_location_maps_to_dashes(self):
        from seisfetch.fdsn import FDSNClient

        with respx.mock:
            route = respx.get(self.URL).respond(200, content=make_mseed())
            FDSNClient().get_raw(
                "IU",
                "ANMO",
                location="",
                starttime="2024-01-15",
                endtime="2024-01-15T01:00:00",
            )
            assert route.calls[0].request.url.params["loc"] == "--"

    def test_404_is_no_data_not_error(self):
        from seisfetch.fdsn import FDSNClient

        with respx.mock:
            respx.get(self.URL).respond(404)
            raw = FDSNClient().get_raw(
                "IU", "ANMO", starttime="2024-01-15", endtime="2024-01-15T01:00:00"
            )
            assert raw == b""

    def test_server_error_raises_typed(self):
        from seisfetch.fdsn import FDSNClient

        with respx.mock:
            respx.get(self.URL).respond(503, text="maintenance")
            with pytest.raises(FDSNError) as exc:
                FDSNClient().get_raw(
                    "IU",
                    "ANMO",
                    starttime="2024-01-15",
                    endtime="2024-01-15T01:00:00",
                )
            assert exc.value.status == 503
