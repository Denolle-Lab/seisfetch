"""Operations-pass tests (2026-08 critique): tuned client config, shared
executor, pagination, BG routing, FDSN failover, bulk memory hygiene, and
post-parse channel filtering."""

import boto3
import pytest

from seisfetch.bulk import BulkRequest, fetch_bulk_numpy, iter_bulk_raw
from seisfetch.exceptions import FetchError
from seisfetch.fdsn import FDSNMultiClient
from seisfetch.s3 import S3OpenClient, route_network
from tests.helpers import make_mseed, make_multichan_mseed

moto = pytest.importorskip("moto")
respx = pytest.importorskip("respx")
mock_aws = moto.mock_aws

ES_URL = "https://service.earthscope.org/fdsnws/dataselect/1/query"
GE_URL = "https://geofon.gfz.de/fdsnws/dataselect/1/query"


class TestClientConfig:
    def test_tuned_boto_config(self):
        c = S3OpenClient(max_workers=24)
        cfg = c._config
        assert cfg.retries == {"mode": "adaptive", "max_attempts": 5}
        assert cfg.connect_timeout == 10.0
        assert cfg.read_timeout == 60.0
        # pool at least as large as the thread fan-out
        assert cfg.max_pool_connections == 24

    def test_shared_executor_reused_and_closable(self):
        c = S3OpenClient(max_workers=2)
        assert c._get_executor() is c._get_executor()
        with c:
            pass
        assert c._executor is None  # context manager shut it down

    def test_bg_routes_to_ncedc(self):
        # The Geysers is a Berkeley/NCEDC network (was mis-routed to SCEDC)
        assert route_network("BG") == "ncedc"


@mock_aws
class TestPaginatedListing:
    def test_list_stations_beyond_1000_keys(self):
        s3 = boto3.client("s3", region_name="us-west-2")
        s3.create_bucket(
            Bucket="scedc-pds",
            CreateBucketConfiguration={"LocationConstraint": "us-west-2"},
        )
        n = 1050  # beyond the single-page list_objects_v2 limit
        for i in range(n):
            sta = f"S{i:04d}"
            key = (
                f"continuous_waveforms/2022/2022_002/"
                f"CI{sta.ljust(5, '_')}BHZ00_2022002.ms"
            )
            s3.put_object(Bucket="scedc-pds", Key=key, Body=b"x")
        c = S3OpenClient(datacenter="scedc", _s3_client=s3)
        stations = c.list_stations("CI", 2022, 2, datacenter="scedc")
        assert len(stations) == n  # unpaginated listing truncated at 1000


class TestFDSNFailover:
    def test_failover_stops_at_first_success(self):
        with respx.mock:
            first = respx.get(ES_URL).respond(200, content=make_mseed())
            second = respx.get(GE_URL).respond(200, content=make_mseed())
            raw = FDSNMultiClient(["EARTHSCOPE", "GEOFON"]).get_raw(
                "IU",
                "ANMO",
                "00",
                "BHZ",
                starttime="2024-01-15",
                endtime="2024-01-15T01:00:00",
            )
            assert len(raw) > 0
            assert first.called
            assert not second.called  # no broadcast to the second provider

    def test_failover_advances_past_empty_provider(self):
        with respx.mock:
            respx.get(ES_URL).respond(404)  # no data at provider 1
            respx.get(GE_URL).respond(200, content=make_mseed())
            raw = FDSNMultiClient(["EARTHSCOPE", "GEOFON"]).get_raw(
                "IU",
                "ANMO",
                "00",
                "BHZ",
                starttime="2024-01-15",
                endtime="2024-01-15T01:00:00",
            )
            assert len(raw) > 0

    def test_all_providers_error_raises(self):
        with respx.mock:
            respx.get(ES_URL).respond(503)
            respx.get(GE_URL).respond(503)
            with pytest.raises(FetchError):
                FDSNMultiClient(["EARTHSCOPE", "GEOFON"]).get_raw(
                    "IU",
                    "ANMO",
                    "00",
                    "BHZ",
                    starttime="2024-01-15",
                    endtime="2024-01-15T01:00:00",
                )

    def test_broadcast_opt_in_queries_all(self):
        with respx.mock:
            first = respx.get(ES_URL).respond(200, content=make_mseed())
            second = respx.get(GE_URL).respond(200, content=make_mseed())
            FDSNMultiClient(["EARTHSCOPE", "GEOFON"], strategy="broadcast").get_raw(
                "IU",
                "ANMO",
                "00",
                "BHZ",
                starttime="2024-01-15",
                endtime="2024-01-15T01:00:00",
            )
            assert first.called and second.called


class TestBulkMemoryHygiene:
    class _FakeClient:
        def get_raw(self, **kwargs):
            return make_mseed()

    def test_numpy_bulk_drops_raw_by_default(self):
        reqs = [BulkRequest("IU", "ANMO", "00", "BHZ", "2024-01-15", "")]
        summary = fetch_bulk_numpy(reqs, self._FakeClient(), max_workers=1)
        r = summary.results[0]
        assert r.success
        assert r.raw == b""  # bytes dropped after parse
        assert r.nbytes > 0  # but accounted
        assert r.bundle is not None and len(r.bundle) >= 1

    def test_keep_raw_opt_in(self):
        reqs = [BulkRequest("IU", "ANMO", "00", "BHZ", "2024-01-15", "")]
        summary = fetch_bulk_numpy(
            reqs, self._FakeClient(), max_workers=1, keep_raw=True
        )
        assert len(summary.results[0].raw) > 0

    def test_iter_bulk_raw_streams(self):
        reqs = [
            BulkRequest("IU", "ANMO", "00", "BHZ", "2024-01-15", ""),
            BulkRequest("IU", "COLA", "00", "BHZ", "2024-01-15", ""),
        ]
        seen = list(iter_bulk_raw(reqs, self._FakeClient(), max_workers=2))
        assert len(seen) == 2 and all(r.success for r in seen)


class TestPostParseChannelFilter:
    @mock_aws
    def test_earthscope_station_day_filtered_to_requested_channel(self):
        from seisfetch.client import SeisfetchClient
        from seisfetch.utils import OPEN_BUCKET, s3_key

        s3 = boto3.client("s3", region_name="us-east-2")
        s3.create_bucket(
            Bucket=OPEN_BUCKET,
            CreateBucketConfiguration={"LocationConstraint": "us-east-2"},
        )
        # station-day object with multiple channels
        s3.put_object(
            Bucket=OPEN_BUCKET,
            Key=s3_key("IU", "ANMO", 2024, 15),
            Body=make_multichan_mseed(),
        )
        c = SeisfetchClient(backend="s3_open", datacenter="earthscope")
        c._client = S3OpenClient(datacenter="earthscope", max_workers=1, _s3_client=s3)
        b_all = c.get_numpy("IU", "ANMO", "2024-01-15", trim=False)
        channels_all = {t.channel for t in b_all.traces}
        assert len(channels_all) > 1
        one = sorted(channels_all)[0]
        b_one = c.get_numpy("IU", "ANMO", "2024-01-15", channel=one, trim=False)
        assert {t.channel for t in b_one.traces} == {one}
