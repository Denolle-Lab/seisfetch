"""Multi-day byte-order determinism and client-level window trim (moto)."""

import boto3
import numpy as np
import pytest
from moto import mock_aws

from seisfetch.s3 import S3OpenClient, _earthscope_key
from seisfetch.utils import OPEN_BUCKET
from tests.helpers import make_mseed


def _day_bytes(day_marker: int) -> bytes:
    # deterministic distinct content per day via npts
    np.random.seed(day_marker)
    return make_mseed(network="IU", station="ANMO", npts=500 + day_marker)


@mock_aws
class TestMultiDayDeterminism:
    def _client(self):
        s3 = boto3.client("s3", region_name="us-east-2")
        s3.create_bucket(
            Bucket=OPEN_BUCKET,
            CreateBucketConfiguration={"LocationConstraint": "us-east-2"},
        )
        self._expected = b""
        for i, doy in enumerate((15, 16, 17)):
            body = _day_bytes(i)
            key = _earthscope_key("IU", "ANMO", 2024, doy)
            s3.put_object(Bucket=OPEN_BUCKET, Key=key, Body=body)
            self._expected += body
        return S3OpenClient(datacenter="earthscope", max_workers=4, _s3_client=s3)

    def test_bytes_in_day_major_submission_order(self):
        c = self._client()
        raw = c.get_raw("IU", "ANMO", "2024-01-15", "2024-01-18")
        assert raw == self._expected

    def test_repeat_runs_identical(self):
        c = self._client()
        runs = {c.get_raw("IU", "ANMO", "2024-01-15", "2024-01-18") for _ in range(5)}
        assert len(runs) == 1


@mock_aws
class TestClientWindowTrim:
    def _fetcher(self):
        s3 = boto3.client("s3", region_name="us-east-2")
        s3.create_bucket(
            Bucket=OPEN_BUCKET,
            CreateBucketConfiguration={"LocationConstraint": "us-east-2"},
        )
        # helper writes 100 Hz data starting 2024-01-15T00:00:00Z
        key = _earthscope_key("IU", "ANMO", 2024, 15)
        s3.put_object(Bucket=OPEN_BUCKET, Key=key, Body=make_mseed(npts=6000))
        from seisfetch.client import SeisfetchClient

        c = SeisfetchClient(backend="s3_open", datacenter="earthscope")
        c._client = S3OpenClient(datacenter="earthscope", max_workers=1, _s3_client=s3)
        return c

    def test_trim_default_cuts_to_window(self):
        c = self._fetcher()
        b = c.get_numpy("IU", "ANMO", "2024-01-15T00:00:10", "2024-01-15T00:00:30")
        t = b.traces[0]
        assert t.npts == pytest.approx(20 * 100.0, abs=1)
        assert t.starttime_ns == pytest.approx(1705276810 * 1e9, abs=1e7)

    def test_trim_false_returns_whole_object(self):
        c = self._fetcher()
        b = c.get_numpy(
            "IU", "ANMO", "2024-01-15T00:00:10", "2024-01-15T00:00:30", trim=False
        )
        assert b.traces[0].npts == 6000
