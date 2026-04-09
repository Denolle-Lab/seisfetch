"""Tests for seisfetch.bulk."""

import csv
from unittest.mock import MagicMock

import numpy as np
import pytest

from seisfetch.bulk import (
    BulkRequest,
    BulkResult,
    BulkSummary,
    fetch_bulk_numpy,
    fetch_bulk_raw,
    requests_from_csv,
    requests_from_list,
)

# ── BulkRequest ───────────────────────────────────────────────────── #


class TestBulkRequest:
    def test_tag(self):
        r = BulkRequest("IU", "ANMO", "00", "BHZ", "2024-01-15", "2024-01-16")
        assert r.tag == "IU.ANMO.00.BHZ"

    def test_to_dict(self):
        r = BulkRequest("CI", "SDD", "", "BHZ", "2024-06-01", "2024-06-02")
        d = r.to_dict()
        assert d["network"] == "CI"
        assert d["starttime"] == "2024-06-01"

    def test_frozen(self):
        r = BulkRequest("IU", "ANMO")
        with pytest.raises(AttributeError):
            r.network = "XX"


# ── BulkResult / BulkSummary ──────────────────────────────────────── #


class TestBulkResult:
    def test_success(self):
        r = BulkResult(
            request=BulkRequest("IU", "ANMO"), raw=b"\x00" * 1000, elapsed_s=0.5
        )
        assert r.success
        assert r.nbytes == 1000
        assert r.throughput_mbps == pytest.approx(0.016, rel=0.1)

    def test_failure(self):
        r = BulkResult(
            request=BulkRequest("XX", "NOPE"), error="not found", elapsed_s=0.1
        )
        assert not r.success

    def test_empty_data(self):
        r = BulkResult(request=BulkRequest("XX", "NOPE"), raw=b"", elapsed_s=0.1)
        assert not r.success


class TestBulkSummary:
    def test_counts(self):
        s = BulkSummary(
            results=[
                BulkResult(BulkRequest("IU", "A"), raw=b"\x00" * 100, elapsed_s=0.1),
                BulkResult(BulkRequest("XX", "B"), error="fail", elapsed_s=0.1),
                BulkResult(BulkRequest("CI", "C"), raw=b"\x00" * 200, elapsed_s=0.2),
            ]
        )
        assert s.total == 3
        assert s.succeeded == 2
        assert s.failed == 1
        assert s.total_bytes == 300
        assert len(s.successful_results) == 2
        assert len(s.failed_results) == 1

    def test_repr(self):
        s = BulkSummary(
            results=[
                BulkResult(BulkRequest("IU", "A"), raw=b"\x00" * 1000, elapsed_s=0.1),
            ]
        )
        assert "1/1" in repr(s)


# ── Request builders ──────────────────────────────────────────────── #


class TestRequestsFromList:
    def test_from_bulk_requests(self):
        reqs = requests_from_list(
            [
                BulkRequest("IU", "ANMO", "00", "BHZ", "2024-01-15", "2024-01-16"),
            ]
        )
        assert len(reqs) == 1
        assert reqs[0].network == "IU"

    def test_from_dicts(self):
        reqs = requests_from_list(
            [
                {
                    "network": "CI",
                    "station": "SDD",
                    "channel": "BHZ",
                    "starttime": "2024-06-01",
                    "endtime": "2024-06-02",
                },
            ]
        )
        assert reqs[0].station == "SDD"

    def test_from_tuples(self):
        reqs = requests_from_list(
            [
                ("IU", "ANMO", "00", "BHZ", "2024-01-15", "2024-01-16"),
                ("CI", "SDD", "", "BHZ", "2024-06-01", "2024-06-02"),
            ]
        )
        assert len(reqs) == 2
        assert reqs[1].network == "CI"

    def test_mixed(self):
        reqs = requests_from_list(
            [
                BulkRequest("IU", "ANMO"),
                {"network": "CI", "station": "SDD"},
                ("BK", "BRK", "00", "BHZ", "2024-01-01", "2024-01-02"),
            ]
        )
        assert len(reqs) == 3

    def test_bad_type_raises(self):
        with pytest.raises(TypeError):
            requests_from_list([42])


class TestRequestsFromCSV:
    def test_basic(self, tmp_path):
        f = tmp_path / "req.csv"
        with open(f, "w", newline="") as fh:
            w = csv.writer(fh)
            w.writerow(["# header comment"])
            w.writerow(["IU", "ANMO", "00", "BHZ", "2024-01-15", "2024-01-16"])
            w.writerow(["CI", "SDD", "", "BHZ", "2024-06-01", "2024-06-02"])
        reqs = requests_from_csv(f)
        assert len(reqs) == 2
        assert reqs[0].network == "IU"
        assert reqs[1].location == "*"  # empty → "*"

    def test_skips_short_rows(self, tmp_path):
        f = tmp_path / "bad.csv"
        with open(f, "w") as fh:
            fh.write("IU,ANMO\n")  # too short
        reqs = requests_from_csv(f)
        assert len(reqs) == 0


# ── fetch_bulk_raw ────────────────────────────────────────────────── #


class TestFetchBulkRaw:
    def test_parallel_fetch(self):
        mock_client = MagicMock()
        mock_client.get_raw.return_value = b"\x00" * 500

        reqs = [
            BulkRequest("IU", "ANMO", "00", "BHZ", "2024-01-15", "2024-01-16"),
            BulkRequest("CI", "SDD", "", "BHZ", "2024-06-01", "2024-06-02"),
            BulkRequest("BK", "BRK", "00", "BHZ", "2024-06-01", "2024-06-02"),
        ]
        summary = fetch_bulk_raw(reqs, mock_client, max_workers=2, progress=None)
        assert summary.total == 3
        assert summary.succeeded == 3
        assert summary.total_bytes == 1500

    def test_handles_failures(self):
        mock_client = MagicMock()
        mock_client.get_raw.side_effect = [
            b"\x00" * 100,
            Exception("boom"),
            b"\x00" * 200,
        ]

        reqs = [BulkRequest("A", "1"), BulkRequest("B", "2"), BulkRequest("C", "3")]
        summary = fetch_bulk_raw(reqs, mock_client, max_workers=1, progress=None)
        assert summary.succeeded == 2
        assert summary.failed == 1

    def test_handles_empty_data(self):
        mock_client = MagicMock()
        mock_client.get_raw.return_value = b""

        summary = fetch_bulk_raw(
            [BulkRequest("X", "Y", starttime="2024-01-01", endtime="2024-01-02")],
            mock_client,
            progress=None,
        )
        assert summary.succeeded == 0
        assert summary.failed == 1

    def test_progress_callback(self):
        mock_client = MagicMock()
        mock_client.get_raw.return_value = b"\x00" * 100

        calls = []

        def _progress(i, total, result):
            calls.append((i, total, result.success))

        reqs = [BulkRequest("IU", "A"), BulkRequest("CI", "B")]
        fetch_bulk_raw(reqs, mock_client, max_workers=1, progress=_progress)
        assert len(calls) == 2
        assert all(c[2] for c in calls)  # all success


class TestFetchBulkNumpy:
    def test_parse_results(self):
        from tests.helpers import make_mseed

        mseed = make_mseed(npts=100)

        mock_client = MagicMock()
        mock_client.get_raw.return_value = mseed

        reqs = [BulkRequest("IU", "ANMO", "00", "BHZ", "2024-01-15", "2024-01-16")]
        summary = fetch_bulk_numpy(reqs, mock_client, max_workers=1, progress=None)
        assert summary.succeeded == 1
        result = summary.successful_results[0]
        assert result.bundle is not None
        assert len(result.bundle) >= 1
        assert isinstance(result.bundle.traces[0].data, np.ndarray)
