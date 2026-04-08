"""Tests for seisfetch.utils — zero ObsPy dependency."""

from datetime import date, datetime, timezone
import pytest

from seisfetch.utils import (
    date_range, date_to_year_doy, s3_key, to_datetime, to_epoch, to_isoformat,
)


class TestS3Key:
    def test_basic(self):
        assert s3_key("IU", "ANMO", 2024, 15) == "miniseed/IU/2024/015/ANMO.IU.2024.015"

    def test_suffix(self):
        assert s3_key("TA", "A04A", 2004, 365, suffix="#2").endswith("#2")

    def test_doy_padded(self):
        assert "/001/" in s3_key("IU", "ANMO", 2024, 1)


class TestTimeConversion:
    def test_from_float(self):
        dt = to_datetime(0.0)
        assert dt.year == 1970

    def test_from_string(self):
        dt = to_datetime("2024-01-15T00:00:00")
        assert dt.year == 2024 and dt.month == 1 and dt.day == 15

    def test_from_datetime(self):
        dt = datetime(2024, 6, 1, tzinfo=timezone.utc)
        assert to_datetime(dt) is dt

    def test_from_obspy(self):
        try:
            from obspy import UTCDateTime
        except ImportError:
            pytest.skip("obspy not installed")
        dt = to_datetime(UTCDateTime("2024-01-15"))
        assert dt.year == 2024

    def test_to_epoch(self):
        assert to_epoch("2024-01-15T00:00:00") == pytest.approx(1705276800.0)

    def test_to_isoformat(self):
        s = to_isoformat(1705276800.0)
        assert s.startswith("2024-01-15")

    def test_bad_type_raises(self):
        with pytest.raises(TypeError):
            to_datetime([1, 2, 3])

    def test_bad_string_raises(self):
        with pytest.raises(ValueError):
            to_datetime("not-a-date")


class TestDateRange:
    def test_single_day(self):
        days = list(date_range("2024-01-15", "2024-01-15"))
        assert len(days) == 1

    def test_multi_day(self):
        days = list(date_range("2024-01-15", "2024-01-17"))
        assert len(days) == 3

    def test_cross_year(self):
        days = list(date_range("2023-12-31", "2024-01-01"))
        assert len(days) == 2

    def test_from_epoch(self):
        days = list(date_range(1705276800.0, 1705363200.0))
        assert len(days) == 2


class TestDateToYearDoy:
    def test_jan_1(self):
        assert date_to_year_doy(date(2024, 1, 1)) == (2024, 1)

    def test_dec_31_leap(self):
        assert date_to_year_doy(date(2024, 12, 31)) == (2024, 366)
