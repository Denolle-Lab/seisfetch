"""Tests for seisfetch CLI (__main__.py)."""
import csv
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock

import numpy as np
import pytest

from seisfetch.__main__ import main


class TestCLIHelp:
    """Verify arg parsing doesn't crash."""

    def test_no_args_exits(self):
        with pytest.raises(SystemExit):
            main()

    def test_help(self):
        with pytest.raises(SystemExit) as exc_info:
            with patch("sys.argv", ["seisfetch", "--help"]):
                main()
        assert exc_info.value.code == 0

    def test_download_help(self):
        with pytest.raises(SystemExit) as exc_info:
            with patch("sys.argv", ["seisfetch", "download", "--help"]):
                main()
        assert exc_info.value.code == 0


class TestCLIInfo:

    def test_providers(self, capsys):
        with patch("sys.argv", ["ef", "info", "--providers"]):
            main()
        out = capsys.readouterr().out
        assert "EARTHSCOPE" in out
        assert "GEOFON" in out

    def test_route(self, capsys):
        with patch("sys.argv", ["ef", "info", "--route", "CI"]):
            main()
        out = capsys.readouterr().out
        assert "scedc" in out

    def test_route_bk(self, capsys):
        with patch("sys.argv", ["ef", "info", "--route", "BK"]):
            main()
        out = capsys.readouterr().out
        assert "ncedc" in out

    def test_route_iu(self, capsys):
        with patch("sys.argv", ["ef", "info", "--route", "IU"]):
            main()
        out = capsys.readouterr().out
        assert "earthscope" in out


class TestCLIDownload:

    @patch("seisfetch.client.SeisfetchClient.get_raw")
    def test_download_writes_file(self, mock_get_raw, tmp_path):
        mock_get_raw.return_value = b"\x00" * 1000
        outfile = str(tmp_path / "test.mseed")
        with patch("sys.argv", [
            "ef", "download", "IU", "ANMO",
            "-s", "2024-01-15", "-e", "2024-01-15T01:00:00",
            "-o", outfile,
        ]):
            main()
        assert Path(outfile).exists()
        assert Path(outfile).stat().st_size == 1000

    @patch("seisfetch.client.SeisfetchClient.get_raw")
    def test_download_no_data_exits(self, mock_get_raw):
        mock_get_raw.return_value = b""
        with patch("sys.argv", [
            "ef", "download", "XX", "NOPE", "-s", "2024-01-15",
        ]):
            with pytest.raises(SystemExit) as exc_info:
                main()
            assert exc_info.value.code == 1


class TestCLINumpySave:

    @patch("seisfetch.client.SeisfetchClient.get_numpy")
    def test_numpy_writes_npz(self, mock_get_numpy, tmp_path):
        from seisfetch.convert import TraceBundle, TraceArray
        mock_get_numpy.return_value = TraceBundle([
            TraceArray("IU", "ANMO", "00", "BHZ", 0, 100.0,
                       np.random.randn(1000).astype(np.float32)),
        ])
        outfile = str(tmp_path / "test.npz")
        with patch("sys.argv", [
            "ef", "numpy", "IU", "ANMO",
            "-s", "2024-01-15", "-e", "2024-01-15T01:00:00",
            "-o", outfile,
        ]):
            main()
        assert Path(outfile).exists()
        loaded = np.load(outfile)
        assert "IU.ANMO.00.BHZ" in loaded
        assert "_metadata" in loaded


class TestCLIBulk:

    @patch("seisfetch.client.SeisfetchClient.get_raw")
    def test_bulk_mseed(self, mock_get_raw, tmp_path):
        mock_get_raw.return_value = b"\x00" * 500

        # Write request CSV
        req_file = tmp_path / "requests.csv"
        with open(req_file, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["# network,station,location,channel,starttime,endtime"])
            writer.writerow(["IU", "ANMO", "00", "BHZ", "2024-01-15", "2024-01-15T01:00:00"])
            writer.writerow(["CI", "SDD", "", "BHZ", "2024-06-01", "2024-06-01T01:00:00"])

        outdir = str(tmp_path / "out")
        with patch("sys.argv", [
            "ef", "bulk", str(req_file), "-o", outdir, "-f", "mseed",
        ]):
            main()

        outpath = Path(outdir)
        assert outpath.exists()
        files = list(outpath.glob("*.mseed"))
        assert len(files) == 2

    @patch("seisfetch.client.SeisfetchClient.get_raw")
    def test_bulk_npz(self, mock_get_raw, tmp_path):
        from tests.helpers import make_mseed
        mock_get_raw.return_value = make_mseed(npts=100)

        req_file = tmp_path / "req.csv"
        with open(req_file, "w", newline="") as f:
            csv.writer(f).writerow(["IU", "ANMO", "00", "BHZ",
                                    "2024-01-15", "2024-01-15T01:00:00"])

        outdir = str(tmp_path / "npz_out")
        with patch("sys.argv", [
            "ef", "bulk", str(req_file), "-o", outdir, "-f", "npz",
        ]):
            main()
        assert len(list(Path(outdir).glob("*.npz"))) == 1
