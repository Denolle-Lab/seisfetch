"""Tests for seisfetch.client."""

import pytest

from seisfetch.client import SeisfetchClient
from seisfetch.fdsn import FDSNClient, FDSNMultiClient


class TestConstruction:
    def test_invalid_backend(self):
        with pytest.raises(ValueError):
            SeisfetchClient(backend="bogus")

    def test_s3_open(self):
        c = SeisfetchClient(backend="s3_open")
        assert c.backend_name == "s3_open"

    def test_fdsn_default(self):
        c = SeisfetchClient(backend="fdsn")
        assert isinstance(c._client, FDSNClient)

    def test_fdsn_single_provider(self):
        c = SeisfetchClient(backend="fdsn", providers="GEOFON")
        assert isinstance(c._client, FDSNClient)
        assert "geofon" in c._client.base_url

    def test_fdsn_multi_provider(self):
        c = SeisfetchClient(backend="fdsn", providers=["EARTHSCOPE", "GEOFON"])
        assert isinstance(c._client, FDSNMultiClient)

    def test_repr(self):
        r = repr(SeisfetchClient(backend="fdsn", providers="GEOFON"))
        assert "fdsn" in r and "GEOFON" in r

    def test_s3_auth_requires_sdk(self):
        try:
            import earthscope_sdk  # noqa: F401

            pytest.skip("earthscope-sdk installed")
        except ImportError:
            with pytest.raises(ImportError, match="earthscope-sdk"):
                SeisfetchClient(backend="s3_auth")

    def test_invalid_provider(self):
        with pytest.raises(ValueError):
            SeisfetchClient(backend="fdsn", providers="NOPE")
