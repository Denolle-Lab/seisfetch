"""Tests for seisfetch.fdsn."""
import pytest
from seisfetch.fdsn import FDSNClient, FDSNMultiClient, list_providers, resolve_provider

try:
    import httpx, respx
    HAS_RESPX = True
except ImportError:
    HAS_RESPX = False

class TestProviderRegistry:
    def test_resolve_earthscope(self):  assert "earthscope" in resolve_provider("EARTHSCOPE")
    def test_resolve_geofon(self):      assert "geofon" in resolve_provider("GEOFON")
    def test_case_insensitive(self):    assert resolve_provider("geofon") == resolve_provider("GEOFON")
    def test_raw_url(self):             assert resolve_provider("https://x.org") == "https://x.org"
    def test_unknown_raises(self):
        with pytest.raises(ValueError): resolve_provider("BOGUS")
    def test_has_majors(self):
        p = list_providers()
        for n in ("EARTHSCOPE", "GEOFON", "INGV", "ETH", "ORFEUS", "USGS"): assert n in p

class TestFDSNClientUnit:
    def test_default(self):       assert "earthscope" in FDSNClient().base_url
    def test_geofon(self):        assert "geofon" in FDSNClient("GEOFON").base_url
    def test_auth(self):          assert "queryauth" in FDSNClient(user="u", password="p")._dataselect_url
    def test_repr(self):          assert "GEOFON" in repr(FDSNClient("GEOFON"))
    def test_starttime_req(self):
        with pytest.raises(ValueError): FDSNClient().get_raw("IU", "ANMO")

@pytest.mark.skipif(not HAS_RESPX, reason="respx not installed")
class TestFDSNClientFetch:
    def test_get_raw(self):
        from tests.helpers import make_mseed
        with respx.mock:
            respx.get("https://service.earthscope.org/fdsnws/dataselect/1/query").respond(200, content=make_mseed())
            assert len(FDSNClient().get_raw("IU", "ANMO", "00", "BHZ", starttime="2024-01-15", endtime="2024-01-15T01:00:00")) > 0

    def test_no_data(self):
        with respx.mock:
            respx.get("https://service.earthscope.org/fdsnws/dataselect/1/query").respond(204)
            assert FDSNClient().get_raw("XX", "NOPE", starttime="2024-01-15", endtime="2024-01-15T01:00:00") == b""

    def test_geofon(self):
        from tests.helpers import make_mseed
        with respx.mock:
            respx.get("https://geofon.gfz.de/fdsnws/dataselect/1/query").respond(200, content=make_mseed())
            assert len(FDSNClient("GEOFON").get_raw("GE", "DAV", "*", "BHZ", starttime="2024-01-15", endtime="2024-01-15T01:00:00")) > 0

class TestFDSNMultiClientUnit:
    def test_default(self):  assert "EARTHSCOPE" in FDSNMultiClient().providers
    def test_custom(self):   assert FDSNMultiClient(["ETH", "GFZ"]).providers == ["ETH", "GFZ"]
    def test_invalid(self):
        with pytest.raises(ValueError): FDSNMultiClient(["BOGUS"])

@pytest.mark.skipif(not HAS_RESPX, reason="respx not installed")
class TestFDSNMultiClientFetch:
    def test_multi_merge(self):
        from tests.helpers import make_mseed
        mseed = make_mseed()
        with respx.mock:
            respx.get("https://service.earthscope.org/fdsnws/dataselect/1/query").respond(200, content=mseed)
            respx.get("https://geofon.gfz.de/fdsnws/dataselect/1/query").respond(200, content=mseed)
            assert len(FDSNMultiClient(["EARTHSCOPE", "GEOFON"]).get_raw("IU", "ANMO", "00", "BHZ", starttime="2024-01-15", endtime="2024-01-15T01:00:00")) > 0

    def test_one_fails(self):
        from tests.helpers import make_mseed
        with respx.mock:
            respx.get("https://service.earthscope.org/fdsnws/dataselect/1/query").respond(200, content=make_mseed())
            respx.get("https://geofon.gfz.de/fdsnws/dataselect/1/query").respond(404)
            assert len(FDSNMultiClient(["EARTHSCOPE", "GEOFON"]).get_raw("IU", "ANMO", "00", "BHZ", starttime="2024-01-15", endtime="2024-01-15T01:00:00")) > 0
