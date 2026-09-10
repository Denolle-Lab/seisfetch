"""EarthScope's two S3 tiers: anonymous Open Data networks on the open
bucket, everything else behind the credentialed v2 access point.

Offline. The restricted tier is exercised with a fake ``earthscope_sdk``
module (so the tests run without the SDK installed) and moto for S3.
"""

from __future__ import annotations

import io
import sys
import types
from datetime import datetime, timedelta, timezone

import boto3
import pytest
from botocore.exceptions import ClientError
from moto import mock_aws

from seisfetch.exceptions import CredentialError, NoDataError
from seisfetch.s3 import (
    S3AuthClient,
    S3OpenClient,
    _earthscope_key,
    earthscope_scope,
    earthscope_tier,
    route_network,
)
from seisfetch.utils import (
    AUTH_ACCESS_POINT,
    EARTHSCOPE_OPEN_NETWORKS,
    OPEN_BUCKET,
    is_earthscope_open,
    is_temporary_network,
)
from tests.helpers import make_mseed

# ── Tier classification ────────────────────────────────────────────── #


class TestTiers:
    def test_open_networks_match_live_bucket_listing(self):
        # listing of s3://earthscope-geophysical-data/miniseed/ on 2026-09-09
        assert EARTHSCOPE_OPEN_NETWORKS == {
            "AK",
            "II",
            "IU",
            "N4",
            "PB",
            "TA",
            "UU",
            "UW",
        }

    @pytest.mark.parametrize("net", sorted(EARTHSCOPE_OPEN_NETWORKS))
    def test_open(self, net):
        assert is_earthscope_open(net)
        assert earthscope_tier(net) == "open"

    @pytest.mark.parametrize("net", ["US", "CC", "LH", "ZI", "5A", "CI"])
    def test_restricted(self, net):
        assert not is_earthscope_open(net)
        assert earthscope_tier(net) == "restricted"

    def test_case_insensitive(self):
        assert is_earthscope_open("iu")
        assert earthscope_tier("uw") == "open"

    def test_open_networks_still_route_to_earthscope(self):
        for net in EARTHSCOPE_OPEN_NETWORKS - {"PB"}:
            assert route_network(net) == "earthscope"
        # PB is shared with NCEDC and routes there; datacenter="earthscope"
        # reads it from Open Data
        assert route_network("PB") == "ncedc"

    def test_pg_routes_to_ncedc(self):
        assert route_network("PG") == "ncedc"


class TestTemporaryNetworks:
    @pytest.mark.parametrize("net", ["1A", "5A", "9R", "XA", "YW", "ZI", "zz"])
    def test_temporary(self, net):
        assert is_temporary_network(net)

    @pytest.mark.parametrize("net", ["IU", "CC", "US", "AK", "N4", ""])
    def test_permanent(self, net):
        assert not is_temporary_network(net)

    def test_scope_permanent_network_only(self):
        assert earthscope_scope("US") == {"network": "FDSN:US"}
        assert earthscope_scope("us", 2019) == {"network": "FDSN:US"}

    def test_scope_temporary_needs_year(self):
        assert earthscope_scope("ZI", 2019) == {"network": "FDSN:ZI", "year": 2019}
        with pytest.raises(ValueError, match="temporary"):
            earthscope_scope("ZI")


# ── Open bucket: tier named in the no-data error ───────────────────── #


@mock_aws
class TestOpenBucketHint:
    def _client(self):
        s3 = boto3.client("s3", region_name="us-east-2")
        s3.create_bucket(
            Bucket=OPEN_BUCKET,
            CreateBucketConfiguration={"LocationConstraint": "us-east-2"},
        )
        return S3OpenClient(datacenter="earthscope", max_workers=1, _s3_client=s3)

    def test_restricted_network_on_open_bucket_points_at_s3_auth(self):
        with pytest.raises(NoDataError) as exc:
            self._client().get_raw("US", "NEW", "2024-01-15")
        msg = str(exc.value)
        assert "s3_auth" in msg and "Open Data" in msg
        assert exc.value.hint is not None

    def test_open_network_missing_station_has_no_hint(self):
        with pytest.raises(NoDataError) as exc:
            self._client().get_raw("IU", "NOPE", "2024-01-15")
        assert exc.value.hint is None
        assert "s3_auth" not in str(exc.value)


# ── Restricted tier: S3AuthClient against a fake SDK ───────────────── #


class _Secret:
    """Stand-in for pydantic SecretStr (earthscope-sdk >= 1.4.1)."""

    def __init__(self, v):
        self._v = v

    def get_secret_value(self):
        return self._v

    def __str__(self):  # what boto3 would sign with if not unwrapped
        return "**********"


class FakeCreds:
    def __init__(self, key_id):
        self.aws_access_key_id = key_id
        self.aws_secret_access_key = _Secret("sekret")
        self.aws_session_token = _Secret("tok")
        self.expiration = datetime.now(tz=timezone.utc) + timedelta(hours=1)


class FakeES:
    """``EarthScopeClient`` double: ``.user.get_aws_credentials`` recorder."""

    def __init__(self, fail=None):
        self.calls = []
        self.fail = fail
        self.user = self
        self._n = 0

    def get_aws_credentials(self, *, role, force=False, ttl_threshold=None, **params):
        self.calls.append({"role": role, "force": force, **params})
        if self.fail is not None:
            raise self.fail
        if force or self._n == 0:
            self._n += 1
        return FakeCreds(f"AKIA{self._n}")


@pytest.fixture
def fake_sdk(monkeypatch):
    """Install a minimal ``earthscope_sdk`` so S3AuthClient can be built
    without the real SDK (and without its OAuth bootstrap)."""
    sdk = types.ModuleType("earthscope_sdk")
    auth = types.ModuleType("earthscope_sdk.auth")
    err = types.ModuleType("earthscope_sdk.auth.error")

    class AuthFlowError(Exception):
        pass

    class UnauthenticatedError(AuthFlowError):
        pass

    class UnauthorizedError(AuthFlowError):
        pass

    err.AuthFlowError = AuthFlowError
    err.UnauthenticatedError = UnauthenticatedError
    err.UnauthorizedError = UnauthorizedError

    class EarthScopeClient:
        def __init__(self, *a, **k):
            raise AssertionError("tests inject _es_client; no OAuth here")

    sdk.EarthScopeClient = EarthScopeClient
    sdk.auth = auth
    auth.error = err
    monkeypatch.setitem(sys.modules, "earthscope_sdk", sdk)
    monkeypatch.setitem(sys.modules, "earthscope_sdk.auth", auth)
    monkeypatch.setitem(sys.modules, "earthscope_sdk.auth.error", err)
    # the version guard reads the INSTALLED distribution's metadata, which a
    # sys.modules double cannot change: on a machine carrying an old real
    # SDK it would refuse the fake. The guard has its own test below.
    monkeypatch.setattr("seisfetch.s3._sdk_version_ok", lambda: True)
    return err


def _restricted_bucket():
    s3 = boto3.client("s3", region_name="us-east-2")
    s3.create_bucket(
        Bucket=AUTH_ACCESS_POINT,
        CreateBucketConfiguration={"LocationConstraint": "us-east-2"},
    )
    return s3


@mock_aws
class TestS3AuthClientRestricted:
    def test_scoped_exchange_and_versioned_key(self, fake_sdk):
        s3 = _restricted_bucket()
        v1 = make_mseed("US", "NEW", npts=100)
        v2 = make_mseed("US", "NEW", npts=200)
        base = _earthscope_key("US", "NEW", 2024, 15)
        s3.put_object(Bucket=AUTH_ACCESS_POINT, Key=base + "#1", Body=v1)
        s3.put_object(Bucket=AUTH_ACCESS_POINT, Key=base + "#2", Body=v2)
        es = FakeES()
        with S3AuthClient(max_workers=1, _es_client=es) as c:
            raw = c.get_raw("US", "NEW", "2024-01-15")
        assert raw == v2  # highest version wins
        assert es.calls == [
            {"role": "s3-miniseed-v2", "force": False, "network": "FDSN:US"}
        ]

    def test_unversioned_object_still_found(self, fake_sdk):
        s3 = _restricted_bucket()
        body = make_mseed("US", "NEW")
        s3.put_object(
            Bucket=AUTH_ACCESS_POINT,
            Key=_earthscope_key("US", "NEW", 2024, 15),
            Body=body,
        )
        c = S3AuthClient(max_workers=1, _es_client=FakeES())
        assert c.get_raw("US", "NEW", "2024-01-15") == body

    def test_temporary_network_scoped_per_year(self, fake_sdk):
        s3 = _restricted_bucket()
        b19 = make_mseed("ZI", "STA", npts=100)
        b20 = make_mseed("ZI", "STA", npts=200)
        s3.put_object(
            Bucket=AUTH_ACCESS_POINT,
            Key=_earthscope_key("ZI", "STA", 2019, 365) + "#1",
            Body=b19,
        )
        s3.put_object(
            Bucket=AUTH_ACCESS_POINT,
            Key=_earthscope_key("ZI", "STA", 2020, 1) + "#1",
            Body=b20,
        )
        es = FakeES()
        c = S3AuthClient(max_workers=2, _es_client=es)
        raw = c.get_raw("ZI", "STA", "2019-12-31", "2020-01-02")
        assert raw == b19 + b20  # day order, deterministic
        scopes = {(call["network"], call.get("year")) for call in es.calls}
        assert scopes == {("FDSN:ZI", 2019), ("FDSN:ZI", 2020)}

    def test_one_exchange_per_scope_across_days(self, fake_sdk):
        s3 = _restricted_bucket()
        for doy in (15, 16, 17):
            s3.put_object(
                Bucket=AUTH_ACCESS_POINT,
                Key=_earthscope_key("US", "NEW", 2024, doy) + "#1",
                Body=make_mseed("US", "NEW"),
            )
        es = FakeES()
        c = S3AuthClient(max_workers=3, _es_client=es)
        c.get_raw("US", "NEW", "2024-01-15", "2024-01-18")
        # three days = three LISTs + three GETs behind ONE exchange
        assert len(es.calls) == 1
        assert len(c._s3_by_scope) == 1
        # a second call on the same scope is served from the cache too
        c.get_raw("US", "NEW", "2024-01-16")
        assert len(es.calls) == 1

    def test_credential_near_expiry_is_renewed(self, fake_sdk):
        s3 = _restricted_bucket()
        s3.put_object(
            Bucket=AUTH_ACCESS_POINT,
            Key=_earthscope_key("US", "NEW", 2024, 15) + "#1",
            Body=make_mseed("US", "NEW"),
        )
        es = FakeES()
        c = S3AuthClient(max_workers=1, _es_client=es)
        c.get_raw("US", "NEW", "2024-01-15")
        creds, client = next(iter(c._s3_by_scope.values()))
        creds.expiration = datetime.now(tz=timezone.utc) + timedelta(seconds=30)
        c.get_raw("US", "NEW", "2024-01-15")
        assert len(es.calls) == 2  # asked again, without force
        assert es.calls[1]["force"] is False

    def test_missing_day_is_missing_not_error(self, fake_sdk):
        s3 = _restricted_bucket()
        body = make_mseed("US", "NEW")
        s3.put_object(
            Bucket=AUTH_ACCESS_POINT,
            Key=_earthscope_key("US", "NEW", 2024, 16) + "#1",
            Body=body,
        )
        c = S3AuthClient(max_workers=2, _es_client=FakeES())
        assert c.get_raw("US", "NEW", "2024-01-15", "2024-01-17") == body
        with pytest.raises(NoDataError):
            c.get_raw("US", "NOPE", "2024-01-15")
        assert c.get_raw("US", "NOPE", "2024-01-15", missing_ok=True) == b""

    def test_open_network_bypasses_credentials(self, fake_sdk):
        s3 = boto3.client("s3", region_name="us-east-2")
        s3.create_bucket(
            Bucket=OPEN_BUCKET,
            CreateBucketConfiguration={"LocationConstraint": "us-east-2"},
        )
        body = make_mseed("IU", "ANMO")
        s3.put_object(
            Bucket=OPEN_BUCKET, Key=_earthscope_key("IU", "ANMO", 2024, 15), Body=body
        )
        es = FakeES()
        c = S3AuthClient(max_workers=1, _es_client=es)
        # moto answers unsigned GETs with 403 (the live bucket does not), so
        # hand the open path the mock client the way every S3OpenClient
        # test does
        c._open = S3OpenClient(datacenter="earthscope", max_workers=1, _s3_client=s3)
        assert c.get_raw("IU", "ANMO", "2024-01-15") == body
        assert es.calls == []  # Open Data: no exchange at all

    def test_prefer_open_false_uses_access_point(self, fake_sdk):
        s3 = _restricted_bucket()
        body = make_mseed("IU", "ANMO")
        s3.put_object(
            Bucket=AUTH_ACCESS_POINT,
            Key=_earthscope_key("IU", "ANMO", 2024, 15) + "#3",
            Body=body,
        )
        es = FakeES()
        c = S3AuthClient(max_workers=1, _es_client=es, prefer_open=False)
        assert c.get_raw("IU", "ANMO", "2024-01-15") == body
        assert es.calls[0]["network"] == "FDSN:IU"

    def test_403_is_credential_error_asked_once(self, fake_sdk):
        _restricted_bucket()
        es = FakeES(fail=fake_sdk.UnauthorizedError("no access"))
        c = S3AuthClient(max_workers=3, _es_client=es)
        with pytest.raises(CredentialError) as exc:
            c.get_raw("LH", "HDSE", "2024-01-15", "2024-01-18")
        assert exc.value.status == 403
        assert exc.value.scope == {"network": "FDSN:LH"}
        assert "data-help@earthscope.org" in exc.value.message
        assert len(es.calls) == 1  # verdict remembered across the 3 days
        # and across calls
        with pytest.raises(CredentialError):
            c.get_raw("LH", "HDSE", "2024-06-01")
        assert len(es.calls) == 1

    def test_401_stops_every_scope(self, fake_sdk):
        _restricted_bucket()
        es = FakeES(fail=fake_sdk.UnauthenticatedError("bad token"))
        c = S3AuthClient(max_workers=1, _es_client=es)
        with pytest.raises(CredentialError) as exc:
            c.get_raw("US", "NEW", "2024-01-15")
        assert exc.value.status == 401 and "es login" in exc.value.message
        with pytest.raises(CredentialError):
            c.get_raw("CC", "SEP", "2024-01-15")
        assert len(es.calls) == 1

    def test_404_network_year_is_no_data(self, fake_sdk):
        _restricted_bucket()

        class Resp:
            status_code = 404
            text = "network FDSN:ZI year 2019 not found"

        exc = Exception("HTTPStatusError")
        exc.response = Resp()
        es = FakeES(fail=exc)
        c = S3AuthClient(max_workers=1, _es_client=es)
        with pytest.raises(NoDataError):
            c.get_raw("ZI", "STA", "2019-06-01", "2019-06-03")
        assert c.get_raw("ZI", "STA", "2019-06-01", missing_ok=True) == b""
        assert len(es.calls) == 1

    def test_400_is_credential_error(self, fake_sdk):
        _restricted_bucket()

        class Resp:
            status_code = 400
            text = "year required"

        exc = Exception("HTTPStatusError")
        exc.response = Resp()
        c = S3AuthClient(max_workers=1, _es_client=FakeES(fail=exc))
        with pytest.raises(CredentialError) as ei:
            c.get_raw("US", "NEW", "2024-01-15")
        assert ei.value.status == 400

    def test_transient_exchange_failure_is_fetch_error_not_verdict(self, fake_sdk):
        from seisfetch.exceptions import FetchError

        _restricted_bucket()

        class Resp:
            status_code = 503
            text = "try later"

        exc = Exception("HTTPStatusError")
        exc.response = Resp()
        es = FakeES(fail=exc)
        c = S3AuthClient(max_workers=1, _es_client=es)
        with pytest.raises(FetchError) as ei:
            c.get_raw("US", "NEW", "2024-01-15")
        assert not isinstance(ei.value, CredentialError)
        # not remembered: the next call asks again
        c.get_raw("US", "NEW", "2024-01-15", on_error="warn", missing_ok=True)
        assert len(es.calls) == 2


class TestS3AuthClientCredentials:
    def test_secret_str_unwrapped_for_boto3(self, fake_sdk):
        c = S3AuthClient(max_workers=1, _es_client=FakeES())
        client = c._boto_client(FakeCreds("AKIAX"))
        creds = client._request_signer._credentials
        assert creds.access_key == "AKIAX"
        assert creds.secret_key == "sekret"
        assert creds.token == "tok"

    def test_expired_token_renews_once_with_force(self, fake_sdk):
        body = make_mseed("US", "NEW")
        base = _earthscope_key("US", "NEW", 2024, 15)

        class StubS3:
            def __init__(self):
                self.fail_next_get = True

            def get_paginator(self, name):
                return self

            def paginate(self, Bucket, Prefix):
                yield {"Contents": [{"Key": Prefix + "#1"}]}

            def get_object(self, Bucket, Key):
                assert Key == base + "#1"
                if self.fail_next_get:
                    self.fail_next_get = False
                    raise ClientError(
                        {
                            "Error": {"Code": "ExpiredToken", "Message": "expired"},
                            "ResponseMetadata": {"HTTPStatusCode": 400},
                        },
                        "GetObject",
                    )
                return {"Body": io.BytesIO(body)}

        stub = StubS3()
        built = []

        def factory(creds):
            built.append(creds.aws_access_key_id)
            return stub

        es = FakeES()
        c = S3AuthClient(max_workers=1, _es_client=es, _s3_factory=factory)
        assert c.get_raw("US", "NEW", "2024-01-15") == body
        # one exchange up front (LIST and GET share it), one forced renewal
        assert [call["force"] for call in es.calls] == [False, True]
        assert built == ["AKIA1", "AKIA2"]  # renewed credential -> new client

    def test_old_sdk_refused(self, fake_sdk, monkeypatch):
        import seisfetch.s3 as s3mod

        monkeypatch.setattr(s3mod, "_sdk_version_ok", lambda: False)
        with pytest.raises(ImportError, match="earthscope-sdk>=1.8"):
            S3AuthClient(_es_client=FakeES())

    def test_sdk_version_check(self, monkeypatch):
        import importlib.metadata as md

        from seisfetch.s3 import _sdk_version_ok

        for v, ok in [
            ("1.8.0", True),
            ("1.10.2", True),
            ("2.0", True),
            ("1.7.2", False),
        ]:
            monkeypatch.setattr(md, "version", lambda name, _v=v: _v)
            assert _sdk_version_ok() is ok, v

        def missing(name):
            raise md.PackageNotFoundError(name)

        monkeypatch.setattr(md, "version", missing)
        assert _sdk_version_ok() is True


@mock_aws
class TestSeisfetchClientAuthBackend:
    def test_get_numpy_via_s3_auth_on_open_network(self, fake_sdk):
        from seisfetch.client import SeisfetchClient

        s3 = boto3.client("s3", region_name="us-east-2")
        s3.create_bucket(
            Bucket=OPEN_BUCKET,
            CreateBucketConfiguration={"LocationConstraint": "us-east-2"},
        )
        s3.put_object(
            Bucket=OPEN_BUCKET,
            Key=_earthscope_key("IU", "ANMO", 2024, 15),
            Body=make_mseed("IU", "ANMO", channel="BHZ", location="00"),
        )
        c = SeisfetchClient(backend="s3_auth")
        c._client = S3AuthClient(max_workers=1, _es_client=FakeES())
        c._client._open = S3OpenClient(
            datacenter="earthscope", max_workers=1, _s3_client=s3
        )
        b = c.get_numpy("IU", "ANMO", "2024-01-15", channel="BHZ", trim=False)
        assert b.ids == ["IU.ANMO.00.BHZ"]
