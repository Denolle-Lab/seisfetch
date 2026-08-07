"""
FDSN Web Services — download raw miniSEED via HTTP.

ObsPy is NEVER used for downloading.  It is only imported (lazily) for:
  1. Provider URL registry (``obspy.clients.fdsn.header.URL_MAPPINGS``)
  2. Station/availability queries via ``get_availability()``

Both fall back gracefully if ObsPy is absent.
"""

from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass

from seisfetch.exceptions import FDSNError, FetchError
from seisfetch.utils import to_epoch, to_isoformat

logger = logging.getLogger(__name__)

# --------------------------------------------------------------------------- #
#  Provider registry — ObsPy's URL_MAPPINGS if available, else built-in
# --------------------------------------------------------------------------- #

_BUILTIN_PROVIDERS = {
    "EARTHSCOPE": "https://service.earthscope.org",
    "IRIS": "https://service.earthscope.org",
    "IRISDMC": "https://service.earthscope.org",
    "IRISPH5": "https://service.earthscope.org",
    "GEOFON": "https://geofon.gfz.de",
    "GFZ": "https://geofon.gfz.de",
    "INGV": "https://webservices.ingv.it",
    "ETH": "https://eida.ethz.ch",
    "ORFEUS": "https://www.orfeus-eu.org",
    "ODC": "https://www.orfeus-eu.org",
    "NCEDC": "https://service.ncedc.org",
    "SCEDC": "https://service.scedc.caltech.edu",
    "USGS": "https://earthquake.usgs.gov",
    "AUSPASS": "https://auspass.edu.au",
    "BGR": "https://eida.bgr.de",
    "BGS": "https://eida.bgs.ac.uk",
    "EMSC": "https://www.seismicportal.eu",
    "GEONET": "https://service.geonet.org.nz",
    "ICGC": "https://ws.icgc.cat",
    "IPGP": "https://ws.ipgp.fr",
    "ISC": "https://www.isc.ac.uk",
    "KNMI": "https://rdsa.knmi.nl",
    "KOERI": "https://eida.koeri.boun.edu.tr",
    "LMU": "https://erde.geophysik.uni-muenchen.de",
    "NIEP": "https://eida-sc3.infp.ro",
    "NOA": "https://eida.gein.noa.gr",
    "NRCAN": "https://earthquakescanada.nrcan.gc.ca",
    "RASPISHAKE": "https://data.raspberryshake.org",
    "RESIF": "https://ws.resif.fr",
    "EPOSFR": "https://seisdata.epos-france.fr",
    "TEXNET": "http://rtserve.beg.utexas.edu",
    "UIB-NORSAR": "https://eida.geo.uib.no",
    "USP": "https://sismo.iag.usp.br",
}

try:
    from obspy.clients.fdsn.header import URL_MAPPINGS as _OBSPY_MAP

    PROVIDERS: dict[str, str] = {**_BUILTIN_PROVIDERS, **_OBSPY_MAP}
except ImportError:
    PROVIDERS = dict(_BUILTIN_PROVIDERS)


def resolve_provider(provider: str) -> str:
    upper = provider.strip().upper()
    if upper in PROVIDERS:
        return PROVIDERS[upper].rstrip("/")
    if provider.startswith("http://") or provider.startswith("https://"):
        return provider.rstrip("/")
    raise ValueError(
        f"Unknown FDSN provider {provider!r}. "
        f"Known: {', '.join(sorted(PROVIDERS))}. Or pass a full URL."
    )


def _iso_to_utc(s: str):
    """ISO 8601 -> aware UTC datetime (stdlib only). Handles 'Z', numeric
    offsets, and fractional seconds of any width; naive times are UTC.
    String comparison of ISO timestamps breaks as soon as one side carries
    an offset or a different fraction width — epochs must be compared as
    datetimes."""
    import re
    from datetime import datetime, timezone

    s = s.strip()
    if not s:
        return None
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    m = re.match(r"^([^.]*\.)(\d+)(.*)$", s)
    if m:
        s = m.group(1) + m.group(2)[:6].ljust(6, "0") + m.group(3)
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


@dataclass(frozen=True)
class ChannelEpoch:
    """One row of fdsnws-station ``format=text&level=channel`` output —
    the lean route to instrument gain and coordinates: one HTTP request,
    stdlib parsing, no StationXML, no ObsPy.

    ``scale`` is the total sensitivity (counts per ``scale_units`` ground
    motion, referenced at ``scale_frequency`` Hz). Dividing raw counts by
    ``scale`` is a GAIN correction: exact at the reference frequency, flat
    to the instrument's passband shape elsewhere. For full deconvolution
    use :mod:`seisfetch.contrib.response`.
    """

    network: str
    station: str
    location: str
    channel: str
    latitude: float
    longitude: float
    elevation: float
    depth: float
    scale: float | None
    scale_frequency: float | None
    scale_units: str | None
    sample_rate: float
    start: str
    end: str | None

    def covers(self, time_iso: str) -> bool:
        t = _iso_to_utc(str(time_iso))
        if t is None:
            raise ValueError(f"unparseable time {time_iso!r}")
        start = _iso_to_utc(self.start)
        end = _iso_to_utc(self.end) if self.end else None
        return (start is None or start <= t) and (end is None or t <= end)


def parse_channel_text(text: str) -> list[ChannelEpoch]:
    """Parse fdsnws-station ``format=text&level=channel`` rows.

    Column order per the FDSN spec: Network|Station|Location|Channel|
    Latitude|Longitude|Elevation|Depth|Azimuth|Dip|SensorDescription|
    Scale|ScaleFreq|ScaleUnits|SampleRate|StartTime|EndTime.
    """

    def _f(v):
        v = v.strip()
        return float(v) if v else None

    out = []
    for line in text.splitlines():
        if not line.strip() or line.startswith("#"):
            continue
        p = line.split("|")
        if len(p) < 17:
            continue
        out.append(
            ChannelEpoch(
                network=p[0].strip(),
                station=p[1].strip(),
                location=p[2].strip(),
                channel=p[3].strip(),
                latitude=float(p[4]),
                longitude=float(p[5]),
                elevation=_f(p[6]) or 0.0,
                depth=_f(p[7]) or 0.0,
                scale=_f(p[11]),
                scale_frequency=_f(p[12]),
                scale_units=(p[13].strip() or None),
                sample_rate=float(p[14]),
                start=p[15].strip(),
                end=(p[16].strip() or None),
            )
        )
    return out


def list_providers() -> dict[str, str]:
    return dict(PROVIDERS)


# --------------------------------------------------------------------------- #
#  HTTP transport — httpx if available, else urllib (stdlib)
# --------------------------------------------------------------------------- #


def _make_session(user=None, password=None, timeout=120.0):
    """Return (session_or_None, use_httpx_bool)."""
    try:
        import httpx

        auth = httpx.DigestAuth(user, password) if user and password else None
        return httpx.Client(timeout=timeout, auth=auth, follow_redirects=True), True
    except ImportError:
        return None, False


def _http_get(
    url,
    params,
    session=None,
    use_httpx=False,
    user=None,
    password=None,
    timeout=120.0,
    nodata_statuses=(204,),
) -> bytes:
    """GET with typed errors: statuses in ``nodata_statuses`` mean "no data"
    and return ``b""``; any other non-2xx raises :class:`FDSNError`."""
    if use_httpx and session:
        resp = session.get(url, params=params)
        if resp.status_code in nodata_statuses:
            return b""
        if resp.status_code >= 400:
            raise FDSNError(resp.status_code, str(resp.url), resp.text[:200])
        return resp.content
    else:
        import urllib.error
        import urllib.parse
        import urllib.request

        qs = urllib.parse.urlencode(params)
        full = f"{url}?{qs}"
        req = urllib.request.Request(full)
        if user and password:
            import base64

            cred = base64.b64encode(f"{user}:{password}".encode()).decode()
            req.add_header("Authorization", f"Basic {cred}")
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.read()
        except urllib.error.HTTPError as e:
            if e.code in nodata_statuses:
                return b""
            raise FDSNError(e.code, full) from e


# --------------------------------------------------------------------------- #
#  FDSNClient — single provider, raw bytes only
# --------------------------------------------------------------------------- #


class FDSNClient:
    """
    Download raw miniSEED from one FDSN server.

    This client does NOT use ObsPy for downloading — only direct HTTP.
    """

    def __init__(
        self,
        provider="EARTHSCOPE",
        user=None,
        password=None,
        timeout=120.0,
        *,
        base_url=None,
    ):
        if base_url is not None:
            provider = base_url
        self._base_url = resolve_provider(provider)
        self._provider_name = provider
        self._user = user
        self._password = password
        self._timeout = timeout
        self._dataselect_url = (
            f"{self._base_url}/fdsnws/dataselect/1/"
            f"{'queryauth' if user and password else 'query'}"
        )
        self._station_url = f"{self._base_url}/fdsnws/station/1/query"
        self._session, self._use_httpx = _make_session(user, password, timeout)

    @property
    def provider(self):
        return self._provider_name

    @property
    def base_url(self):
        return self._base_url

    def get_raw(
        self,
        network,
        station,
        location="*",
        channel="*",
        starttime=None,
        endtime=None,
        **kwargs,
    ) -> bytes:
        """Fetch raw miniSEED bytes via HTTP.  No ObsPy involved."""
        if starttime is None:
            raise ValueError("starttime is required")
        if endtime is None:
            endtime = to_epoch(starttime) + 86400
        # FDSN semantics: "*" is a real wildcard and passes through;
        # "" (blank location) is spelled "--" on the wire. The old code
        # rewrote "*" to "--", silently excluding every location-coded
        # channel (critique B3).
        if location == "":
            loc_param = "--"
        else:
            loc_param = location
        params = {
            "net": network,
            "sta": station,
            "loc": loc_param,
            "cha": channel,
            "start": to_isoformat(starttime),
            "end": to_isoformat(endtime),
            "format": "miniseed",
            "nodata": "404",
        }
        params.update(kwargs)
        t0 = time.perf_counter()
        # we request nodata=404, so 404 means "no data", not "bad URL"
        raw = _http_get(
            self._dataselect_url,
            params,
            self._session,
            self._use_httpx,
            self._user,
            self._password,
            self._timeout,
            nodata_statuses=(204, 404),
        )
        elapsed = time.perf_counter() - t0
        if raw:
            logger.info(
                "[%s] %d B in %.2fs (%.1f Mbps)",
                self._provider_name,
                len(raw),
                elapsed,
                (len(raw) * 8 / 1e6) / max(elapsed, 1e-9),
            )
        return raw

    def get_station_text(
        self,
        network="*",
        station="*",
        location="*",
        channel="*",
        starttime=None,
        endtime=None,
        level="station",
        format="text",
    ) -> str:
        """Query fdsnws-station (raw HTTP, no ObsPy).  Returns text."""
        params = {
            "net": network,
            "sta": station,
            "loc": location,
            "cha": channel,
            "level": level,
            "format": format,
            "nodata": "404",
        }
        if starttime:
            params["start"] = to_isoformat(starttime)
        if endtime:
            params["end"] = to_isoformat(endtime)
        raw = _http_get(
            self._station_url,
            params,
            self._session,
            self._use_httpx,
            self._user,
            self._password,
            self._timeout,
        )
        return raw.decode("utf-8", errors="replace")

    def get_availability(self, **kwargs):
        """
        Query station availability via ObsPy's FDSN Client.

        **Requires ObsPy.**  This is the ONLY method that uses ObsPy,
        and it uses ObsPy's client for discovery/metadata only — never
        for downloading waveform data.

        Returns
        -------
        obspy.Inventory
        """
        try:
            from obspy.clients.fdsn import Client as ObspyClient
        except ImportError:
            raise ImportError(
                "ObsPy is required for station availability queries. "
                "Install with: pip install obspy"
            )
        client = ObspyClient(self._base_url)
        return client.get_stations(**kwargs)

    def close(self):
        if self._use_httpx and hasattr(self._session, "close"):
            self._session.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def __repr__(self):
        return f"FDSNClient({self._provider_name!r}, url={self._base_url!r})"


# --------------------------------------------------------------------------- #
#  FDSNMultiClient — parallel fan-out, raw bytes
# --------------------------------------------------------------------------- #


class FDSNMultiClient:
    """Multi-provider FDSN downloads.

    ``strategy="failover"`` (default) queries providers IN ORDER and returns
    the first non-empty result — one request against one community service
    at a time. ``strategy="broadcast"`` (the old default) queries every
    provider concurrently and concatenates all results; it multiplies load
    on shared FDSN services and produces duplicate records when several
    providers archive the same network, so opt in only when you really want
    a cross-archive union.
    """

    DEFAULT_PROVIDERS = ("EARTHSCOPE", "GEOFON", "ORFEUS", "INGV")

    def __init__(
        self, providers=None, max_workers=4, timeout=120.0, strategy="failover"
    ):
        if providers is None:
            providers = list(self.DEFAULT_PROVIDERS)
        if strategy not in ("failover", "broadcast"):
            raise ValueError("strategy must be 'failover' or 'broadcast'")
        self._provider_names = list(providers)
        self._clients = [FDSNClient(provider=p, timeout=timeout) for p in providers]
        self._max_workers = max_workers
        self._strategy = strategy

    @property
    def providers(self):
        return list(self._provider_names)

    def get_raw(
        self,
        network,
        station,
        location="*",
        channel="*",
        starttime=None,
        endtime=None,
        **kwargs,
    ) -> bytes:
        def _fetch(c):
            return c.get_raw(
                network, station, location, channel, starttime, endtime, **kwargs
            )

        failures: list[tuple[str, str, str]] = []

        if self._strategy == "failover":
            for c in self._clients:
                try:
                    raw = _fetch(c)
                except Exception as e:
                    failures.append((c.provider, type(e).__name__, str(e)))
                    logger.warning("[multi] %s failed: %s", c.provider, e)
                    continue
                if raw:
                    return raw
            if failures and len(failures) == len(self._clients):
                raise FetchError(failures)
            return b""

        # broadcast: provider (submission) order — deterministic output
        chunks = []
        with ThreadPoolExecutor(max_workers=self._max_workers) as pool:
            futs = [(pool.submit(_fetch, c), c) for c in self._clients]
            for f, c in futs:
                try:
                    raw = f.result()
                    if raw:
                        chunks.append(raw)
                except Exception as e:
                    failures.append((c.provider, type(e).__name__, str(e)))
                    logger.warning("[multi] %s failed: %s", c.provider, e)
        if not chunks and failures:
            raise FetchError(failures)
        return b"".join(chunks)

    def close(self):
        for c in self._clients:
            c.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def __repr__(self):
        return f"FDSNMultiClient([{', '.join(self._provider_names)}])"


# --------------------------------------------------------------------------- #
#  ObspyFDSNClient — download via ObsPy's FDSN client (for non-US servers)
# --------------------------------------------------------------------------- #


class ObspyFDSNClient:
    """
    Download waveforms via ObsPy's ``obspy.clients.fdsn.Client``.

    **Requires ObsPy** (``pip install seisfetch[obspy]``).

    Use this for non-US FDSN servers where the direct HTTP path
    (FDSNClient) may not work due to authentication quirks, routing
    tokens, or non-standard endpoints.  ObsPy handles these edge cases
    via its extensive provider support.

    Parameters
    ----------
    provider : str
        Provider name (e.g. ``"GEOFON"``, ``"INGV"``, ``"ETH"``) or a
        full base URL.
    user, password : str, optional
        Credentials for authenticated endpoints.
    """

    def __init__(self, provider="EARTHSCOPE", user=None, password=None):
        try:
            from obspy.clients.fdsn import Client as ObspyClient
        except ImportError:
            raise ImportError(
                "ObsPy is required for ObspyFDSNClient. "
                "Install with: pip install seisfetch[obspy]"
            )
        kwargs = {}
        if user and password:
            kwargs["user"] = user
            kwargs["password"] = password
        self._client = ObspyClient(provider, **kwargs)
        self._provider_name = provider

    @property
    def provider(self):
        return self._provider_name

    def get_raw(
        self,
        network,
        station,
        location="*",
        channel="*",
        starttime=None,
        endtime=None,
        **kwargs,
    ) -> bytes:
        """
        Fetch raw miniSEED bytes via ObsPy's FDSN client.

        ObsPy downloads and returns an in-memory Stream; we serialize
        it back to miniSEED bytes so it integrates with seisfetch's
        ``parse_mseed()`` pipeline.
        """
        import io as _io

        from obspy import UTCDateTime

        from seisfetch.utils import to_epoch

        if starttime is None:
            raise ValueError("starttime is required")
        t1 = UTCDateTime(to_epoch(starttime))
        if endtime is None:
            t2 = t1 + 86400
        else:
            t2 = UTCDateTime(to_epoch(endtime))

        loc = location if location != "*" else "*"
        cha = channel if channel != "*" else "*"
        t0 = __import__("time").perf_counter()
        try:
            st = self._client.get_waveforms(
                network, station, loc, cha, t1, t2, **kwargs
            )
        except Exception as e:
            # only genuine "no data" maps to empty bytes; anything else
            # (auth, transport, server errors) propagates
            if type(e).__name__ == "FDSNNoDataException":
                logger.info(
                    "[obspy-fdsn] %s.%s: no data from %s",
                    network,
                    station,
                    self._provider_name,
                )
                return b""
            raise
        elapsed = __import__("time").perf_counter() - t0
        buf = _io.BytesIO()
        st.write(buf, format="MSEED")
        raw = buf.getvalue()
        if raw:
            logger.info(
                "[obspy-fdsn/%s] %d B in %.2fs", self._provider_name, len(raw), elapsed
            )
        return raw

    def get_waveforms(
        self,
        network,
        station,
        location="*",
        channel="*",
        starttime=None,
        endtime=None,
        **kwargs,
    ):
        """
        Fetch an ObsPy Stream directly (no round-trip through miniSEED bytes).
        """
        from obspy import UTCDateTime

        from seisfetch.utils import to_epoch

        if starttime is None:
            raise ValueError("starttime is required")
        t1 = UTCDateTime(to_epoch(starttime))
        if endtime is None:
            t2 = t1 + 86400
        else:
            t2 = UTCDateTime(to_epoch(endtime))

        loc = location if location != "*" else "*"
        cha = channel if channel != "*" else "*"
        return self._client.get_waveforms(network, station, loc, cha, t1, t2, **kwargs)

    def get_availability(self, **kwargs):
        """Query station metadata via ObsPy's FDSN client."""
        return self._client.get_stations(**kwargs)

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def __repr__(self):
        return f"ObspyFDSNClient({self._provider_name!r})"
