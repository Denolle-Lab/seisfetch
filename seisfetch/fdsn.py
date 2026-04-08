"""
FDSN Web Services — download raw miniSEED via HTTP.

ObsPy is NEVER used for downloading.  It is only imported (lazily) for:
  1. Provider URL registry (``obspy.clients.fdsn.header.URL_MAPPINGS``)
  2. Station/availability queries via ``get_availability()``

Both fall back gracefully if ObsPy is absent.
"""
from __future__ import annotations
import io, logging, time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, Sequence

from seisfetch.utils import to_isoformat, to_epoch

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
    PROVIDERS: dict[str, str] = dict(_OBSPY_MAP)
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
        f"Known: {', '.join(sorted(PROVIDERS))}. Or pass a full URL.")


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
        return httpx.Client(timeout=timeout, auth=auth,
                            follow_redirects=True), True
    except ImportError:
        return None, False


def _http_get(url, params, session=None, use_httpx=False,
              user=None, password=None, timeout=120.0) -> bytes:
    if use_httpx and session:
        resp = session.get(url, params=params)
        if resp.status_code == 204:
            return b""
        resp.raise_for_status()
        return resp.content
    else:
        import urllib.parse, urllib.request
        qs = urllib.parse.urlencode(params)
        req = urllib.request.Request(f"{url}?{qs}")
        if user and password:
            import base64
            cred = base64.b64encode(f"{user}:{password}".encode()).decode()
            req.add_header("Authorization", f"Basic {cred}")
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.read()


# --------------------------------------------------------------------------- #
#  FDSNClient — single provider, raw bytes only
# --------------------------------------------------------------------------- #

class FDSNClient:
    """
    Download raw miniSEED from one FDSN server.

    This client does NOT use ObsPy for downloading — only direct HTTP.
    """

    def __init__(self, provider="EARTHSCOPE", user=None, password=None,
                 timeout=120.0, *, base_url=None):
        if base_url is not None:
            provider = base_url
        self._base_url = resolve_provider(provider)
        self._provider_name = provider
        self._user = user
        self._password = password
        self._timeout = timeout
        self._dataselect_url = (
            f"{self._base_url}/fdsnws/dataselect/1/"
            f"{'queryauth' if user and password else 'query'}")
        self._station_url = f"{self._base_url}/fdsnws/station/1/query"
        self._session, self._use_httpx = _make_session(user, password, timeout)

    @property
    def provider(self): return self._provider_name
    @property
    def base_url(self): return self._base_url

    def get_raw(self, network, station, location="*", channel="*",
                starttime=None, endtime=None, **kwargs) -> bytes:
        """Fetch raw miniSEED bytes via HTTP.  No ObsPy involved."""
        if starttime is None:
            raise ValueError("starttime is required")
        if endtime is None:
            endtime = to_epoch(starttime) + 86400
        params = {
            "net": network, "sta": station,
            "loc": location.replace("*", "--") if location == "*" else location,
            "cha": channel,
            "start": to_isoformat(starttime), "end": to_isoformat(endtime),
            "format": "miniseed", "nodata": "404",
        }
        params.update(kwargs)
        t0 = time.perf_counter()
        raw = _http_get(self._dataselect_url, params, self._session,
                        self._use_httpx, self._user, self._password,
                        self._timeout)
        elapsed = time.perf_counter() - t0
        if raw:
            logger.info("[%s] %d B in %.2fs (%.1f Mbps)",
                        self._provider_name, len(raw), elapsed,
                        (len(raw)*8/1e6)/max(elapsed, 1e-9))
        return raw

    def get_station_text(self, network="*", station="*", location="*",
                         channel="*", starttime=None, endtime=None,
                         level="station", format="text") -> str:
        """Query fdsnws-station (raw HTTP, no ObsPy).  Returns text."""
        params = {"net": network, "sta": station, "loc": location,
                  "cha": channel, "level": level, "format": format,
                  "nodata": "404"}
        if starttime: params["start"] = to_isoformat(starttime)
        if endtime:   params["end"] = to_isoformat(endtime)
        raw = _http_get(self._station_url, params, self._session,
                        self._use_httpx, self._user, self._password,
                        self._timeout)
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
                "Install with: pip install obspy")
        client = ObspyClient(self._base_url)
        return client.get_stations(**kwargs)

    def close(self):
        if self._use_httpx and hasattr(self._session, "close"):
            self._session.close()

    def __enter__(self): return self
    def __exit__(self, *exc): self.close()
    def __repr__(self):
        return f"FDSNClient({self._provider_name!r}, url={self._base_url!r})"


# --------------------------------------------------------------------------- #
#  FDSNMultiClient — parallel fan-out, raw bytes
# --------------------------------------------------------------------------- #

class FDSNMultiClient:
    """Fan-out raw miniSEED downloads to multiple FDSN providers."""

    DEFAULT_PROVIDERS = ("EARTHSCOPE", "GEOFON", "ORFEUS", "INGV")

    def __init__(self, providers=None, max_workers=4, timeout=120.0):
        if providers is None:
            providers = list(self.DEFAULT_PROVIDERS)
        self._provider_names = list(providers)
        self._clients = [FDSNClient(provider=p, timeout=timeout) for p in providers]
        self._max_workers = max_workers

    @property
    def providers(self): return list(self._provider_names)

    def get_raw(self, network, station, location="*", channel="*",
                starttime=None, endtime=None, **kwargs) -> bytes:
        chunks = []
        def _fetch(c):
            return c.get_raw(network, station, location, channel,
                             starttime, endtime, **kwargs)
        with ThreadPoolExecutor(max_workers=self._max_workers) as pool:
            futs = {pool.submit(_fetch, c): c for c in self._clients}
            for f in as_completed(futs):
                try:
                    raw = f.result()
                    if raw: chunks.append(raw)
                except Exception:
                    logger.warning("[multi] %s failed",
                                   futs[f].provider, exc_info=True)
        return b"".join(chunks)

    def close(self):
        for c in self._clients: c.close()
    def __enter__(self): return self
    def __exit__(self, *exc): self.close()
    def __repr__(self):
        return f"FDSNMultiClient([{', '.join(self._provider_names)}])"
