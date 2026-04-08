"""
Unified seismic data client.

Primary path: get_raw() → get_numpy() → get_xarray()
ObsPy interop: get_waveforms(), get_availability()  (lazy import)
"""
from __future__ import annotations
import logging
from typing import Literal, Optional, Sequence

logger = logging.getLogger(__name__)
Backend = Literal["s3_open", "s3_auth", "fdsn"]


class SeisfetchClient:
    """
    High-level client for fetching seismic waveforms.

    Core methods (no ObsPy):
      ``get_raw()``    → raw miniSEED bytes
      ``get_numpy()``  → TraceBundle of numpy arrays (pymseed C decoder)
      ``get_xarray()`` → xarray.Dataset (requires xarray)

    ObsPy interop (requires ``pip install obspy``):
      ``get_waveforms()``    → ObsPy Stream
      ``get_availability()`` → ObsPy Inventory (station metadata)
    """

    def __init__(self, backend: Backend = "s3_open",
                 providers: Optional[str | Sequence[str]] = None,
                 max_workers: int = 8,
                 datacenter: Optional[str] = None,
                 fdsn_user=None, fdsn_password=None,
                 fdsn_base_url=None):
        self.backend_name = backend
        if backend == "s3_open":
            from seisfetch.s3 import S3OpenClient
            self._client = S3OpenClient(datacenter=datacenter,
                                        max_workers=max_workers)
        elif backend == "s3_auth":
            from seisfetch.s3 import S3AuthClient
            self._client = S3AuthClient(max_workers=max_workers)
        elif backend == "fdsn":
            self._client = self._build_fdsn(
                providers, max_workers, fdsn_user, fdsn_password, fdsn_base_url)
        else:
            raise ValueError(f"Unknown backend {backend!r}")

    @staticmethod
    def _build_fdsn(providers, max_workers, user, password, base_url):
        from seisfetch.fdsn import FDSNClient, FDSNMultiClient
        if base_url and providers is None:
            providers = base_url
        if providers is None:
            providers = "EARTHSCOPE"
        if isinstance(providers, str):
            return FDSNClient(provider=providers, user=user, password=password)
        return FDSNMultiClient(providers=list(providers), max_workers=max_workers)

    # ── Core: raw bytes ──────────────────────────────────────────────── #

    def get_raw(self, network, station, starttime=None, endtime=None,
                location="*", channel="*", **kwargs) -> bytes:
        """Download raw miniSEED bytes.  No parsing, no ObsPy."""
        return self._client.get_raw(
            network=network, station=station, starttime=starttime,
            endtime=endtime, location=location, channel=channel, **kwargs)

    # ── Core: numpy arrays (pymseed) ─────────────────────────────────── #

    def get_numpy(self, network, station, starttime=None, endtime=None,
                  location="*", channel="*", **kwargs):
        """Fetch → parse (pymseed) → TraceBundle of numpy arrays."""
        from seisfetch.convert import parse_mseed, TraceBundle
        raw = self.get_raw(network, station, starttime, endtime,
                           location, channel, **kwargs)
        return parse_mseed(raw) if raw else TraceBundle()

    # ── Optional: xarray Dataset ──────────────────────────────────────── #

    def get_xarray(self, network, station, starttime=None, endtime=None,
                   location="*", channel="*", **kwargs):
        """Fetch → parse → xarray.Dataset. **Requires xarray.**"""
        from seisfetch.convert import bundle_to_xarray
        return bundle_to_xarray(self.get_numpy(
            network, station, starttime, endtime, location, channel, **kwargs))

    # ── Optional: ObsPy Stream (interop) ──────────────────────────────── #

    def get_waveforms(self, network, station, starttime=None, endtime=None,
                      location="*", channel="*", **kwargs):
        """
        Fetch → parse → ObsPy Stream.  **Requires ObsPy.**

        This does NOT use ObsPy for downloading.  Data is fetched via
        raw HTTP / S3 and decoded with pymseed, then converted to
        ObsPy objects for downstream processing (filtering, etc.).
        """
        from seisfetch.convert import bundle_to_obspy
        return bundle_to_obspy(self.get_numpy(
            network, station, starttime, endtime, location, channel, **kwargs))

    # ── Optional: station availability (ObsPy FDSN client) ────────────── #

    def get_availability(self, **kwargs):
        """
        Query station availability via ObsPy's FDSN client.
        **Requires ObsPy.**

        This is the ONLY place ObsPy's network client is used.
        For non-EarthScope providers (GEOFON, INGV, etc.), this delegates
        to ObsPy's ``Client.get_stations()`` for metadata discovery.
        """
        if hasattr(self._client, "get_availability"):
            return self._client.get_availability(**kwargs)
        raise NotImplementedError(
            "get_availability() requires backend='fdsn'")

    # ── Bulk: parallel multi-request ─────────────────────────────────── #

    def get_raw_bulk(self, requests, max_workers=16, progress=None):
        """
        Fetch raw miniSEED for many requests in parallel.

        Parameters
        ----------
        requests : list
            List of BulkRequest, dicts, or tuples
            ``(net, sta, loc, cha, start, end)``.
        max_workers : int
            Thread pool size (default 16).
        progress : callable, optional
            ``progress(completed, total, result)`` called after each request.

        Returns
        -------
        BulkSummary
        """
        from seisfetch.bulk import (
            fetch_bulk_raw, requests_from_list,
        )
        reqs = requests_from_list(requests)
        return fetch_bulk_raw(reqs, self, max_workers=max_workers,
                              progress=progress)

    def get_numpy_bulk(self, requests, max_workers=16, progress=None):
        """
        Fetch + parse miniSEED for many requests in parallel.

        Each successful result has ``result.bundle`` (a TraceBundle).

        Parameters
        ----------
        requests : list
            List of BulkRequest, dicts, or tuples.
        max_workers : int
        progress : callable, optional

        Returns
        -------
        BulkSummary
        """
        from seisfetch.bulk import (
            fetch_bulk_numpy, requests_from_list,
        )
        reqs = requests_from_list(requests)
        return fetch_bulk_numpy(reqs, self, max_workers=max_workers,
                                progress=progress)

    # ── Lifecycle ─────────────────────────────────────────────────────── #

    def close(self):
        if hasattr(self._client, "close"): self._client.close()
    def __enter__(self): return self
    def __exit__(self, *exc): self.close()
    def __repr__(self):
        extra = ""
        if hasattr(self._client, "provider"):
            extra = f", provider={self._client.provider!r}"
        elif hasattr(self._client, "providers"):
            extra = f", providers={self._client.providers!r}"
        return f"SeisfetchClient(backend={self.backend_name!r}{extra})"
