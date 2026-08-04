"""
seisfetch: Fast seismic miniSEED from EarthScope, SCEDC, NCEDC,
and 37+ FDSN servers.

Core deps: numpy + boto3 + pymseed.  No ObsPy required.

S3 archives:
  EarthScope  s3://earthscope-geophysical-data  (us-east-2, auth via earthscope-sdk)
  SCEDC       s3://scedc-pds                    (us-west-2)
  NCEDC       s3://ncedc-pds                    (us-east-2)

Optional outputs:
  pandas  → bundle_to_metadata_table(), write_metadata_csv()
  obspy   → get_waveforms(), bundle_to_obspy(), get_availability()
  xarray  → get_xarray(), bundle_to_xarray()
  zarr    → to_zarr()

See THIRD_PARTY_NOTICES.md for full attribution and licenses.
"""

from seisfetch.convert import (
    ChannelMetadata,
    GapInfo,
    TraceArray,
    TraceBundle,
    bundle_to_inventory,
    bundle_to_metadata_table,
    bundle_to_obspy,
    bundle_to_xarray,
    metadata_table_to_dict,
    parse_mseed,
    to_zarr,
    write_metadata_csv,
)

# Transport layers (boto3, httpx, ...) are imported lazily via PEP 562 so
# `import seisfetch` stays fast on cold starts (Lambda/containers): parsing
# needs only pymseed+numpy.
_LAZY = {
    "BulkRequest": "seisfetch.bulk",
    "BulkResult": "seisfetch.bulk",
    "BulkSummary": "seisfetch.bulk",
    "fetch_bulk_numpy": "seisfetch.bulk",
    "fetch_bulk_raw": "seisfetch.bulk",
    "requests_from_csv": "seisfetch.bulk",
    "requests_from_list": "seisfetch.bulk",
    "SeisfetchClient": "seisfetch.client",
    "FDSNClient": "seisfetch.fdsn",
    "FDSNMultiClient": "seisfetch.fdsn",
    "ObspyFDSNClient": "seisfetch.fdsn",
    "list_providers": "seisfetch.fdsn",
    "resolve_provider": "seisfetch.fdsn",
    "S3AuthClient": "seisfetch.s3",
    "SeisfetchError": "seisfetch.exceptions",
    "FetchError": "seisfetch.exceptions",
    "NoDataError": "seisfetch.exceptions",
    "FDSNError": "seisfetch.exceptions",
    "S3OpenClient": "seisfetch.s3",
    "route_network": "seisfetch.s3",
}


def __getattr__(name):
    if name in _LAZY:
        import importlib

        return getattr(importlib.import_module(_LAZY[name]), name)
    raise AttributeError(f"module 'seisfetch' has no attribute {name!r}")


def __dir__():
    return sorted(list(globals()) + list(_LAZY))


# Earth2Studio adapters — lazy import (requires earth2studio + xarray)
try:
    from seisfetch.earth2 import (
        SeismicDataFrameSource,
        SeismicDataSource,
        bundle_to_earth2,
    )
except ImportError:  # earth2studio / xarray not installed
    pass

try:
    from importlib.metadata import version as _pkg_version

    __version__ = _pkg_version("seisfetch")
except Exception:  # not installed (e.g. vendored copy)
    __version__ = "0.3.0"
__all__ = [
    "SeisfetchClient",
    "S3OpenClient",
    "S3AuthClient",
    "FDSNClient",
    "FDSNMultiClient",
    "ObspyFDSNClient",
    "list_providers",
    "resolve_provider",
    "parse_mseed",
    "TraceArray",
    "TraceBundle",
    "GapInfo",
    "ChannelMetadata",
    "bundle_to_obspy",
    "bundle_to_inventory",
    "bundle_to_metadata_table",
    "bundle_to_xarray",
    "metadata_table_to_dict",
    "to_zarr",
    "write_metadata_csv",
    "route_network",
    "BulkRequest",
    "BulkResult",
    "BulkSummary",
    "fetch_bulk_raw",
    "fetch_bulk_numpy",
    "requests_from_list",
    "requests_from_csv",
    "SeismicDataSource",
    "SeismicDataFrameSource",
    "bundle_to_earth2",
]
