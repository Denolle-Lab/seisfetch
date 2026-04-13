"""
seisfetch: Fast seismic miniSEED from EarthScope, SCEDC, NCEDC,
and 37+ FDSN servers.

Core deps: numpy + boto3 + pymseed.  No ObsPy required.

S3 archives (anonymous, open data):
  EarthScope  s3://earthscope-geophysical-data  (us-east-2)
  SCEDC       s3://scedc-pds                    (us-west-2)
  NCEDC       s3://ncedc-pds                    (us-east-2)

Optional outputs:
  obspy   → get_waveforms(), bundle_to_obspy(), get_availability()
  xarray  → get_xarray(), bundle_to_xarray()
  zarr    → to_zarr()

See THIRD_PARTY_NOTICES.md for full attribution and licenses.
"""

from seisfetch.bulk import (
    BulkRequest,
    BulkResult,
    BulkSummary,
    fetch_bulk_numpy,
    fetch_bulk_raw,
    requests_from_csv,
    requests_from_list,
)
from seisfetch.client import SeisfetchClient
from seisfetch.convert import (
    ChannelMetadata,
    GapInfo,
    TraceArray,
    TraceBundle,
    bundle_to_inventory,
    bundle_to_obspy,
    bundle_to_xarray,
    parse_mseed,
    to_zarr,
)
from seisfetch.fdsn import (
    FDSNClient,
    FDSNMultiClient,
    ObspyFDSNClient,
    list_providers,
    resolve_provider,
)
from seisfetch.s3 import S3AuthClient, S3OpenClient, route_network

# Earth2Studio adapters — lazy import (requires earth2studio + xarray)
try:
    from seisfetch.earth2 import (
        SeismicDataFrameSource,
        SeismicDataSource,
        bundle_to_earth2,
    )
except ImportError:  # earth2studio / xarray not installed
    pass

__version__ = "0.2.0"
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
    "bundle_to_xarray",
    "to_zarr",
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
