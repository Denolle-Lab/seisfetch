# seisfetch

Cloud-first seismic waveform access for EarthScope, SCEDC, NCEDC, and fallback FDSN services.

`seisfetch` is built around one core path:

```text
cloud archive / HTTP  ->  raw miniSEED bytes  ->  pymseed  ->  numpy arrays
                                                      |
                                                      +-> xarray
                                                      +-> zarr
                                                      +-> ObsPy
                                                      +-> Earth2Studio adapters
```

The design goal is simple:

- prefer cloud-native archive access first
- use FDSN second, as a fallback path
- decode miniSEED straight into numpy without requiring ObsPy in the main pipeline
- stay compatible with sparse sensor and Earth2Studio workflows

## Overall Structure

The intended acquisition order is:

1. `s3_open` for SCEDC and NCEDC open buckets
2. `s3_auth` for EarthScope S3 access
3. `fdsn` only when the archive-backed path is unavailable or the network is not served from those buckets

At the package level, the main interfaces are:

- `SeisfetchClient.get_raw()` -> raw miniSEED bytes
- `SeisfetchClient.get_numpy()` -> `TraceBundle` of numpy arrays
- `SeisfetchClient.get_xarray()` -> `xarray.Dataset`
- `SeisfetchClient.get_waveforms()` -> ObsPy `Stream`
- `SeismicDataFrameSource` / `SeismicDataSource` -> Earth2Studio-compatible adapters

## Workflow

### 1. Cloud-first archive access

`seisfetch` routes by network code:

| Network family | Preferred source | Backend |
|---|---|---|
| `CI`, other SCEDC-routed networks | SCEDC open S3 | `s3_open` |
| `BK`, other NCEDC-routed networks | NCEDC open S3 | `s3_open` |
| `IU`, `UW`, `TA`, other EarthScope-routed networks | EarthScope S3 | `s3_auth` |

Archive details:

| Archive | Bucket | Region | Auth |
|---|---|---|---|
| EarthScope | `earthscope-geophysical-data` | `us-east-2` | EarthScope SDK credentials |
| SCEDC | `scedc-pds` | `us-west-2` | none |
| NCEDC | `ncedc-pds` | `us-east-2` | none |

Notes:

- SCEDC and NCEDC are per-channel archives, so you should pass `channel=...`.
- EarthScope stores station-day miniSEED objects and currently requires authenticated access through `earthscope-sdk`.

### 2. FDSN second

Use `backend="fdsn"` when:

- the desired network is not available from EarthScope / SCEDC / NCEDC S3
- the archive-backed attempt fails and you want a fallback provider
- you need a non-US provider such as GEOFON, INGV, ETH, ORFEUS, etc.

This keeps the default workflow archive-first instead of HTTP-first.

### 3. Decode directly to numpy

The central decode path is:

- fetch raw miniSEED bytes
- decode with [`pymseed`](https://github.com/EarthScope/pymseed)
- work in numpy immediately

ObsPy is not required for this path.

### 4. Convert only when needed

Once you have a `TraceBundle`, you can convert to:

- `xarray.Dataset` for labeled arrays and ML/data workflows
- `zarr` for chunked cloud/local persistence
- ObsPy `Stream` for classical seismology tooling
- Earth2Studio adapters for sparse sensor and digital twin/data assimilation workflows

## Why This Package

Existing seismic client workflows often couple:

- transport
- decode
- metadata
- downstream object model

`seisfetch` separates those concerns:

- transport: S3 or HTTP
- decode: miniSEED -> numpy
- output: xarray / zarr / ObsPy / Earth2Studio only when requested

That makes it useful for:

- cloud-native waveform mining
- sparse sensor ingestion
- foundation-model training pipelines
- Earth2Studio interoperability
- data assimilation and digital twin workflows

## Quick Start

### Open S3: SCEDC / NCEDC

```python
from seisfetch import SeisfetchClient

client = SeisfetchClient(backend="s3_open")

bundle = client.get_numpy(
    "CI",
    "ABL",
    channel="BHZ",
    starttime="2024-01-15T00:00:00",
    endtime="2024-01-15T00:10:00",
)

print(bundle.ids)
arrays = bundle.to_dict()
```

### EarthScope S3 (authenticated)

Requires `earthscope-sdk` and an EarthScope account that has been granted the
`s3-miniseed` role. See [EarthScope Credentials](#earthscope-credentials).

```python
client = SeisfetchClient(backend="s3_auth")

bundle = client.get_numpy(
    "IU",
    "ANMO",
    location="00",
    channel="BHZ",
    starttime="2024-01-15T00:00:00",
    endtime="2024-01-15T00:01:00",
)
```

### EarthScope via FDSN (no S3 role required)

Works for any logged-in EarthScope account, including those without direct S3
access. Good for `TA`, `IU`, `US`, `UW`, `_GSN`, etc.:

```python
client = SeisfetchClient(backend="fdsn", providers="EARTHSCOPE")

bundle = client.get_numpy(
    "TA",
    "034A",
    channel="BHZ",
    starttime="2010-06-01T00:00:00",
    endtime="2010-06-01T00:05:00",
)
print(bundle.ids)            # ['TA.034A..BHZ']
print(bundle.to_dict()[bundle.ids[0]].shape)
```

### Station discovery

`get_stations()` queries `fdsnws-station` and auto-routes to SCEDC, NCEDC, or
EarthScope based on the network code. No ObsPy required.

```python
rows = client.get_stations(
    "TA", channel="BHZ",
    starttime="2010-06-01", endtime="2010-06-02",
)
for r in rows[:3]:
    print(r["Network"], r["Station"], r["Channel"], r["Latitude"], r["Longitude"])
```

### FDSN fallback

```python
client = SeisfetchClient(backend="fdsn", providers="GEOFON")

bundle = client.get_numpy(
    "GE",
    "BKB",
    channel="BHZ",
    starttime="2011-03-11T06:00:00",
    endtime="2011-03-11T06:05:00",
)
```

### xarray output

```python
ds = client.get_xarray(
    "CI",
    "ABL",
    channel="BHZ",
    starttime="2024-01-15T00:00:00",
    endtime="2024-01-15T00:10:00",
)
```

### Earth2Studio-compatible sparse sensor output

```python
from datetime import datetime
from seisfetch import SeismicDataFrameSource

df_source = SeismicDataFrameSource(bundle)
df = df_source(datetime(2024, 1, 15), list(ds.data_vars))
```

### Metadata table + zarr sidecar

```python
from seisfetch import bundle_to_metadata_table, to_zarr, write_metadata_csv

metadata_table = bundle_to_metadata_table(bundle)
to_zarr(bundle, "quickstart.zarr", metadata=metadata_table)
write_metadata_csv(metadata_table, "quickstart.zarr")
```

## Dependency Tuning

You do not need every dependency for every workflow.

### Minimal core

Use this when you only want miniSEED -> numpy from S3:

```bash
pip install seisfetch
```

Includes:

- `numpy`
- `boto3`
- `pymseed`

### Add FDSN fallback

Use this if you want direct HTTP fallback providers:

```bash
pip install "seisfetch[fdsn]"
```

Adds:

- `httpx`

### Add authenticated EarthScope S3

Use this if you need EarthScope archive access:

```bash
pip install "seisfetch[auth]"
pip install earthscope-cli
```

Adds:

- `earthscope-sdk`
- EarthScope CLI login flow

### Add xarray

Use this for labeled arrays and ML pipelines:

```bash
pip install "seisfetch[xarray]"
```

### Add metadata tables

Use this if you want canonical metadata tables, CSV sidecars, or metadata-aware zarr output:

```bash
pip install "seisfetch[pandas]"
```

### Add zarr

Use this for chunked persistent stores:

```bash
pip install "seisfetch[zarr]"
```

### Add ObsPy

Use this only if you need ObsPy interop or ObsPy-backed FDSN behavior:

```bash
pip install "seisfetch[obspy]"
```

### Suggested dependency bundles

| Need | Install |
|---|---|
| S3 open data -> numpy only | `pip install seisfetch` |
| Metadata table / metadata.csv export | `pip install "seisfetch[pandas]"` |
| Archive-first + FDSN fallback | `pip install "seisfetch[fdsn]"` |
| EarthScope + SCEDC + NCEDC | `pip install "seisfetch[auth]"` and `pip install earthscope-cli` |
| xarray / ML / Earth2Studio-style workflows | `pip install "seisfetch[xarray]"` |
| zarr persistence | `pip install "seisfetch[zarr]"` |
| ObsPy interop | `pip install "seisfetch[obspy]"` |
| Most common research stack | `pip install "seisfetch[fdsn,auth,xarray,zarr,obspy]"` |

## Installation Modes

### pip

For scripts and lightweight pipelines:

```bash
pip install seisfetch
```

From source:

```bash
git clone https://github.com/Denolle-Lab/seisfetch
cd seisfetch
pip install .
```

### pixi

For notebook work and development:

```bash
git clone https://github.com/Denolle-Lab/seisfetch
cd seisfetch
pixi install
pixi install -e notebooks
pixi run -e notebooks kernel-install
```

The notebook environment is the intended Jupyter environment for this repo.

## EarthScope Credentials

EarthScope has **two access tiers**:

1. **FDSN web service** (`backend="fdsn", providers="EARTHSCOPE"`) — available
   to any logged-in account. No special role required.
2. **Direct S3** (`backend="s3_auth"`) — requires the `s3-miniseed` IAM role to
   be granted on your account. This is faster and cheaper when you are running
   in `us-east-2`.

### Setup

```bash
pip install "seisfetch[auth]"
pip install earthscope-cli
es login
```

The `[auth]` extra already installs `earthscope-sdk`, so there is no separate SDK install step.

### Verify (CLI)

```bash
# 1. Confirm you are logged in
es user get-profile                # prints your name, email, institution

# 2. Confirm direct-S3 role is granted (only needed for backend="s3_auth")
es user get-aws-credentials        # prints temporary AWS keys, or an error
```

A response of `"You are not allowed to assume role 's3-miniseed'"` or
`UnauthorizedError` means your account is logged in but **direct S3 is not
enabled yet**. Use `backend="fdsn", providers="EARTHSCOPE"` in the meantime
and email `data-help@earthscope.org` to request the `s3-miniseed` role.

### Verify (Python)

```python
from earthscope_sdk import EarthScopeClient

with EarthScopeClient() as client:
    print(client.user.get_profile())
    try:
        creds = client.user.get_aws_credentials()
        print("S3 role granted:", creds.aws_access_key_id[:8])
    except Exception as exc:
        print("S3 role NOT granted:", exc)
```

### Headless / CI

```bash
es user get-refresh-token
export ES_OAUTH2__REFRESH_TOKEN="<your-refresh-token>"
```

## Architecture

```text
SeisfetchClient
|
+- get_raw()        -> raw miniSEED bytes
+- get_numpy()      -> TraceBundle (numpy)
+- get_xarray()     -> xarray.Dataset
+- get_waveforms()  -> ObsPy Stream
|
+- backend="s3_open"
|  +- SCEDC open bucket
|  +- NCEDC open bucket
|  +- auto-routing by network code
|
+- backend="s3_auth"
|  +- EarthScope S3 via earthscope-sdk credentials
|
+- backend="fdsn"
|  +- single-provider HTTP client
|  +- multi-provider fan-out client
|
+- backend="obspy_fdsn"
   +- ObsPy-backed fallback for harder FDSN cases
```

## Earth2Studio Compatibility

The package includes adapters in `seisfetch.earth2` for Earth2Studio-style usage:

- `SeismicDataSource`
- `SeismicDataFrameSource`
- `bundle_to_earth2`

These are intended for:

- sparse sensor tables
- observation pipelines
- foundation-model data preparation
- Earth2Studio / digital twin workflows

Typical path:

```text
miniSEED -> numpy -> xarray / sparse dataframe -> Earth2Studio adapter
```

## Recipes

A few common end-to-end tasks the package is designed for.

### Mine a single station-day to numpy

```python
from seisfetch import SeisfetchClient

bundle = SeisfetchClient(backend="s3_open").get_numpy(
    "CI", "ABL", channel="BHZ",
    starttime="2024-01-15T00:00:00",
    endtime="2024-01-16T00:00:00",
)
data = bundle.to_dict()["CI.ABL..BHZ"]   # int32 numpy array, full day
```

### Bulk fetch many station-channels in parallel

```python
requests = [
    {"network": "CI", "station": s, "channel": c,
     "starttime": "2025-04-14T17:07:30", "endtime": "2025-04-14T17:10:30"}
    for s in ["ABL", "SDD", "PASC"]
    for c in ["BHZ", "BHN", "BHE"]
]
client = SeisfetchClient(backend="s3_open")
summary = client.get_numpy_bulk(requests, max_workers=8)
print(summary.succeeded, "/", summary.total)
```

Missing station-channel combinations are reported in `summary.failed` and do
not abort the run.

### Archive-first with FDSN fallback

```python
from seisfetch import SeisfetchClient, route_network

def get_archive_first(net, sta, *, starttime, endtime, location="*", channel="*",
                       fallback="GEOFON"):
    dc = route_network(net)
    primary = "s3_open" if dc in {"scedc", "ncedc"} else "s3_auth"
    try:
        return SeisfetchClient(backend=primary).get_numpy(
            net, sta, location=location, channel=channel,
            starttime=starttime, endtime=endtime,
        )
    except Exception:
        return SeisfetchClient(backend="fdsn", providers=fallback).get_numpy(
            net, sta, location=location, channel=channel,
            starttime=starttime, endtime=endtime,
        )
```

### Persist a multi-channel package to zarr with metadata sidecar

```python
from seisfetch import bundle_to_metadata_table, to_zarr, write_metadata_csv

metadata = bundle_to_metadata_table(bundle)
to_zarr(bundle, "package.zarr", metadata=metadata)
write_metadata_csv(metadata, "package.zarr")
```

The resulting `package.zarr/` contains channel groups plus a
`metadata/channel_table` group readable with `xarray.open_zarr(...,
group="metadata/channel_table")`.

### Earth2Studio sparse-sensor handoff

```python
from datetime import datetime
from seisfetch import SeismicDataFrameSource, bundle_to_xarray

ds = bundle_to_xarray(bundle)
df = SeismicDataFrameSource(bundle)(datetime(2024, 1, 15), list(ds.data_vars))
df[["time", "variable", "network", "station", "channel",
     "sampling_rate", "amplitude_rms", "num_samples"]].head()
```

## Command Line

Examples:

```bash
# SCEDC open S3 (no auth)
seisfetch download CI ABL -s 2024-01-15 -e 2024-01-15T01:00:00 -c BHZ -o data.mseed
seisfetch numpy    CI SDD -s 2024-06-01 -c BHZ -o data.npz
seisfetch zarr     CI ABL -s 2024-01-15 -c BHZ -o data.zarr

# EarthScope (requires `es login` and the s3-miniseed role)
seisfetch download IU ANMO -s 2024-01-15 -e 2024-01-15T01:00:00 -c BHZ --backend s3_auth -o anmo.mseed

# Routing and provider info
seisfetch info --route CI
seisfetch info --providers

# Bulk
seisfetch bulk requests.csv -o output/ -f npz
```

## Notebooks

See [notebooks/](notebooks/) for worked examples:

- [01_quickstart.ipynb](notebooks/01_quickstart.ipynb)
- [02_bulk_mining.ipynb](notebooks/02_bulk_mining.ipynb)
- [03_xarray_zarr_pipeline.ipynb](notebooks/03_xarray_zarr_pipeline.ipynb)
- [04_earth2studio_interop.ipynb](notebooks/04_earth2studio_interop.ipynb)

Notebook setup instructions are in [notebooks/README.md](notebooks/README.md).

## Tests

```bash
pixi run test
pixi run test-cov
pixi run test-int
```

## Dependencies

| Package | Status | Role |
|---|---|---|
| `numpy` | core | array container |
| `boto3` | core | S3 transport |
| `pymseed` | core | miniSEED decode |
| `httpx` | optional `[fdsn]` | FDSN HTTP client |
| `pandas` | optional `[pandas]` | canonical metadata tables and CSV export |
| `xarray` | optional `[xarray]` | labeled dataset output |
| `zarr` | optional `[zarr]` | persistent chunked storage |
| `obspy` | optional `[obspy]` | ObsPy interop and alternative FDSN backend |
| `earthscope-sdk` | optional `[auth]` | EarthScope S3 credentials |

See [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md) for attribution and licenses.

## Citation

When using data accessed through `seisfetch`:

- EarthScope: cite the network operators and NSF SAGE facility
- SCEDC: doi:[10.7909/C3WD3xH1](https://doi.org/10.7909/C3WD3xH1)
- NCEDC: doi:[10.7932/NCEDC](https://doi.org/10.7932/NCEDC)
- other FDSN providers: cite the underlying network/provider

Software references:

- `pymseed` / `libmseed`: EarthScope Data Services
- NoisePy S3 access pattern: Jiang & Denolle (2020), doi:[10.1785/0220190364](https://doi.org/10.1785/0220190364)
- ObsPy: Beyreuther et al. (2010), doi:[10.1785/gssrl.81.3.530](https://doi.org/10.1785/gssrl.81.3.530)

## License

MIT. See [LICENSE](LICENSE).
