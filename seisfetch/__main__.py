"""
Command-line interface for seisfetch.

Usage:
    python -m seisfetch download  IU ANMO --start 2024-01-15 --end 2024-01-15T01:00:00
    python -m seisfetch numpy     CI SDD  --start 2024-06-01 --channel BHZ -o data.npz
    python -m seisfetch zarr      IU ANMO --start 2024-01-15 --channel BHZ -o data.zarr
    python -m seisfetch info      --providers
    python -m seisfetch info      --networks
    python -m seisfetch info      --stations IU 2024 15
    python -m seisfetch bulk      requests.csv -o output_dir/
"""

from __future__ import annotations

import argparse
import csv
import logging
import sys
import time
from pathlib import Path


def _add_common_args(parser: argparse.ArgumentParser):
    """Add NSLC + time + backend args shared across subcommands."""
    parser.add_argument("network", help="FDSN network code (e.g. IU, CI, BK)")
    parser.add_argument("station", help="Station code (e.g. ANMO, SDD)")
    parser.add_argument(
        "-s", "--start", required=True, help="Start time (ISO 8601 or epoch seconds)"
    )
    parser.add_argument(
        "-e", "--end", default=None, help="End time (default: start + 1 day)"
    )
    parser.add_argument(
        "-c", "--channel", default="*", help="Channel code (e.g. BHZ, HH?, default: *)"
    )
    parser.add_argument(
        "-l", "--location", default="*", help="Location code (default: *)"
    )
    parser.add_argument(
        "-b",
        "--backend",
        default="s3_open",
        choices=["s3_open", "s3_auth", "fdsn"],
        help="Data backend (default: s3_open)",
    )
    parser.add_argument(
        "--datacenter",
        default=None,
        choices=["earthscope", "scedc", "ncedc"],
        help="Force S3 datacenter (default: auto-route)",
    )
    parser.add_argument(
        "--providers",
        default=None,
        help="FDSN provider(s), comma-separated (for --backend fdsn)",
    )
    parser.add_argument(
        "-w",
        "--workers",
        type=int,
        default=8,
        help="Parallel download threads (default: 8)",
    )


def cmd_download(args):
    """Download raw miniSEED and save to file."""
    from seisfetch.client import SeisfetchClient

    providers = args.providers.split(",") if args.providers else None
    client = SeisfetchClient(
        backend=args.backend,
        datacenter=args.datacenter,
        providers=providers,
        max_workers=args.workers,
    )

    t0 = time.perf_counter()
    raw = client.get_raw(
        args.network,
        args.station,
        starttime=args.start,
        endtime=args.end,
        channel=args.channel,
        location=args.location,
    )
    elapsed = time.perf_counter() - t0

    if not raw:
        print("No data returned.", file=sys.stderr)
        sys.exit(1)

    outfile = args.output or f"{args.network}.{args.station}.mseed"
    Path(outfile).write_bytes(raw)
    mbps = (len(raw) * 8 / 1e6) / max(elapsed, 1e-9)
    print(f"Wrote {outfile}  ({len(raw):,} bytes, {elapsed:.2f}s, {mbps:.1f} Mbps)")


def cmd_numpy(args):
    """Download, parse with pymseed, save as .npz."""
    import numpy as np

    from seisfetch.client import SeisfetchClient

    providers = args.providers.split(",") if args.providers else None
    client = SeisfetchClient(
        backend=args.backend,
        datacenter=args.datacenter,
        providers=providers,
        max_workers=args.workers,
    )

    t0 = time.perf_counter()
    bundle = client.get_numpy(
        args.network,
        args.station,
        starttime=args.start,
        endtime=args.end,
        channel=args.channel,
        location=args.location,
    )
    elapsed = time.perf_counter() - t0

    if len(bundle) == 0:
        print("No data returned.", file=sys.stderr)
        sys.exit(1)

    outfile = args.output or f"{args.network}.{args.station}.npz"
    arrays = bundle.to_dict()

    # Save with metadata
    save_kwargs = {}
    for nslc, data in arrays.items():
        save_kwargs[nslc] = data
    # Store trace metadata as a structured string
    meta_lines = []
    for t in bundle.traces:
        meta_lines.append(f"{t.id},{t.starttime_ns},{t.sampling_rate},{t.npts}")
    save_kwargs["_metadata"] = np.array(meta_lines, dtype="U")

    np.savez_compressed(outfile, **save_kwargs)
    total_samples = sum(a.size for a in arrays.values())
    print(
        f"Wrote {outfile}  ({len(arrays)} channels, "
        f"{total_samples:,} samples, {elapsed:.2f}s)"
    )


def cmd_zarr(args):
    """Download, parse, save as zarr store."""
    from seisfetch.client import SeisfetchClient
    from seisfetch.convert import to_zarr

    providers = args.providers.split(",") if args.providers else None
    client = SeisfetchClient(
        backend=args.backend,
        datacenter=args.datacenter,
        providers=providers,
        max_workers=args.workers,
    )

    t0 = time.perf_counter()
    ds = client.get_xarray(
        args.network,
        args.station,
        starttime=args.start,
        endtime=args.end,
        channel=args.channel,
        location=args.location,
    )
    elapsed_fetch = time.perf_counter() - t0

    if len(ds.data_vars) == 0:
        print("No data returned.", file=sys.stderr)
        sys.exit(1)

    outdir = args.output or f"{args.network}.{args.station}.zarr"
    t1 = time.perf_counter()
    to_zarr(ds, outdir)
    elapsed_write = time.perf_counter() - t1

    total_samples = sum(ds[v].size for v in ds.data_vars)
    print(
        f"Wrote {outdir}  ({len(ds.data_vars)} channels, "
        f"{total_samples:,} samples, "
        f"fetch={elapsed_fetch:.2f}s, write={elapsed_write:.2f}s)"
    )


def cmd_info(args):
    """Show providers, networks, or stations."""
    if args.info_providers:
        from seisfetch.fdsn import list_providers

        for name, url in sorted(list_providers().items()):
            print(f"  {name:20s} {url}")
        return

    if args.info_networks:
        dc = args.info_datacenter or "earthscope"
        from seisfetch.s3 import S3OpenClient

        client = S3OpenClient()
        nets = client.list_networks(datacenter=dc)
        print(f"Networks in {dc} ({len(nets)}):")
        for n in nets:
            print(f"  {n}")
        return

    if args.info_stations:
        parts = args.info_stations
        if len(parts) != 3:
            print("Usage: info --stations NETWORK YEAR DOY", file=sys.stderr)
            sys.exit(1)
        net, year, doy = parts[0], int(parts[1]), int(parts[2])
        from seisfetch.s3 import S3OpenClient

        client = S3OpenClient()
        stations = client.list_stations(net, year, doy)
        print(f"Stations for {net} on {year}.{doy:03d} ({len(stations)}):")
        for s in stations:
            print(f"  {s}")
        return

    if args.info_route:
        from seisfetch.s3 import route_network

        net = args.info_route.upper()
        dc = route_network(net)
        from seisfetch.s3 import DATACENTERS

        bucket = DATACENTERS[dc]["bucket"]
        region = DATACENTERS[dc]["region"]
        print(f"{net} → {dc}  (s3://{bucket}, {region})")
        return

    print("Use --providers, --networks, --stations, or --route. See --help.")


def cmd_bulk(args):
    """
    Bulk download from a request file.

    File format (CSV, one row per request):
        network,station,location,channel,starttime,endtime

    Example:
        IU,ANMO,00,BHZ,2024-01-15,2024-01-15T01:00:00
        CI,SDD,,BHZ,2024-06-01,2024-06-01T01:00:00
        BK,BRK,00,BHZ,2024-06-01,2024-06-01T01:00:00
    """
    import numpy as np

    from seisfetch.client import SeisfetchClient
    from seisfetch.convert import parse_mseed

    request_file = args.request_file
    outdir = Path(args.output or "bulk_output")
    outdir.mkdir(parents=True, exist_ok=True)
    fmt = args.format

    providers = args.providers.split(",") if args.providers else None
    client = SeisfetchClient(
        backend=args.backend,
        datacenter=args.datacenter,
        providers=providers,
        max_workers=args.workers,
    )

    # Parse request file
    requests = []
    with open(request_file) as f:
        reader = csv.reader(f)
        for row in reader:
            row = [c.strip() for c in row]
            if not row or row[0].startswith("#"):
                continue
            if len(row) < 6:
                print(f"Skipping malformed row: {row}", file=sys.stderr)
                continue
            requests.append(
                {
                    "network": row[0],
                    "station": row[1],
                    "location": row[2] or "*",
                    "channel": row[3] or "*",
                    "starttime": row[4],
                    "endtime": row[5],
                }
            )

    print(f"Processing {len(requests)} requests → {outdir}/")
    t0_total = time.perf_counter()
    success = 0

    for i, req in enumerate(requests, 1):
        tag = f"{req['network']}.{req['station']}.{req['location']}.{req['channel']}"
        try:
            t0 = time.perf_counter()
            raw = client.get_raw(**req)
            elapsed = time.perf_counter() - t0

            if not raw:
                print(f"  [{i}/{len(requests)}] {tag}: no data")
                continue

            if fmt == "mseed":
                outfile = outdir / f"{tag}.mseed"
                outfile.write_bytes(raw)
            elif fmt == "npz":
                bundle = parse_mseed(raw)
                outfile = outdir / f"{tag}.npz"
                arrays = bundle.to_dict()
                np.savez_compressed(str(outfile), **arrays)
            elif fmt == "zarr":
                from seisfetch.convert import to_zarr

                bundle = parse_mseed(raw)
                store = str(outdir / f"{tag}.zarr")
                to_zarr(bundle, store)
                outfile = Path(store)

            mbps = (len(raw) * 8 / 1e6) / max(elapsed, 1e-9)
            print(
                f"  [{i}/{len(requests)}] {tag}: "
                f"{len(raw):,} B, {elapsed:.2f}s, {mbps:.1f} Mbps → {outfile.name}"
            )
            success += 1

        except Exception as e:
            print(f"  [{i}/{len(requests)}] {tag}: FAILED — {e}", file=sys.stderr)

    elapsed_total = time.perf_counter() - t0_total
    print(f"\nDone: {success}/{len(requests)} succeeded in {elapsed_total:.1f}s")


def main():
    parser = argparse.ArgumentParser(
        prog="seisfetch",
        description="Fast seismic miniSEED from EarthScope, SCEDC, NCEDC & FDSN.",
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Enable debug logging"
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # ── download ──────────────────────────────────────────────────── #
    p = sub.add_parser("download", help="Download raw miniSEED to file")
    _add_common_args(p)
    p.add_argument("-o", "--output", default=None, help="Output filename")

    # ── numpy ─────────────────────────────────────────────────────── #
    p = sub.add_parser("numpy", help="Download and save as compressed .npz")
    _add_common_args(p)
    p.add_argument("-o", "--output", default=None, help="Output filename")

    # ── zarr ──────────────────────────────────────────────────────── #
    p = sub.add_parser("zarr", help="Download and save as zarr store")
    _add_common_args(p)
    p.add_argument("-o", "--output", default=None, help="Output directory")

    # ── info ──────────────────────────────────────────────────────── #
    p = sub.add_parser("info", help="Show providers, networks, stations, routing")
    p.add_argument(
        "--providers",
        dest="info_providers",
        action="store_true",
        help="List all FDSN providers",
    )
    p.add_argument(
        "--networks",
        dest="info_networks",
        action="store_true",
        help="List networks in an S3 bucket",
    )
    p.add_argument(
        "--datacenter",
        dest="info_datacenter",
        default=None,
        choices=["earthscope", "scedc", "ncedc"],
        help="Which datacenter to query (default: earthscope)",
    )
    p.add_argument(
        "--stations",
        dest="info_stations",
        nargs=3,
        metavar=("NET", "YEAR", "DOY"),
        help="List stations: --stations IU 2024 15",
    )
    p.add_argument(
        "--route",
        dest="info_route",
        metavar="NET",
        help="Show auto-routing for a network code",
    )

    # ── bulk ──────────────────────────────────────────────────────── #
    p = sub.add_parser(
        "bulk",
        help="Bulk download from a CSV request file",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
Request file format (CSV):
  network,station,location,channel,starttime,endtime

Example:
  IU,ANMO,00,BHZ,2024-01-15,2024-01-15T01:00:00
  CI,SDD,,BHZ,2024-06-01,2024-06-01T01:00:00
  BK,BRK,00,BHZ,2024-06-01,2024-06-01T01:00:00
""",
    )
    p.add_argument("request_file", help="Path to CSV request file")
    p.add_argument(
        "-o", "--output", default=None, help="Output directory (default: bulk_output/)"
    )
    p.add_argument(
        "-f",
        "--format",
        default="mseed",
        choices=["mseed", "npz", "zarr"],
        help="Output format (default: mseed)",
    )
    p.add_argument(
        "-b", "--backend", default="s3_open", choices=["s3_open", "s3_auth", "fdsn"]
    )
    p.add_argument(
        "--datacenter", default=None, choices=["earthscope", "scedc", "ncedc"]
    )
    p.add_argument(
        "--providers", default=None, help="FDSN provider(s), comma-separated"
    )
    p.add_argument("-w", "--workers", type=int, default=8)

    args = parser.parse_args()

    if args.verbose:
        logging.basicConfig(
            level=logging.DEBUG, format="%(levelname)s %(name)s: %(message)s"
        )
    else:
        logging.basicConfig(level=logging.WARNING)

    dispatch = {
        "download": cmd_download,
        "numpy": cmd_numpy,
        "zarr": cmd_zarr,
        "info": cmd_info,
        "bulk": cmd_bulk,
    }
    dispatch[args.command](args)


if __name__ == "__main__":
    main()
