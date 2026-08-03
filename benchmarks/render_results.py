"""
Regenerate ``benchmarks/RESULTS.md`` from ``benchmarks/results/*.json``.

Deterministic: results are sorted by machine tag, then by timestamp, and the
output carries no generation timestamp, so re-running on the same inputs
yields an identical file.

Usage:
    pixi run python -m benchmarks.render_results
"""

from __future__ import annotations

import json
from pathlib import Path

BENCH_DIR = Path(__file__).resolve().parent
RESULTS_DIR = BENCH_DIR / "results"
OUTPUT = BENCH_DIR / "RESULTS.md"

HEADER = """\
# seisfetch benchmark results

Auto-generated from `benchmarks/results/*.json` by
`pixi run python -m benchmarks.render_results` — do not edit by hand.
"""


def _fmt(v) -> str:
    if v is None:
        return "—"
    if isinstance(v, float):
        return f"{v:g}"
    return str(v)


def _table(headers: list[str], rows: list[list]) -> list[str]:
    lines = ["| " + " | ".join(headers) + " |"]
    lines.append("|" + "|".join(" --- " for _ in headers) + "|")
    for row in rows:
        lines.append("| " + " | ".join(_fmt(c) for c in row) + " |")
    return lines


def render_parse(data: dict) -> list[str]:
    headers = [
        "File",
        "Size (MB)",
        "seisfetch min (ms)",
        "seisfetch MB/s",
        "pymseed bare min (ms)",
        "pymseed bare MB/s",
        "ObsPy min (ms)",
        "ObsPy MB/s",
    ]
    rows = []
    for fname in sorted(data):
        entry = data[fname]
        row = [f"`{fname}`", round(entry["bytes"] / 1e6, 3)]
        for key in ("seisfetch", "pymseed_bare", "obspy"):
            sub = entry.get(key, {})
            if "min_ms" in sub:
                row += [sub["min_ms"], sub["mb_per_s"]]
            else:
                row += [sub.get("skipped") or sub.get("error") or "—", "—"]
        rows.append(row)
    return _table(headers, rows)


def render_cold_import(data: dict) -> list[str]:
    headers = ["Module", "min (s)", "mean (s)"]
    rows = []
    for module in sorted(data):
        entry = data[module]
        if "error" in entry:
            rows.append([f"`{module}`", f"error: {entry['error']}", "—"])
        else:
            rows.append([f"`{module}`", entry["min_s"], entry["mean_s"]])
    return _table(headers, rows)


def render_memory(data: dict) -> list[str]:
    lines = [f"File: `{data.get('file', '?')}` ({_fmt(data.get('bytes'))} bytes)", ""]
    headers = ["Parser", "Peak RSS (MB)", "tracemalloc peak (MB)"]
    rows = []
    for parser in ("seisfetch", "obspy"):
        entry = data.get(parser)
        if entry is None:
            continue
        if "error" in entry:
            rows.append([parser, f"error: {entry['error']}", "—"])
        else:
            rows.append([parser, entry["peak_rss_mb"], entry["tracemalloc_peak_mb"]])
    return lines + _table(headers, rows)


def render_footprint(data: dict) -> list[str]:
    headers = ["Package", "Installed size (MB)"]
    rows = []
    for pkg in sorted(data):
        entry = data[pkg]
        if "error" in entry:
            rows.append([f"`{pkg}`", f"error: {entry['error']}"])
        else:
            rows.append([f"`{pkg}`", entry["installed_mb"]])
    return _table(headers, rows)


def render_s3_pull(data: dict) -> list[str]:
    target = data.get("target", {})
    lines = []
    if target:
        lines += [
            "Target: "
            f"{target.get('network')}.{target.get('station')}"
            f".{target.get('channel')} {target.get('date')} (SCEDC)",
            "",
        ]
    headers = ["Client", "Bytes", "Elapsed (s)", "Mbps"]
    rows = []
    for client in ("seisfetch", "boto3_baseline"):
        entry = data.get(client)
        if entry is None:
            continue
        if "error" in entry:
            rows.append([client, f"error: {entry['error']}", "—", "—"])
        else:
            rows.append([client, entry["bytes"], entry["elapsed_s"], entry["mbps"]])
    return lines + _table(headers, rows)


SUITE_RENDERERS = {
    "parse": render_parse,
    "cold_import": render_cold_import,
    "memory": render_memory,
    "footprint": render_footprint,
    "s3_pull": render_s3_pull,
}

SUITE_TITLES = {
    "parse": "Parse (miniSEED → numpy)",
    "cold_import": "Cold import",
    "memory": "Memory (11 MB day file)",
    "footprint": "Install footprint",
    "s3_pull": "Live S3 pull",
}


def render_run(payload: dict) -> list[str]:
    machine = payload.get("machine", {})
    lines = [f"### {payload.get('timestamp', '?')}", ""]
    meta = [
        f"platform: {machine.get('platform')}",
        f"cpus: {machine.get('cpu_count')}",
        f"python: {machine.get('python')}",
        f"seisfetch: {machine.get('seisfetch_sha')}",
        f"pymseed: {machine.get('pymseed')}",
        f"numpy: {machine.get('numpy')}",
        f"obspy: {machine.get('obspy')}",
    ]
    if machine.get("container_limits"):
        meta.append(f"limits: {machine['container_limits']}")
    lines += ["- " + "\n- ".join(meta), ""]

    for suite in sorted(payload.get("suites", {})):
        data = payload["suites"][suite]
        lines.append(f"#### {SUITE_TITLES.get(suite, suite)}")
        lines.append("")
        if isinstance(data, dict) and data.get("skipped"):
            lines += [f"Skipped: {data['skipped']}", ""]
            continue
        renderer = SUITE_RENDERERS.get(suite)
        if renderer is None:
            lines += ["```json", json.dumps(data, indent=2, sort_keys=True), "```"]
        else:
            lines += renderer(data)
        lines.append("")
    return lines


def main():
    payloads = []
    for path in sorted(RESULTS_DIR.glob("*.json")):
        payloads.append(json.loads(path.read_text()))
    payloads.sort(
        key=lambda p: (p.get("machine", {}).get("tag", ""), p.get("timestamp", ""))
    )

    lines = [HEADER]
    current_tag = None
    for payload in payloads:
        tag = payload.get("machine", {}).get("tag", "unknown")
        if tag != current_tag:
            lines += [f"## {tag}", ""]
            current_tag = tag
        lines += render_run(payload)

    text = "\n".join(lines).rstrip() + "\n"
    OUTPUT.write_text(text)
    print(f"wrote {OUTPUT} ({len(payloads)} result file(s))")


if __name__ == "__main__":
    main()
