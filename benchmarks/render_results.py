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
PLOTS_DIR = BENCH_DIR / "plots"
OUTPUT = BENCH_DIR / "RESULTS.md"
OUTPUT_HTML = BENCH_DIR / "RESULTS.html"

# validated categorical palette (fixed slot order; dataviz-checked)
C_SF = "#2a78d6"  # seisfetch
C_OB = "#eb6834"  # obspy
C_BARE = "#1baf7a"  # bare pymseed

HEADER = """\
# seisfetch benchmark results

Auto-generated from `benchmarks/results/*.json` by
`pixi run python -m benchmarks.render_results` — do not edit by hand.
A self-contained HTML version with the same content lives at
[`RESULTS.html`](RESULTS.html).
"""

PLOT_SECTION = """\
## Plots

The seisfetch vs obspy comparison at a glance (latest run per machine;
SVGs regenerate with the tables):

![Parse time](plots/parse.svg)

![Cold import](plots/cold_import.svg)

![Parse memory](plots/memory.svg)

![Installed footprint](plots/footprint.svg)
"""


def _latest_per_tag(payloads: list[dict]) -> dict[str, dict]:
    """Latest run per machine tag, in a stable display order."""
    order = ["m1-native", "fargate-class", "lambda-1g", "lambda-512m"]
    latest: dict[str, dict] = {}
    for p in payloads:
        tag = p.get("machine", {}).get("tag", "?")
        if tag not in latest or p.get("timestamp", "") > latest[tag].get(
            "timestamp", ""
        ):
            latest[tag] = p
    ordered = {t: latest[t] for t in order if t in latest}
    for t, p in latest.items():
        ordered.setdefault(t, p)
    return ordered


def _grouped_bars(ax, tags, series, title, ylabel):
    """Thin grouped bars, direct value labels, recessive frame."""
    import numpy as np

    x = np.arange(len(tags))
    n = len(series)
    width = 0.8 / n
    for i, (label, values, color) in enumerate(series):
        pos = x - 0.4 + width * (i + 0.5)
        bars = ax.bar(pos, values, width * 0.9, label=label, color=color)
        for b, v in zip(bars, values):
            if v is not None and v == v:
                ax.annotate(
                    f"{v:g}",
                    (b.get_x() + b.get_width() / 2, v),
                    ha="center",
                    va="bottom",
                    fontsize=7.5,
                    color="#444444",
                )
    ax.set_xticks(x, tags, fontsize=8.5)
    ax.set_ylabel(ylabel, fontsize=9)
    ax.set_title(title, fontsize=10)
    ax.legend(frameon=False, fontsize=8.5)
    ax.spines[["top", "right"]].set_visible(False)
    ax.grid(axis="y", alpha=0.25, linewidth=0.5)
    ax.set_axisbelow(True)


def render_plots(payloads: list[dict]) -> bool:
    """Write benchmarks/plots/*.svg. Returns False when matplotlib is
    absent (plots are a dev nicety, never a core dependency)."""
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        return False

    latest = _latest_per_tag(payloads)
    tags = list(latest)
    PLOTS_DIR.mkdir(exist_ok=True)

    def suite(tag, name):
        return latest[tag].get("suites", {}).get(name, {})

    def make(fname, title, ylabel, series):
        fig, ax = plt.subplots(figsize=(6.8, 2.9), dpi=110)
        _grouped_bars(ax, tags, series, title, ylabel)
        fig.tight_layout()
        fig.savefig(PLOTS_DIR / fname, format="svg", metadata={"Date": None})
        plt.close(fig)

    day = "tests/bench.mseed"
    make(
        "parse.svg",
        "Parse 11 MB Steim2 channel-day (min of 5, warm)",
        "ms",
        [
            (
                "seisfetch",
                [
                    suite(t, "parse").get(day, {}).get("seisfetch", {}).get("min_ms")
                    for t in tags
                ],
                C_SF,
            ),
            (
                "obspy",
                [
                    suite(t, "parse").get(day, {}).get("obspy", {}).get("min_ms")
                    for t in tags
                ],
                C_OB,
            ),
            (
                "bare pymseed",
                [
                    suite(t, "parse").get(day, {}).get("pymseed_bare", {}).get("min_ms")
                    for t in tags
                ],
                C_BARE,
            ),
        ],
    )
    make(
        "cold_import.svg",
        "Cold import (fresh interpreter, min of 5)",
        "s",
        [
            (
                "seisfetch",
                [
                    suite(t, "cold_import").get("seisfetch", {}).get("min_s")
                    for t in tags
                ],
                C_SF,
            ),
            (
                "obspy",
                [suite(t, "cold_import").get("obspy", {}).get("min_s") for t in tags],
                C_OB,
            ),
        ],
    )
    make(
        "memory.svg",
        "Parse memory, 11 MB day file (tracemalloc peak)",
        "MB",
        [
            (
                "seisfetch",
                [
                    suite(t, "memory").get("seisfetch", {}).get("tracemalloc_peak_mb")
                    for t in tags
                ],
                C_SF,
            ),
            (
                "obspy",
                [
                    suite(t, "memory").get("obspy", {}).get("tracemalloc_peak_mb")
                    for t in tags
                ],
                C_OB,
            ),
        ],
    )
    foot_tags = [t for t in tags if suite(t, "footprint")]
    if foot_tags:
        fig, ax = plt.subplots(figsize=(4.4, 2.9), dpi=110)
        _grouped_bars(
            ax,
            foot_tags,
            [
                (
                    "seisfetch core",
                    [
                        suite(t, "footprint")
                        .get("seisfetch_core", {})
                        .get("installed_mb")
                        for t in foot_tags
                    ],
                    C_SF,
                ),
                (
                    "obspy",
                    [
                        suite(t, "footprint").get("obspy", {}).get("installed_mb")
                        for t in foot_tags
                    ],
                    C_OB,
                ),
            ],
            "Installed footprint (fresh venv)",
            "MB",
        )
        ax.axhline(250, color="#777777", lw=0.8, ls=":")
        ax.annotate(
            "AWS Lambda layer limit (250 MB)",
            (0.02, 0.88),
            xycoords="axes fraction",
            fontsize=7.5,
            color="#555555",
        )
        fig.tight_layout()
        fig.savefig(PLOTS_DIR / "footprint.svg", format="svg", metadata={"Date": None})
        plt.close(fig)
    return True


def render_html(md_text: str) -> None:
    """Self-contained HTML twin of RESULTS.md: tables + inlined SVGs.

    Stdlib-only conversion (headers, tables, images, code) — no markdown
    library, no new dependencies."""
    import html as html_mod
    import re

    def inline_img(m):
        rel = m.group(2)
        path = BENCH_DIR / rel
        if not path.exists():
            return ""
        if path.suffix == ".png":
            import base64

            b64 = base64.b64encode(path.read_bytes()).decode()
            return (
                f'<figure><img alt="{m.group(1)}" '
                f'src="data:image/png;base64,{b64}"/></figure>'
            )
        svg = path.read_text().replace(chr(35), "%23")
        return (
            f'<figure><img alt="{m.group(1)}" '
            f'src="data:image/svg+xml;utf8,{svg}"/></figure>'
        )

    lines_out = [
        """<!doctype html><meta charset="utf-8">
<title>seisfetch benchmarks</title>
<style>
body{font:15px/1.55 -apple-system,system-ui,sans-serif;max-width:60rem;
margin:2rem auto;padding:0 1rem;color:#1c2326;background:#fbfbfa}
table{border-collapse:collapse;font-size:.85rem;margin:.8rem 0;
font-variant-numeric:tabular-nums}
th{text-align:left;border-bottom:2px solid #d8ddda;padding:.3rem .7rem .3rem 0}
td{border-bottom:1px solid #e4e8e5;padding:.3rem .7rem .3rem 0}
code{background:#eef0ee;padding:.08em .3em;border-radius:3px;font-size:.85em}
h1,h2,h3{line-height:1.25}figure{margin:1rem 0}img{max-width:100%}
</style>"""
    ]
    in_table = False
    for line in md_text.splitlines():
        img = re.match(r"!\[([^\]]*)\]\(([^)]+)\)", line.strip())
        if img:
            lines_out.append(inline_img(img))
            continue
        if line.startswith("|"):
            cells = [c.strip() for c in line.strip().strip("|").split("|")]
            if all(set(c) <= {"-", " ", ":"} and c for c in cells):
                continue  # separator row
            tag = "th" if not in_table else "td"
            if not in_table:
                lines_out.append("<table>")
                in_table = True
            row = "".join(
                f"<{tag}>{html_mod.escape(c).replace('`', '')}</{tag}>" for c in cells
            )
            lines_out.append(f"<tr>{row}</tr>")
            continue
        if in_table:
            lines_out.append("</table>")
            in_table = False
        if line.startswith("### "):
            lines_out.append(f"<h3>{html_mod.escape(line[4:])}</h3>")
        elif line.startswith("## "):
            lines_out.append(f"<h2>{html_mod.escape(line[3:])}</h2>")
        elif line.startswith("# "):
            lines_out.append(f"<h1>{html_mod.escape(line[2:])}</h1>")
        elif line.startswith("- "):
            lines_out.append(f"<div>&bull; {html_mod.escape(line[2:])}</div>")
        elif line.strip():
            text = html_mod.escape(line)
            text = re.sub(r"`([^`]+)`", r"<code>\1</code>", text)
            lines_out.append(f"<p>{text}</p>")
    if in_table:
        lines_out.append("</table>")
    OUTPUT_HTML.write_text("\n".join(lines_out))


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


EQUIV_FILES = [
    (
        "ccf_eval_ci_pasc_2022-01-02.json",
        "Single-station (CI.PASC day, SCEDC): EN/EZ/NZ cross-component + ZZ "
        "autocorrelation through real noisepy at 40 sps",
    ),
    (
        "xcorr_three_archives_2022-01-02.json",
        "Cross-station, three archives (CI.PASC/SCEDC x BK.PKD/NCEDC x "
        "II.PFO/EarthScope) through real noisepy at 20 sps — the "
        "Fourier-resample and sub-sample-alignment branches run inside "
        "the chain",
    ),
]


def render_equivalence() -> list[str]:
    """NoisePy CCF equivalence: obspy-fed vs seisfetch-fed paths.

    Renders the harness JSONs (benchmarks/noisepy_eval/run_*_eval.py) and
    embeds the visual-validation figures exported from notebook 06."""
    lines = [
        "## NoisePy equivalence (obspy-free path)",
        "",
        "Identical archive bytes fed through (A) obspy.read + noisepy "
        "`preprocess_raw` and (B) seisfetch parse + "
        "`contrib.noisepy_adapter` ports, then noisepy's own "
        "`compute_fft`/`correlate`. Pass requires **bit-identity** "
        "(`max_abs_diff == 0.0`). Harnesses: "
        "`benchmarks/noisepy_eval/run_ccf_eval.py` and `run_xcorr_eval.py` "
        "(integration tests in `tests/precision/`).",
        "",
    ]
    found = False
    for fname, caption in EQUIV_FILES:
        path = RESULTS_DIR / fname
        if not path.exists():
            continue
        found = True
        data = json.loads(path.read_text())
        lines += [f"### {caption}", ""]
        headers = ["Pair", "max abs diff", "waveform corr", "pass"]
        rows = [
            [
                pair,
                f"{r.get('max_abs_diff', float('nan')):.1e}",
                f"{r.get('waveform_corr', float('nan')):.9f}",
                "PASS" if r.get("pass") else "FAIL",
            ]
            for pair, r in sorted(data.items())
        ]
        lines += _table(headers, rows)
        lines.append("")
    if not found:
        return []
    fig_lines = []
    for fname, alt in (
        ("xcorr_stacks.png", "CCF stack progression, four pairs"),
        ("xcorr_section.png", "Stacked CCF record section vs distance"),
    ):
        if (PLOTS_DIR / fname).exists():
            fig_lines.append(f"![{alt}](plots/{fname})")
            fig_lines.append("")
    if fig_lines:
        lines += [
            "Visual validation (notebook "
            "`notebooks/06_cross_correlation_three_archives.ipynb`: two "
            "months, four stations, three archives, response removed with "
            "`seisfetch.contrib.response` — no obspy in the chain):",
            "",
        ] + fig_lines
    return lines


def main():
    payloads = []
    for path in sorted(RESULTS_DIR.glob("*.json")):
        payload = json.loads(path.read_text())
        if "machine" not in payload or "suites" not in payload:
            # auxiliary result files (e.g. CCF equivalence JSON) are not
            # benchmark runs — rendered by render_equivalence() instead
            continue
        payloads.append(payload)
    payloads.sort(
        key=lambda p: (p.get("machine", {}).get("tag", ""), p.get("timestamp", ""))
    )

    have_plots = render_plots(payloads)
    lines = [HEADER]
    lines += render_equivalence()
    if have_plots:
        lines.append(PLOT_SECTION)
    current_tag = None
    for payload in payloads:
        tag = payload.get("machine", {}).get("tag", "unknown")
        if tag != current_tag:
            lines += [f"## {tag}", ""]
            current_tag = tag
        lines += render_run(payload)

    text = "\n".join(lines).rstrip() + "\n"
    OUTPUT.write_text(text)
    render_html(OUTPUT.read_text())
    print(f"wrote {OUTPUT} ({len(payloads)} result file(s))")


if __name__ == "__main__":
    main()
