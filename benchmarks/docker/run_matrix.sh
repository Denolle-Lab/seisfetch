#!/usr/bin/env bash
# Run the offline benchmark suites under Fargate- and Lambda-class cgroup
# limits. Results land in benchmarks/results/ (mounted).
#
# On an M1 host this runs linux/arm64 natively (no qemu skew); absolute
# numbers are arm64 — the seisfetch-vs-obspy RATIOS are the portable claim.
set -euo pipefail
cd "$(dirname "$0")/../.."

docker build -f benchmarks/docker/Dockerfile.bench -t seisfetch-bench .

SHA="$(git rev-parse --short HEAD 2>/dev/null || echo unknown)"

run() {
  local tag="$1" cpus="$2" mem="$3"
  echo "== ${tag}: --cpus=${cpus} --memory=${mem}"
  docker run --rm --cpus="${cpus}" --memory="${mem}" \
    -e SEISFETCH_SHA="${SHA}" \
    -v "$(pwd)/benchmarks/results:/repo/benchmarks/results" \
    seisfetch-bench \
    --suite parse,cold_import,memory --tag "${tag}" --limits "${cpus}cpu/${mem}"
}

run fargate-class 2 4g
run lambda-1g 0.6 1g
run lambda-512m 0.5 512m

python -m benchmarks.render_results 2>/dev/null || \
  echo "render RESULTS.md from the host env: pixi run python -m benchmarks.render_results"
