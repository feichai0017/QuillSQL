#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

sample_size="${QUILL_BENCH_SAMPLE_SIZE:-10}"
features="${QUILL_BENCH_FEATURES:-}"

if [[ -n "${features}" ]]; then
  cargo bench --features "${features}" --bench jit_micro -- --sample-size "${sample_size}"
else
  cargo bench --bench jit_micro -- --sample-size "${sample_size}"
fi
