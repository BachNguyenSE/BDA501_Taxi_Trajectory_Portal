#!/usr/bin/env bash
# Bước 2. Spark batch: silver, gold, Spark SQL, EXPLAIN, thí nghiệm broadcast vs sort-merge join (src/batch/job_spark_batch.py).
# Dùng: scripts/run_pipeline.sh [--sample|--full] [--skip-bench]   (mặc định --sample)
# Cần chạy run_eda.sh cùng chế độ trước (bronze). Spark UI: http://localhost:4040 khi đang chạy.
set -euo pipefail
cd "$(dirname "$0")/.."
MODE=sample; EXTRA=()
for a in "$@"; do
  case "$a" in --full) MODE=full;; --sample) MODE=sample;; --skip-bench) EXTRA+=(--skip-bench);; --bench-only) EXTRA+=(--bench-only);; esac
done
docker compose run --rm --no-deps --service-ports spark python src/batch/job_spark_batch.py --mode "$MODE" ${EXTRA[@]+"${EXTRA[@]}"}
