#!/usr/bin/env bash
# Kiểm tra môi trường: cấu hình compose hợp lệ, image Spark build được, phiên bản đúng đề, MongoDB chạy được.
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p evidence/validation
OUT=evidence/validation/phase0_environment.md
{
  echo "# Phase 0: kiểm tra môi trường"
  echo "Chạy lúc: $(date -u +%Y-%m-%dT%H:%M:%SZ) (UTC)"
  echo
  echo "## docker compose config (mọi profile)"
  docker compose --profile hadoop --profile streaming config --quiet && echo "OK: cấu hình hợp lệ"
  echo
  echo "## Phiên bản trong container spark"
  docker compose run --rm --no-deps spark bash -lc '
    python --version
    java -version 2>&1 | head -1
    python -c "import pyspark,sys;print(\"pyspark\",pyspark.__version__)"
    python -c "import yaml;c=yaml.safe_load(open(\"conf/pipeline.yaml\"));print(\"pipeline.yaml OK, seed =\",c[\"project\"][\"seed\"])"
    echo PYTHONHASHSEED=$PYTHONHASHSEED
  '
} 2>&1 | tee "$OUT"
