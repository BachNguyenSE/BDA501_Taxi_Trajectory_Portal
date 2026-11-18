#!/usr/bin/env bash
# Bước 1. Tạo tầng bronze và thống kê mô tả (src/batch/job_bronze_eda.py). Dùng: scripts/run_eda.sh [--sample|--full]  (mặc định --sample)
# Ghi evidence/bronze/ và evidence/eda/. Chạy sample trước, full sau.
set -euo pipefail
cd "$(dirname "$0")/.."
MODE=sample
[[ "${1:-}" == "--full" ]] && MODE=full
docker compose run --rm --no-deps spark python src/batch/job_bronze_eda.py --mode "$MODE"
