#!/usr/bin/env bash
# Bước 5. Nạp gold vào MongoDB (src/serving/job_load_mongo.py), chuẩn bị dữ liệu giao diện, mở Streamlit tại http://localhost:8501
# Cần có gold (scripts/run_pipeline.sh --full) và model (python src/ml/job_train_eta.py --mode full) trước.
set -euo pipefail
cd "$(dirname "$0")/.."
docker compose up -d mongo
docker compose run --rm spark python src/serving/job_load_mongo.py
docker compose run --rm --no-deps spark python src/serving/make_ui_data.py >/dev/null
docker compose up -d ui api
echo "Giao diện: http://localhost:8501   ·   REST API: http://localhost:8000/docs"
