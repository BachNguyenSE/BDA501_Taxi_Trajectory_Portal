#!/usr/bin/env bash
# Bước 4. Huấn luyện, đánh giá model ETA và khoảng ước lượng (src/ml/job_train_eta.py, src/ml/job_eta_interval.py). Dùng: scripts/run_ml.sh [--sample|--full]  (mặc định --sample)
# Cần gold và silver của cùng chế độ (scripts/run_pipeline.sh). --full lưu model vào models/.
set -euo pipefail
cd "$(dirname "$0")/.."
MODE=sample
[[ "${1:-}" == "--full" ]] && MODE=full
docker compose run --rm --no-deps spark python src/ml/job_train_eta.py --mode "$MODE"
# Khoảng ước lượng 80% (mở rộng ngoài đề, đã duyệt), chỉ chạy với model full
if [[ "$MODE" == "full" ]]; then
  docker compose run --rm --no-deps spark python src/ml/job_eta_interval.py
fi
