# BDA501 Final Project · Porto Taxi ETA (Card F, nhóm 4)

Ước lượng thời gian di chuyển, vùng có khả năng tắc và tuyến gợi ý từ quỹ đạo GPS taxi Porto (ECML/PKDD 2015).
Batch (Spark, MapReduce, Spark ML) và streaming (Kafka, Spark Structured Streaming) cùng đổ vào MongoDB, giao diện Streamlit.

- Kiến trúc: [docs/architecture.png](docs/architecture.png), giải thích từng mục: [docs/ARCHITECTURE_GUIDE.md](docs/ARCHITECTURE_GUIDE.md)
- Chống leakage: [docs/DATA_LEAKAGE.md](docs/DATA_LEAKAGE.md)
- Bằng chứng (checklist A–H, streaming, red flag): [evidence/README.md](evidence/README.md)
- Project card: [evidence/H_project_card.md](evidence/H_project_card.md)

## Yêu cầu

Docker Desktop (khoảng 8 GB RAM cho Docker, 12 CPU), khoảng 10 GB đĩa trống. Mọi bước chạy trong container với phiên bản cố định:
Python 3.12, JDK 17, PySpark 4.0.1, Hadoop 3.5.0, MongoDB 8.0, Kafka 3.9.1. Seed 42, `PYTHONHASHSEED=42`, Spark session UTC.
Cấu hình tập trung: [conf/pipeline.yaml](conf/pipeline.yaml).

## Chạy lại từ đầu

Đặt `train.csv` (Kaggle "Taxi Service Trajectory - Prediction Challenge, ECML PKDD 2015") vào `data/raw/`. Mỗi bước chạy `--sample` trước, `--full` sau.

```bash
docker compose build spark
```
```bash
./scripts/check_env.sh
```
```bash
./scripts/run_eda.sh --full
```
```bash
./scripts/run_pipeline.sh --full
```
```bash
./scripts/run_mapreduce.sh --full
```
```bash
./scripts/run_ml.sh --full
```
```bash
./scripts/run_demo.sh
```
```bash
./scripts/run_streaming.sh
```

| Bước | Script | Ghi chính | Thời gian đo được (full) |
|---|---|---|---|
| Kiểm tra môi trường | `check_env.sh` | `evidence/validation/phase0_environment.md` | |
| Bronze + EDA | `run_eda.sh` | `data/bronze`, `evidence/bronze`, `evidence/eda` | ~22 s |
| Silver, gold, Spark SQL, EXPLAIN, benchmark | `run_pipeline.sh` | `data/silver`, `data/gold`, `evidence/spark` | ~1–2 phút |
| MapReduce trên YARN + đối chiếu | `run_mapreduce.sh` | `evidence/mapreduce` | ~130 s cho job (giả lập amd64) |
| Spark ML | `run_ml.sh` | `models/`, `evidence/ml` | ~60 s |
| Khoảng ước lượng 80% (mở rộng) | `docker compose run --rm --no-deps spark python src/ml/job_eta_interval.py` | `models/interval.json`, `evidence/ml/interval_validation.md` | ~1 phút |
| MongoDB + giao diện | `run_demo.sh` | `evidence/mongo`, http://localhost:8501 | |
| Streaming | `run_streaming.sh` | `evidence/streaming` | ~4–5 phút |

Không chạy profile `hadoop` cùng lúc với Spark (không đủ RAM); các script tự dừng dịch vụ không cần.

Kiểm tra thêm: `docker compose run --rm --no-deps spark python src/checks/check_routing.py` (bộ lọc cạnh đồ thị), `docker compose run --rm spark python src/checks/f3_end_to_end.py` (F3).

## Unit test

```bash
docker compose run --rm --no-deps spark python -m pytest tests -q
```

31 test: hợp đồng bronze, silver và gold (gồm "không lọc bbox trước lag"), mapper / combiner / reducer khớp Spark, tìm tuyến, đánh giá model và leakage, khớp feature giữa phục vụ và huấn luyện, ground truth của producer.

## Soạn code trong VS Code

```bash
./scripts/setup_local_env.sh
```

Tạo `.venv` (Python 3.12) chỉ để có gợi ý code. Pipeline vẫn chạy trong Docker.

## Nhãn [I] / [P]

[I] là phần đã implement và đã chạy, có bằng chứng trong `evidence/`. [P] là phần chỉ thiết kế: nguồn GPS live thật, lịch ETL và huấn luyện lại định kỳ, HDFS hoặc S3 cho cụm, REST `POST /estimate`. Bản phát lại dữ liệu ×60 **không phải real-time**.
