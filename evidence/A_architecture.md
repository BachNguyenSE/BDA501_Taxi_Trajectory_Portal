# A. Kiến trúc

Sơ đồ: `docs/architecture.png` (`docs/architecture.svg`). Giải thích từng mục: `docs/ARCHITECTURE_GUIDE.md`.

Nhãn: **[I]** đã implement và đã chạy, có file trong `evidence/`; **[P]** chỉ thiết kế.

| Mục | Thành phần | Nhãn | Code | Bằng chứng |
|---|---|---|---|---|
| 1 | Kaggle · Porto `train.csv` | [I] (đặt vào `data/raw` thủ công) | `scripts/download_data.sh` mới là khung | `eda/initial_profile.md` |
| 2 | Bronze | [I] | `src/batch/bronze.py`, `src/batch/job_bronze_eda.py` | `bronze/phase1_summary.md` |
| 3 | Data lake Parquet (raw, bronze, silver, gold) | [I] | `data/` (Docker volume) | `D_data_validation.md` |
| 4 | `holidays.csv` | [I] | `data/raw/holidays.csv` | `bronze/phase1_summary.md` |
| 5 | Spark batch, Spark SQL | [I] | `src/batch/silver.py`, `src/batch/gold.py`, `src/batch/job_spark_batch.py` | `spark/phase2_summary.md`, B, C |
| 6 | MapReduce (Hadoop Streaming, YARN + HDFS) | [I] | `src/mapreduce/` | `mapreduce/phase3_summary.md`, ảnh YARN UI |
| 7 | Spark ML | [I] | `src/ml/evaluation.py`, `src/ml/job_train_eta.py` | `ml/E_model_validation.md` |
| 8 | `models/` | [I] | `models/features.json` | `ml/E_model_validation.md` |
| 9 | MongoDB | [I] | `src/serving/job_load_mongo.py` | `mongo/G_nosql.md` |
| 10 | Streamlit UI + REST API | [I] | `src/serving/app.py`, `page_*.py`, `api.py`, `estimate.py`, `routing.py` | `serving/F3_end_to_end.md`, `serving/api.md`, ảnh UI |
| 11 | Taxi GPS live thật | **[P]** | không có | — |
| 12 | Replay producer | [I] | `src/streaming/producer.py` | `streaming/producer_manifest.json` |
| 13 | Kafka | [I] | `docker-compose.yml` (profile streaming) | `streaming/kafka_topic.txt` |
| 14 | Structured Streaming | [I] | `src/streaming/stream_job.py` | `streaming/summary.md` |

**[P] khác:** lịch ETL hằng ngày và huấn luyện lại hằng tháng (cron), HDFS hoặc S3 làm kho cho cụm (HDFS chỉ dùng thật cho job MapReduce), triển khai cụm hoặc cloud.

**Khác biệt so với sơ đồ gốc của nhóm, đã thống nhất:** dùng tên `zone_*` cho collection; `holidays.csv` có 10 ngày (9 lễ quốc gia + São João); thêm khóa `trip_uid` vì `TRIP_ID` không duy nhất.
