# Mã nguồn: đọc từ đâu

Mỗi thư mục là một **cụm trong sơ đồ kiến trúc** (`docs/architecture.png`). Trong mỗi cụm:
- file **thư viện** (tên theo nội dung, ví dụ `silver.py`) chứa logic và có unit test;
- file **job** (tên bắt đầu bằng `job_`) là một bước chạy được của pipeline: đọc dữ liệu, gọi thư viện, ghi `data/` và `evidence/`.

Mỗi file job có phần chú thích đầu theo cùng một khuôn: *làm gì, mục trong sơ đồ, đọc gì, ghi gì, chạy thế nào*.
Mỗi thư mục có `__init__.py` liệt kê các file bên trong.

```
src/
├── core/        dùng chung: cấu hình, SparkSession, hàm địa lý
├── batch/       mục 2, 3, 5   bronze → silver → gold, Spark SQL, EXPLAIN, benchmark
├── mapreduce/   mục 6         Hadoop Streaming, đối chiếu với Spark
├── ml/          mục 7, 8      model ETA, đánh giá, chống leakage, khoảng ước lượng
├── serving/     mục 9, 10     MongoDB, tìm tuyến, estimate(), giao diện Streamlit
├── streaming/   mục 12–14     replay producer, Kafka, Structured Streaming, đối soát
└── checks/      script kiểm tra, sinh bằng chứng (không thuộc pipeline chính)
```

## Thứ tự đọc gợi ý (theo đường đi của dữ liệu)

| # | File | Đọc để hiểu |
|---|---|---|
| 1 | `core/common.py` | cấu hình và SparkSession dùng ở mọi nơi |
| 2 | `batch/bronze.py` | dữ liệu gốc được parse và phân loại lỗi thế nào |
| 3 | `batch/silver.py` | cách nối hai điểm GPS thành đoạn 15 giây |
| 4 | `batch/gold.py` | các bảng tổng hợp: tốc độ zone × giờ, đồ thị zone, feature chuyến |
| 5 | `batch/job_spark_batch.py` | chạy cả chuỗi trên, Spark SQL, EXPLAIN, thí nghiệm join |
| 6 | `mapreduce/mapper.py`, `reducer.py` | cùng phép tính viết lại bằng MapReduce để đối chiếu |
| 7 | `ml/job_train_eta.py` | chia train/test theo thời gian, baseline, GBT, kiểm tra leakage |
| 8 | `serving/estimate.py` | từ A, B, giờ ra ETA, tuyến, zone tắc, lớp live |
| 9 | `serving/app.py` | giao diện |
| 10 | `streaming/producer.py`, `stream_job.py` | luồng streaming |

## Chạy

Mọi lệnh chạy trong Docker qua `scripts/run_*.sh` (xem `README.md` ở thư mục gốc). Trong container, `PYTHONPATH=/workspace/src`
nên import theo tên cụm: `from batch.silver import build_segments`, `from core.common import load_config`.
Có thể chạy một job trực tiếp, ví dụ `python src/batch/job_spark_batch.py --mode sample` hoặc `python -m batch.job_spark_batch --mode sample`.
