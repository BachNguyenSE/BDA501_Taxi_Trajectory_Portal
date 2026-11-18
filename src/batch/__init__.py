"""batch: mục 2, 3, 5 của sơ đồ. Từ CSV gốc ra bronze → silver → gold, cùng Spark SQL.

Thư viện (logic từng tầng, có unit test):
  bronze.py   parse POLYLINE, GIỮ MỌI DÒNG, gắn reject_reason
  silver.py   chuyến sạch + đoạn 15 giây (explode + lag, KHÔNG lọc bbox trước lag), cờ nhiễu GPS
  gold.py     zones, zone_hour_speed, zone_graph_edges, trip_features
Job (chạy được, ghi data/ và evidence/):
  job_bronze_eda.py    bước 1: bronze + thống kê mô tả      (scripts/run_eda.sh)
  job_spark_batch.py   bước 2: silver, gold, SQL, EXPLAIN, benchmark join   (scripts/run_pipeline.sh)
"""
