"""mapreduce: mục 6 của sơ đồ. Hadoop Streaming trên YARN + HDFS, CHỈ để đối chiếu độc lập với Spark.

mr_common.py                 logic dùng chung cho mapper, chỉ dùng thư viện chuẩn (image Hadoop không có pyspark)
mapper.py                    đọc dòng CSV raw, phát mỗi đoạn hợp lệ theo khóa "zone_id,hour"
reducer.py                   cộng dồn km, giây, số đoạn; dùng làm cả combiner
export_mr_conf.py            sinh mr_conf.json từ conf/pipeline.yaml để mapper dùng cùng tham số
job_mapreduce_reconcile.py   bước 3: so từng key với Spark SQL   (gọi ở cuối scripts/run_mapreduce.sh)
"""
