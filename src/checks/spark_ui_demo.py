"""Chạy lại truy vấn Q3 và phép join của thí nghiệm B trên gold full rồi giữ Spark UI mở 120 giây để chụp ảnh.
Chạy: docker compose run --rm --no-deps --service-ports spark python src/checks/spark_ui_demo.py"""
import time

from core.common import ROOT, get_spark, load_config  # noqa: E402
from batch.job_spark_batch import JOIN_BENCH, Q3  # noqa: E402

cfg = load_config()
spark = get_spark(cfg, "spark-ui-demo")
for t in ["trip_features", "zone_hour_speed"]:
    spark.read.parquet(str(ROOT / "data" / "gold" / t)).createOrReplaceTempView(t)
spark.sparkContext.setJobDescription("Q3: join chuyến với zone × giờ tại điểm xuất phát")
spark.sql(Q3).collect()
spark.sparkContext.setJobDescription("B: broadcast hash join")
spark.sql(JOIN_BENCH.format(hint="BROADCAST")).write.format("noop").mode("overwrite").save()
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
spark.conf.set("spark.sql.adaptive.autoBroadcastJoinThreshold", "-1")
spark.sparkContext.setJobDescription("B: sort-merge join")
spark.sql(JOIN_BENCH.format(hint="MERGE")).write.format("noop").mode("overwrite").save()
print("UI_READY", flush=True)
time.sleep(120)
