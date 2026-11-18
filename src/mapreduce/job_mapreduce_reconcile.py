"""Job: ĐỐI CHIẾU kết quả MapReduce với Spark SQL theo từng key (zone_id, hour).

Mục trong sơ đồ : 6 (MapReduce, mũi tên "đối chiếu" sang 5).
Làm gì          : Spark tính lại từ CÙNG file raw mà MapReduce đọc, bằng chính các hàm bronze/silver,
                  rồi so từng key với kết quả job Hadoop; so counter của mapper với bronze; đọc thông tin job từ YARN.
                  Hai câu Spark SQL:
                    - SQL_MR_LOGIC: cùng logic với MapReduce (đọc từng dòng nên đếm cả bản ghi lặp)
                    - SQL_SILVER  : đúng bảng silver (đã bỏ bản ghi lặp)
                  MapReduce phải khớp SQL_MR_LOGIC từng key; lệch với SQL_SILVER phải đúng bằng phần ghi lặp.
                  Ngưỡng sai số km: n_segments * 1e-6 (làm tròn khi ghi ra text).
Đọc             : data/raw/train.csv (hoặc sample), data/mr/<mode>/mr_zone_hour.tsv, evidence/mapreduce/job_<mode>.log
Ghi             : evidence/mapreduce/reconcile_<mode>.json
Chạy            : tự gọi ở cuối scripts/run_mapreduce.sh [--full] (sau khi đã dừng Hadoop)
Lưu ý           : mapper/combiner/reducer nằm ở src/mapreduce/.
"""
import argparse
import json
import re

from pyspark.sql import functions as F

from batch.bronze import build_bronze, read_raw
from core.common import ROOT, first_row, get_spark, load_config
from batch.silver import build_segments, build_trips

SQL_MR_LOGIC = """
SELECT zone_id, hour, SUM(km * w) AS total_km, SUM(w) * {dt} AS total_s, SUM(w) AS n_segments
FROM seg_weighted GROUP BY zone_id, hour
"""
SQL_SILVER = """
SELECT zone_id, hour, SUM(km) AS total_km, COUNT(*) * {dt} AS total_s, COUNT(*) AS n_segments
FROM seg_weighted GROUP BY zone_id, hour
"""


def parse_counters(log_text):
    """Lấy mọi dòng dạng '<tên>=<số>' trong phần counter của log job."""
    out = {}
    for line in log_text.splitlines():
        m = re.match(r"^\s+([A-Za-z][A-Za-z0-9_ ()\-.:]*?)=(\d+)\s*$", line)
        if m:
            out[m.group(1).strip()] = int(m.group(2))
    return out


def compare(spark, mr, sp, tol_per_seg):
    j = mr.alias("m").join(sp.alias("s"), ["zone_id", "hour"], "full_outer")
    d = j.select(
        "zone_id", "hour",
        F.col("m.total_km").alias("mr_km"), F.col("s.total_km").alias("sp_km"),
        F.col("m.total_s").alias("mr_s"), F.col("s.total_s").alias("sp_s"),
        F.col("m.n_segments").alias("mr_n"), F.col("s.n_segments").alias("sp_n"),
    ).withColumn("km_diff", F.abs(F.col("mr_km") - F.col("sp_km"))) \
     .withColumn("km_tol", F.greatest(F.col("mr_n"), F.col("sp_n")) * tol_per_seg)
    bad = d.filter(F.col("mr_n").isNull() | F.col("sp_n").isNull() | (F.col("mr_n") != F.col("sp_n"))
                   | (F.col("mr_s") != F.col("sp_s")) | (F.col("km_diff") > F.col("km_tol")))
    agg = first_row(d.agg(
        F.count("mr_n").alias("keys_mr"), F.count("sp_n").alias("keys_spark"),
        F.sum(F.col("sp_n").isNull().cast("int")).alias("keys_only_in_mr"),
        F.sum(F.col("mr_n").isNull().cast("int")).alias("keys_only_in_spark"),
        F.sum((F.col("mr_n") != F.col("sp_n")).cast("int")).alias("keys_n_segments_differ"),
        F.sum((F.col("km_diff") > F.col("km_tol")).cast("int")).alias("keys_km_over_tolerance"),
        F.max("km_diff").alias("max_abs_km_diff"),
        F.max(F.col("km_diff") / F.col("km_tol")).alias("max_km_diff_over_tolerance_ratio"),
        F.sum("mr_n").alias("total_segments_mr"), F.sum("sp_n").alias("total_segments_spark"),
        F.sum("mr_km").alias("total_km_mr"), F.sum("sp_km").alias("total_km_spark"),
    )).asDict()
    agg["keys_failing_any_check"] = bad.count()
    agg["examples_failing"] = [r.asDict() for r in bad.limit(20).collect()]
    return agg


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["sample", "full"], default="sample")
    m = ap.parse_args().mode
    cfg = load_config()
    dt = cfg["cleaning"]["gps_interval_s"]
    tol = cfg["mapreduce"]["tolerance_km_per_segment"]
    spark = get_spark(cfg, f"job-mapreduce-reconcile-{m}")
    spark.sparkContext.setLogLevel("WARN")
    ev = ROOT / "evidence" / "mapreduce"

    raw_path = ROOT / ("data/sample/raw/train_sample.csv" if m == "sample" else cfg["paths"]["raw_csv"])
    bronze = build_bronze(read_raw(spark, raw_path), cfg)
    valid = bronze.filter(F.col("reject_reason").isNull())
    w = valid.groupBy("trip_uid").agg(F.count("*").alias("w"))
    seg = build_segments(build_trips(bronze, cfg), cfg).join(w, "trip_uid")
    seg.createOrReplaceTempView("seg_weighted")
    sp_mr = spark.sql(SQL_MR_LOGIC.format(dt=dt))
    sp_silver = spark.sql(SQL_SILVER.format(dt=dt))

    mr = (spark.read.option("sep", "\t").csv(str(ROOT / "data" / "mr" / m / "mr_zone_hour.tsv"))
          .toDF("key", "total_km", "total_s", "n_segments")
          .select(F.split("key", ",")[0].cast("int").alias("zone_id"), F.split("key", ",")[1].cast("int").alias("hour"),
                  F.col("total_km").cast("double"), F.col("total_s").cast("long"), F.col("n_segments").cast("long")))

    log = (ev / f"job_{m}.log").read_text(encoding="utf-8", errors="replace")
    counters = parse_counters(log)
    reasons = {(r["reject_reason"] or "valid"): r["count"] for r in bronze.groupBy("reject_reason").count().collect()}
    counter_check = {
        k: {"mapreduce": counters.get(k if k != "valid" else "valid_trips"), "spark_bronze": reasons.get(k, 0)}
        for k in ["missing_data", "bad_json", "too_few_points", "too_long", "valid"]
    }
    for v in counter_check.values():
        v["match"] = v["mapreduce"] == v["spark_bronze"]

    apps = json.loads((ev / f"yarn_apps_{m}.json").read_text() or "{}")
    app = [a for a in ((apps.get("apps") or {}).get("app") or []) if a.get("name") == f"porto-zone-hour-{m}"]
    app = sorted(app, key=lambda a: a["finishedTime"])[-1] if app else {}

    result = {
        "mode": m,
        "input": str(raw_path.relative_to(ROOT)),
        "tolerance_km_per_segment": tol,
        "vs_spark_same_logic_as_mr": compare(spark, mr, sp_mr, tol),
        "vs_spark_silver_dedup": compare(spark, mr, sp_silver, tol),
        "duplicate_rows_counted_by_mr_only": first_row(w.agg(F.sum(F.col("w") - 1))).asDict(),
        "counter_check_vs_bronze": counter_check,
        "job_counters": {k: counters.get(k) for k in [
            "Launched map tasks", "Launched reduce tasks", "Map input records", "Map output records",
            "Combine input records", "Combine output records", "Reduce input records", "Reduce output records",
            "Reduce shuffle bytes", "HDFS: Number of bytes read", "rows", "header", "valid_trips",
            "pairs_out_bbox", "pairs_fast_in_bbox", "segments_emitted", "bad_csv"]},
        "yarn_app": {k: app.get(k) for k in ["id", "name", "state", "finalStatus", "elapsedTime", "allocatedMB"]},
        "wall": json.loads((ev / f"wall_{m}.json").read_text()),
    }
    (ev / f"reconcile_{m}.json").write_text(json.dumps(result, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    spark.stop()


if __name__ == "__main__":
    main()
