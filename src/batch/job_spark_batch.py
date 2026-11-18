"""Job: Spark batch, từ bronze ra SILVER, GOLD, cùng Spark SQL, EXPLAIN và thí nghiệm hiệu năng.

Mục trong sơ đồ : 3 (data lake), 5 (Spark batch).
Làm gì          : 1. silver/trips: chuyến hợp lệ, bỏ bản ghi lặp cùng trip_uid, gắn cờ nhiễu GPS
                  2. silver/segments: đoạn 15 giây (explode + lag, KHÔNG lọc bbox trước lag)
                  3. gold: zones, zone_hour_speed, zone_graph_edges, trip_features
                  4. đối soát số đoạn bằng cách đếm độc lập; hồ sơ nhiễu (chỉ đo, chưa lọc)
                  5. Spark SQL Q1-Q3 và EXPLAIN FORMATTED; thí nghiệm broadcast so với sort-merge join
Đọc             : data/bronze (hoặc data/sample/bronze), data/raw/holidays.csv
Ghi             : data/silver, data/gold (Parquet); evidence/spark/*_<mode>.json|.txt
Chạy            : scripts/run_pipeline.sh [--sample|--full] [--skip-bench] [--bench-only]
                  (bên trong: python src/batch/job_spark_batch.py --mode sample|full ...)
Lưu ý           : logic tầng nằm ở src/batch/silver.py, src/batch/gold.py; file này điều phối, đo và viết evidence.
"""
import argparse
import json
import statistics
import time
import urllib.request

from pyspark import StorageLevel
from pyspark.sql import functions as F

from core.common import ROOT, get_spark, load_config, first_row
from batch.gold import build_edges, build_trip_features, build_zone_hour_speed, build_zones
from batch.silver import build_segments, build_trips

EV = ROOT / "evidence" / "spark"

# ---------------------------------------------------------------- Spark SQL
# Giờ cao điểm cho Q1 là giả định (8-9h, 17-19h ngày thường); Q2 cho thấy tốc độ theo giờ để đối chiếu.
Q1 = """
SELECT z.zone_id, z.gx, z.gy, round(z.center_lat, 4) AS center_lat, round(z.center_lon, 4) AS center_lon,
       SUM(h.total_km) / (SUM(h.total_s) / 3600.0) AS speed_kmh,
       SUM(h.n_segments) AS n_segments
FROM zone_hour_speed h JOIN zones z ON h.zone_id = z.zone_id
WHERE h.is_weekend = false AND h.hour IN (8, 9, 17, 18, 19)
GROUP BY z.zone_id, z.gx, z.gy, z.center_lat, z.center_lon
HAVING SUM(h.n_segments) >= {min_seg}
ORDER BY speed_kmh ASC
LIMIT 10
"""
Q2 = """
SELECT hour, is_weekend,
       SUM(total_km) / (SUM(total_s) / 3600.0) AS speed_kmh,
       SUM(n_segments) AS n_segments
FROM zone_hour_speed
GROUP BY hour, is_weekend
ORDER BY is_weekend, hour
"""
Q3 = """
SELECT floor(h.avg_speed_kmh / 5) * 5 AS origin_speed_bin_kmh,
       COUNT(*) AS n_trips,
       SUM(t.duration_min) / SUM(t.straight_km) AS min_per_straight_km,
       AVG(t.duration_min) AS avg_duration_min
FROM trip_features t
JOIN zone_hour_speed h
  ON t.origin_zone = h.zone_id AND t.hour = h.hour AND t.is_weekend = h.is_weekend
WHERE t.split = 'train'
GROUP BY floor(h.avg_speed_kmh / 5) * 5
ORDER BY origin_speed_bin_kmh
"""
# Cùng phép join của Q3, không gom nhóm, để đo riêng chi phí join.
JOIN_BENCH = """
SELECT /*+ {hint}(h) */ t.trip_id, t.duration_min, t.straight_km, h.avg_speed_kmh
FROM trip_features t
JOIN zone_hour_speed h
  ON t.origin_zone = h.zone_id AND t.hour = h.hour AND t.is_weekend = h.is_weekend
"""


def write_json(name, obj):
    EV.mkdir(parents=True, exist_ok=True)
    with open(EV / name, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2, default=str)


def rows(df):
    return [r.asDict() for r in df.collect()]


# ---------------------------------------------------------------- Spark REST
def rest(spark, path):
    sc = spark.sparkContext
    url = f"{sc.uiWebUrl}/api/v1/applications/{sc.applicationId}/{path}"
    with urllib.request.urlopen(url, timeout=30) as r:
        return json.load(r)


def completed_stages(spark):
    return {s["stageId"]: s for s in rest(spark, "stages?status=complete")}


def measure(spark, sql, label):
    """Chạy query bằng noop sink, trả về thời gian và shuffle đọc/ghi từ Spark REST."""
    before = set(completed_stages(spark))
    spark.sparkContext.setJobDescription(label)
    t0 = time.time()
    df = spark.sql(sql)
    df.write.format("noop").mode("overwrite").save()
    secs = time.time() - t0
    time.sleep(3)  # chờ listener cập nhật REST
    new = [s for sid, s in completed_stages(spark).items() if sid not in before]
    final_plan = df._jdf.queryExecution().executedPlan().toString()
    return {
        "label": label,
        "seconds": round(secs, 3),
        "shuffle_read_bytes": sum(s.get("shuffleReadBytes", 0) for s in new),
        "shuffle_write_bytes": sum(s.get("shuffleWriteBytes", 0) for s in new),
        "n_stages": len(new),
        "join_in_final_plan": [j for j in ["BroadcastHashJoin", "SortMergeJoin", "ShuffledHashJoin"] if j in final_plan],
    }


def benchmark(spark, runs=7, warmup=1):
    out = {"broadcast": [], "sort_merge": []}
    warm = {"broadcast": [], "sort_merge": []}
    for i in range(warmup + runs):  # xen kẽ hai cách; các lần warmup đầu không tính vào median
        dst = warm if i < warmup else out
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(10 * 1024 * 1024))
        spark.conf.set("spark.sql.adaptive.autoBroadcastJoinThreshold", str(10 * 1024 * 1024))
        dst["broadcast"].append(measure(spark, JOIN_BENCH.format(hint="BROADCAST"), f"bench-broadcast-{i}"))
        # tắt mọi đường tự chuyển sang broadcast, kể cả của AQE
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
        spark.conf.set("spark.sql.adaptive.autoBroadcastJoinThreshold", "-1")
        dst["sort_merge"].append(measure(spark, JOIN_BENCH.format(hint="MERGE"), f"bench-smj-{i}"))
    spark.conf.unset("spark.sql.autoBroadcastJoinThreshold")
    spark.conf.unset("spark.sql.adaptive.autoBroadcastJoinThreshold")
    summary = {}
    for k, v in out.items():
        summary[k] = {
            "median_seconds": statistics.median(x["seconds"] for x in v),
            "min_seconds": min(x["seconds"] for x in v),
            "max_seconds": max(x["seconds"] for x in v),
            "median_shuffle_read_mb": statistics.median(x["shuffle_read_bytes"] for x in v) / 1e6,
            "median_shuffle_write_mb": statistics.median(x["shuffle_write_bytes"] for x in v) / 1e6,
            "join_types_seen": sorted({j for x in v for j in x["join_in_final_plan"]}),
        }
    return {"runs": out, "warmup_runs_excluded": warm, "summary": summary}


# ---------------------------------------------------------------- hồ sơ nhiễu
def noise_profile(tf, trips):
    """Chỉ ĐO tỉ lệ; các mốc là mốc tham khảo để đo, chưa phải ngưỡng lọc."""
    n = tf.count()
    speed = F.col("straight_km") / (F.col("duration_min") / 60.0)

    def cnt(cond):
        return tf.filter(cond).count()

    q = tf.withColumn("v", speed)
    jumps = tf.groupBy(
        F.when(F.col("n_jumps") == 0, "0").when(F.col("n_jumps") == 1, "1").when(F.col("n_jumps") <= 5, "2-5")
        .otherwise(">5").alias("n_jumps_bucket")).count()
    by_jump = tf.groupBy("has_gps_jump").agg(
        F.count("*").alias("n"),
        F.expr("percentile_approx(duration_min, 0.5)").alias("p50_duration_min"),
        F.expr("percentile_approx(straight_km, 0.5)").alias("p50_straight_km"),
        F.expr("percentile_approx(duration_min / greatest(straight_km, 0.01), 0.5)").alias("p50_min_per_km"),
    )
    return {
        "trips": n,
        "duplicate_trip_id_rows": n - tf.select("trip_id").distinct().count(),
        "duplicate_trip_uid_rows": n - tf.select("trip_uid").distinct().count(),
        "n_jumps_distribution": {r["n_jumps_bucket"]: r["count"] for r in jumps.collect()},
        "jump_vs_no_jump": rows(by_jump),
        "straight_km_quantiles": dict(zip(["p01", "p05", "p50", "p95", "p99"],
                                          tf.approxQuantile("straight_km", [0.01, 0.05, 0.5, 0.95, 0.99], 0.001))),
        "straight_km_below": {str(x): cnt(F.col("straight_km") < x) for x in [0.05, 0.1, 0.2, 0.5]},
        "straight_speed_kmh_quantiles": dict(zip(["p01", "p05", "p50", "p95", "p99"],
                                                 q.approxQuantile("v", [0.01, 0.05, 0.5, 0.95, 0.99], 0.001))),
        "straight_speed_kmh_below": {str(x): q.filter(F.col("v") < x).count() for x in [1, 2, 5]},
        "straight_speed_kmh_above": {str(x): q.filter(F.col("v") > x).count() for x in [80, 100, 150]},
        "duration_min_below": {str(x): cnt(F.col("duration_min") < x) for x in [0.5, 1, 2]},
        "origin_outside_bbox": cnt(~F.col("origin_in_bbox")),
        "dest_outside_bbox": cnt(~F.col("dest_in_bbox")),
        "either_outside_bbox": cnt(~F.col("origin_in_bbox") | ~F.col("dest_in_bbox")),
        "split_counts": {r["split"]: r["count"] for r in tf.groupBy("split").count().collect()},
        "split_counts_with_jump": {r["split"]: r["count"] for r in tf.filter("has_gps_jump").groupBy("split").count().collect()},
    }


# ---------------------------------------------------------------- main
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["sample", "full"], default="sample")
    ap.add_argument("--skip-bench", action="store_true")
    ap.add_argument("--bench-only", action="store_true", help="chỉ chạy thí nghiệm join trên gold đã có")
    args = ap.parse_args()
    m = args.mode
    cfg = load_config()
    spark = get_spark(cfg, f"job-spark-batch-{m}")
    spark.sparkContext.setLogLevel("WARN")
    timings = {}
    t_all = time.time()

    base = ROOT / ("data/sample" if m == "sample" else "data")
    bronze_p = base / "bronze"
    silver_trips_p, silver_seg_p = base / "silver" / "trips", base / "silver" / "segments"
    gold_p = base / "gold"

    if args.bench_only:
        for name in ["trip_features", "zone_hour_speed"]:
            spark.read.parquet(str(gold_p / name)).createOrReplaceTempView(name)
        b = benchmark(spark)
        b["setup"] = {"query": JOIN_BENCH.strip(), "sink": "noop", "runs_per_variant": 7, "warmup_per_variant": 1,
                      "master": spark.sparkContext.master,
                      "left_rows": spark.table("trip_features").count(), "right_rows": spark.table("zone_hour_speed").count(),
                      "shuffle_partitions": cfg["spark"]["shuffle_partitions"], "spark_version": spark.version}
        write_json(f"B_benchmark_{m}.json", b)
        print(json.dumps(b["summary"], indent=2))
        spark.stop()
        return

    # ---- silver
    t0 = time.time()
    build_trips(spark.read.parquet(str(bronze_p)), cfg).write.mode("overwrite").parquet(str(silver_trips_p))
    trips = spark.read.parquet(str(silver_trips_p))
    timings["silver_trips_s"] = round(time.time() - t0, 1)

    t0 = time.time()
    build_segments(trips, cfg).write.mode("overwrite").parquet(str(silver_seg_p))
    timings["silver_segments_s"] = round(time.time() - t0, 1)
    # đề: không cache() toàn bộ; đoạn được dùng lại cho 3 bảng gold nên persist DISK_ONLY
    seg = spark.read.parquet(str(silver_seg_p)).persist(StorageLevel.DISK_ONLY)

    # ---- gold
    t0 = time.time()
    hol = spark.read.option("header", True).csv(str(ROOT / cfg["paths"]["holidays_csv"]))
    gold = {
        "zones": build_zones(seg, cfg),
        "zone_hour_speed": build_zone_hour_speed(seg, cfg),
        "zone_graph_edges": build_edges(seg, cfg),
        "trip_features": build_trip_features(trips, hol, cfg),
    }
    for name, df in gold.items():
        df.write.mode("overwrite").parquet(str(gold_p / name))
        spark.read.parquet(str(gold_p / name)).createOrReplaceTempView(name)
    timings["gold_s"] = round(time.time() - t0, 1)

    # ---- đối soát số đoạn: đếm theo mảng (không shuffle) phải khớp bảng silver
    pairs = first_row(trips.agg(*[F.sum(c).alias(c) for c in ["n_pairs", "n_pairs_out_bbox", "n_pairs_fast_in_bbox"]])).asDict()
    n_seg = seg.count()
    expected = pairs["n_pairs"] - pairs["n_pairs_out_bbox"] - pairs["n_pairs_fast_in_bbox"]
    tf = spark.table("trip_features")
    counts = {
        "mode": m,
        "bronze_valid_rows": spark.read.parquet(str(bronze_p)).filter("reject_reason is null").count(),
        "silver_trips": trips.count(),
        "silver_segments": n_seg,
        "pairs_total": pairs["n_pairs"],
        "pairs_dropped_outside_bbox": pairs["n_pairs_out_bbox"],
        "pairs_dropped_speed_over_limit_in_bbox": pairs["n_pairs_fast_in_bbox"],
        "segments_expected_from_arrays": expected,
        "segments_match": expected == n_seg,
        "trip_uid_unique": trips.select("trip_uid").distinct().count() == trips.count(),
        "trip_id_shared_by_several_trips": trips.count() - trips.select("trip_id").distinct().count(),
        "gold_rows": {k: spark.table(k).count() for k in gold},
        "nulls": {
            "zone_hour_speed": first_row(spark.table("zone_hour_speed").select(
                [F.sum(F.col(c).isNull().cast("int")).alias(c) for c in ["zone_id", "hour", "is_weekend", "total_km", "total_s"]])).asDict(),
            "trip_features": first_row(tf.select(
                [F.sum(F.col(c).isNull().cast("int")).alias(c) for c in
                 ["straight_km", "bearing_deg", "hour", "day_of_week", "is_weekend", "is_holiday", "duration_min", "origin_zone", "dest_zone"]])).asDict(),
        },
        "ranges": {
            "zone_hour_speed.avg_speed_kmh": first_row(spark.table("zone_hour_speed").agg(F.min("avg_speed_kmh"), F.max("avg_speed_kmh")))[:],
            "zone_graph_edges.travel_time_s": first_row(spark.table("zone_graph_edges").agg(F.min("travel_time_s"), F.max("travel_time_s")))[:],
            "trip_features.straight_km": first_row(tf.agg(F.min("straight_km"), F.max("straight_km")))[:],
            "trip_features.bearing_deg": first_row(tf.agg(F.min("bearing_deg"), F.max("bearing_deg")))[:],
        },
        "holidays_trips": tf.filter("is_holiday").count(),
        "timings_s": timings,
    }
    write_json(f"D_counts_{m}.json", counts)
    write_json(f"noise_profile_{m}.json", noise_profile(tf, trips))

    # ---- Spark SQL + EXPLAIN FORMATTED
    min_seg = 1000 if m == "full" else 20
    queries = {"Q1_congested_zones_peak": Q1.format(min_seg=min_seg), "Q2_speed_by_hour": Q2, "Q3_trip_join_origin_zone_hour": Q3}
    plans = []
    for name, q in queries.items():
        t0 = time.time()
        res = rows(spark.sql(q))
        write_json(f"{name}_{m}.json", {"sql": q.strip(), "seconds": round(time.time() - t0, 2), "rows": res})
        plans.append(f"===== {name} =====\n{q.strip()}\n\n" + first_row(spark.sql("EXPLAIN FORMATTED " + q))[0])
    for hint in ["BROADCAST", "MERGE"]:
        if hint == "MERGE":
            spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
        q = JOIN_BENCH.format(hint=hint)
        plans.append(f"===== JOIN_BENCH hint={hint} =====\n{q.strip()}\n\n" + first_row(spark.sql("EXPLAIN FORMATTED " + q))[0])
    spark.conf.unset("spark.sql.autoBroadcastJoinThreshold")
    EV.mkdir(parents=True, exist_ok=True)
    (EV / f"C_query_plan_{m}.txt").write_text("\n\n".join(plans), encoding="utf-8")

    # ---- thí nghiệm broadcast vs sort-merge
    if not args.skip_bench:
        b = benchmark(spark)
        b["setup"] = {
            "query": JOIN_BENCH.strip(), "sink": "noop", "runs_per_variant": 7, "warmup_per_variant": 1,
            "master": spark.sparkContext.master,
            "left_rows": spark.table("trip_features").count(), "right_rows": spark.table("zone_hour_speed").count(),
            "shuffle_partitions": cfg["spark"]["shuffle_partitions"], "spark_version": spark.version,
        }
        write_json(f"B_benchmark_{m}.json", b)

    seg.unpersist()
    timings["total_s"] = round(time.time() - t_all, 1)
    print(json.dumps({"counts": counts, "timings": timings}, indent=2, default=str))
    spark.stop()


if __name__ == "__main__":
    main()
