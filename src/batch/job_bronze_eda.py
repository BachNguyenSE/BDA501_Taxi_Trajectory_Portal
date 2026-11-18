"""Job: tạo tầng BRONZE và thống kê mô tả (EDA) từ train.csv.

Mục trong sơ đồ : 2 (Tải + bronze), 3 (data lake), 4 (holidays.csv được kiểm ở bước sau).
Làm gì          : đọc raw CSV, parse POLYLINE, GIỮ MỌI DÒNG và gắn reject_reason
                  (missing_data > bad_json > too_few_points > too_long); đo số dòng theo lý do,
                  phân bố thời lượng, chuyến theo tháng/giờ (giờ Lisbon), tỉ lệ bước nhảy GPS, điểm ngoài bbox.
Đọc             : data/raw/train.csv
Ghi             : data/bronze (full) hoặc data/sample/bronze (sample), Parquet
                  evidence/bronze/reject_counts_<mode>.json, evidence/eda/eda_<mode>.json
Chạy            : scripts/run_eda.sh [--sample|--full]
                  (bên trong: python src/batch/job_bronze_eda.py --mode sample|full)
Lưu ý           : logic bronze nằm ở src/batch/bronze.py; file này chỉ điều phối và đo.
                  Mọi con số trong evidence là số đo thật của lần chạy.
"""
import argparse
import json
import time

from pyspark.sql import functions as F

from batch.bronze import build_bronze, read_raw
from core.common import ROOT, get_spark, load_config, first_row


def haversine_expr(a, b):
    """Khoảng cách km giữa hai điểm [lon, lat] (biểu thức Spark SQL)."""
    return (
        f"2*6371*asin(sqrt(pow(sin(radians({b}[1]-{a}[1])/2),2)"
        f"+cos(radians({a}[1]))*cos(radians({b}[1]))*pow(sin(radians({b}[0]-{a}[0])/2),2)))"
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["sample", "full"], default="sample")
    args = ap.parse_args()

    cfg = load_config()
    spark = get_spark(cfg, f"job-bronze-eda-{args.mode}")
    spark.sparkContext.setLogLevel("WARN")
    t0 = time.time()
    c, g = cfg["cleaning"], cfg["grid"]["bbox"]
    seed = cfg["project"]["seed"]

    raw = read_raw(spark, ROOT / cfg["paths"]["raw_csv"])
    if args.mode == "sample":
        raw = raw.sample(False, cfg["spark"]["sample_fraction"], seed)
        out = ROOT / "data" / "sample" / "bronze"
    else:
        out = ROOT / cfg["paths"]["bronze"]

    bronze = build_bronze(raw, cfg)
    bronze.write.mode("overwrite").parquet(str(out))
    b = spark.read.parquet(str(out))  # đọc lại từ đĩa để mọi số liệu lấy từ file đã ghi
    b.createOrReplaceTempView("bronze")

    total = b.count()
    reasons = {r["reject_reason"] or "valid": r["n"] for r in b.groupBy("reject_reason").agg(F.count("*").alias("n")).collect()}
    (ROOT / "evidence" / "bronze").mkdir(parents=True, exist_ok=True)
    (ROOT / "evidence" / "eda").mkdir(parents=True, exist_ok=True)
    with open(ROOT / "evidence" / "bronze" / f"reject_counts_{args.mode}.json", "w", encoding="utf-8") as f:
        json.dump({"mode": args.mode, "total_rows": total, "by_reason": reasons,
                   "precedence": "missing_data > bad_json > too_few_points > too_long"}, f, ensure_ascii=False, indent=2)

    valid = b.filter(F.col("reject_reason").isNull())
    n_valid = valid.count()

    # null và khoảng giá trị của các cột chính (toàn bộ dòng bronze)
    nulls = first_row(b.select([F.sum(F.col(x).isNull().cast("int")).alias(x) for x in
                      ["trip_id", "call_type", "taxi_id", "start_epoch", "start_ts_local", "polyline", "duration_s"]])).asDict()
    rng = first_row(b.agg(F.min("start_ts_utc").alias("min_ts_utc"), F.max("start_ts_utc").alias("max_ts_utc"),
                F.min("n_points").alias("min_points"), F.max("n_points").alias("max_points"))).asDict()
    rng = {k: str(v) for k, v in rng.items()}

    dur_q = valid.approxQuantile("duration_s", [0.0, 0.25, 0.5, 0.75, 0.9, 0.99, 1.0], 0.001)
    dur_q = {k: v / 60 for k, v in zip(["min", "p25", "p50", "p75", "p90", "p99", "max"], dur_q)}  # phút

    # chuyến theo tháng và giờ, theo GIỜ ĐỊA PHƯƠNG, trên chuyến hợp lệ
    by_month = {r["m"]: r["n"] for r in valid.groupBy(F.date_format("start_ts_local", "yyyy-MM").alias("m")).count().withColumnRenamed("count", "n").collect()}
    by_hour = {int(r["h"]): r["n"] for r in valid.groupBy(F.hour("start_ts_local").alias("h")).count().withColumnRenamed("count", "n").collect()}

    # chất lượng GPS trên chuyến hợp lệ: tốc độ bước 15 s > max_speed_kmh, điểm ngoài bbox
    steps = f"transform(sequence(1, size(polyline)-1), i -> {haversine_expr('polyline[i-1]', 'polyline[i]')})"
    dt_h = c["gps_interval_s"] / 3600.0
    q = valid.selectExpr(
        f"array_max({steps}) / {dt_h} AS max_kmh",
        f"size(filter(polyline, p -> p[0] < {g['lon_min']} or p[0] > {g['lon_max']} or p[1] < {g['lat_min']} or p[1] > {g['lat_max']})) AS n_out",
        "size(polyline) AS n_pts",
    ).cache()
    qa = first_row(q.agg(
        F.count("*").alias("n"),
        F.sum((F.col("max_kmh") > c["max_speed_kmh"]).cast("int")).alias("trips_with_jump"),
        F.sum("n_out").alias("points_out_bbox"),
        F.sum("n_pts").alias("points_total"),
    )).asDict()

    eda = {
        "mode": args.mode,
        "seconds": round(time.time() - t0, 1),
        "rows_bronze": total,
        "rows_valid": n_valid,
        "reject_counts": reasons,
        "nulls_per_column": nulls,
        "ranges": rng,
        "duration_min_valid": dur_q,
        "trips_by_month_local_valid": dict(sorted(by_month.items())),
        "trips_by_hour_local_valid": dict(sorted(by_hour.items())),
        "gps_quality_valid": {
            "trips": qa["n"],
            "trips_with_jump_over_%dkmh" % c["max_speed_kmh"]: qa["trips_with_jump"],
            "share_trips_with_jump": qa["trips_with_jump"] / qa["n"],
            "points_total": qa["points_total"],
            "points_outside_bbox": qa["points_out_bbox"],
            "share_points_outside_bbox": qa["points_out_bbox"] / qa["points_total"],
        },
    }
    with open(ROOT / "evidence" / "eda" / f"eda_{args.mode}.json", "w", encoding="utf-8") as f:
        json.dump(eda, f, ensure_ascii=False, indent=2, default=str)
    spark.stop()
    print(json.dumps(eda, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
