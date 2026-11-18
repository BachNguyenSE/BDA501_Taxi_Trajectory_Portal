"""Spark Structured Streaming (mục 14): 2 query trên cùng topic Kafka.

Quality: đếm message và JSON hỏng theo micro-batch, ghi stream_quality.
Live   : parse -> withWatermark(event_time, 10 phút) -> dropDuplicatesWithinWatermark(trip_id, seq)
         -> lọc bbox và tốc độ -> window(15 phút, trượt 5 phút) × zone -> foreachBatch upsert MongoDB
         (_id = zone|window_start), chế độ append: chỉ ghi cửa sổ ĐÃ ĐÓNG (watermark đã vượt qua).
Trigger 5 giây, có checkpoint và maxOffsetsPerTrigger. Chỉ ghi collection streaming, không đụng bảng batch.
Job tự dừng khi producer đã xong và cả hai query không còn dữ liệu mới; ghi số liệu từ progress vào evidence.
"""
import json
import time
from datetime import timezone
from zoneinfo import ZoneInfo


from pymongo import DESCENDING, ASCENDING, MongoClient, ReplaceOne  # noqa: E402
from pyspark.sql import DataFrame, SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.types import DoubleType, IntegerType, LongType, StringType, StructField, StructType  # noqa: E402

from core.common import ROOT, load_config  # noqa: E402
from core.geo import haversine_km, in_bbox, zone_id  # noqa: E402

SCHEMA = StructType([
    StructField("trip_id", StringType()), StructField("taxi_id", LongType()), StructField("seq", IntegerType()),
    StructField("event_time", LongType()), StructField("lon", DoubleType()), StructField("lat", DoubleType()),
    StructField("prev_lon", DoubleType()), StructField("prev_lat", DoubleType()),
])
REQUIRED = ["trip_id", "seq", "event_time", "lon", "lat", "prev_lon", "prev_lat"]


def parse(values: DataFrame) -> DataFrame:
    """values có cột json (string). Thêm cột m (struct) và is_valid (JSON đọc được, đủ trường bắt buộc)."""
    m = F.from_json(F.col("json"), SCHEMA)
    ok = F.lit(True)
    for c in REQUIRED:
        ok = ok & m[c].isNotNull()
    return values.withColumn("m", m).withColumn("is_valid", F.coalesce(ok, F.lit(False)))


def clean_segments(valid: DataFrame, cfg) -> DataFrame:
    """Cùng quy tắc với batch: hai đầu trong bbox, tốc độ <= max_speed_kmh, zone của trung điểm."""
    b, c, grid = cfg["grid"]["bbox"], cfg["cleaning"], cfg["grid"]
    km = haversine_km(F.col("prev_lon"), F.col("prev_lat"), F.col("lon"), F.col("lat"))
    return (valid.withColumn("km", km)
            .filter(in_bbox(F.col("prev_lon"), F.col("prev_lat"), b) & in_bbox(F.col("lon"), F.col("lat"), b)
                    & (F.col("km") * (3600.0 / c["gps_interval_s"]) <= c["max_speed_kmh"]))
            .withColumn("zone_id", zone_id((F.col("prev_lon") + F.col("lon")) / 2, (F.col("prev_lat") + F.col("lat")) / 2, grid)))


def main():
    cfg = load_config()
    st, mc = cfg["streaming"], cfg["mongo"]
    sp, kc = st["spark"], st["kafka"]
    tz = ZoneInfo(cfg["project"]["timezone_local"])
    dt = cfg["cleaning"]["gps_interval_s"]
    ck = ROOT / cfg["paths"]["checkpoints"] / "stream"
    run_dir = ROOT / "data" / "stream"
    run_dir.mkdir(parents=True, exist_ok=True)
    db = MongoClient(mc["uri"], serverSelectionTimeoutMS=10000)[mc["database"]]
    db.zone_speed_live.create_index([("window_end", DESCENDING), ("zone_id", ASCENDING)], name="window_end_zone")

    spark = (SparkSession.builder.master("local[*]").appName("phase6-streaming")
             .config("spark.driver.memory", cfg["spark"]["driver_memory"])
             .config("spark.sql.shuffle.partitions", "6")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1")
             .config("spark.jars.ivy", str(ROOT / ".ivy2"))
             .config("spark.sql.streaming.numRecentProgressUpdates", "10000")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    src = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", kc["bootstrap"]).option("subscribe", kc["topic"])
           .option("startingOffsets", "earliest").option("maxOffsetsPerTrigger", sp["max_offsets_per_trigger"])
           .load())
    parsed = parse(src.select(F.col("value").cast("string").alias("json")))

    # ---- query 1: chất lượng
    def write_quality(df, batch_id):
        r = df.agg(F.count("*").alias("n"), F.sum((~F.col("is_valid")).cast("int")).alias("bad")).first()
        n, bad = (r["n"], r["bad"] or 0) if r else (0, 0)
        if n:
            db.stream_quality.replace_one({"_id": f"quality|{batch_id}"},
                                          {"batch_id": batch_id, "n_messages": n, "n_corrupt_json": bad,
                                           "processed_at": time.time()}, upsert=True)

    q_quality = (parsed.writeStream.queryName("quality").foreachBatch(write_quality)
                 .option("checkpointLocation", str(ck / "quality")).trigger(processingTime=sp["trigger"]).start())

    # ---- query 2: live
    valid = (parsed.filter("is_valid").select("m.*").withColumn("event_ts", F.timestamp_seconds("event_time")))
    agg = (clean_segments(valid.withWatermark("event_ts", sp["watermark"])
                          .dropDuplicatesWithinWatermark(sp["dedup_keys"]), cfg)
           .groupBy(F.window("event_ts", sp["window"], sp["slide"]), "zone_id")
           .agg(F.sum("km").alias("total_km"), F.count("*").alias("n_segments")))

    def write_live(df, batch_id):
        ops = []
        for r in df.collect():
            ws_utc = r["window"]["start"].replace(tzinfo=timezone.utc)
            we_utc = r["window"]["end"].replace(tzinfo=timezone.utc)
            ws, we = ws_utc.astimezone(tz).replace(tzinfo=None), we_utc.astimezone(tz).replace(tzinfo=None)
            doc = {"zone_id": int(r["zone_id"]), "window_start": ws, "window_end": we,   # giờ Lisbon
                   "window_start_utc": ws_utc.replace(tzinfo=None), "window_end_utc": we_utc.replace(tzinfo=None),
                   "total_km": float(r["total_km"]), "n_segments": int(r["n_segments"]),
                   "total_s": int(r["n_segments"]) * dt, "batch_id": batch_id}
            ops.append(ReplaceOne({"_id": f"{doc['zone_id']}|{ws.isoformat()}"}, doc, upsert=True))
        if ops:
            db.zone_speed_live.bulk_write(ops, ordered=False)

    q_live = (agg.writeStream.queryName("live").outputMode("append").foreachBatch(write_live)
              .option("checkpointLocation", str(ck / "live")).trigger(processingTime=sp["trigger"]).start())

    (run_dir / "job_ready.json").write_text(json.dumps({"ready": time.time()}))
    print("STREAM_JOB_READY", flush=True)

    # ---- chờ producer xong và hai query hết dữ liệu mới
    idle, t_start = 0, time.time()
    while True:
        time.sleep(5)
        done = (run_dir / "producer_done.json").exists()
        lp = [q.lastProgress for q in (q_quality, q_live)]
        no_input = all(p is not None and p["numInputRows"] == 0 for p in lp)
        idle = idle + 1 if (done and no_input) else 0
        if idle >= 6 or time.time() - t_start > 1800:
            break

    def summarize(q):
        prog = q.recentProgress
        ops = {}
        for p in prog:
            for o in p.get("stateOperators", []):
                d = ops.setdefault(o["operatorName"], {"numRowsDroppedByWatermark": 0, "customMetrics": {}})
                d["numRowsDroppedByWatermark"] += o.get("numRowsDroppedByWatermark", 0)
                for k, v in (o.get("customMetrics") or {}).items():
                    if k in ("numDroppedDuplicateRows",):
                        d["customMetrics"][k] = d["customMetrics"].get(k, 0) + v
        return {"batches": len(prog), "numInputRows": sum(p["numInputRows"] for p in prog),
                "stateOperators": ops,
                "last_watermark": (prog[-1].get("eventTime") or {}).get("watermark") if prog else None,
                "batch_duration_ms_p50": sorted(p["batchDuration"] for p in prog)[len(prog) // 2] if prog else None}

    metrics = {"quality": summarize(q_quality), "live": summarize(q_live), "ran_seconds": round(time.time() - t_start, 1),
               "config": {k: sp[k] for k in ["trigger", "watermark", "window", "slide", "max_offsets_per_trigger", "dedup_keys"]}}
    q_quality.stop()
    q_live.stop()
    ev = ROOT / "evidence" / "streaming"
    ev.mkdir(parents=True, exist_ok=True)
    (ev / "spark_stream_metrics.json").write_text(json.dumps(metrics, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    print("STREAM_JOB_DONE", json.dumps(metrics, default=str), flush=True)
    spark.stop()


if __name__ == "__main__":
    main()
