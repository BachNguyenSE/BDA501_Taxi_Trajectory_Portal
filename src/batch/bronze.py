"""Tầng bronze (mục 2): parse POLYLINE, GIỮ MỌI DÒNG, gắn reject_reason.

Thứ tự ưu tiên khi một dòng vi phạm nhiều điều kiện (mỗi dòng chỉ có một lý do):
    missing_data > bad_json > too_few_points > too_long
Khóa: trip_uid (TRIP_ID gốc có trùng giữa các chuyến khác nhau).
Nhãn: duration_s = (số điểm - 1) * gps_interval_s.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, DoubleType

POLYLINE_SCHEMA = ArrayType(ArrayType(DoubleType()))


def read_raw(spark, path) -> DataFrame:
    return (
        spark.read.option("header", True)
        .option("quote", '"')
        .option("escape", '"')
        .option("multiLine", False)
        .csv(str(path))  # mọi cột là string, không infer schema
    )


def build_bronze(raw: DataFrame, cfg: dict) -> DataFrame:
    c = cfg["cleaning"]
    tz = cfg["project"]["timezone_local"]
    interval = c["gps_interval_s"]

    poly = F.from_json(F.col("POLYLINE"), POLYLINE_SCHEMA)  # JSON hỏng -> null

    df = raw.select(
        F.col("TRIP_ID").alias("trip_id"),
        # TRIP_ID không duy nhất (cùng xe, cùng giây xuất phát): thêm khóa duy nhất theo nội dung polyline
        F.concat_ws("-", F.col("TRIP_ID"), F.substring(F.md5(F.col("POLYLINE")), 1, 12)).alias("trip_uid"),
        F.col("CALL_TYPE").alias("call_type"),
        F.col("TAXI_ID").cast("long").alias("taxi_id"),
        F.col("TIMESTAMP").cast("long").alias("start_epoch"),
        (F.col("MISSING_DATA") == "True").alias("missing_data"),
        poly.alias("polyline"),
    )
    df = (
        df.withColumn("n_points", n_points_of(df))
        .withColumn("duration_s", F.when(F.col("n_points") >= c["min_points"], (F.col("n_points") - 1) * interval))
        .withColumn("start_ts_utc", F.timestamp_seconds("start_epoch"))
        .withColumn("start_ts_local", F.from_utc_timestamp("start_ts_utc", tz))
    )
    reason = (
        F.when(F.col("missing_data"), "missing_data")
        .when(F.col("polyline").isNull(), "bad_json")
        .when(F.col("n_points") < c["min_points"], "too_few_points")
        .when(F.col("duration_s") > c["max_trip_min"] * 60, "too_long")
    )
    return df.withColumn("reject_reason", reason).select(
        "trip_id", "trip_uid", "call_type", "taxi_id", "start_epoch", "start_ts_utc", "start_ts_local",
        "missing_data", "n_points", "duration_s", "reject_reason", "polyline",
    )


def n_points_of(df: DataFrame):
    return F.when(F.col("polyline").isNull(), F.lit(0)).otherwise(F.size("polyline"))
