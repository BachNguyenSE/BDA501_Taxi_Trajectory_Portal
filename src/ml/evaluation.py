"""Hàm dùng chung cho Phase 4 (mục 7): đánh giá, baseline tốc độ lịch sử, kiểm tra leakage."""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from core.common import first_row

# Chỉ thông tin có TRƯỚC khi xuất phát (docs/DATA_LEAKAGE.md mục 1)
ALLOWED_FEATURES = {"straight_km", "bearing_deg", "hour", "day_of_week", "is_weekend", "is_holiday"}
# Tính từ chính quỹ đạo, tức từ nhãn: cấm làm feature
FORBIDDEN_FEATURES = {"n_points", "duration_s", "duration_min", "max_speed_kmh", "n_jumps", "has_gps_jump"}


def check_features(features):
    bad = set(features) - ALLOWED_FEATURES
    if bad or set(features) & FORBIDDEN_FEATURES:
        raise ValueError(f"feature không được phép: {sorted(bad | (set(features) & FORBIDDEN_FEATURES))}")
    return True


def evaluate(df: DataFrame, pred="pred", label="duration_min"):
    """MAE, RMSE, R², signed error (= trung bình dự đoán - thực tế; dương là đoán dài hơn thực tế)."""
    err = F.col(pred) - F.col(label)
    r = first_row(df.agg(
        F.count("*").alias("n"),
        F.avg(F.abs(err)).alias("mae"),
        F.sqrt(F.avg(err * err)).alias("rmse"),
        F.avg(err).alias("signed_error"),
        F.avg(label).alias("mean_label"),
        F.sum(err * err).alias("sse"),
    )).asDict()
    sst = first_row(df.agg(F.sum(F.pow(F.col(label) - F.lit(r["mean_label"]), 2))))[0]
    r["r2"] = 1.0 - r.pop("sse") / sst
    return r


def historical_speed_tables(seg: DataFrame, cutoff: str, dt: int, min_segments: int):
    """Tốc độ lịch sử zone × giờ × cuối tuần, CHỈ từ đoạn bắt đầu trước mốc chia (giờ địa phương).

    Ô có dưới min_segments đoạn bị bỏ, khi đó baseline lùi về tốc độ toàn thành phố theo giờ.
    Tốc độ = tổng km / tổng giờ (gồm cả đoạn dừng), không lấy trung bình của trung bình.
    """
    s = seg.filter(F.col("seg_start_local") < F.lit(cutoff).cast("timestamp"))
    zone = (s.groupBy("zone_id", "hour", "is_weekend").agg(F.sum("km").alias("km"), F.count("*").alias("n"))
            .filter(F.col("n") >= min_segments)
            .select(F.col("zone_id").alias("origin_zone"), "hour", "is_weekend",
                    (F.col("km") / (F.col("n") * dt / 3600.0)).alias("zone_speed")))
    city = (s.groupBy("hour", "is_weekend").agg(F.sum("km").alias("km"), F.count("*").alias("n"))
            .select("hour", "is_weekend", (F.col("km") / (F.col("n") * dt / 3600.0)).alias("city_speed")))
    return s, zone, city


def historical_speed_baseline(trips: DataFrame, zone: DataFrame, city: DataFrame):
    """Baseline đề yêu cầu: khoảng cách (đường thẳng A-B) / tốc độ lịch sử tại zone xuất phát."""
    t = (trips.join(F.broadcast(zone), ["origin_zone", "hour", "is_weekend"], "left")
         .join(F.broadcast(city), ["hour", "is_weekend"], "left"))
    v = F.when(F.col("zone_speed") > 0, F.col("zone_speed")).otherwise(F.col("city_speed"))
    return (t.withColumn("used_zone_speed", F.coalesce(F.col("zone_speed") > 0, F.lit(False)))
            .withColumn("pred", F.col("straight_km") / v * 60.0))
