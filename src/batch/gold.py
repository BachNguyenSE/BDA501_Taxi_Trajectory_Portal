"""Tầng gold (mục 3, 5): zones, zone_hour_speed, zone_graph_edges, trip_features.

Lưu ý leakage: các bảng ở đây tính trên TOÀN BỘ dữ liệu, dùng cho hiển thị và truy vấn.
Bản chỉ từ dữ liệu train (cho baseline, feature đồ thị) làm ở Phase 4.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from core.geo import bearing_deg, haversine_km, in_bbox, zone_center, zone_gx, zone_gy, zone_id


def build_zone_hour_speed(seg: DataFrame, cfg: dict) -> DataFrame:
    dt = cfg["cleaning"]["gps_interval_s"]
    return (
        seg.groupBy("zone_id", "hour", "is_weekend")
        .agg(F.sum("km").alias("total_km"), F.count("*").alias("n_segments"))
        .withColumn("total_s", F.col("n_segments") * dt)  # mỗi đoạn đúng dt giây
        .withColumn("avg_speed_kmh", F.col("total_km") / (F.col("total_s") / 3600.0))  # tổng km / tổng giờ
    )


def build_zones(seg: DataFrame, cfg: dict) -> DataFrame:
    grid = cfg["grid"]
    lon, lat = zone_center(F.col("zone_id"), grid)
    return (
        seg.groupBy("zone_id").agg(F.count("*").alias("n_segments"))
        .select("zone_id", zone_gx(F.col("zone_id"), grid).alias("gx"), zone_gy(F.col("zone_id"), grid).alias("gy"),
                lon.alias("center_lon"), lat.alias("center_lat"), "n_segments")
    )


def build_edges(seg: DataFrame, cfg: dict) -> DataFrame:
    """Cạnh có hướng giữa hai zone LIỀN KỀ (8 hướng) mà đoạn thực sự đi qua."""
    grid, dt = cfg["grid"], cfg["cleaning"]["gps_interval_s"]
    f, t = F.col("zone_from"), F.col("zone_to")
    cheb = F.greatest(F.abs(zone_gx(f, grid) - zone_gx(t, grid)), F.abs(zone_gy(f, grid) - zone_gy(t, grid)))
    e = (
        seg.filter((f != t) & (cheb == 1))
        .groupBy("zone_from", "zone_to")
        .agg(F.count("*").alias("n_transitions"), F.sum("km").alias("total_km"))
        .withColumn("total_s", F.col("n_transitions") * dt)
        .withColumn("avg_speed_kmh", F.col("total_km") / (F.col("total_s") / 3600.0))
    )
    flon, flat = zone_center(F.col("zone_from"), grid)
    tlon, tlat = zone_center(F.col("zone_to"), grid)
    return (
        e.withColumn("center_km", haversine_km(flon, flat, tlon, tlat))
        # thời gian đi từ tâm zone này sang tâm zone kia với tốc độ đo được trên cạnh
        .withColumn("travel_time_s", F.col("center_km") / F.col("avg_speed_kmh") * 3600.0)
    )


def build_trip_features(trips: DataFrame, holidays: DataFrame, cfg: dict) -> DataFrame:
    grid, b = cfg["grid"], cfg["grid"]["bbox"]
    split = cfg["split"]
    o, d = F.col("polyline")[0], F.element_at("polyline", -1)
    t = trips.select(
        "trip_id", "trip_uid", "taxi_id", "start_ts_local", "duration_s", "n_points",
        "max_speed_kmh", "n_jumps", "has_gps_jump",
        o[0].alias("origin_lon"), o[1].alias("origin_lat"), d[0].alias("dest_lon"), d[1].alias("dest_lat"),
    )
    olon, olat, dlon, dlat = (F.col(x) for x in ["origin_lon", "origin_lat", "dest_lon", "dest_lat"])
    dow = F.dayofweek("start_ts_local")  # 1 = Chủ nhật ... 7 = Thứ bảy
    t = (
        t.withColumn("straight_km", haversine_km(olon, olat, dlon, dlat))
        .withColumn("bearing_deg", bearing_deg(olon, olat, dlon, dlat))
        .withColumn("hour", F.hour("start_ts_local"))
        .withColumn("day_of_week", dow)
        .withColumn("is_weekend", dow.isin(1, 7))
        .withColumn("local_date", F.to_date("start_ts_local"))
        .withColumn("origin_in_bbox", in_bbox(olon, olat, b))
        .withColumn("dest_in_bbox", in_bbox(dlon, dlat, b))
        .withColumn("origin_zone", F.when(F.col("origin_in_bbox"), zone_id(olon, olat, grid)))
        .withColumn("dest_zone", F.when(F.col("dest_in_bbox"), zone_id(dlon, dlat, grid)))
        .withColumn("duration_min", F.col("duration_s") / 60.0)
    )
    h = holidays
    if not cfg["holidays"]["include_municipal"]:
        h = h.filter(F.col("scope") == "national")
    h = h.select(F.to_date("date").alias("local_date")).distinct().withColumn("is_holiday", F.lit(True))
    t = t.join(F.broadcast(h), "local_date", "left").fillna({"is_holiday": False})
    ts = F.col("start_ts_local")
    return t.withColumn(
        "split",
        F.when(ts < F.lit(split["train_before"]).cast("timestamp"), "train")
        .when(ts < F.lit("2014-07-01").cast("timestamp"), "test")   # test = 05-06/2014 theo đề
        .otherwise("out_of_range"),                                   # 124 chuyến rơi sang 01/07 giờ Lisbon
    )
