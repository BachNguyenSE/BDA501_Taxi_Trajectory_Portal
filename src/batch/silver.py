"""Tầng silver (mục 5): chuyến sạch và đoạn 15 giây sạch.

Quy tắc đoạn theo đề:
  - explode POLYLINE thành điểm, window `lag` theo chuyến để ghép hai điểm liên tiếp;
  - KHÔNG lọc bbox trước lag (lọc trước sẽ ghép hai điểm không liền nhau);
  - giữ đoạn khi cả hai đầu trong bbox và tốc độ <= max_speed_kmh;
  - zone của đoạn = zone của trung điểm; giờ = giờ địa phương lúc BẮT ĐẦU đoạn.

Chuyến ghi lặp (cùng trip_uid) chỉ giữ một bản; bronze vẫn giữ mọi dòng.

Cờ nhiễu GPS trên chuyến (đề xuất đã duyệt: gắn cờ, không xóa):
  max_speed_kmh, n_jumps (số bước > max_speed_kmh), has_gps_jump.
"""
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from core.geo import haversine_km, haversine_sql, in_bbox, in_bbox_sql, zone_id


def build_trips(bronze: DataFrame, cfg: dict) -> DataFrame:
    c, b = cfg["cleaning"], cfg["grid"]["bbox"]
    per_h = 3600.0 / c["gps_interval_s"]
    vmax = c["max_speed_kmh"]
    pair_ok = f"({in_bbox_sql('polyline[i-1]', b)} and {in_bbox_sql('polyline[i]', b)})"
    # Cùng trip_uid = cùng TRIP_ID và cùng quỹ đạo: một chuyến bị ghi lặp. Giữ một bản, chọn cố định theo call_type.
    first = Window.partitionBy("trip_uid").orderBy("call_type", "taxi_id")
    valid = (bronze.filter(F.col("reject_reason").isNull())
             .withColumn("_rn", F.row_number().over(first)).filter(F.col("_rn") == 1).drop("_rn"))
    return (
        valid
        .withColumn("step_kmh", F.expr(
            f"transform(sequence(1, size(polyline)-1), i -> {haversine_sql('polyline[i-1]', 'polyline[i]')} * {per_h})"))
        .withColumn("pair_in_bbox", F.expr(f"transform(sequence(1, size(polyline)-1), i -> {pair_ok})"))
        .select(
            "trip_id", "trip_uid", "taxi_id", "call_type", "start_epoch", "start_ts_utc", "start_ts_local",
            "n_points", "duration_s", "polyline",
            F.array_max("step_kmh").alias("max_speed_kmh"),
            F.expr(f"size(filter(step_kmh, s -> s > {vmax}))").alias("n_jumps"),
            # đếm để đối soát số đoạn: tổng cặp, cặp ngoài bbox, cặp trong bbox nhưng quá tốc độ
            (F.col("n_points") - 1).alias("n_pairs"),
            F.expr("size(filter(pair_in_bbox, x -> not x))").alias("n_pairs_out_bbox"),
            F.expr(f"size(filter(sequence(0, size(step_kmh)-1), j -> pair_in_bbox[j] and step_kmh[j] > {vmax}))")
            .alias("n_pairs_fast_in_bbox"),
        )
        .withColumn("has_gps_jump", F.col("n_jumps") > 0)
    )


def build_segments(trips: DataFrame, cfg: dict) -> DataFrame:
    c, grid = cfg["cleaning"], cfg["grid"]
    b, tz, dt = grid["bbox"], cfg["project"]["timezone_local"], c["gps_interval_s"]

    pts = trips.select("trip_uid", "start_epoch", F.posexplode("polyline").alias("seq", "p")).select(
        "trip_uid", "start_epoch", "seq", F.col("p")[0].alias("lon1"), F.col("p")[1].alias("lat1"))
    w = Window.partitionBy("trip_uid").orderBy("seq")  # trip_id gốc không duy nhất
    s = (
        pts.withColumn("lon0", F.lag("lon1").over(w))
        .withColumn("lat0", F.lag("lat1").over(w))
        .filter(F.col("lon0").isNotNull())            # điểm đầu chuyến không có đoạn
        .withColumn("km", haversine_km(F.col("lon0"), F.col("lat0"), F.col("lon1"), F.col("lat1")))
        .withColumn("speed_kmh", F.col("km") * (3600.0 / dt))
    )
    # lọc SAU lag
    s = s.filter(in_bbox(F.col("lon0"), F.col("lat0"), b) & in_bbox(F.col("lon1"), F.col("lat1"), b)
                 & (F.col("speed_kmh") <= c["max_speed_kmh"]))
    seg_local = F.from_utc_timestamp(F.timestamp_seconds(F.col("start_epoch") + (F.col("seq") - 1) * dt), tz)
    dow = F.dayofweek(seg_local)  # 1 = Chủ nhật ... 7 = Thứ bảy
    return s.select(
        "trip_uid", "seq", "lon0", "lat0", "lon1", "lat1", "km", "speed_kmh",
        seg_local.alias("seg_start_local"),
        F.hour(seg_local).alias("hour"),
        dow.isin(1, 7).alias("is_weekend"),
        zone_id((F.col("lon0") + F.col("lon1")) / 2, (F.col("lat0") + F.col("lat1")) / 2, grid).alias("zone_id"),
        zone_id(F.col("lon0"), F.col("lat0"), grid).alias("zone_from"),
        zone_id(F.col("lon1"), F.col("lat1"), grid).alias("zone_to"),
    )
