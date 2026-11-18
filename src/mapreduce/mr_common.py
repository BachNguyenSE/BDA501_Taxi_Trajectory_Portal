"""Logic dùng chung cho mapper (mục 6). Chỉ dùng thư viện chuẩn: image Hadoop không có pyyaml hay pyspark.

Tham số đọc từ mr_conf.json (sinh từ conf/pipeline.yaml bằng export_mr_conf.py), gửi kèm job qua -files.
Logic phải GIỐNG Spark (src/batch/bronze.py, src/batch/silver.py) để đối chiếu từng key:
  - reject_reason: missing_data > bad_json > too_few_points > too_long
  - ghép hai điểm liên tiếp trên polyline GỐC, không lọc bbox trước
  - giữ đoạn khi cả hai đầu trong bbox và tốc độ <= max_speed_kmh
  - zone = zone của trung điểm; giờ = giờ địa phương lúc bắt đầu đoạn
"""
import json
import math
import os
from datetime import datetime
from zoneinfo import ZoneInfo

EARTH_R_KM = 6371.0
# Thứ tự cột của train.csv
I_TRIP_ID, I_TIMESTAMP, I_MISSING, I_POLYLINE = 0, 5, 7, 8


def load_conf(path=None):
    path = path or os.environ.get("MR_CONF", "mr_conf.json")  # -files đặt symlink ở thư mục làm việc
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def haversine_km(lon1, lat1, lon2, lat2):
    # cùng thứ tự phép tính với src/core/geo.py
    a = (math.sin((math.radians(lat2) - math.radians(lat1)) / 2) ** 2
         + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2))
         * math.sin((math.radians(lon2) - math.radians(lon1)) / 2) ** 2)
    return 2 * EARTH_R_KM * math.asin(math.sqrt(a))


def in_bbox(lon, lat, b):
    return b["lon_min"] <= lon <= b["lon_max"] and b["lat_min"] <= lat <= b["lat_max"]


def zone_id(lon, lat, conf):
    b, c = conf["bbox"], conf["cell_deg"]
    gx = min(math.floor((lon - b["lon_min"]) / c), conf["n_cols"] - 1)
    gy = min(math.floor((lat - b["lat_min"]) / c), conf["n_rows"] - 1)
    return gy * conf["n_cols"] + gx


def classify(row, conf):
    """Trả về (reject_reason hoặc None, polyline hoặc None)."""
    if row[I_MISSING] == "True":
        return "missing_data", None
    try:
        pts = json.loads(row[I_POLYLINE])
        if not isinstance(pts, list):
            raise ValueError
    except (ValueError, TypeError):
        return "bad_json", None
    n = len(pts)
    if n < conf["min_points"]:
        return "too_few_points", None
    if (n - 1) * conf["gps_interval_s"] > conf["max_trip_min"] * 60:
        return "too_long", None
    return None, pts


def segments(start_epoch, pts, conf, tz):
    """Sinh (zone_id, hour, km) cho các đoạn giữ lại, kèm đếm lý do bỏ đoạn."""
    dt, vmax, b = conf["gps_interval_s"], conf["max_speed_kmh"], conf["bbox"]
    out, n_out_bbox, n_fast = [], 0, 0
    for i in range(1, len(pts)):
        (lon0, lat0), (lon1, lat1) = pts[i - 1], pts[i]
        km = haversine_km(lon0, lat0, lon1, lat1)
        if not (in_bbox(lon0, lat0, b) and in_bbox(lon1, lat1, b)):
            n_out_bbox += 1
            continue
        if km * (3600.0 / dt) > vmax:
            n_fast += 1
            continue
        hour = datetime.fromtimestamp(start_epoch + (i - 1) * dt, tz).hour
        out.append((zone_id((lon0 + lon1) / 2, (lat0 + lat1) / 2, conf), hour, km))
    return out, n_out_bbox, n_fast


def get_tz(conf):
    return ZoneInfo(conf["timezone_local"])
