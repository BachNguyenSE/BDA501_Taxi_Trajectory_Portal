"""Hàm estimate (mục 10): đầu vào A, B, thời điểm xuất phát; đầu ra đúng 4 trường của đề:
    predicted_duration_min, route, congested_zones, live
và thêm predicted_interval_min (khoảng 80%, mở rộng ngoài đề, đã duyệt).

- ETA: model Spark GBT đã đánh giá ở Phase 4 (models/features.json chỉ ra model phục vụ).
  Feature tính bằng Python với CÙNG công thức như src/core/geo.py và src/batch/gold.py
  (tests/test_estimate.py so khớp với bảng gold).
- Tuyến: Dijkstra trên đồ thị zone (đọc từ MongoDB), thời gian cạnh theo tốc độ zone × giờ,
  bỏ cạnh dưới routing.min_edge_transitions lượt.
- Mức tắc LỊCH SỬ của zone trên tuyến (thông thoáng / đông / tắc): tốc độ zone tại giờ đó / tốc độ cả ngày của chính zone.
- Live: chỉ ĐÁNH DẤU, không đổi ETA. Cửa sổ live đã đóng gần nhất, so với tốc độ lịch sử
  cùng zone, cùng giờ; tỉ lệ < mongo.live_slow_ratio thì là "chậm hơn bình thường".
"""
import csv
import json
import math
from datetime import datetime

import numpy as np
import pandas as pd

from core.common import ROOT, load_config
from ml.interval import lookup
from serving.routing import ZoneGraph, hourly_edge_times

EARTH_R_KM = 6371.0


# ---------------------------------------------------------------- feature (giống src/core/geo.py, src/batch/gold.py)
def haversine_km(lon1, lat1, lon2, lat2):
    a = (math.sin((math.radians(lat2) - math.radians(lat1)) / 2) ** 2
         + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2))
         * math.sin((math.radians(lon2) - math.radians(lon1)) / 2) ** 2)
    return 2 * EARTH_R_KM * math.asin(math.sqrt(a))


def bearing_deg(lon1, lat1, lon2, lat2):
    p1, p2, dl = math.radians(lat1), math.radians(lat2), math.radians(lon2 - lon1)
    y = math.sin(dl) * math.cos(p2)
    x = math.cos(p1) * math.sin(p2) - math.sin(p1) * math.cos(p2) * math.cos(dl)
    return (math.degrees(math.atan2(y, x)) + 360) % 360.0


def spark_dayofweek(d: datetime):
    """Như F.dayofweek của Spark: 1 = Chủ nhật ... 7 = Thứ bảy."""
    return d.isoweekday() % 7 + 1


def zone_of(lon, lat, grid):
    b, c = grid["bbox"], grid["cell_deg"]
    if not (b["lon_min"] <= lon <= b["lon_max"] and b["lat_min"] <= lat <= b["lat_max"]):
        return None
    gx = min(math.floor((lon - b["lon_min"]) / c), grid["n_cols"] - 1)
    gy = min(math.floor((lat - b["lat_min"]) / c), grid["n_rows"] - 1)
    return gy * grid["n_cols"] + gx


def load_holidays(cfg):
    with open(ROOT / cfg["paths"]["holidays_csv"], encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    if not cfg["holidays"]["include_municipal"]:
        rows = [r for r in rows if r["scope"] == "national"]
    return {r["date"] for r in rows}


def make_features(origin, destination, dep_local: datetime, holidays):
    """origin, destination = (lat, lon). dep_local = giờ Lisbon, không kèm múi giờ."""
    (olat, olon), (dlat, dlon) = origin, destination
    dow = spark_dayofweek(dep_local)
    return {
        "straight_km": haversine_km(olon, olat, dlon, dlat),
        "bearing_deg": bearing_deg(olon, olat, dlon, dlat),
        "hour": dep_local.hour,
        "day_of_week": dow,
        "is_weekend": dow in (1, 7),
        "is_holiday": dep_local.strftime("%Y-%m-%d") in holidays,
    }


# ---------------------------------------------------------------- dịch vụ
class Estimator:
    def __init__(self, cfg=None, spark=None, db=None):
        from pymongo import MongoClient

        self.cfg = cfg or load_config()
        mc = self.cfg["mongo"]
        self.db = db if db is not None else MongoClient(mc["uri"], serverSelectionTimeoutMS=10000)[mc["database"]]
        self.meta = json.loads((ROOT / self.cfg["paths"]["models"] / "features.json").read_text(encoding="utf-8"))
        self.holidays = load_holidays(self.cfg)
        ip = ROOT / self.cfg["paths"]["models"] / "interval.json"   # mở rộng ngoài đề: khoảng 80%
        self.interval = json.loads(ip.read_text(encoding="utf-8")) if ip.exists() else None
        self.spark = spark
        self._model = None
        self.zones = pd.DataFrame(list(self.db.zones.find({}, {"_id": 0}))).set_index("zone_id")
        self.zhs = pd.DataFrame(list(self.db.zone_hour_speed.find({}, {"_id": 0})))
        self.edges = pd.DataFrame(list(self.db.zone_graph_edges.find({}, {"_id": 0})))
        allday = self.zhs.groupby("zone_id")[["total_km", "total_s"]].sum()
        self.allday_speed = allday["total_km"] / (allday["total_s"] / 3600.0)
        self._graphs = {}

    # ---- model
    def model(self):
        if self._model is None:
            from pyspark.ml.regression import GBTRegressionModel
            from core.common import get_spark
            if self.spark is None:
                self.spark = get_spark(self.cfg, "serving", driver_memory="1g")  # chỉ dự đoán từng dòng
                self.spark.sparkContext.setLogLevel("ERROR")
            self._model = GBTRegressionModel.load(str(ROOT / self.cfg["paths"]["models"] / self.meta["serving_model"]))
        return self._model

    def predict(self, feats: dict) -> float:
        from pyspark.ml.linalg import Vectors
        vec = Vectors.dense([float(feats[f]) for f in self.meta["features"]])  # đúng thứ tự trong features.json
        df = self.spark_df([(vec,)])
        model = self.model()
        row = model.transform(df).first()
        if row is None:
            raise ValueError("model không trả về dự đoán")
        return float(row[model.getPredictionCol()])

    def spark_df(self, rows):
        self.model()
        return self.spark.createDataFrame(rows, ["features"])

    # ---- đồ thị theo giờ
    def graph(self, hour, is_weekend):
        k = (hour, is_weekend)
        if k not in self._graphs:
            r = self.cfg["routing"]
            times, _ = hourly_edge_times(self.edges, self.zhs, hour, is_weekend, r["min_zone_hour_segments"])
            self._graphs[k] = ZoneGraph(self.edges, min_transitions=r["min_edge_transitions"], times=times)
        return self._graphs[k]

    def snap(self, zone_id, lat, lon, g):
        """Zone không có trong mạng chính (ngoài bbox, vùng rìa) thì lấy zone gần nhất trong mạng chính."""
        if zone_id is not None and g.in_main(zone_id):
            return zone_id, False
        main = [int(z) for z, c in zip(g.zone_ids, g.component) if c == g.main_component]
        cz = self.zones.loc[main]
        d = (cz["center_lat"] - lat) ** 2 + ((cz["center_lon"] - lon) * math.cos(math.radians(lat))) ** 2
        return int(d.idxmin()), True

    def congestion(self, zone_id, hour, is_weekend):
        u = self.cfg["ui"]
        row = self.zhs[(self.zhs["zone_id"] == zone_id) & (self.zhs["hour"] == hour) & (self.zhs["is_weekend"] == is_weekend)]
        if row.empty or int(row["n_segments"].iloc[0]) < self.cfg["routing"]["min_zone_hour_segments"]:
            return {"level": "không đủ dữ liệu", "speed_kmh": None, "ratio_vs_all_day": None}
        v = float(row["total_km"].iloc[0] / (row["total_s"].iloc[0] / 3600.0))
        ratio = v / float(self.allday_speed[zone_id])
        level = "tắc" if ratio < u["congestion_ratio_jam"] else "đông" if ratio < u["congestion_ratio_slow"] else "thông thoáng"
        return {"level": level, "speed_kmh": round(v, 2), "ratio_vs_all_day": round(ratio, 3)}

    def live(self, zones_on_route):
        """Cửa sổ live đã đóng gần nhất; Phase 6 ghi collection zone_speed_live."""
        latest = self.db.zone_speed_live.find_one({}, sort=[("window_end", -1)])
        if latest is None:
            return {"available": False, "reason": "chưa có dữ liệu live (zone_speed_live trống)", "slow_zones": []}
        we = latest["window_end"]
        docs = {d["zone_id"]: d for d in self.db.zone_speed_live.find({"window_end": we, "zone_id": {"$in": zones_on_route}})}
        ws = docs[next(iter(docs))]["window_start"] if docs else latest["window_start"]
        h, wk = ws.hour, spark_dayofweek(ws) in (1, 7)  # giờ Lisbon do Phase 6 ghi
        slow = []
        for z, d in docs.items():
            hist = self.zhs[(self.zhs["zone_id"] == z) & (self.zhs["hour"] == h) & (self.zhs["is_weekend"] == wk)]
            if hist.empty or d.get("total_s", 0) <= 0:
                continue
            hv = float(hist["total_km"].iloc[0] / (hist["total_s"].iloc[0] / 3600.0))
            lv = float(d["total_km"] / (d["total_s"] / 3600.0))
            if hv > 0 and lv / hv < self.cfg["mongo"]["live_slow_ratio"]:
                slow.append({"zone_id": int(z), "live_kmh": round(lv, 2), "hist_kmh": round(hv, 2), "ratio": round(lv / hv, 3)})
        return {"available": True, "window_start": str(ws), "window_end": str(we),
                "zones_with_live_data": len(docs), "slow_zones": slow}

    # ---- API chính
    def estimate(self, origin, destination, departure_time: datetime):
        grid = self.cfg["grid"]
        feats = make_features(origin, destination, departure_time, self.holidays)
        eta = self.predict(feats)
        g = self.graph(feats["hour"], feats["is_weekend"])
        oz, o_snap = self.snap(zone_of(origin[1], origin[0], grid), origin[0], origin[1], g)
        dz, d_snap = self.snap(zone_of(destination[1], destination[0], grid), destination[0], destination[1], g)
        r = g.route(oz, dz)
        route = []
        for z in (r.zones if r else []):
            zc = self.zones.loc[z]
            route.append({"zone_id": int(z), "gx": int(zc["gx"]), "gy": int(zc["gy"]),
                          "center_lat": float(zc["center_lat"]), "center_lon": float(zc["center_lon"]),
                          **self.congestion(z, feats["hour"], feats["is_weekend"])})
        return {
            "predicted_duration_min": round(eta, 2),
            # mở rộng ngoài đề: khoảng mà khoảng 80% chuyến tương tự rơi vào (độ bao phủ đo trên test: evidence/ml/interval_validation.md)
            "predicted_interval_min": list(lookup(self.interval, eta)) if self.interval else None,
            "route": route,
            "congested_zones": [z["zone_id"] for z in route if z["level"] == "tắc"],
            "live": self.live([z["zone_id"] for z in route]),
            "debug": {"features": feats, "origin_zone": oz, "dest_zone": dz, "origin_snapped": o_snap,
                      "dest_snapped": d_snap, "graph_route_time_min": round(r.time_s / 60, 2) if r else None,
                      "model": self.meta["serving_model"]},
        }
