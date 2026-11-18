"""Chuẩn bị dữ liệu cho giao diện: địa điểm có sẵn và chuyến mẫu từ tập test.

Địa điểm: tọa độ XẤP XỈ theo hiểu biết của nhóm (không lấy từ dataset), được kiểm tra bằng dữ liệu:
nằm trong bbox, zone có đủ dữ liệu (>= 1.000 đoạn) và thuộc mạng đường chính của đồ thị.
Chuyến mẫu: chọn ngẫu nhiên (seed 42) trong tập test, chỉ lấy chuyến có điểm đầu và cuối thuộc mạng
chính, không có bước nhảy GPS, đường thẳng >= 1 km. Dùng để copy thử và làm bằng chứng F3.
"""
import json

import pandas as pd

from core.common import ROOT, load_config
from serving.estimate import zone_of
from serving.routing import ZoneGraph

LANDMARKS = [  # (tên, lat, lon) xấp xỉ
    ("Sân bay Porto (nhà ga, Francisco Sá Carneiro)", 41.2378, -8.6707),
    ("Ga São Bento", 41.1456, -8.6105),
    ("Ga Campanhã", 41.1489, -8.5855),
    ("Quảng trường Aliados", 41.1496, -8.6110),
    ("Tháp Clérigos", 41.1458, -8.6145),
    ("Ribeira (bờ sông Douro)", 41.1406, -8.6131),
    ("Casa da Música", 41.1588, -8.6307),
    ("Sân vận động Dragão", 41.1618, -8.5838),
    ("Sân vận động Bessa", 41.1622, -8.6431),
    ("Bệnh viện São João", 41.1820, -8.6017),
    ("Đại học Kỹ thuật FEUP", 41.1780, -8.5980),
    ("Trung tâm thương mại NorteShopping", 41.1820, -8.6550),
    ("Bãi biển Matosinhos", 41.1760, -8.6880),
    ("Foz do Douro", 41.1480, -8.6760),
    ("Cais de Gaia (bờ nam sông)", 41.1370, -8.6130),
]
MIN_ZONE_SEGMENTS = 1000


def main():
    cfg = load_config()
    gold = ROOT / cfg["paths"]["gold"]
    zones = pd.read_parquet(gold / "zones").set_index("zone_id")
    edges = pd.read_parquet(gold / "zone_graph_edges")
    g = ZoneGraph(edges, min_transitions=cfg["routing"]["min_edge_transitions"])
    out_dir = ROOT / "data" / "ui"
    out_dir.mkdir(parents=True, exist_ok=True)

    lm = []
    for name, lat, lon in LANDMARKS:
        z = zone_of(lon, lat, cfg["grid"])
        nseg = int(zones.loc[z, "n_segments"]) if z is not None and z in zones.index else 0
        ok = z is not None and nseg >= MIN_ZONE_SEGMENTS and g.in_main(z)
        lm.append({"name": name, "lat": lat, "lon": lon, "zone_id": z, "zone_segments": nseg, "ok": bool(ok)})
    (ROOT / cfg["ui"]["landmarks"]).write_text(json.dumps(lm, ensure_ascii=False, indent=2), encoding="utf-8")

    tf = pd.read_parquet(gold / "trip_features")
    t = tf[(tf["split"] == "test") & ~tf["has_gps_jump"] & (tf["straight_km"] >= 1)
           & tf["origin_zone"].notna() & tf["dest_zone"].notna()]
    t = t[t["origin_zone"].astype(int).map(g.in_main) & t["dest_zone"].astype(int).map(g.in_main)]
    t = t.sample(n=20, random_state=cfg["project"]["seed"]).sort_values("start_ts_local")
    ts = pd.to_datetime(t["start_ts_local"])
    if ts.dt.tz is not None:
        ts = ts.dt.tz_convert("UTC").dt.tz_localize(None)
    trips = [{"trip_uid": r["trip_uid"], "start_local": s.strftime("%Y-%m-%d %H:%M:%S"),
              "origin": [round(r["origin_lat"], 6), round(r["origin_lon"], 6)],
              "destination": [round(r["dest_lat"], 6), round(r["dest_lon"], 6)],
              "straight_km": round(r["straight_km"], 3), "actual_duration_min": round(r["duration_min"], 2)}
             for (_, r), s in zip(t.iterrows(), ts)]
    (ROOT / cfg["ui"]["sample_trips"]).write_text(json.dumps(trips, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"landmarks": lm, "sample_trips": len(trips)}, ensure_ascii=False, indent=1))


if __name__ == "__main__":
    main()
