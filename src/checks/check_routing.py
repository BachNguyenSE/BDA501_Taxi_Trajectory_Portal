"""Đo ảnh hưởng của bộ lọc cạnh ít lượt lên đồ thị zone và tuyến của chuyến thật.

Chạy: python src/checks/check_routing.py   ->  evidence/spark/routing_check.json
Mẫu: 3.000 chuyến tập test (seed 42) có điểm đầu và cuối trong bbox.
Lưu ý: đồ thị ở đây là bảng gold trên TOÀN BỘ dữ liệu (dùng cho tìm đường trên giao diện),
nên phép so tuyến với thời gian thực tế chỉ để kiểm tra hợp lý, KHÔNG phải đánh giá model.
"""
import json
import time

import numpy as np
import pandas as pd

from core.common import ROOT, load_config
from serving.routing import ZoneGraph, hourly_edge_times


def main():
    cfg = load_config()
    gold = ROOT / cfg["paths"]["gold"]
    edges = pd.read_parquet(gold / "zone_graph_edges")
    tf = pd.read_parquet(gold / "trip_features",
                         columns=["trip_uid", "split", "origin_zone", "dest_zone", "duration_min", "has_gps_jump", "hour", "is_weekend"])
    trips = tf[(tf["split"] == "test") & tf["origin_zone"].notna() & tf["dest_zone"].notna()]
    trips = trips.sample(n=3000, random_state=cfg["project"]["seed"]).astype({"origin_zone": int, "dest_zone": int})

    sparse = set(map(tuple, edges.loc[edges["n_transitions"] < 5, ["zone_from", "zone_to"]].to_numpy()))
    base = None
    out = {"sample_trips": len(trips), "edges_total": len(edges), "thresholds": {}}
    for k in [1, 5, 20]:
        t0 = time.time()
        g = ZoneGraph(edges, min_transitions=k)
        main_zones = {int(z) for z, c in zip(g.zone_ids, g.component) if c == g.main_component}
        if base is None:
            base = main_zones
        res = []
        for o, grp in trips.groupby("origin_zone"):
            r = g.routes_from(o, grp["dest_zone"].unique())
            for _, t in grp.iterrows():
                rt = r[int(t["dest_zone"])]
                res.append((rt, t["duration_min"]))
        found = [(rt, d) for rt, d in res if rt is not None]
        times = np.array([rt.time_s / 60 for rt, _ in found])
        ratio = np.array([rt.time_s / 60 / d for rt, d in found if d > 0])
        uses_sparse = sum(any((a, b) in sparse for a, b in zip(rt.zones, rt.zones[1:])) for rt, _ in found)
        out["thresholds"][k] = {
            "edges_used": g.n_edges,
            "nodes": len(g.zone_ids),
            "main_strong_component_size": g.main_size,
            "zones_lost_from_main_vs_no_filter": sorted(base - main_zones),
            "n_zones_lost": len(base - main_zones),
            "trips_with_route": len(found),
            "trips_without_route": len(res) - len(found),
            "route_time_min": {"p50": float(np.median(times)), "p99": float(np.percentile(times, 99)), "max": float(times.max())},
            "routes_over_60_min": int((times > 60).sum()),
            "routes_using_edge_below_5_transitions": int(uses_sparse),
            "route_time_over_actual_duration_p50": float(np.median(ratio)),
            "seconds": round(time.time() - t0, 2),
        }
    # ---- so hai cách tính thời gian cạnh, cùng ngưỡng lọc đã chọn
    k = cfg["routing"]["min_edge_transitions"]
    zhs = pd.read_parquet(gold / "zone_hour_speed")
    cmp = {}
    for method in ["edge_crossing_speed", "zone_hour_speed"]:
        found, used_total = [], {"zone_hour": 0, "zone_all_day": 0, "city_hour": 0}
        for (h, w), grp_hw in trips.groupby(["hour", "is_weekend"]):
            times = None
            if method == "zone_hour_speed":
                times, used = hourly_edge_times(edges, zhs, int(h), bool(w), cfg["routing"]["min_zone_hour_segments"])
                for kk in used_total:
                    used_total[kk] += used[kk]
            g = ZoneGraph(edges, min_transitions=k, times=times)
            for o, grp in grp_hw.groupby("origin_zone"):
                r = g.routes_from(o, grp["dest_zone"].unique())
                for _, t in grp.iterrows():
                    rt = r[int(t["dest_zone"])]
                    if rt is not None:
                        found.append((rt.time_s / 60, t["duration_min"], bool(t["has_gps_jump"])))
        a = np.array([x[0] for x in found]); d = np.array([x[1] for x in found]); j = np.array([x[2] for x in found])
        ok = d > 0
        cmp[method] = {
            "trips_with_route": len(found),
            "route_time_min_p50": float(np.median(a)),
            "actual_duration_min_p50": float(np.median(d)),
            "ratio_route_over_actual_p50": float(np.median(a[ok] / d[ok])),
            "ratio_route_over_actual_p50_no_gps_jump": float(np.median(a[ok & ~j] / d[ok & ~j])),
            "pearson_corr_route_vs_actual": float(np.corrcoef(a, d)[0, 1]),
            "fallback_counts_zone_endpoints": used_total if method == "zone_hour_speed" else None,
        }
    out["edge_time_comparison_at_threshold"] = {"min_edge_transitions": k, **cmp}

    path = ROOT / "evidence" / "spark" / "routing_check.json"
    path.write_text(json.dumps(out, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(out["edge_time_comparison_at_threshold"], indent=2))
    print(json.dumps({k: {kk: vv for kk, vv in v.items() if kk != "zones_lost_from_main_vs_no_filter"}
                      for k, v in out["thresholds"].items()}, indent=2))


if __name__ == "__main__":
    main()
